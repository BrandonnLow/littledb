package db

import (
	"errors"
	"fmt"
	"sort"

	"github.com/BrandonnLow/littledb/internal/record"
)

// ErrTxnFinished is returned by any Txn method called after Commit or
// Rollback. Once a transaction reaches a terminal state, it cannot be
// reused.
var ErrTxnFinished = errors.New("db: transaction already finished")

// Txn is a single transaction with snapshot-isolation reads and
// atomic multi-key commit. A Txn is intended for use by ONE goroutine
// at a time; concurrent use of a single Txn is undefined.
//
// Concurrent Txns from different goroutines are safe: each holds its
// own read snapshot and its own write buffer.
type Txn struct {
	db       *DB
	readSnap uint64
	writes   map[string]txnWrite
	finished bool
}

type txnWrite struct {
	op    record.Op
	value []byte // nil for deletes; defensive copy of user value for puts
}

// Begin starts a new transaction. The returned Txn captures a read
// snapshot at this moment — reads will see all data committed before
// Begin returned, and nothing committed afterwards (by this Txn or
// any other writer).
func (db *DB) Begin() *Txn {
	db.mu.RLock()
	defer db.mu.RUnlock()
	// Read snapshot is the applied watermark: under deferred apply
	// (replication leader) nextTimestamp may already count an in-flight
	// commit whose data is not yet in the memtable. Snapshotting at
	// appliedTS guarantees everything with ts <= readSnap is visible, so a
	// read-modify-write txn that misses such a commit will see it as newer at
	// commit time and conflict, rather than silently overwriting it.
	t := &Txn{
		db:       db,
		readSnap: db.appliedTS,
		writes:   make(map[string]txnWrite),
	}
	db.registerTxn(t)
	return t
}

// Get returns the value of key visible to this transaction. Local
// buffered writes shadow committed state (read-your-own-writes).
func (t *Txn) Get(key []byte) ([]byte, error) {
	if t.finished {
		return nil, ErrTxnFinished
	}
	if w, ok := t.writes[string(key)]; ok {
		if w.op == record.OpDelete {
			return nil, ErrKeyNotFound
		}
		return append([]byte(nil), w.value...), nil
	}
	return t.db.GetAsOf(key, t.readSnap)
}

// Put buffers a write of (key, value). The change is invisible to
// any other reader until Commit is called.
func (t *Txn) Put(key, value []byte) error {
	if t.finished {
		return ErrTxnFinished
	}
	t.writes[string(key)] = txnWrite{
		op:    record.OpPut,
		value: append([]byte(nil), value...),
	}
	return nil
}

// Delete buffers a delete of key. Same semantics as Put: invisible
// outside the txn until Commit.
func (t *Txn) Delete(key []byte) error {
	if t.finished {
		return ErrTxnFinished
	}
	t.writes[string(key)] = txnWrite{op: record.OpDelete}
	return nil
}

// Rollback discards all buffered writes. After Rollback, the Txn is
// finished and no other method may be called on it.
func (t *Txn) Rollback() error {
	if t.finished {
		return ErrTxnFinished
	}
	t.writes = nil
	t.finished = true
	t.db.unregisterTxn(t)
	return nil
}

func (t *Txn) Commit() error {
	if t.finished {
		return ErrTxnFinished
	}
	defer t.db.unregisterTxn(t)

	// A replicated leader replaces the entire commit path with its own
	// orchestration (PrepareCommit + log + replicate + apply).
	t.db.mu.RLock()
	override := t.db.commitOverride
	t.db.mu.RUnlock()
	if override != nil {
		return override(t)
	}

	// Single-node path: conflict-check, allocate, log, and apply, all under
	// the write lock.
	t.db.mu.Lock()
	defer t.db.mu.Unlock()

	if t.db.closed {
		t.finished = true
		return errClosed
	}

	if len(t.writes) == 0 {
		t.finished = true
		return nil
	}

	for k := range t.writes {
		has, err := t.db.hasCommitNewerThanLocked([]byte(k), t.readSnap)
		if err != nil {
			t.finished = true
			return fmt.Errorf("db: conflict check: %w", err)
		}
		if has {
			t.finished = true
			return ErrConflict
		}
	}

	commitTS := t.db.nextTimestamp
	t.db.nextTimestamp++

	keys := make([]string, 0, len(t.writes))
	for k := range t.writes {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		w := t.writes[k]
		rec := &record.Record{
			Op:        w.op,
			Timestamp: commitTS,
			Key:       []byte(k),
			Value:     w.value,
		}
		if _, err := t.db.wal.Append(rec); err != nil {
			t.finished = true
			return fmt.Errorf("db: commit wal data: %w", err)
		}
	}

	commitRec := &record.Record{Op: record.OpCommit, Timestamp: commitTS}
	if _, err := t.db.wal.Append(commitRec); err != nil {
		t.finished = true
		return fmt.Errorf("db: commit wal marker: %w", err)
	}

	for _, k := range keys {
		w := t.writes[k]
		switch w.op {
		case record.OpPut:
			if err := t.db.memtable.Put([]byte(k), w.value, commitTS); err != nil {
				panic(fmt.Sprintf("db: memtable.Put after WAL commit (ts=%d): %v", commitTS, err))
			}
		case record.OpDelete:
			if err := t.db.memtable.Delete([]byte(k), commitTS); err != nil {
				panic(fmt.Sprintf("db: memtable.Delete after WAL commit (ts=%d): %v", commitTS, err))
			}
		}
	}

	t.finished = true
	t.db.appliedTS = commitTS

	if t.db.memtable.ApproximateSize() >= t.db.opts.MemtableSizeMax {
		if err := t.db.flushLocked(); err != nil {
			return fmt.Errorf("db: flush after commit: %w", err)
		}
		t.db.signalCompact()
	}
	return nil

}

// PrepareCommit performs the leader-side preparation of a replicated commit:
// first-committer-wins conflict detection, commit-timestamp allocation, and
// building the encoded entry (data records + OpCommit). It does NOT touch the
// WAL or the memtable — the replication layer is responsible for AppendToLog
// (durability) and ApplyEntry (visibility) once the entry is committed. The
// txn is marked finished.
//
// Returns (nil, 0, nil) for an empty txn, ErrConflict on a write-write
// conflict (without allocating a timestamp), or errClosed if the DB is closed.
func (db *DB) PrepareCommit(t *Txn) (entry []byte, commitTS uint64, err error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if t.finished {
		return nil, 0, ErrTxnFinished
	}
	if db.closed {
		t.finished = true
		return nil, 0, errClosed
	}
	if len(t.writes) == 0 {
		t.finished = true
		return nil, 0, nil
	}

	for k := range t.writes {
		has, cerr := db.hasCommitNewerThanLocked([]byte(k), t.readSnap)
		if cerr != nil {
			t.finished = true
			return nil, 0, fmt.Errorf("db: conflict check: %w", cerr)
		}
		if has {
			t.finished = true
			return nil, 0, ErrConflict
		}
	}

	commitTS = db.nextTimestamp
	db.nextTimestamp++
	keys := make([]string, 0, len(t.writes))
	for k := range t.writes {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		w := t.writes[k]
		rec := &record.Record{Op: w.op, Timestamp: commitTS, Key: []byte(k), Value: w.value}
		entry = append(entry, record.Encode(rec)...)
	}
	commitRec := &record.Record{Op: record.OpCommit, Timestamp: commitTS}
	entry = append(entry, record.Encode(commitRec)...)

	t.finished = true
	return entry, commitTS, nil
}
