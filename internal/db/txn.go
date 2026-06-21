package db

import (
	"errors"
	"fmt"
	"sort"

	"github.com/BrandonnLow/littledb/internal/record"
)

var ErrTxnFinished = errors.New("db: transaction already finished")

type Txn struct {
	db       *DB
	readSnap uint64
	writes   map[string]txnWrite
	finished bool
}

type txnWrite struct {
	op    record.Op
	value []byte
}

func (db *DB) Begin() *Txn {
	db.mu.RLock()
	defer db.mu.RUnlock()
	// Read snapshot is the applied watermark, not nextTimestamp: under
	// deferred apply (replication leader) nextTimestamp may already count an
	// in-flight commit whose data is not yet in the memtable. Snapshotting at
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

func (t *Txn) Delete(key []byte) error {
	if t.finished {
		return ErrTxnFinished
	}
	t.writes[string(key)] = txnWrite{op: record.OpDelete}
	return nil
}

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
// data WAL or the memtable — the replication layer persists the entry to its
// Raft log file for replication, and ApplyEntry writes the data WAL and applies
// to the memtable once the entry is committed. The txn is marked finished.
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
