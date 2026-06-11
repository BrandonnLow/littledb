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
	// readSnap is the most recent assigned timestamp. nextTimestamp is
	// next-to-assign; nextTimestamp - 1 is the latest committed value.
	// On a brand-new DB nextTimestamp is 1, so readSnap is 0 and the
	// first txn sees nothing — correct, since nothing has been
	// committed yet.
	//
	// Defensive guard: a future bug that lets nextTimestamp reach 0
	// (or wrap past max-uint64) would otherwise produce readSnap =
	// ^uint64(0) and a txn that silently sees every committed write.
	readSnap := db.nextTimestamp
	if readSnap > 0 {
		readSnap--
	}
	t := &Txn{
		db:       db,
		readSnap: readSnap,
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

// Commit atomically writes all buffered changes. On success, every
// write becomes visible to subsequent readers at the same commit
// timestamp. On failure (e.g., conflict with a concurrent committer,
// or a WAL write error), the Txn is left in the finished state and
// the DB is left in a consistent state — any records that did reach
// the WAL will be discarded on restart because no OpCommit record
// follows them.
func (t *Txn) Commit() error {
	if t.finished {
		return ErrTxnFinished
	}

	// Always remove from the active registry once Commit returns
	// (including on panic — defers run during unwinding).
	// unregisterTxn only acquires activeTxnsMu; no path holds
	// activeTxnsMu while waiting for db.mu, so calling it with or
	// without db.mu still held is deadlock-free. The defer order
	// relative to the db.mu.Unlock below is incidental, not
	// load-bearing.
	defer t.db.unregisterTxn(t)

	t.db.mu.Lock()
	defer t.db.mu.Unlock()

	if t.db.closed {
		t.finished = true
		return errClosed
	}

	// Empty txn: nothing to do, but still mark finished.
	if len(t.writes) == 0 {
		t.finished = true
		return nil
	}

	// First-committer-wins conflict detection. For each key we want
	// to write, ask the storage layer: has anyone committed a newer
	// version since our readSnap? If yes, our writes are based on
	// stale data — abort without touching the WAL.
	//
	// Must happen under the write lock, before allocating commitTS:
	// otherwise a concurrent committer could slip in between the
	// check and the allocation, and we'd miss their write.
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

	// Allocate one commit timestamp for all writes in this txn.
	commitTS := t.db.nextTimestamp
	t.db.nextTimestamp++

	// Sort keys for deterministic WAL order (helps testing and review).
	keys := make([]string, 0, len(t.writes))
	for k := range t.writes {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// When a replicator is attached (leader role), accumulate the txn's
	// encoded records into entry as we append them, to ship to followers.
	repl := t.db.replicator
	var entry []byte

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
		if repl != nil {
			entry = append(entry, record.Encode(rec)...)
		}
	}

	// Append the commit marker. Once this record is durable, the txn
	// is officially committed. A crash before this point leaves an
	// uncommitted partial txn that recovery will discard.
	commitRec := &record.Record{Op: record.OpCommit, Timestamp: commitTS}
	if _, err := t.db.wal.Append(commitRec); err != nil {
		t.finished = true
		return fmt.Errorf("db: commit wal marker: %w", err)
	}

	// Replicate after WAL durability and before the writes become visible.
	// Called under db.mu, so commits replicate one at a time; a quorum must
	// acknowledge before we proceed to the memtable apply.
	if repl != nil {
		entry = append(entry, record.Encode(commitRec)...)
		if err := repl.Replicate(entry, commitTS); err != nil {
			t.finished = true
			return fmt.Errorf("db: replicate: %w", err)
		}
	}

	// Apply to memtable. After this point the writes are visible to
	// concurrent readers. The memtable.Put/Delete calls cannot fail
	// (only ErrFrozen is possible, and we never freeze the active
	// memtable outside flushLocked, which we hold the lock against).
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

	// Check flush threshold once at the end of the txn, rather than
	// after each buffered write. A large txn could push the memtable
	// well over the limit; the flush will catch up.
	if t.db.memtable.ApproximateSize() >= t.db.opts.MemtableSizeMax {
		if err := t.db.flushLocked(); err != nil {
			return fmt.Errorf("db: flush after commit: %w", err)
		}
		t.db.signalCompact()
	}
	return nil
}
