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

	// session and seq drive exactly-once dedup. When session is non-empty, the
	// commit is stamped with (session, seq) on its OpCommit marker; a retry that
	// reuses the same pair is recognized as a duplicate at prepare/apply time and
	// does not re-apply. Empty session means no dedup (a plain, at-least-once write).
	session []byte
	seq     uint64
}

// SetSession stamps the transaction with a client session id and a per-session,
// monotonically increasing sequence number, enabling exactly-once semantics: if a
// commit is retried (e.g. after a leader change returns ErrMaybeCommitted) with the
// same (session, seq), it applies at most once. The caller must reuse the same seq
// across retries of the same logical operation, and advance it only for a new one.
func (t *Txn) SetSession(session []byte, seq uint64) {
	t.session = append([]byte(nil), session...)
	t.seq = seq
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

	// Exactly-once dedup, BEFORE the conflict check. This runs under db.mu, the
	// same lock ApplyEntry updates the session table and the memtable under, so the
	// dedup view and the conflict-check view are consistent: if this session's seq
	// is already applied, the retry short-circuits to success (nil entry) here; if
	// it is not yet applied, neither is its earlier attempt's write, so the conflict
	// check below cannot see it and reject the retry — it is proposed and deduped as
	// a no-op at apply. Either way the retry never self-conflicts.
	if len(t.session) > 0 && t.seq <= db.sessions[string(t.session)] {
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
	if len(t.session) > 0 {
		// Carry (session, seq) on the marker: Key=session, Value=seq. OpCommit's
		// key/value are otherwise unused and it lives only in the WAL and the Raft
		// entry (never an SSTable), so this needs no record-format change.
		commitRec.Key = append([]byte(nil), t.session...)
		commitRec.Value = encodeSeq(t.seq)
	}
	entry = append(entry, record.Encode(commitRec)...)

	t.finished = true
	return entry, commitTS, nil
}

// PrepareNoop allocates a fresh commit timestamp and returns the encoding of a
// standalone OpCommit marker with no data records — a no-op log entry. Applied
// via ApplyEntry it appends one OpCommit to the data WAL (so the recovered
// applied index stays in lockstep with the Raft index) and mutates no key. Its
// purpose is Raft's read-index prerequisite: a newly elected leader commits an
// entry in its OWN term so its commit index is trustworthy, without having to
// wait for a client write. Like PrepareCommit it touches neither the WAL nor the
// memtable here — the replication layer logs the entry and ApplyEntry applies it
// once committed.
func (db *DB) PrepareNoop() (entry []byte, commitTS uint64, err error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return nil, 0, errClosed
	}
	commitTS = db.nextTimestamp
	db.nextTimestamp++
	commitRec := &record.Record{Op: record.OpCommit, Timestamp: commitTS}
	return record.Encode(commitRec), commitTS, nil
}
