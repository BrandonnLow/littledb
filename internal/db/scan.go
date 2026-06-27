package db

import (
	"bytes"
	"container/heap"
	"errors"
	"fmt"

	"github.com/BrandonnLow/littledb/internal/memtable"
	"github.com/BrandonnLow/littledb/internal/mvcckey"
	"github.com/BrandonnLow/littledb/internal/record"
	"github.com/BrandonnLow/littledb/internal/sstable"
)

// versionCursor is a forward cursor over one layer's records, yielding them
// in encoded-key order (userKey ascending, timestamp descending). The
// memtable layers are backed by materialized slices; SSTable layers are
// backed by lazy *sstable.Iterator (which satisfies this interface directly).
type versionCursor interface {
	Valid() bool
	EncKey() []byte
	Op() record.Op
	Value() []byte
	Advance()
	Err() error
	Close() error
}

// sliceCursor adapts a materialized memtable range snapshot to versionCursor.
type sliceCursor struct {
	entries []memtable.VersionEntry
	i       int
}

func (c *sliceCursor) Valid() bool    { return c.i < len(c.entries) }
func (c *sliceCursor) EncKey() []byte { return c.entries[c.i].EncKey }
func (c *sliceCursor) Op() record.Op  { return record.Op(c.entries[c.i].Op) }
func (c *sliceCursor) Value() []byte  { return c.entries[c.i].Value }
func (c *sliceCursor) Advance()       { c.i++ }
func (c *sliceCursor) Err() error     { return nil }
func (c *sliceCursor) Close() error   { return nil }

// heapCursor wraps a cursor with a layer priority used to break ties between
// identical encoded keys: lower priority = newer layer wins.
type heapCursor struct {
	cur      versionCursor
	priority int
}

type cursorHeap []*heapCursor

func (h cursorHeap) Len() int { return len(h) }
func (h cursorHeap) Less(i, j int) bool {
	if c := bytes.Compare(h[i].cur.EncKey(), h[j].cur.EncKey()); c != 0 {
		return c < 0
	}
	return h[i].priority < h[j].priority
}
func (h cursorHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *cursorHeap) Push(x any)   { *h = append(*h, x.(*heapCursor)) }
func (h *cursorHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// mergeIterator merges several versionCursors into one stream in encoded-key
// order, dropping any record whose encoded key is identical to the one just
// emitted (the newer layer, popped first via priority, wins the tie). Each
// emitted encKey/value is copied so it stays valid after the next Advance.
type mergeIterator struct {
	h       cursorHeap
	cursors []versionCursor

	encKey []byte
	value  []byte
	op     record.Op
	valid  bool
	err    error

	lastEnc []byte
}

func newMergeIterator(cursors []versionCursor) *mergeIterator {
	m := &mergeIterator{cursors: cursors}
	for i, c := range cursors {
		if c.Valid() {
			m.h = append(m.h, &heapCursor{cur: c, priority: i})
		}
	}
	heap.Init(&m.h)
	m.Advance()
	return m
}

func (m *mergeIterator) Advance() {
	for m.h.Len() > 0 {
		top := m.h[0]
		// Capture (and copy) before advancing the underlying cursor, whose
		// buffer may be reused on Advance.
		enc := append([]byte(nil), top.cur.EncKey()...)
		op := top.cur.Op()
		val := append([]byte(nil), top.cur.Value()...)

		top.cur.Advance()
		if err := top.cur.Err(); err != nil {
			m.err = err
			m.valid = false
			return
		}
		if top.cur.Valid() {
			heap.Fix(&m.h, 0)
		} else {
			heap.Pop(&m.h)
		}

		if m.lastEnc != nil && bytes.Equal(enc, m.lastEnc) {
			continue // duplicate version from an older layer; skip
		}
		m.lastEnc = enc
		m.encKey, m.op, m.value = enc, op, val
		m.valid = true
		return
	}
	m.valid = false
}

func (m *mergeIterator) Close() error {
	var firstErr error
	for _, c := range m.cursors {
		if err := c.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Iterator is a forward, snapshot-consistent range scan over live values.
// Keys are returned in ascending order; for each userKey only the newest
// version visible at the scan's snapshot is yielded, and userKeys whose
// visible version is a tombstone (deleted as of the snapshot) are skipped.
//
// The layer set and snapshot are captured at Scan time, so writes committed
// after Scan returns are invisible to the iterator. An Iterator is for use by
// a single goroutine; call Close when done.
type Iterator struct {
	merge    *mergeIterator
	snapshot uint64

	key    []byte
	value  []byte
	err    error
	done   bool
	closed bool
}

// Next advances to the next live key visible at the snapshot, returning false
// when the range is exhausted or an error occurs (see Err).
func (it *Iterator) Next() bool {
	if it.done || it.closed || it.err != nil {
		return false
	}
	for it.merge.valid {
		uk, _, ok := mvcckey.Decode(it.merge.encKey)
		if !ok {
			it.err = errors.New("db: scan: malformed encoded key")
			return false
		}
		curUK := append([]byte(nil), uk...)

		var emitVal []byte
		var emitFound, decided bool

		// Walk every version of curUK (timestamp descending). The first one
		// with ts <= snapshot is the visible version; a tombstone there means
		// the key is deleted as of the snapshot.
		for it.merge.valid {
			vuk, vts, ok := mvcckey.Decode(it.merge.encKey)
			if !ok {
				it.err = errors.New("db: scan: malformed encoded key")
				return false
			}
			if !bytes.Equal(vuk, curUK) {
				break // reached the next userKey
			}
			if !decided && vts <= it.snapshot {
				decided = true
				if it.merge.op == record.OpPut {
					emitVal = append([]byte(nil), it.merge.value...)
					emitFound = true
				}
			}
			it.merge.Advance()
			if it.merge.err != nil {
				it.err = it.merge.err
				return false
			}
		}

		if emitFound {
			it.key = curUK
			it.value = emitVal
			return true
		}
		// Not visible / deleted at the snapshot: fall through to next userKey.
	}
	it.done = true
	return false
}

// Key returns the current key. Valid until the next Next call.
func (it *Iterator) Key() []byte { return it.key }

// Value returns the current value. Valid until the next Next call.
func (it *Iterator) Value() []byte { return it.value }

// Err returns the first error encountered during iteration, if any.
func (it *Iterator) Err() error { return it.err }

// Close releases the iterator's per-layer cursors. It does NOT close the
// underlying SSTable readers — those are shared with the live DB.
func (it *Iterator) Close() error {
	if it.closed {
		return nil
	}
	it.closed = true
	return it.merge.Close()
}

// Scan returns a forward iterator over live values whose key is in
// [start, end), as visible at the given snapshot. nil start means "from the
// first key"; nil end means "through the last."
//
// The active memtable, frozen memtable, and SSTable set are captured under
// the read lock at call time, giving a consistent view; the snapshot bounds
// visibility within those layers, so later writes are invisible. SSTable
// readers are referenced, not copied, and are kept alive by the iterator for
// its lifetime even if a concurrent compaction swaps them out of the DB.
func (db *DB) Scan(start, end []byte, snapshot uint64) (*Iterator, error) {
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return nil, errClosed
	}
	activeMT := db.memtable
	frozenMT := db.frozen
	ssts := make([]*sstable.Reader, len(db.sstables))
	copy(ssts, db.sstables)
	db.mu.RUnlock()

	// Priority encodes recency for tie-breaking on identical versions:
	// active memtable newest, then frozen, then SSTables newest-first.
	cursors := []versionCursor{
		&sliceCursor{entries: activeMT.RangeSnapshot(start, end)},
	}
	if frozenMT != nil {
		cursors = append(cursors, &sliceCursor{entries: frozenMT.RangeSnapshot(start, end)})
	}
	for _, r := range ssts {
		c, err := r.NewIterator(start, end)
		if err != nil {
			for _, cc := range cursors {
				cc.Close()
			}
			return nil, fmt.Errorf("db: scan: %w", err)
		}
		cursors = append(cursors, c)
	}

	it := &Iterator{
		merge:    newMergeIterator(cursors),
		snapshot: snapshot,
	}
	if it.merge.err != nil {
		it.err = it.merge.err
	}
	return it, nil
}

// SnapshotScan captures the applied frontier and a pinned, snapshot-consistent
// iterator over all live keys as of that frontier, for building a Raft snapshot
// to ship to a far-behind follower. It returns the iterator plus the applied
// Raft index and the timestamp the snapshot is taken at.
//
// lastIncludedIndex (= appliedIndex) and snapshotTS (= appliedTS) are read in
// the SAME read-lock section that captures the layer set, so the pair is exactly
// consistent: snapshotTS is the commit timestamp of the entry at
// lastIncludedIndex (ts is monotonic with applied index), never a later one.
// This is what makes the snapshot content match the base index precisely — a
// follower that installs it and sets base = lastIncludedIndex will not have
// entries beyond the base re-driven through ApplyEntry.
//
// Pinning at appliedTS is also what makes the scan complete and GC-immune:
// every stored version has ts <= appliedTS = snapshotTS, GC never drops a key's
// newest stored version, and the captured SSTable readers keep their (possibly
// later unlinked) files alive for the iterator's lifetime. So a long stream off
// this iterator stays consistent under concurrent flush and compaction with no
// read-lease. (Deriving snapshotTS from the log entry instead would leave
// snapshotTS < appliedTS at scan time and reopen that gap.)
func (db *DB) SnapshotScan() (it *Iterator, lastIncludedIndex, snapshotTS uint64, err error) {
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return nil, 0, 0, errClosed
	}
	lastIncludedIndex = db.appliedIndex
	snapshotTS = db.appliedTS
	activeMT := db.memtable
	frozenMT := db.frozen
	ssts := make([]*sstable.Reader, len(db.sstables))
	copy(ssts, db.sstables)
	db.mu.RUnlock()

	cursors := []versionCursor{
		&sliceCursor{entries: activeMT.RangeSnapshot(nil, nil)},
	}
	if frozenMT != nil {
		cursors = append(cursors, &sliceCursor{entries: frozenMT.RangeSnapshot(nil, nil)})
	}
	for _, r := range ssts {
		c, cerr := r.NewIterator(nil, nil)
		if cerr != nil {
			for _, cc := range cursors {
				cc.Close()
			}
			return nil, 0, 0, fmt.Errorf("db: snapshot scan: %w", cerr)
		}
		cursors = append(cursors, c)
	}

	it = &Iterator{merge: newMergeIterator(cursors), snapshot: snapshotTS}
	if it.merge.err != nil {
		it.err = it.merge.err
	}
	return it, lastIncludedIndex, snapshotTS, nil
}

// ScanNow returns a Scan at the latest committed snapshot
// (nextTimestamp - 1) — the range-scan analogue of Get.
func (db *DB) ScanNow(start, end []byte) (*Iterator, error) {
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return nil, errClosed
	}
	snap := db.nextTimestamp
	db.mu.RUnlock()
	if snap > 0 {
		snap--
	}
	return db.Scan(start, end, snap)
}
