// Package memtable implements the in-memory write buffer of the LSM tree.
//
// A Memtable wraps a sorted skiplist with three additions the LSM tree
// needs:
//
//  1. Locking: many concurrent readers, one writer
//  2. Tombstones: Delete records a deletion marker rather than removing
//     the key, so the marker can propagate to SSTables and mask older data
//  3. Size accounting: an approximate byte count, so the DB can decide
//     when to freeze and flush
//
// On disk, tombstones are stored as a 1-byte op prefix in front of the
// value bytes. Get exposes the op to the caller; Iterate yields tombstones
// in their proper sorted position so a flush can write them to an SSTable.
package memtable

import (
	"bytes"
	"errors"
	"sync"

	"github.com/BrandonnLow/littledb/internal/skiplist"
)

// Op identifies a put or delete in the memtable.
type Op byte

const (
	OpPut    Op = 1
	OpDelete Op = 2
)

// ErrFrozen is returned by Put and Delete when the memtable is frozen.
var ErrFrozen = errors.New("memtable: frozen")

// entryOverhead is the approximate per-entry memory cost beyond the key
// and value bytes themselves (skiplist Node header, next slice header,
// map bookkeeping). Used only for the flush threshold heuristic.
const entryOverhead = 64

// Memtable is a sorted, in-memory map with tombstone semantics.
type Memtable struct {
	mu     sync.RWMutex
	sl     *skiplist.Skiplist
	size   int64 // approximate bytes used
	frozen bool
}

// New returns an empty memtable.
func New() *Memtable {
	return &Memtable{sl: skiplist.New()}
}

// Put inserts or replaces a value.
func (m *Memtable) Put(key, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.frozen {
		return ErrFrozen
	}
	m.writeLocked(key, value, OpPut)
	return nil
}

// Delete writes a tombstone for key. Always succeeds (unless the memtable
// is frozen), regardless of whether the key was previously present —
// tombstones may need to mask values in older SSTables.
func (m *Memtable) Delete(key []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.frozen {
		return ErrFrozen
	}
	m.writeLocked(key, nil, OpDelete)
	return nil
}

// writeLocked encodes (op, value) into a single stored slice and writes
// it into the skiplist. Caller must hold the write lock.
func (m *Memtable) writeLocked(key, value []byte, op Op) {
	stored := encode(op, value)
	// Size accounting: if the key already exists we're replacing its
	// stored value, so subtract the old size first.
	if oldStored, ok := m.sl.Get(key); ok {
		m.size -= int64(len(key)) + int64(len(oldStored)) + entryOverhead
	}
	m.sl.Put(key, stored)
	m.size += int64(len(key)) + int64(len(stored)) + entryOverhead
}

// Get returns the value and op for key. found is false when the key is
// not in this memtable at all; when found is true, op tells the caller
// whether it is live (OpPut) or a tombstone (OpDelete).
//
// The returned value is a copy; safe to retain past further writes.
func (m *Memtable) Get(key []byte) (value []byte, op Op, found bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	stored, ok := m.sl.Get(key)
	if !ok {
		return nil, 0, false
	}
	op, raw := decode(stored)
	if op == OpDelete {
		return nil, OpDelete, true
	}
	return append([]byte(nil), raw...), OpPut, true
}

// ApproximateSize returns a heuristic byte count. Used by the DB to
// decide when to freeze and flush.
func (m *Memtable) ApproximateSize() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.size
}

// Len returns the number of entries (live + tombstones).
func (m *Memtable) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.sl.Len()
}

// Freeze marks the memtable read-only. After Freeze, Put and Delete
// return ErrFrozen; reads continue to work.
func (m *Memtable) Freeze() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.frozen = true
}

// IsFrozen reports whether the memtable has been frozen.
func (m *Memtable) IsFrozen() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.frozen
}

// Iterate yields each entry in sorted key order. fn receives the op so
// the caller can distinguish live entries from tombstones — both must
// be visible to a flush so the resulting SSTable carries the tombstones
// that mask older overlapping SSTables.
//
// Return false from fn to stop iterating.
//
// The slices passed to fn alias internal storage; treat them as read-only
// for the duration of the call.
func (m *Memtable) Iterate(fn func(key, value []byte, op Op) bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	m.sl.Iterate(func(k, stored []byte) bool {
		op, raw := decode(stored)
		return fn(k, raw, op)
	})
}

// encode prepends the op byte to value.
func encode(op Op, value []byte) []byte {
	out := make([]byte, 1+len(value))
	out[0] = byte(op)
	copy(out[1:], value)
	return out
}

// decode splits a stored slice into op and the original value bytes.
// The returned slice aliases stored; do not modify it.
func decode(stored []byte) (Op, []byte) {
	if len(stored) == 0 {
		return 0, nil
	}
	return Op(stored[0]), stored[1:]
}

// Equal compares two byte slices. Provided for tests; not part of the
// public API surface but kept here to avoid an extra import in callers.
func Equal(a, b []byte) bool { return bytes.Equal(a, b) }
