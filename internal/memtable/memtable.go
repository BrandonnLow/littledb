// Package memtable is the in-memory write buffer. MVCC-aware: each entry has a timestamp,
// multiple versions of one userKey coexist, and reads are "as of" a snapshot timestamp.
//
// Under the hood we wrap a skiplist whose keys are MVCC-encoded
// (userKey || ^timestamp big-endian). The encoding sorts by userKey
// ascending then timestamp descending, so all versions of one
// userKey cluster together with the newest first. A GetAsOf seeks to
// Encode(userKey, snapshot) and takes the first key ≥ that target;
// the seek lands on the newest version with timestamp ≤ snapshot.
package memtable

import (
	"bytes"
	"errors"
	"sync"

	"github.com/BrandonnLow/littledb/internal/mvcckey"
	"github.com/BrandonnLow/littledb/internal/skiplist"
)

type Op byte

const (
	OpPut    Op = 1
	OpDelete Op = 2
)

var ErrFrozen = errors.New("memtable: frozen")

// entryOverhead is an estimate of the per-record bookkeeping cost
// (skiplist node + value header + Go object overhead). Used for the
// approximate-size accounting that drives flush triggering.
const entryOverhead = 64

// Memtable wraps a skiplist with locking, tombstones, size tracking,
// and freezing.
type Memtable struct {
	mu     sync.RWMutex
	sl     *skiplist.Skiplist
	size   int64
	frozen bool
}

func New() *Memtable { return &Memtable{sl: skiplist.New()} }

// Put writes (userKey, value) at timestamp ts. The (userKey, ts) pair
// is the unique identity of this version; future reads at snapshots
// >= ts that don't find a newer version will return this value.
func (m *Memtable) Put(userKey, value []byte, ts uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.frozen {
		return ErrFrozen
	}
	m.writeLocked(userKey, value, OpPut, ts)
	return nil
}

// Delete writes a tombstone at (userKey, ts). Reads at snapshots >= ts
// that don't find a newer version will return "not found."
func (m *Memtable) Delete(userKey []byte, ts uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.frozen {
		return ErrFrozen
	}
	m.writeLocked(userKey, nil, OpDelete, ts)
	return nil
}

func (m *Memtable) writeLocked(userKey, value []byte, op Op, ts uint64) {
	encKey := mvcckey.Encode(userKey, ts)
	stored := encodeValue(op, value)
	// Two writes at the same (userKey, ts) overwrite each other in the
	// skiplist; this shouldn't happen in normal operation because the
	// DB's timestamp counter is monotonic, but we tolerate it for
	// recovery replays.
	if oldStored, ok := m.sl.Get(encKey); ok {
		m.size -= int64(len(encKey)) + int64(len(oldStored)) + entryOverhead
	}
	m.sl.Put(encKey, stored)
	m.size += int64(len(encKey)) + int64(len(stored)) + entryOverhead
}

// GetAsOf returns the version of userKey visible at snapshot ts.
//
//   - (value, OpPut, true)   — a live value
//   - (nil,   OpDelete, true) — masked by a tombstone (treat as absent)
//   - (nil,   0,        false) — userKey has no version at ts ≤ snapshot
func (m *Memtable) GetAsOf(userKey []byte, snapshot uint64) (value []byte, op Op, found bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	target := mvcckey.Encode(userKey, snapshot)
	node := m.sl.SeekGE(target)
	if node == nil {
		return nil, 0, false
	}
	nodeUserKey, _, ok := mvcckey.Decode(node.Key())
	if !ok || !bytes.Equal(nodeUserKey, userKey) {
		// Seek moved past userKey; no visible version exists.
		return nil, 0, false
	}
	op, raw := decodeValue(node.Value())
	if op == OpDelete {
		return nil, OpDelete, true
	}
	// Copy the value so callers can retain it past the next write to
	// the underlying skiplist node.
	return append([]byte(nil), raw...), OpPut, true
}

func (m *Memtable) ApproximateSize() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.size
}

func (m *Memtable) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.sl.Len()
}

func (m *Memtable) Freeze() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.frozen = true
}

func (m *Memtable) IsFrozen() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.frozen
}

// Iterate walks every (userKey, value, op, timestamp) tuple in sorted
// order: userKey ascending, then timestamp descending. Return false
// from fn to stop early.
func (m *Memtable) Iterate(fn func(userKey, value []byte, op Op, ts uint64) bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	m.sl.Iterate(func(encKey, stored []byte) bool {
		userKey, ts, _ := mvcckey.Decode(encKey)
		op, raw := decodeValue(stored)
		return fn(userKey, raw, op, ts)
	})
}

// NewestVersionTS returns the timestamp of the newest stored version
// of userKey. Used by the DB's commit-time conflict check to decide
// whether any committer has touched the key since this Txn's Begin.
//
// Implementation: Encode(userKey, ^uint64(0)) has the smallest
// possible suffix for userKey (^uint64(0) becomes 0x00..00 after the
// bitwise NOT), so SeekGE lands on the smallest existing encoded key
// for userKey — which, given descending-ts encoding, is its newest
// version.
func (m *Memtable) NewestVersionTS(userKey []byte) (ts uint64, found bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	target := mvcckey.Encode(userKey, ^uint64(0))
	node := m.sl.SeekGE(target)
	if node == nil {
		return 0, false
	}
	nodeUserKey, nodeTS, ok := mvcckey.Decode(node.Key())
	if !ok || !bytes.Equal(nodeUserKey, userKey) {
		return 0, false
	}
	return nodeTS, true
}

// VersionCountForTesting returns the number of distinct versions of
// userKey stored in this memtable. Used to verify GC behaviour.
func (m *Memtable) VersionCountForTesting(userKey []byte) int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	target := mvcckey.Encode(userKey, ^uint64(0))
	count := 0
	for n := m.sl.SeekGE(target); n != nil; n = n.Next() {
		nodeUserKey, _, ok := mvcckey.Decode(n.Key())
		if !ok || !bytes.Equal(nodeUserKey, userKey) {
			break
		}
		count++
	}
	return count
}

// encodeValue and decodeValue pack the op byte in front of the value.
// Tombstones store an empty payload; live values store their bytes.

func encodeValue(op Op, value []byte) []byte {
	out := make([]byte, 1+len(value))
	out[0] = byte(op)
	copy(out[1:], value)
	return out
}

func decodeValue(stored []byte) (Op, []byte) {
	if len(stored) == 0 {
		return 0, nil
	}
	return Op(stored[0]), stored[1:]
}
