package memtable

import (
	"bytes"

	"github.com/BrandonnLow/littledb/internal/mvcckey"
)

// VersionEntry is one stored version, copied out so it stays valid for use
// outside the memtable's lock.
type VersionEntry struct {
	EncKey []byte
	Value  []byte
	Op     Op
}

// RangeSnapshot copies every stored version whose userKey is in [start, end)
// into a freshly-allocated slice, in encoded-key order (userKey ascending,
// then timestamp descending). nil start means "from the first key"; nil end
// means "through the last."
//
// The read lock is held only for the duration of the copy, so a long scan
// over the returned slice does not block writers — and the snapshot reflects
// the memtable's contents at the moment of the call, which is what a
// snapshot-consistent scan wants. The memtable is size-bounded, so the copy
// is bounded too.
func (m *Memtable) RangeSnapshot(start, end []byte) []VersionEntry {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var target []byte
	if start != nil {
		target = mvcckey.Encode(start, ^uint64(0))
	}

	var out []VersionEntry
	for n := m.sl.SeekGE(target); n != nil; n = n.Next() {
		enc := n.Key()
		uk, _, ok := mvcckey.Decode(enc)
		if !ok {
			continue
		}
		if end != nil && bytes.Compare(uk, end) >= 0 {
			break
		}
		op, raw := decodeValue(n.Value())
		out = append(out, VersionEntry{
			EncKey: append([]byte(nil), enc...),
			Value:  append([]byte(nil), raw...),
			Op:     op,
		})
	}
	return out
}
