package sstable

import (
	"bytes"
	"errors"
	"fmt"
	"sort"

	"github.com/BrandonnLow/littledb/internal/mvcckey"
	"github.com/BrandonnLow/littledb/internal/record"
)

// Iterator is a forward cursor over an SSTable's records whose userKey
// falls in [start, end). It yields records in encoded-key order — userKey
// ascending, then timestamp descending within a userKey — reading data
// blocks lazily as it advances, so a bounded scan touches only the blocks
// it needs.
//
// The iterator holds a reference to its Reader but NEVER closes it: the
// Reader is shared with the live DB and with other iterators. Close here
// only drops the iterator's own block buffer.
type Iterator struct {
	r       *Reader
	endUser []byte // exclusive userKey upper bound; nil = unbounded

	blockIdx int
	buf      []byte
	offset   int64

	encKey []byte
	value  []byte
	op     record.Op
	valid  bool
	err    error
}

// NewIterator returns a cursor over records with userKey in [start, end).
// nil start means "from the first key"; nil end means "through the last."
func (r *Reader) NewIterator(start, end []byte) (*Iterator, error) {
	if r.closed {
		return nil, errors.New("sstable: iterator on closed reader")
	}
	it := &Iterator{r: r, endUser: end}
	if len(r.index) == 0 {
		return it, nil // Valid() == false
	}

	startBlock := 0
	var startTarget []byte
	if start != nil {
		// Encode(start, ^0) is the smallest encoded key with userKey==start,
		// so the first encoded key >= it is the first key with userKey>=start.
		startTarget = mvcckey.Encode(start, ^uint64(0))
		n := len(r.index)
		hi := sort.Search(n, func(i int) bool {
			return bytes.Compare(r.index[i].firstKey, startTarget) > 0
		})
		if hi > 0 {
			startBlock = hi - 1
		}
	}

	it.blockIdx = startBlock
	if err := it.loadBlock(startBlock); err != nil {
		it.err = err
		return it, err
	}

	// Position on the first record at-or-after the start target that is also
	// within the end bound.
	for {
		enc, val, op, ok := it.nextRaw()
		if !ok {
			it.valid = false
			return it, it.err
		}
		if startTarget != nil && bytes.Compare(enc, startTarget) < 0 {
			continue
		}
		if !it.inRange(enc) {
			it.valid = false
			return it, nil
		}
		it.set(enc, val, op)
		return it, nil
	}
}

func (it *Iterator) loadBlock(i int) error {
	blk := it.r.index[i]
	buf := make([]byte, blk.blockSize)
	if _, err := it.r.f.ReadAt(buf, blk.blockOffset); err != nil {
		return fmt.Errorf("sstable: iterator read block %d: %w", i, err)
	}
	it.r.blockReadCount.Add(1)
	it.buf = buf
	it.offset = 0
	return nil
}

// nextRaw returns the next record from the current position, crossing block
// boundaries lazily. ok=false at end-of-file or on error (recorded in it.err).
func (it *Iterator) nextRaw() (enc, val []byte, op record.Op, ok bool) {
	if it.offset >= int64(len(it.buf)) {
		if it.blockIdx+1 >= len(it.r.index) {
			return nil, nil, 0, false
		}
		it.blockIdx++
		if err := it.loadBlock(it.blockIdx); err != nil {
			it.err = err
			return nil, nil, 0, false
		}
	}
	rec, n, derr := record.Decode(it.buf[it.offset:])
	if derr != nil {
		it.err = fmt.Errorf("sstable %s: iterator decode: %w", it.r.path, derr)
		return nil, nil, 0, false
	}
	it.offset += int64(n)
	return rec.Key, rec.Value, rec.Op, true
}

func (it *Iterator) inRange(enc []byte) bool {
	if it.endUser == nil {
		return true
	}
	uk, _, ok := mvcckey.Decode(enc)
	if !ok {
		return false
	}
	return bytes.Compare(uk, it.endUser) < 0
}

func (it *Iterator) set(enc, val []byte, op record.Op) {
	it.encKey = enc
	it.value = val
	it.op = op
	it.valid = true
}

// Valid reports whether the iterator is positioned on a record.
func (it *Iterator) Valid() bool { return it.valid }

// EncKey returns the current MVCC-encoded key. The slice aliases the
// iterator's block buffer and is invalidated by the next Advance.
func (it *Iterator) EncKey() []byte { return it.encKey }

// Value returns the current value, aliasing the block buffer.
func (it *Iterator) Value() []byte { return it.value }

// Op returns the current record's operation.
func (it *Iterator) Op() record.Op { return it.op }

// Err returns the first error encountered, if any.
func (it *Iterator) Err() error { return it.err }

// Advance moves to the next in-range record.
func (it *Iterator) Advance() {
	if !it.valid {
		return
	}
	enc, val, op, ok := it.nextRaw()
	if !ok || !it.inRange(enc) {
		it.valid = false
		return
	}
	it.set(enc, val, op)
}

// Close releases the iterator's block buffer. It does NOT close the
// underlying Reader.
func (it *Iterator) Close() error {
	it.valid = false
	it.buf = nil
	it.encKey, it.value = nil, nil
	return nil
}
