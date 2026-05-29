// Package bloom implements a bloom filter for SSTables.
//
// A bloom filter answers "is this key definitely not in the set, or
// possibly in it?" with no false negatives and a tunable false-positive
// rate. SSTables use this to skip files that definitely don't contain
// a key on the read path.
//
// We use the standard Kirsch–Mitzenmacher double-hashing technique:
// one 64-bit hash per key, split into two 32-bit halves, and the k
// hash positions are derived as h1 + i*h2 for i = 0..k-1. This gives
// k effective hash functions for the cost of one.
//
// The hash is FNV-1a (non-cryptographic, fast, deterministic — no
// seeds — so the serialized filter is reproducible across runs).
package bloom

import (
	"errors"
	"fmt"
	"hash/fnv"
	"math"
)

// Filter is an immutable-once-built bloom filter.
type Filter struct {
	bits []byte // bit array; bit i is at byte i/8, bit position i%8
	m    uint64 // total bit count == len(bits) * 8
	k    uint8  // number of hash functions
}

// New returns a filter sized to hold expectedKeys at the given
// bitsPerKey. With 10 bits/key the false-positive rate is ~1%;
// LevelDB and RocksDB use this default.
//
// expectedKeys < 1 is treated as 1 to avoid zero-size filters that
// would always return MayContain==true. bitsPerKey is clamped to the
// range [1, 64] — past that the marginal FPR gain isn't worth the
// space.
func New(expectedKeys, bitsPerKey int) *Filter {
	if expectedKeys < 1 {
		expectedKeys = 1
	}
	if bitsPerKey < 1 {
		bitsPerKey = 1
	}
	if bitsPerKey > 64 {
		bitsPerKey = 64
	}

	// m = expectedKeys * bitsPerKey, rounded up to a byte.
	mBits := uint64(expectedKeys) * uint64(bitsPerKey)
	mBytes := (mBits + 7) / 8
	if mBytes < 1 {
		mBytes = 1
	}
	mBits = mBytes * 8

	// Optimal k = (m/n) * ln(2) = bitsPerKey * 0.6931...
	// Round to nearest int; clamp to [1, 30].
	kFloat := float64(bitsPerKey) * math.Ln2
	k := int(math.Round(kFloat))
	if k < 1 {
		k = 1
	}
	if k > 30 {
		k = 30
	}

	return &Filter{
		bits: make([]byte, mBytes),
		m:    mBits,
		k:    uint8(k),
	}
}

// Add records key in the filter.
func (f *Filter) Add(key []byte) {
	h1, h2 := hashPair(key)
	for i := uint8(0); i < f.k; i++ {
		pos := (h1 + uint64(i)*h2) % f.m
		f.bits[pos>>3] |= 1 << (pos & 7)
	}
}

// MayContain reports whether key may be in the filter. Returns false
// only if key was definitely never Added; returns true if key was
// added or if a collision makes it look added.
func (f *Filter) MayContain(key []byte) bool {
	if f.m == 0 {
		return false
	}
	h1, h2 := hashPair(key)
	for i := uint8(0); i < f.k; i++ {
		pos := (h1 + uint64(i)*h2) % f.m
		if f.bits[pos>>3]&(1<<(pos&7)) == 0 {
			return false
		}
	}
	return true
}

// hashPair returns two 32-bit halves of a 64-bit FNV-1a hash, widened
// to uint64 for arithmetic. Two integers is enough to derive k hash
// positions via h1 + i*h2.
func hashPair(key []byte) (h1, h2 uint64) {
	h := fnv.New64a()
	h.Write(key)
	sum := h.Sum64()
	return sum & 0xFFFFFFFF, sum >> 32
}

// NumBits returns the total bit count of the underlying array.
func (f *Filter) NumBits() int { return int(f.m) }

// NumHashes returns k, the number of hash functions in use.
func (f *Filter) NumHashes() int { return int(f.k) }

// Bytes returns the serialized form of the filter. Layout:
//
// byte 0:    k (number of hash functions)
// bytes 1..: raw bit array
//
// The returned slice is a fresh copy; callers may retain or modify it.
// m is recoverable as (len(bytes) - 1) * 8.
func (f *Filter) Bytes() []byte {
	out := make([]byte, 1+len(f.bits))
	out[0] = f.k
	copy(out[1:], f.bits)
	return out
}

// Load reconstructs a Filter from data produced by Bytes.
func Load(data []byte) (*Filter, error) {
	if len(data) < 1 {
		return nil, errors.New("bloom: empty data")
	}
	k := data[0]
	if k < 1 || k > 30 {
		return nil, fmt.Errorf("bloom: invalid k=%d", k)
	}
	bits := make([]byte, len(data)-1)
	copy(bits, data[1:])
	return &Filter{
		bits: bits,
		m:    uint64(len(bits)) * 8,
		k:    k,
	}, nil
}
