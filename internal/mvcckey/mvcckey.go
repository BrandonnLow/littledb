// Package mvcckey encodes (userKey, timestamp) pairs into single byte
// strings that sort lexicographically in MVCC-friendly order:
//
//  1. userKey ascending
//  2. timestamp descending (within the same userKey)
//
// Encoding: userKey || bigEndian(^timestamp). The bitwise-NOT and
// big-endian together give descending byte-wise sort for descending
// numeric timestamps.
//
// This means: for a fixed userKey, all versions cluster together in
// sorted order, with the newest version first. A read "as of snapshot
// T" can find userKey K's value by seeking to Encode(K, T) and taking
// the first key ≥ that target — the encoded key with the largest
// timestamp ≤ T sorts at exactly that position.
package mvcckey

import (
	"bytes"
	"encoding/binary"
)

// TimestampSize is the fixed byte length of the timestamp suffix.
const TimestampSize = 8

// Encode returns userKey || bigEndian(^timestamp). The returned slice
// is a fresh allocation; callers may retain or modify it.
func Encode(userKey []byte, timestamp uint64) []byte {
	enc := make([]byte, len(userKey)+TimestampSize)
	copy(enc, userKey)
	binary.BigEndian.PutUint64(enc[len(userKey):], ^timestamp)
	return enc
}

// Compare orders two encoded keys the way this package's encoding intends:
// userKey ascending, then timestamp descending within a userKey. It returns
// -1, 0, or +1 like bytes.Compare.
//
// It is NOT bytes.Compare over the whole encoded key, and the difference is
// load-bearing whenever one userKey is a strict prefix of another. Compare the
// raw bytes of Encode("cat", ts) and Encode("cats", ts):
//
//	Encode("cat",  30) = 63 61 74 ff ff ff ff ff ff ff e1
//	Encode("cats", 30) = 63 61 74 73 ff ff ff ff ff ff ff e1
//	                              ^^    ^^
//
// At the fourth byte, "cat" has run out of userKey and is already into its
// timestamp suffix — which begins 0xff for any timestamp below 2^56, and 0xff
// outranks every byte a printable key is likely to hold. So a raw byte
// comparison sorts "cat" AFTER "cats", inverting the userKey order the rest of
// the engine assumes.
//
// Point lookups tolerated that (every version of a userKey still clustered
// contiguously, so SeekGE plus a userKey-match check still landed correctly),
// but range scans did not: keys came back out of order, and a bounded scan
// could stop early on an out-of-range userKey while an in-range one still lay
// ahead of it — silently omitting a live key. Splitting the fixed-width suffix
// off before comparing removes the ambiguity: the userKey portions are compared
// against each other and never against a neighbour's timestamp bytes.
//
// Comparing the suffixes with bytes.Compare gives descending timestamps for
// free, since the suffix is the big-endian bitwise NOT of the timestamp.
//
// A slice too short to carry a suffix cannot be a well-formed encoded key; such
// inputs fall back to a whole-slice byte comparison so the ordering stays total
// rather than panicking.
func Compare(a, b []byte) int {
	if len(a) < TimestampSize || len(b) < TimestampSize {
		return bytes.Compare(a, b)
	}
	ca, cb := len(a)-TimestampSize, len(b)-TimestampSize
	if c := bytes.Compare(a[:ca], b[:cb]); c != 0 {
		return c
	}
	return bytes.Compare(a[ca:], b[cb:])
}

// Decode splits an encoded key back into userKey and timestamp. The
// returned userKey is a sub-slice of enc; copy it if you need to
// retain it past enc's lifetime. ok is false if enc is too short.
func Decode(enc []byte) (userKey []byte, timestamp uint64, ok bool) {
	if len(enc) < TimestampSize {
		return nil, 0, false
	}
	cut := len(enc) - TimestampSize
	return enc[:cut], ^binary.BigEndian.Uint64(enc[cut:]), true
}

// UserKey returns just the user-key portion of an encoded key.
// The returned slice aliases enc.
func UserKey(enc []byte) []byte {
	if len(enc) < TimestampSize {
		return nil
	}
	return enc[:len(enc)-TimestampSize]
}

// Timestamp returns just the timestamp from an encoded key.
func Timestamp(enc []byte) uint64 {
	if len(enc) < TimestampSize {
		return 0
	}
	return ^binary.BigEndian.Uint64(enc[len(enc)-TimestampSize:])
}
