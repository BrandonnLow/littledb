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

import "encoding/binary"

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
