// Package record is the on-disk record format used by the WAL and
// SSTable layers.
//
// Layout:
//
//	CRC32 (4) | Op (1) | Timestamp (8) | KeyLen (4) | ValueLen (4) | Key | Value
//
// The CRC covers everything after itself (op + timestamp + lengths +
// key + value). All multi-byte integers are little-endian. CRC is
// CRC-32C (Castagnoli).
//
// The MVCC layer uses Timestamp field to version individual writes.
package record

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
)

// Op identifies the kind of operation a Record represents.
type Op byte

const (
	OpPut    Op = 1
	OpDelete Op = 2
	// OpCommit is the txn-commit marker. A record with this op has no
	// key or value; its Timestamp identifies the txn whose preceding
	// data records (sharing that Timestamp) should be applied as a
	// group. WAL recovery buffers data records and drains them when
	// the matching OpCommit appears.
	OpCommit Op = 3
)

// HeaderSize is the fixed size of a record's header in bytes:
// CRC32(4) + Op(1) + Timestamp(8) + KeyLen(4) + ValueLen(4).
const HeaderSize = 21

// Header field offsets within the encoded bytes. Useful for code that
// needs to peek at lengths before allocating the full record buffer
// (the WAL does this in ReadAt).
const (
	OffsetCRC       = 0
	OffsetOp        = 4
	OffsetTimestamp = 5
	OffsetKeyLen    = 13
	OffsetValueLen  = 17
)

var (
	// ErrCorrupt is returned by Decode when a record's CRC does not match.
	ErrCorrupt = errors.New("record: crc mismatch")
	// ErrInvalidOp is returned by Decode when the Op byte is unrecognized.
	ErrInvalidOp = errors.New("record: invalid op")
)

// crcTable is the Castagnoli polynomial table, the modern choice for storage
// CRC32 (used by RocksDB, ext4, btrfs). Hardware-accelerated on most CPUs.
var crcTable = crc32.MakeTable(crc32.Castagnoli)

// Record is the in-memory representation of one durable write.
type Record struct {
	Op        Op
	Timestamp uint64
	Key       []byte
	Value     []byte
}

// Encode serializes r to a fresh byte slice.
func Encode(r *Record) []byte {
	buf := make([]byte, HeaderSize+len(r.Key)+len(r.Value))

	// Write body first so we can checksum it before writing the CRC.
	buf[OffsetOp] = byte(r.Op)
	binary.LittleEndian.PutUint64(buf[OffsetTimestamp:OffsetTimestamp+8], r.Timestamp)
	binary.LittleEndian.PutUint32(buf[OffsetKeyLen:OffsetKeyLen+4], uint32(len(r.Key)))
	binary.LittleEndian.PutUint32(buf[OffsetValueLen:OffsetValueLen+4], uint32(len(r.Value)))
	copy(buf[HeaderSize:], r.Key)
	copy(buf[HeaderSize+len(r.Key):], r.Value)

	// Compute CRC over everything after the CRC field itself.
	crc := crc32.Checksum(buf[OffsetOp:], crcTable)
	binary.LittleEndian.PutUint32(buf[OffsetCRC:OffsetCRC+4], crc)

	return buf
}

// Decode parses one record from the start of b. Returns the record,
// the number of bytes consumed, and an error.
//
// Error semantics drive WAL crash recovery:
//   - io.EOF: b is empty (clean end of log).
//   - io.ErrUnexpectedEOF: b is shorter than the declared record size
//     (a torn write that was interrupted mid-flush).
//   - ErrCorrupt: the full record's bytes are present but the CRC
//     does not match (bit rot or wild write).
//   - ErrInvalidOp: CRC matched but the op byte is unrecognized;
//     refuse to start rather than silently drop data.
func Decode(b []byte) (*Record, int, error) {
	if len(b) == 0 {
		return nil, 0, io.EOF
	}
	if len(b) < HeaderSize {
		return nil, 0, io.ErrUnexpectedEOF
	}
	storedCRC := binary.LittleEndian.Uint32(b[OffsetCRC : OffsetCRC+4])
	op := Op(b[OffsetOp])
	timestamp := binary.LittleEndian.Uint64(b[OffsetTimestamp : OffsetTimestamp+8])
	keyLen := binary.LittleEndian.Uint32(b[OffsetKeyLen : OffsetKeyLen+4])
	valueLen := binary.LittleEndian.Uint32(b[OffsetValueLen : OffsetValueLen+4])
	total := HeaderSize + int(keyLen) + int(valueLen)
	if len(b) < total {
		return nil, 0, io.ErrUnexpectedEOF
	}

	// Verify CRC over body before trusting any contents.
	actualCRC := crc32.Checksum(b[OffsetOp:total], crcTable)
	if actualCRC != storedCRC {
		return nil, 0, ErrCorrupt
	}

	if op != OpPut && op != OpDelete && op != OpCommit {
		return nil, 0, ErrInvalidOp
	}

	return &Record{
		Op:        op,
		Timestamp: timestamp,
		Key:       append([]byte(nil), b[HeaderSize:HeaderSize+keyLen]...),
		Value:     append([]byte(nil), b[HeaderSize+keyLen:total]...),
	}, total, nil
}

// HeaderLengths returns (keyLen, valueLen) declared in the first
// HeaderSize bytes of hdr. Used by callers that need to size a full
// record buffer before calling Decode (the WAL reads the fixed-size
// header first, then the body of the indicated size).
func HeaderLengths(hdr []byte) (keyLen, valueLen uint32, err error) {
	if len(hdr) < HeaderSize {
		return 0, 0, io.ErrUnexpectedEOF
	}
	keyLen = binary.LittleEndian.Uint32(hdr[OffsetKeyLen : OffsetKeyLen+4])
	valueLen = binary.LittleEndian.Uint32(hdr[OffsetValueLen : OffsetValueLen+4])
	return keyLen, valueLen, nil
}
