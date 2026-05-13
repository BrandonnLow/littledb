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
	OpPut Op = 1
	OpDelete Op = 2
)

// HeaderSize is the fixed size of a record's header in bytes:
// CRC32(4) + Op(1) + KeyLen(4) + ValueLen(4).
const HeaderSize = 13

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
	Op Op
	Key []byte
	Value []byte
}

// Encode returns the binary encoding of r.
// The output is HeaderSize + len(r.Key) + len(r.Value) bytes.
func Encode(r *Record) []byte {
	buf := make([]byte, HeaderSize+len(r.Key)+len(r.Value))

	// Write body first so we can checksum it before writing the CRC.
	buf[4] = byte(r.Op)
	binary.LittleEndian.PutUint32(buf[5:9], uint32(len(r.Key)))
	binary.LittleEndian.PutUint32(buf[9:13], uint32(len(r.Value)))
	copy(buf[HeaderSize:], r.Key)
	copy(buf[HeaderSize+len(r.Key):], r.Value)

	// Compute CRC over everything after the CRC field itself.
	crc := crc32.Checksum(buf[4:], crcTable)
	binary.LittleEndian.PutUint32(buf[0:4], crc)

	return buf
}

// Decode parses a single record from the start of b.
// It returns the record, the number of bytes consumed, and any error.
//
// Errors are layered for crash recovery:
//   - io.EOF              — b is empty; caller is at clean end-of-stream.
//   - io.ErrUnexpectedEOF — b is non-empty but does not contain a full record.
//   - ErrCorrupt          — a full record was read but its CRC does not match.
//   - ErrInvalidOp        — the op byte is not a known operation.
func Decode(b []byte) (*Record, int, error) {
	if len(b) == 0 {
		return nil, 0, io.EOF
	}
	if len(b) < HeaderSize {
		return nil, 0, io.ErrUnexpectedEOF
	}


	storedCRC := binary.LittleEndian.Uint32(b[0:4])
	
	op := Op(b[4])
	keyLen := binary.LittleEndian.Uint32(b[5:9])
	valueLen := binary.LittleEndian.Uint32(b[9:13])

	total := HeaderSize + int(keyLen) + int(valueLen)
	if len(b) < total {
		return nil, 0, io.ErrUnexpectedEOF
	}

	// Verify CRC over body before trusting any contents.
	actualCRC := crc32.Checksum(b[4:total], crcTable)

	if actualCRC != storedCRC {
		return nil, 0, ErrCorrupt
	}

	if op != OpPut && op != OpDelete {
		return nil, 0, ErrInvalidOp
	}

	// Copy key and value so the returned Record does not alias the caller's buffer.
	// This matters because the WAL will reuse its read buffer across decode calls.
	rec := &Record{
		Op: op,
		Key: append([]byte(nil), b[HeaderSize:HeaderSize+keyLen]...),
		Value: append([]byte(nil), b[HeaderSize+keyLen:total]...),
	}
	return rec, total, nil

}