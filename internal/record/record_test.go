package record

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
)

func TestHeaderSize(t *testing.T) {
	if HeaderSize != 21 {
		t.Errorf("HeaderSize = %d, want 21", HeaderSize)
	}
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	r := &Record{
		Op:        OpPut,
		Timestamp: 0xDEADBEEFCAFE,
		Key:       []byte("hello"),
		Value:     []byte("world"),
	}
	buf := Encode(r)
	got, n, err := Decode(buf)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(buf) {
		t.Errorf("consumed %d, want %d", n, len(buf))
	}
	if got.Op != r.Op {
		t.Errorf("Op = %d, want %d", got.Op, r.Op)
	}
	if got.Timestamp != r.Timestamp {
		t.Errorf("Timestamp = %x, want %x", got.Timestamp, r.Timestamp)
	}
	if !bytes.Equal(got.Key, r.Key) {
		t.Errorf("Key = %q, want %q", got.Key, r.Key)
	}
	if !bytes.Equal(got.Value, r.Value) {
		t.Errorf("Value = %q, want %q", got.Value, r.Value)
	}
}

func TestEncodeDecodeZeroTimestamp(t *testing.T) {
	r := &Record{Op: OpPut, Key: []byte("k"), Value: []byte("v")}
	buf := Encode(r)
	got, _, err := Decode(buf)
	if err != nil {
		t.Fatal(err)
	}
	if got.Timestamp != 0 {
		t.Errorf("Timestamp = %d, want 0", got.Timestamp)
	}
}

func TestEncodeDecodeDelete(t *testing.T) {
	r := &Record{Op: OpDelete, Timestamp: 42, Key: []byte("k")}
	buf := Encode(r)
	got, _, err := Decode(buf)
	if err != nil {
		t.Fatal(err)
	}
	if got.Op != OpDelete || got.Timestamp != 42 || !bytes.Equal(got.Key, []byte("k")) {
		t.Errorf("got %+v", got)
	}
	if len(got.Value) != 0 {
		t.Errorf("Value = %q, want empty", got.Value)
	}
}

func TestDecodeEmpty(t *testing.T) {
	if _, _, err := Decode(nil); !errors.Is(err, io.EOF) {
		t.Errorf("Decode(nil) err = %v, want io.EOF", err)
	}
}

func TestDecodeTruncatedHeader(t *testing.T) {
	buf := []byte{1, 2, 3, 4, 5}
	if _, _, err := Decode(buf); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("err = %v, want ErrUnexpectedEOF", err)
	}
}

func TestDecodeTruncatedBody(t *testing.T) {
	r := &Record{Op: OpPut, Timestamp: 1, Key: []byte("hello"), Value: []byte("world")}
	buf := Encode(r)
	for chop := 1; chop < len(buf)-HeaderSize; chop++ {
		short := buf[:len(buf)-chop]
		if _, _, err := Decode(short); !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Errorf("chop=%d: err = %v, want ErrUnexpectedEOF", chop, err)
		}
	}
}

func TestDecodeCorruptCRC(t *testing.T) {
	r := &Record{Op: OpPut, Timestamp: 1, Key: []byte("k"), Value: []byte("v")}
	buf := Encode(r)
	buf[len(buf)-1] ^= 0xFF
	if _, _, err := Decode(buf); !errors.Is(err, ErrCorrupt) {
		t.Errorf("err = %v, want ErrCorrupt", err)
	}
}

func TestDecodeCorruptTimestamp(t *testing.T) {
	r := &Record{Op: OpPut, Timestamp: 12345, Key: []byte("k"), Value: []byte("v")}
	buf := Encode(r)
	buf[OffsetTimestamp] ^= 0xFF
	if _, _, err := Decode(buf); !errors.Is(err, ErrCorrupt) {
		t.Errorf("err = %v, want ErrCorrupt", err)
	}
}

func TestDecodeInvalidOp(t *testing.T) {
	r := &Record{Op: 99, Timestamp: 0, Key: []byte("k"), Value: []byte("v")}
	buf := Encode(r)
	if _, _, err := Decode(buf); !errors.Is(err, ErrInvalidOp) {
		t.Errorf("err = %v, want ErrInvalidOp", err)
	}
}

func TestHeaderLengths(t *testing.T) {
	r := &Record{Op: OpPut, Timestamp: 7, Key: []byte("hello"), Value: []byte("world!")}
	buf := Encode(r)
	keyLen, valueLen, err := HeaderLengths(buf[:HeaderSize])
	if err != nil {
		t.Fatal(err)
	}
	if keyLen != 5 || valueLen != 6 {
		t.Errorf("got (%d, %d), want (5, 6)", keyLen, valueLen)
	}
}

func TestHeaderLengthsShortBuffer(t *testing.T) {
	if _, _, err := HeaderLengths([]byte{1, 2, 3}); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("err = %v, want ErrUnexpectedEOF", err)
	}
}

func TestEncodingLayout(t *testing.T) {
	r := &Record{Op: OpPut, Timestamp: 0x0102030405060708, Key: []byte("ab"), Value: []byte("cde")}
	buf := Encode(r)

	if Op(buf[OffsetOp]) != OpPut {
		t.Errorf("byte 4 = %d, want OpPut", buf[OffsetOp])
	}
	gotTS := binary.LittleEndian.Uint64(buf[OffsetTimestamp : OffsetTimestamp+8])
	if gotTS != 0x0102030405060708 {
		t.Errorf("timestamp bytes = %x, want 0x0102030405060708", gotTS)
	}
	gotKL := binary.LittleEndian.Uint32(buf[OffsetKeyLen : OffsetKeyLen+4])
	if gotKL != 2 {
		t.Errorf("keyLen = %d, want 2", gotKL)
	}
	gotVL := binary.LittleEndian.Uint32(buf[OffsetValueLen : OffsetValueLen+4])
	if gotVL != 3 {
		t.Errorf("valueLen = %d, want 3", gotVL)
	}
	if !bytes.Equal(buf[HeaderSize:HeaderSize+2], []byte("ab")) {
		t.Errorf("key bytes = %q, want 'ab'", buf[HeaderSize:HeaderSize+2])
	}
	if !bytes.Equal(buf[HeaderSize+2:HeaderSize+5], []byte("cde")) {
		t.Errorf("value bytes = %q, want 'cde'", buf[HeaderSize+2:HeaderSize+5])
	}
}
