package record

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"testing"
)

func TestRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		rec  *Record
	}{
		{"simple put", &Record{Op: OpPut, Key: []byte("hello"), Value: []byte("world")}},
		{"tombstone", &Record{Op: OpDelete, Key: []byte("k"), Value: nil}},
		{"unicode key and value", &Record{Op: OpPut, Key: []byte("café"), Value: []byte("☕")}},
		{"binary value", &Record{Op: OpPut, Key: []byte("k"), Value: []byte{0x00, 0xFF, 0xDE, 0xAD, 0xBE, 0xEF}}},
		{"long key", &Record{Op: OpPut, Key: bytes.Repeat([]byte("a"), 10000), Value: []byte("v")}},
		{"long value", &Record{Op: OpPut, Key: []byte("k"), Value: bytes.Repeat([]byte("x"), 100000)}},
		{"empty key and value", &Record{Op: OpPut, Key: []byte{}, Value: []byte{}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := Encode(tt.rec)
			decoded, n, err := Decode(encoded)
			if err != nil {
				t.Fatalf("decode failed: %v", err)
			}
			if n != len(encoded) {
				t.Errorf("consumed %d bytes, want %d", n, len(encoded))
			}
			if decoded.Op != tt.rec.Op {
				t.Errorf("op = %d, want %d", decoded.Op, tt.rec.Op)
			}
			if !bytes.Equal(decoded.Key, tt.rec.Key) {
				t.Errorf("key mismatch: got %q, want %q", decoded.Key, tt.rec.Key)
			}
			if !bytes.Equal(decoded.Value, tt.rec.Value) {
				t.Errorf("value differs: got %d bytes, want %d", len(decoded.Value), len(tt.rec.Value))
			}
		})
	}
}

func TestDecodeEmpty(t *testing.T) {
	_, n, err := Decode(nil)
	if !errors.Is(err, io.EOF) {
		t.Errorf("err = %v, want io.EOF", err)
	}
	if n != 0 {
		t.Errorf("n = %d, want 0", n)
	}

	_, _, err = Decode([]byte{})
	if !errors.Is(err, io.EOF) {
		t.Errorf("empty slice: err = %v, want io.EOF", err)
	}
}

func TestDecodeTruncated(t *testing.T) {
	rec := &Record{Op: OpPut, Key: []byte("hello"), Value: []byte("world")}
	full := Encode(rec)
	for i := 1; i < len(full); i++ {
		t.Run(fmt.Sprintf("len_%d", i), func(t *testing.T) {
			_, _, err := Decode(full[:i])
			if !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Errorf("err = %v, want io.ErrUnexpectedEOF", err)
			}
		})
	}
}

func TestDecodeCorrupt(t *testing.T) {
	rec := &Record{Op: OpPut, Key: []byte("hello"), Value: []byte("world")}
	encoded := Encode(rec)
	// A single-bit flip in any position must cause decode to fail.
	// Bit flips in the length fields (KeyLen or ValueLen) may produce
	// io.ErrUnexpectedEOF — the new length no longer fits in the buffer.
	// Flips elsewhere produce ErrCorrupt via the CRC check.
	// Both errors are valid recovery signals: "this tail is unsafe, stop reading".
	for i := 0; i < len(encoded); i++ {
		t.Run(fmt.Sprintf("byte_%d", i), func(t *testing.T) {
			corrupted := make([]byte, len(encoded))
			copy(corrupted, encoded)
			corrupted[i] ^= 0x01
			_, _, err := Decode(corrupted)
			if !errors.Is(err, ErrCorrupt) && !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Errorf("byte %d flip: err = %v, want ErrCorrupt or io.ErrUnexpectedEOF", i, err)
			}
		})
	}
}

func TestDecodeMultipleRecords(t *testing.T) {
	// Simulate reading several records sequentially from one buffer,
	// as the WAL will do during recovery.
	recs := []*Record{
		{Op: OpPut, Key: []byte("a"), Value: []byte("1")},
		{Op: OpPut, Key: []byte("bb"), Value: []byte("22")},
		{Op: OpDelete, Key: []byte("a"), Value: nil},
	}
	var buf []byte
	for _, r := range recs {
		buf = append(buf, Encode(r)...)
	}

	var got []*Record
	offset := 0
	for offset < len(buf) {
		r, n, err := Decode(buf[offset:])
		if err != nil {
			t.Fatalf("decode at offset %d: %v", offset, err)
		}
		got = append(got, r)
		offset += n
	}

	if len(got) != len(recs) {
		t.Fatalf("decoded %d records, want %d", len(got), len(recs))
	}
	for i, r := range got {
		if r.Op != recs[i].Op || !bytes.Equal(r.Key, recs[i].Key) || !bytes.Equal(r.Value, recs[i].Value) {
			t.Errorf("record %d mismatch", i)
		}
	}
}
