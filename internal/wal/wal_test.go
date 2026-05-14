package wal

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/BrandonnLow/littledb/internal/record"
)

// makeRecs returns three small test records of mixed types.
func makeRecs() []*record.Record {
	return []*record.Record{
		{Op: record.OpPut, Key: []byte("a"), Value: []byte("1")},
		{Op: record.OpPut, Key: []byte("bb"), Value: []byte("two")},
		{Op: record.OpDelete, Key: []byte("a"), Value: nil},
	}
}

func recsEqual(a, b *record.Record) bool {
	return a.Op == b.Op && bytes.Equal(a.Key, b.Key) && bytes.Equal(a.Value, b.Value)
}

func TestOpenEmptyDir(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if w.Size() != 0 {
		t.Errorf("size = %d, want 0", w.Size())
	}
	if err := w.Close(); err != nil {
		t.Errorf("close: %v", err)
	}
}

func TestAppendAndScan(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer w.Close()

	recs := makeRecs()
	offsets := make([]int64, len(recs))
	for i, r := range recs {
		off, err := w.Append(r)
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		offsets[i] = off
	}

	for i := 1; i < len(offsets); i++ {
		if offsets[i] <= offsets[i-1] {
			t.Errorf("offsets not increasing: %v", offsets)
		}
	}

	var got []*record.Record
	var gotOffsets []int64
	err = w.Scan(func(off int64, r *record.Record) error {
		got = append(got, r)
		gotOffsets = append(gotOffsets, off)
		return nil
	})
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(got) != len(recs) {
		t.Fatalf("scanned %d records, want %d", len(got), len(recs))
	}
	for i := range recs {
		if !recsEqual(got[i], recs[i]) {
			t.Errorf("record %d differs", i)
		}
		if gotOffsets[i] != offsets[i] {
			t.Errorf("offset %d: got %d, want %d", i, gotOffsets[i], offsets[i])
		}
	}
}

func TestReadAt(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer w.Close()

	recs := makeRecs()
	offsets := make([]int64, len(recs))
	for i, r := range recs {
		off, _ := w.Append(r)
		offsets[i] = off
	}

	for i, r := range recs {
		got, err := w.ReadAt(offsets[i])
		if err != nil {
			t.Errorf("readat %d: %v", i, err)
			continue
		}
		if !recsEqual(got, r) {
			t.Errorf("readat %d: record differs", i)
		}
	}
}

func TestReadAtInvalidOffset(t *testing.T) {
	dir := t.TempDir()
	w, _ := Open(dir)
	defer w.Close()
	w.Append(&record.Record{Op: record.OpPut, Key: []byte("k"), Value: []byte("v")})

	if _, err := w.ReadAt(-1); err == nil {
		t.Error("readat negative offset: want error, got nil")
	}
	if _, err := w.ReadAt(1 << 30); err == nil {
		t.Error("readat huge offset: want error, got nil")
	}
}

func TestReopenAfterClose(t *testing.T) {
	dir := t.TempDir()
	recs := makeRecs()

	w, _ := Open(dir)
	for _, r := range recs {
		w.Append(r)
	}
	w.Close()

	w2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer w2.Close()

	var got []*record.Record
	w2.Scan(func(off int64, r *record.Record) error {
		got = append(got, r)
		return nil
	})
	if len(got) != len(recs) {
		t.Fatalf("after reopen: %d records, want %d", len(got), len(recs))
	}
	for i := range recs {
		if !recsEqual(got[i], recs[i]) {
			t.Errorf("after reopen: record %d differs", i)
		}
	}
}

func TestRecoveryGarbageAtEnd(t *testing.T) {
	dir := t.TempDir()
	recs := makeRecs()

	w, _ := Open(dir)
	for _, r := range recs {
		w.Append(r)
	}
	goodSize := w.Size()
	w.Close()

	path := filepath.Join(dir, logFileName)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0x01})
	f.Close()

	w2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen with garbage tail: %v", err)
	}
	defer w2.Close()

	if w2.Size() != goodSize {
		t.Errorf("after recovery size = %d, want %d (file should be truncated)", w2.Size(), goodSize)
	}

	var got []*record.Record
	w2.Scan(func(off int64, r *record.Record) error {
		got = append(got, r)
		return nil
	})
	if len(got) != len(recs) {
		t.Errorf("after recovery: %d records, want %d", len(got), len(recs))
	}
}

func TestRecoveryTruncatedLastRecord(t *testing.T) {
	dir := t.TempDir()
	recs := makeRecs()

	w, _ := Open(dir)
	offsets := make([]int64, len(recs))
	for i, r := range recs {
		offsets[i], _ = w.Append(r)
	}
	w.Close()

	path := filepath.Join(dir, logFileName)
	if err := os.Truncate(path, offsets[len(recs)-1]+3); err != nil {
		t.Fatal(err)
	}

	w2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen with truncated tail: %v", err)
	}
	defer w2.Close()

	if w2.Size() != offsets[len(recs)-1] {
		t.Errorf("after recovery size = %d, want %d", w2.Size(), offsets[len(recs)-1])
	}

	var got []*record.Record
	w2.Scan(func(off int64, r *record.Record) error {
		got = append(got, r)
		return nil
	})
	if len(got) != len(recs)-1 {
		t.Errorf("after recovery: %d records, want %d", len(got), len(recs)-1)
	}
}

func TestRecoveryCorruptLastRecord(t *testing.T) {
	dir := t.TempDir()
	recs := makeRecs()

	w, _ := Open(dir)
	offsets := make([]int64, len(recs))
	for i, r := range recs {
		offsets[i], _ = w.Append(r)
	}
	w.Close()

	path := filepath.Join(dir, logFileName)
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	flipAt := offsets[len(recs)-1] + record.HeaderSize
	one := []byte{0}
	f.ReadAt(one, flipAt)
	one[0] ^= 0x01
	f.WriteAt(one, flipAt)
	f.Close()

	w2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen with corrupt last: %v", err)
	}
	defer w2.Close()

	if w2.Size() != offsets[len(recs)-1] {
		t.Errorf("after recovery size = %d, want %d", w2.Size(), offsets[len(recs)-1])
	}

	var got []*record.Record
	w2.Scan(func(off int64, r *record.Record) error {
		got = append(got, r)
		return nil
	})
	if len(got) != len(recs)-1 {
		t.Errorf("after recovery: %d records, want %d", len(got), len(recs)-1)
	}
}

func TestAppendAfterRecovery(t *testing.T) {
	dir := t.TempDir()
	recs := makeRecs()

	w, _ := Open(dir)
	for _, r := range recs {
		w.Append(r)
	}
	goodSize := w.Size()
	w.Close()

	path := filepath.Join(dir, logFileName)
	f, _ := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	f.Write([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0x01})
	f.Close()

	w2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	if w2.Size() != goodSize {
		t.Fatalf("after recovery size = %d, want %d", w2.Size(), goodSize)
	}

	newRec := &record.Record{Op: record.OpPut, Key: []byte("post-recovery"), Value: []byte("ok")}
	off, err := w2.Append(newRec)
	if err != nil {
		t.Fatal(err)
	}
	if off != goodSize {
		t.Errorf("new append at %d, want %d", off, goodSize)
	}

	got, err := w2.ReadAt(off)
	if err != nil {
		t.Fatal(err)
	}
	if !recsEqual(got, newRec) {
		t.Error("post-recovery record readback differs")
	}
}

func TestCloseIdempotent(t *testing.T) {
	dir := t.TempDir()
	w, _ := Open(dir)
	if err := w.Close(); err != nil {
		t.Errorf("first close: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Errorf("second close: %v", err)
	}
}

func TestOperationsAfterClose(t *testing.T) {
	dir := t.TempDir()
	w, _ := Open(dir)
	w.Close()

	if _, err := w.Append(&record.Record{Op: record.OpPut, Key: []byte("k"), Value: []byte("v")}); err == nil {
		t.Error("append after close: want error, got nil")
	}
	if _, err := w.ReadAt(0); err == nil {
		t.Error("readat after close: want error, got nil")
	}
	if err := w.Scan(func(off int64, r *record.Record) error { return nil }); err == nil {
		t.Error("scan after close: want error, got nil")
	}
}

func TestLargeRecords(t *testing.T) {
	dir := t.TempDir()
	w, _ := Open(dir)
	defer w.Close()

	big := bytes.Repeat([]byte("x"), 1<<20) // 1 MiB value
	rec := &record.Record{Op: record.OpPut, Key: []byte("big"), Value: big}
	off, err := w.Append(rec)
	if err != nil {
		t.Fatal(err)
	}
	got, err := w.ReadAt(off)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.Value, big) {
		t.Error("large value differs after roundtrip")
	}
}
