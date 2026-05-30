package sstable

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/BrandonnLow/littledb/internal/record"
)

func makePath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "000001.sst")
}

func writeKeys(t *testing.T, triples ...any) string {
	t.Helper()
	path := makePath(t)
	if len(triples)%3 != 0 {
		t.Fatalf("writeKeys: triples must come in groups of 3, got %d args", len(triples))
	}
	w, err := NewWriter(path, len(triples)/3)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < len(triples); i += 3 {
		op := triples[i].(record.Op)
		k := []byte(triples[i+1].(string))
		v := []byte(triples[i+2].(string))
		if err := w.Add(op, k, v); err != nil {
			t.Fatalf("Add %q: %v", k, err)
		}
	}
	if err := w.Finish(); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestRoundTrip(t *testing.T) {
	path := writeKeys(t,
		record.OpPut, "alpha", "1",
		record.OpPut, "bravo", "2",
		record.OpPut, "charlie", "3",
	)
	r, err := OpenReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	type entry struct {
		k, v string
		op   record.Op
	}
	var got []entry
	r.Iterate(func(op record.Op, k, v []byte) bool {
		got = append(got, entry{string(k), string(v), op})
		return true
	})
	want := []entry{
		{"alpha", "1", record.OpPut},
		{"bravo", "2", record.OpPut},
		{"charlie", "3", record.OpPut},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d, want %d: %+v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("[%d] got %+v want %+v", i, got[i], want[i])
		}
	}
}

func TestGetHits(t *testing.T) {
	path := writeKeys(t,
		record.OpPut, "a", "1",
		record.OpPut, "b", "2",
		record.OpPut, "c", "3",
	)
	r, _ := OpenReader(path)
	defer r.Close()

	for _, c := range []struct{ k, v string }{{"a", "1"}, {"b", "2"}, {"c", "3"}} {
		v, op, found, err := r.Get([]byte(c.k))
		if err != nil || !found || op != record.OpPut || !bytes.Equal(v, []byte(c.v)) {
			t.Errorf("Get %q: got (%q, %d, %v, %v)", c.k, v, op, found, err)
		}
	}
}

func TestGetMiss(t *testing.T) {
	path := writeKeys(t,
		record.OpPut, "a", "1",
		record.OpPut, "c", "3",
	)
	r, _ := OpenReader(path)
	defer r.Close()

	for _, k := range []string{"0", "b", "z"} {
		_, _, found, err := r.Get([]byte(k))
		if err != nil || found {
			t.Errorf("Get %q: found=%v err=%v", k, found, err)
		}
	}
}

func TestGetTombstone(t *testing.T) {
	path := writeKeys(t,
		record.OpPut, "a", "live",
		record.OpDelete, "b", "",
		record.OpPut, "c", "live2",
	)
	r, _ := OpenReader(path)
	defer r.Close()

	v, op, found, err := r.Get([]byte("b"))
	if err != nil || !found || op != record.OpDelete || v != nil {
		t.Errorf("tombstone get: got (%q, %d, %v, %v)", v, op, found, err)
	}
}

func TestEmptySSTable(t *testing.T) {
	path := makePath(t)
	w, _ := NewWriter(path, 0)
	if err := w.Finish(); err != nil {
		t.Fatal(err)
	}

	r, err := OpenReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	if _, _, found, err := r.Get([]byte("anything")); err != nil || found {
		t.Errorf("empty: found=%v err=%v", found, err)
	}
	if r.NumBlocks() != 0 {
		t.Errorf("empty NumBlocks = %d, want 0", r.NumBlocks())
	}
}

func TestOutOfOrder(t *testing.T) {
	w, _ := NewWriter(makePath(t), 10)
	defer w.Abort()
	w.Add(record.OpPut, []byte("b"), []byte("2"))
	if err := w.Add(record.OpPut, []byte("a"), []byte("1")); !errors.Is(err, ErrOutOfOrder) {
		t.Errorf("err = %v, want ErrOutOfOrder", err)
	}
}

func TestDuplicateKey(t *testing.T) {
	w, _ := NewWriter(makePath(t), 10)
	defer w.Abort()
	w.Add(record.OpPut, []byte("a"), []byte("1"))
	if err := w.Add(record.OpPut, []byte("a"), []byte("2")); !errors.Is(err, ErrDuplicate) {
		t.Errorf("err = %v, want ErrDuplicate", err)
	}
}

func TestAbortRemovesTempFile(t *testing.T) {
	path := makePath(t)
	w, _ := NewWriter(path, 10)
	w.Add(record.OpPut, []byte("k"), []byte("v"))
	if err := w.Abort(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("final file exists: %v", err)
	}
	if _, err := os.Stat(path + ".tmp"); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("temp file exists: %v", err)
	}
}

func TestAtomicCreation(t *testing.T) {
	path := makePath(t)
	w, _ := NewWriter(path, 10)
	w.Add(record.OpPut, []byte("k"), []byte("v"))
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("final file visible before Finish: %v", err)
	}
	w.Finish()
	if _, err := os.Stat(path); err != nil {
		t.Errorf("final file missing after Finish: %v", err)
	}
	if _, err := os.Stat(path + ".tmp"); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("temp file still present after Finish: %v", err)
	}
}

func TestBadMagic(t *testing.T) {
	path := writeKeys(t, record.OpPut, "a", "1")
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	info, _ := f.Stat()
	one := []byte{0}
	f.ReadAt(one, info.Size()-1)
	one[0] ^= 0xFF
	f.WriteAt(one, info.Size()-1)
	f.Close()

	_, err = OpenReader(path)
	if !errors.Is(err, ErrBadMagic) {
		t.Errorf("err = %v, want ErrBadMagic", err)
	}
}

func TestTooSmallToBeSSTable(t *testing.T) {
	path := makePath(t)
	if err := os.WriteFile(path, []byte("short"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenReader(path); err == nil {
		t.Error("expected error on too-small file")
	}
}

func TestMultiBlockSSTable(t *testing.T) {
	const n = 5000
	path := makePath(t)
	w, _ := NewWriter(path, n)
	value := bytes.Repeat([]byte("x"), 200)
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("k%06d", i))
		if err := w.Add(record.OpPut, k, value); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Finish(); err != nil {
		t.Fatal(err)
	}

	r, err := OpenReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	if r.NumBlocks() < 10 {
		t.Errorf("expected many blocks, got %d", r.NumBlocks())
	}

	for _, i := range []int{0, 1, n / 4, n / 2, 3 * n / 4, n - 2, n - 1} {
		k := []byte(fmt.Sprintf("k%06d", i))
		v, op, found, err := r.Get(k)
		if err != nil || !found || op != record.OpPut || !bytes.Equal(v, value) {
			t.Errorf("Get %q: got found=%v err=%v op=%d", k, found, err, op)
		}
	}

	for _, k := range []string{"a", "k999999", "z"} {
		_, _, found, err := r.Get([]byte(k))
		if err != nil || found {
			t.Errorf("Get %q: found=%v err=%v", k, found, err)
		}
	}
}

func TestIterateAcrossBlocks(t *testing.T) {
	const n = 1000
	path := makePath(t)
	w, _ := NewWriter(path, n)
	value := bytes.Repeat([]byte("v"), 200)
	for i := 0; i < n; i++ {
		w.Add(record.OpPut, []byte(fmt.Sprintf("k%06d", i)), value)
	}
	w.Finish()

	r, _ := OpenReader(path)
	defer r.Close()

	count := 0
	r.Iterate(func(op record.Op, k, v []byte) bool {
		if op != record.OpPut {
			t.Errorf("entry %d: op = %d", count, op)
		}
		want := []byte(fmt.Sprintf("k%06d", count))
		if !bytes.Equal(k, want) {
			t.Errorf("entry %d: key = %q, want %q", count, k, want)
		}
		count++
		return true
	})
	if count != n {
		t.Errorf("iterated %d, want %d", count, n)
	}
}

func TestOversizedRecord(t *testing.T) {
	path := makePath(t)
	w, _ := NewWriter(path, 2)
	bigVal := bytes.Repeat([]byte("X"), 10_000)
	w.Add(record.OpPut, []byte("big"), bigVal)
	w.Add(record.OpPut, []byte("small"), []byte("v"))
	w.Finish()

	r, _ := OpenReader(path)
	defer r.Close()

	v, op, found, err := r.Get([]byte("big"))
	if err != nil || !found || op != record.OpPut || !bytes.Equal(v, bigVal) {
		t.Errorf("big: found=%v err=%v op=%d len(v)=%d", found, err, op, len(v))
	}
	v, op, found, err = r.Get([]byte("small"))
	if err != nil || !found || op != record.OpPut || !bytes.Equal(v, []byte("v")) {
		t.Errorf("small: found=%v err=%v op=%d v=%q", found, err, op, v)
	}
}

func TestAddAfterFinish(t *testing.T) {
	w, _ := NewWriter(makePath(t), 10)
	w.Add(record.OpPut, []byte("a"), []byte("1"))
	w.Finish()
	if err := w.Add(record.OpPut, []byte("b"), []byte("2")); err == nil {
		t.Error("Add after Finish: want error")
	}
}

func TestAbortAfterFinishIsNoop(t *testing.T) {
	w, _ := NewWriter(makePath(t), 10)
	w.Add(record.OpPut, []byte("a"), []byte("1"))
	w.Finish()
	if err := w.Abort(); err != nil {
		t.Errorf("Abort after Finish: %v", err)
	}
}

// TestBloomFilterSkipsBlockReads is the payoff test. Build an SSTable
// with 10k keys; query 10k absent keys; assert the block-read count
// is well under the no-filter baseline.
func TestBloomFilterSkipsBlockReads(t *testing.T) {
	const n = 10_000
	path := makePath(t)
	w, _ := NewWriter(path, n)
	value := bytes.Repeat([]byte("v"), 50)
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("present-%06d", i))
		if err := w.Add(record.OpPut, k, value); err != nil {
			t.Fatal(err)
		}
	}
	w.Finish()

	r, err := OpenReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	const queries = 10_000
	for i := 0; i < queries; i++ {
		k := []byte(fmt.Sprintf("absent-%06d", i))
		_, _, found, _ := r.Get(k)
		if found {
			t.Fatalf("false positive in DB lookup for %q", k)
		}
	}

	reads := r.BlockReadsForTesting()
	t.Logf("block reads = %d for %d absent queries (FPR effective = %.3f%%)",
		reads, queries, 100*float64(reads)/float64(queries))
	if reads > 300 {
		t.Errorf("too many block reads: got %d, want <= 300 (filter not working)", reads)
	}
}

// TestBloomFilterDoesNotBreakHits verifies that present-key reads still
// work (the bloom should never say "no" for an added key).
func TestBloomFilterDoesNotBreakHits(t *testing.T) {
	const n = 5_000
	path := makePath(t)
	w, _ := NewWriter(path, n)
	value := []byte("v")
	keys := make([][]byte, n)
	for i := 0; i < n; i++ {
		keys[i] = []byte(fmt.Sprintf("k%05d", i))
		w.Add(record.OpPut, keys[i], value)
	}
	w.Finish()

	r, _ := OpenReader(path)
	defer r.Close()

	for _, k := range keys {
		_, op, found, err := r.Get(k)
		if err != nil || !found || op != record.OpPut {
			t.Errorf("Get %q: found=%v op=%d err=%v", k, found, op, err)
		}
	}
}
