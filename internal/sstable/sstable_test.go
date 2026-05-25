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
	w, err := NewWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(triples)%3 != 0 {
		t.Fatalf("writeKeys: triples must come in groups of 3, got %d args", len(triples))
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
		key, val string
		op       record.Op
	}
	var got []entry
	err = r.Iterate(func(op record.Op, k, v []byte) bool {
		got = append(got, entry{string(k), string(v), op})
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
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
			t.Errorf("[%d] got %+v, want %+v", i, got[i], want[i])
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
		if err != nil {
			t.Errorf("Get %q: %v", c.k, err)
			continue
		}
		if !found || op != record.OpPut {
			t.Errorf("Get %q: found=%v op=%d", c.k, found, op)
		}
		if !bytes.Equal(v, []byte(c.v)) {
			t.Errorf("Get %q: got %q want %q", c.k, v, c.v)
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

	_, _, found, err := r.Get([]byte("0"))
	if err != nil || found {
		t.Errorf("Get '0': found=%v err=%v", found, err)
	}
	_, _, found, err = r.Get([]byte("b"))
	if err != nil || found {
		t.Errorf("Get 'b': found=%v err=%v", found, err)
	}
	_, _, found, err = r.Get([]byte("z"))
	if err != nil || found {
		t.Errorf("Get 'z': found=%v err=%v", found, err)
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
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Error("tombstone not found")
	}
	if op != record.OpDelete {
		t.Errorf("op = %d, want OpDelete", op)
	}
	if v != nil {
		t.Errorf("tombstone value = %q, want nil", v)
	}
}

func TestEmptySSTable(t *testing.T) {
	path := makePath(t)
	w, err := NewWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Finish(); err != nil {
		t.Fatal(err)
	}

	r, err := OpenReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	_, _, found, err := r.Get([]byte("anything"))
	if err != nil || found {
		t.Errorf("empty SSTable: found=%v err=%v", found, err)
	}

	called := false
	r.Iterate(func(op record.Op, k, v []byte) bool {
		called = true
		return true
	})
	if called {
		t.Error("Iterate on empty SSTable called fn")
	}
}

func TestOutOfOrder(t *testing.T) {
	path := makePath(t)
	w, _ := NewWriter(path)
	defer w.Abort()

	if err := w.Add(record.OpPut, []byte("b"), []byte("2")); err != nil {
		t.Fatal(err)
	}
	if err := w.Add(record.OpPut, []byte("a"), []byte("1")); !errors.Is(err, ErrOutOfOrder) {
		t.Errorf("err = %v, want ErrOutOfOrder", err)
	}
}

func TestDuplicateKey(t *testing.T) {
	path := makePath(t)
	w, _ := NewWriter(path)
	defer w.Abort()

	if err := w.Add(record.OpPut, []byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := w.Add(record.OpPut, []byte("a"), []byte("2")); !errors.Is(err, ErrDuplicate) {
		t.Errorf("err = %v, want ErrDuplicate", err)
	}
}

func TestAbortRemovesTempFile(t *testing.T) {
	path := makePath(t)
	w, _ := NewWriter(path)
	w.Add(record.OpPut, []byte("k"), []byte("v"))

	if err := w.Abort(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("final file exists after abort: %v", err)
	}
	if _, err := os.Stat(path + ".tmp"); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("temp file exists after abort: %v", err)
	}
}

func TestAtomicCreation(t *testing.T) {
	path := makePath(t)
	w, _ := NewWriter(path)
	w.Add(record.OpPut, []byte("k"), []byte("v"))

	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("final file visible before Finish: %v", err)
	}
	if _, err := os.Stat(path + ".tmp"); err != nil {
		t.Errorf("temp file missing during write: %v", err)
	}

	if err := w.Finish(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Errorf("final file missing after Finish: %v", err)
	}
	if _, err := os.Stat(path + ".tmp"); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("temp file still present after Finish: %v", err)
	}
}

func TestTruncatedFile(t *testing.T) {
	path := writeKeys(t,
		record.OpPut, "a", "1",
		record.OpPut, "b", "2",
		record.OpPut, "c", "3",
	)

	info, _ := os.Stat(path)
	if err := os.Truncate(path, info.Size()-5); err != nil {
		t.Fatal(err)
	}

	r, err := OpenReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	_, _, _, err = r.Get([]byte("c"))
	if err == nil {
		t.Error("Get past truncation: err = nil, want non-nil")
	}
}

func TestLargeSSTable(t *testing.T) {
	const n = 5000
	path := makePath(t)
	w, _ := NewWriter(path)
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("k%06d", i))
		v := []byte(fmt.Sprintf("v%d", i))
		if err := w.Add(record.OpPut, k, v); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Finish(); err != nil {
		t.Fatal(err)
	}

	r, _ := OpenReader(path)
	defer r.Close()

	for _, i := range []int{0, n / 2, n - 1} {
		k := []byte(fmt.Sprintf("k%06d", i))
		want := []byte(fmt.Sprintf("v%d", i))
		v, op, found, err := r.Get(k)
		if err != nil {
			t.Errorf("Get %q: %v", k, err)
			continue
		}
		if !found || op != record.OpPut || !bytes.Equal(v, want) {
			t.Errorf("Get %q: got (%q, %d, %v)", k, v, op, found)
		}
	}
}

func TestAddAfterFinish(t *testing.T) {
	path := makePath(t)
	w, _ := NewWriter(path)
	w.Add(record.OpPut, []byte("a"), []byte("1"))
	w.Finish()

	if err := w.Add(record.OpPut, []byte("b"), []byte("2")); err == nil {
		t.Error("Add after Finish: err = nil, want non-nil")
	}
}

func TestAbortAfterFinishIsNoop(t *testing.T) {
	path := makePath(t) + "-2"
	w, _ := NewWriter(path)
	w.Add(record.OpPut, []byte("a"), []byte("1"))
	w.Finish()
	if err := w.Abort(); err != nil {
		t.Errorf("Abort after Finish: %v", err)
	}
}
