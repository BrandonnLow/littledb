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

func TestRoundTripSingleVersion(t *testing.T) {
	path := makePath(t)
	w, _ := NewWriter(path, 3)
	w.Add(record.OpPut, []byte("alpha"), []byte("1"), 100)
	w.Add(record.OpPut, []byte("bravo"), []byte("2"), 100)
	w.Add(record.OpPut, []byte("charlie"), []byte("3"), 100)
	if err := w.Finish(); err != nil {
		t.Fatal(err)
	}

	r, err := OpenReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	for _, c := range []struct{ k, v string }{{"alpha", "1"}, {"bravo", "2"}, {"charlie", "3"}} {
		v, op, found, err := r.GetAsOf([]byte(c.k), 200)
		if err != nil || !found || op != record.OpPut || !bytes.Equal(v, []byte(c.v)) {
			t.Errorf("Get %q: got (%q, %d, %v, %v)", c.k, v, op, found, err)
		}
	}
}

func TestGetAsOfWithMultipleVersions(t *testing.T) {
	path := makePath(t)
	w, _ := NewWriter(path, 3)
	w.Add(record.OpPut, []byte("k"), []byte("v3"), 300)
	w.Add(record.OpPut, []byte("k"), []byte("v2"), 200)
	w.Add(record.OpPut, []byte("k"), []byte("v1"), 100)
	w.Finish()

	r, _ := OpenReader(path)
	defer r.Close()

	for _, c := range []struct {
		snapshot uint64
		want     string
		found    bool
	}{
		{50, "", false},
		{100, "v1", true},
		{150, "v1", true},
		{200, "v2", true},
		{250, "v2", true},
		{300, "v3", true},
		{1000, "v3", true},
	} {
		v, op, found, err := r.GetAsOf([]byte("k"), c.snapshot)
		if err != nil {
			t.Errorf("snapshot=%d: err=%v", c.snapshot, err)
			continue
		}
		if found != c.found {
			t.Errorf("snapshot=%d: found=%v, want %v", c.snapshot, found, c.found)
			continue
		}
		if c.found && (op != record.OpPut || string(v) != c.want) {
			t.Errorf("snapshot=%d: got (%q, %d), want (%q, OpPut)", c.snapshot, v, op, c.want)
		}
	}
}

func TestGetAsOfTombstone(t *testing.T) {
	path := makePath(t)
	w, _ := NewWriter(path, 3)
	w.Add(record.OpPut, []byte("k"), []byte("v3"), 300)
	w.Add(record.OpDelete, []byte("k"), nil, 200)
	w.Add(record.OpPut, []byte("k"), []byte("v1"), 100)
	w.Finish()

	r, _ := OpenReader(path)
	defer r.Close()

	_, op, found, _ := r.GetAsOf([]byte("k"), 250)
	if !found || op != record.OpDelete {
		t.Errorf("snapshot=250: op=%d found=%v, want (OpDelete, true)", op, found)
	}

	v, op, found, _ := r.GetAsOf([]byte("k"), 300)
	if !found || op != record.OpPut || !bytes.Equal(v, []byte("v3")) {
		t.Errorf("snapshot=300: got (%q, %d, %v)", v, op, found)
	}

	v, op, found, _ = r.GetAsOf([]byte("k"), 100)
	if !found || op != record.OpPut || !bytes.Equal(v, []byte("v1")) {
		t.Errorf("snapshot=100: got (%q, %d, %v)", v, op, found)
	}
}

// TestBloomMatchesAnyTimestamp is the critical regression test for the
// "bloom hashes userKey not encoded key" invariant. We store userKey
// at ts=100 only; a lookup at ts=200 (an encoded key never written)
// must still find the version at ts=100.
func TestBloomMatchesAnyTimestamp(t *testing.T) {
	path := makePath(t)
	w, _ := NewWriter(path, 1)
	w.Add(record.OpPut, []byte("k"), []byte("v"), 100)
	w.Finish()

	r, _ := OpenReader(path)
	defer r.Close()

	v, op, found, _ := r.GetAsOf([]byte("k"), 200)
	if !found || op != record.OpPut || !bytes.Equal(v, []byte("v")) {
		t.Errorf("lookup at unwritten timestamp: got (%q, %d, %v)", v, op, found)
	}
}

func TestGetAsOfMiss(t *testing.T) {
	path := makePath(t)
	w, _ := NewWriter(path, 2)
	w.Add(record.OpPut, []byte("a"), []byte("1"), 100)
	w.Add(record.OpPut, []byte("c"), []byte("3"), 100)
	w.Finish()

	r, _ := OpenReader(path)
	defer r.Close()

	for _, k := range []string{"0", "b", "z"} {
		_, _, found, _ := r.GetAsOf([]byte(k), 200)
		if found {
			t.Errorf("Get %q: should miss", k)
		}
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
	if r.MaxTimestamp() != 0 {
		t.Errorf("empty MaxTimestamp = %d, want 0", r.MaxTimestamp())
	}
	if _, _, found, _ := r.GetAsOf([]byte("anything"), 100); found {
		t.Error("empty SSTable returned found=true")
	}
}

func TestMaxTimestampInFooter(t *testing.T) {
	path := makePath(t)
	w, _ := NewWriter(path, 3)
	w.Add(record.OpPut, []byte("a"), []byte("a"), 500)
	w.Add(record.OpPut, []byte("b"), []byte("b"), 1000)
	w.Add(record.OpPut, []byte("c"), []byte("c"), 750)
	w.Finish()

	r, _ := OpenReader(path)
	defer r.Close()
	if got := r.MaxTimestamp(); got != 1000 {
		t.Errorf("MaxTimestamp = %d, want 1000", got)
	}
}

func TestMultiBlockSSTableWithMVCC(t *testing.T) {
	const n = 1000
	path := makePath(t)
	w, _ := NewWriter(path, n)
	value := bytes.Repeat([]byte("x"), 100)
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("k%05d", i))
		w.Add(record.OpPut, k, value, 200)
		w.Add(record.OpPut, k, []byte("old"), 100)
	}
	w.Finish()

	r, _ := OpenReader(path)
	defer r.Close()
	if r.NumBlocks() < 5 {
		t.Errorf("expected many blocks, got %d", r.NumBlocks())
	}

	for _, i := range []int{0, n / 2, n - 1} {
		k := []byte(fmt.Sprintf("k%05d", i))
		v, _, found, _ := r.GetAsOf(k, 200)
		if !found || !bytes.Equal(v, value) {
			t.Errorf("ts=200 i=%d: got (%q, %v)", i, v, found)
		}
		v, _, found, _ = r.GetAsOf(k, 100)
		if !found || string(v) != "old" {
			t.Errorf("ts=100 i=%d: got (%q, %v)", i, v, found)
		}
	}
}

func TestOutOfOrder(t *testing.T) {
	w, _ := NewWriter(makePath(t), 2)
	defer w.Abort()
	w.Add(record.OpPut, []byte("b"), []byte("2"), 100)
	if err := w.Add(record.OpPut, []byte("b"), []byte("2'"), 100); !errors.Is(err, ErrDuplicate) {
		t.Errorf("err = %v, want ErrDuplicate", err)
	}
	if err := w.Add(record.OpPut, []byte("a"), []byte("1"), 100); !errors.Is(err, ErrOutOfOrder) {
		t.Errorf("err = %v, want ErrOutOfOrder", err)
	}
}

func TestSameUserKeyAscendingTimestampIsOutOfOrder(t *testing.T) {
	w, _ := NewWriter(makePath(t), 2)
	defer w.Abort()
	w.Add(record.OpPut, []byte("k"), []byte("v1"), 100)
	if err := w.Add(record.OpPut, []byte("k"), []byte("v2"), 200); !errors.Is(err, ErrOutOfOrder) {
		t.Errorf("err = %v, want ErrOutOfOrder (within userKey, ts must DESCEND)", err)
	}
}

func TestBadMagic(t *testing.T) {
	path := makePath(t)
	w, _ := NewWriter(path, 1)
	w.Add(record.OpPut, []byte("a"), []byte("1"), 100)
	w.Finish()

	f, _ := os.OpenFile(path, os.O_RDWR, 0)
	info, _ := f.Stat()
	one := []byte{0}
	f.ReadAt(one, info.Size()-1)
	one[0] ^= 0xFF
	f.WriteAt(one, info.Size()-1)
	f.Close()

	if _, err := OpenReader(path); !errors.Is(err, ErrBadMagic) {
		t.Errorf("err = %v, want ErrBadMagic", err)
	}
}

func TestBloomFilterStillSkipsAbsentKeys(t *testing.T) {
	const n = 5000
	path := makePath(t)
	w, _ := NewWriter(path, n)
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("present-%06d", i))
		w.Add(record.OpPut, k, []byte("v"), 100)
	}
	w.Finish()

	r, _ := OpenReader(path)
	defer r.Close()

	for i := 0; i < 5000; i++ {
		_, _, found, _ := r.GetAsOf([]byte(fmt.Sprintf("absent-%06d", i)), 100)
		if found {
			t.Fatalf("false positive at i=%d", i)
		}
	}
	reads := r.BlockReadsForTesting()
	t.Logf("block reads = %d for 5000 absent queries", reads)
	if reads > 200 {
		t.Errorf("too many block reads: %d (filter not working?)", reads)
	}
}
