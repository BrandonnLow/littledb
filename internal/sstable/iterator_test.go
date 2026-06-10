package sstable

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/BrandonnLow/littledb/internal/mvcckey"
	"github.com/BrandonnLow/littledb/internal/record"
)

type ver struct {
	key string
	ts  uint64
	op  record.Op
	val string
}

func writeSST(t *testing.T, vers []ver) *Reader {
	t.Helper()
	path := filepath.Join(t.TempDir(), "000001.sst")
	w, err := NewWriter(path, len(vers))
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range vers {
		if err := w.Add(v.op, []byte(v.key), []byte(v.val), v.ts); err != nil {
			t.Fatalf("Add(%s@%d): %v", v.key, v.ts, err)
		}
	}
	if err := w.Finish(); err != nil {
		t.Fatal(err)
	}
	r, err := OpenReader(path)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func collectIter(t *testing.T, it *Iterator) []ver {
	t.Helper()
	var out []ver
	for it.Valid() {
		uk, ts, ok := mvcckey.Decode(it.EncKey())
		if !ok {
			t.Fatal("malformed encoded key")
		}
		out = append(out, ver{string(uk), ts, it.Op(), string(it.Value())})
		it.Advance()
	}
	if err := it.Err(); err != nil {
		t.Fatalf("iterator err: %v", err)
	}
	return out
}

func keysOf(vers []ver) []string {
	out := make([]string, len(vers))
	for i, v := range vers {
		out[i] = fmt.Sprintf("%s@%d", v.key, v.ts)
	}
	return out
}

func eqKeys(got []ver, want []string) bool {
	g := keysOf(got)
	if len(g) != len(want) {
		return false
	}
	for i := range want {
		if g[i] != want[i] {
			return false
		}
	}
	return true
}

func sampleSST(t *testing.T) *Reader {
	return writeSST(t, []ver{
		{"a", 20, record.OpPut, "a20"},
		{"a", 10, record.OpPut, "a10"},
		{"b", 30, record.OpDelete, ""},
		{"c", 5, record.OpPut, "c5"},
		{"d", 100, record.OpPut, "d100"},
		{"d", 50, record.OpPut, "d50"},
	})
}

func TestIterFullRange(t *testing.T) {
	r := sampleSST(t)
	defer r.Close()
	it, err := r.NewIterator(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	got := collectIter(t, it)
	want := []string{"a@20", "a@10", "b@30", "c@5", "d@100", "d@50"}
	if !eqKeys(got, want) {
		t.Errorf("full: got %v, want %v", keysOf(got), want)
	}
}

func TestIterBounds(t *testing.T) {
	r := sampleSST(t)
	defer r.Close()
	cases := []struct {
		start, end string
		want       []string
	}{
		{"b", "d", []string{"b@30", "c@5"}},          // [b,d): b and c, all their versions
		{"a", "c", []string{"a@20", "a@10", "b@30"}}, // start inclusive, end exclusive
		{"c", "", []string{"c@5", "d@100", "d@50"}},  // nil end = through last
		{"0", "a", nil}, // before everything, end excludes a
		{"e", "", nil},  // past everything
		{"b", "b", nil}, // empty range
	}
	for _, c := range cases {
		var start, end []byte
		if c.start != "" {
			start = []byte(c.start)
		}
		if c.end != "" {
			end = []byte(c.end)
		}
		it, err := r.NewIterator(start, end)
		if err != nil {
			t.Fatalf("[%s,%s): %v", c.start, c.end, err)
		}
		got := collectIter(t, it)
		if !eqKeys(got, c.want) {
			t.Errorf("[%s,%s): got %v, want %v", c.start, c.end, keysOf(got), c.want)
		}
		it.Close()
	}
}

func TestIterEmptySSTable(t *testing.T) {
	r := writeSST(t, nil)
	defer r.Close()
	it, err := r.NewIterator(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if it.Valid() {
		t.Error("empty sstable iterator should not be valid")
	}
}

func TestIterMultiBlock(t *testing.T) {
	// Force many blocks (4 KB blocks, ~120-byte records) and confirm a
	// bounded scan crosses block boundaries correctly.
	const n = 2000
	var vers []ver
	value := string(make([]byte, 100))
	for i := 0; i < n; i++ {
		vers = append(vers, ver{fmt.Sprintf("k%05d", i), 100, record.OpPut, value})
	}
	r := writeSST(t, vers)
	defer r.Close()
	if r.NumBlocks() < 5 {
		t.Fatalf("expected multiple blocks, got %d", r.NumBlocks())
	}

	it, err := r.NewIterator([]byte("k00100"), []byte("k00200"))
	if err != nil {
		t.Fatal(err)
	}
	got := collectIter(t, it)
	if len(got) != 100 {
		t.Fatalf("bounded multi-block scan got %d records, want 100", len(got))
	}
	if got[0].key != "k00100" || got[len(got)-1].key != "k00199" {
		t.Errorf("range ends: first=%s last=%s, want k00100..k00199", got[0].key, got[len(got)-1].key)
	}
}
