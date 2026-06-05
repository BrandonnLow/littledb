package memtable

import (
	"bytes"
	"testing"
)

func TestPutGetSingleVersion(t *testing.T) {
	m := New()
	if err := m.Put([]byte("k"), []byte("v"), 100); err != nil {
		t.Fatal(err)
	}
	v, op, found := m.GetAsOf([]byte("k"), 100)
	if !found || op != OpPut || !bytes.Equal(v, []byte("v")) {
		t.Errorf("got (%q, %d, %v)", v, op, found)
	}
}

func TestMultipleVersionsCoexist(t *testing.T) {
	m := New()
	m.Put([]byte("k"), []byte("v1"), 100)
	m.Put([]byte("k"), []byte("v2"), 200)
	m.Put([]byte("k"), []byte("v3"), 300)

	for _, c := range []struct {
		snapshot uint64
		want     string
	}{
		{50, ""},
		{100, "v1"},
		{150, "v1"},
		{200, "v2"},
		{250, "v2"},
		{300, "v3"},
		{1000, "v3"},
	} {
		v, op, found := m.GetAsOf([]byte("k"), c.snapshot)
		if c.want == "" {
			if found {
				t.Errorf("snapshot=%d: got (%q, %d, true), want not-found", c.snapshot, v, op)
			}
			continue
		}
		if !found || op != OpPut || string(v) != c.want {
			t.Errorf("snapshot=%d: got (%q, %d, %v), want (%q, OpPut, true)",
				c.snapshot, v, op, found, c.want)
		}
	}
}

func TestTombstoneMasks(t *testing.T) {
	m := New()
	m.Put([]byte("k"), []byte("v1"), 100)
	m.Delete([]byte("k"), 200)
	m.Put([]byte("k"), []byte("v3"), 300)

	v, op, found := m.GetAsOf([]byte("k"), 100)
	if !found || op != OpPut || !bytes.Equal(v, []byte("v1")) {
		t.Errorf("ts=100: got (%q, %d, %v)", v, op, found)
	}

	_, op, found = m.GetAsOf([]byte("k"), 200)
	if !found || op != OpDelete {
		t.Errorf("ts=200: op=%d found=%v, want (OpDelete, true)", op, found)
	}

	_, op, found = m.GetAsOf([]byte("k"), 250)
	if !found || op != OpDelete {
		t.Errorf("ts=250: op=%d found=%v", op, found)
	}

	v, op, found = m.GetAsOf([]byte("k"), 300)
	if !found || op != OpPut || !bytes.Equal(v, []byte("v3")) {
		t.Errorf("ts=300: got (%q, %d, %v)", v, op, found)
	}
}

func TestGetAsOfMissingKey(t *testing.T) {
	m := New()
	m.Put([]byte("k"), []byte("v"), 100)
	if _, _, found := m.GetAsOf([]byte("other"), 200); found {
		t.Error("found a key that was never put")
	}
}

func TestPutFrozenReturnsError(t *testing.T) {
	m := New()
	m.Freeze()
	if err := m.Put([]byte("k"), []byte("v"), 1); err != ErrFrozen {
		t.Errorf("Put on frozen: err=%v, want ErrFrozen", err)
	}
}

func TestIterateYieldsAllVersionsSorted(t *testing.T) {
	m := New()
	m.Put([]byte("b"), []byte("b50"), 50)
	m.Put([]byte("a"), []byte("a100"), 100)
	m.Put([]byte("a"), []byte("a200"), 200)
	m.Put([]byte("b"), []byte("b100"), 100)
	m.Delete([]byte("a"), 300)

	type entry struct {
		k, v string
		op   Op
		ts   uint64
	}
	var got []entry
	m.Iterate(func(uk, v []byte, op Op, ts uint64) bool {
		got = append(got, entry{string(uk), string(v), op, ts})
		return true
	})
	want := []entry{
		{"a", "", OpDelete, 300},
		{"a", "a200", OpPut, 200},
		{"a", "a100", OpPut, 100},
		{"b", "b100", OpPut, 100},
		{"b", "b50", OpPut, 50},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d entries, want %d: %+v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("[%d] got %+v want %+v", i, got[i], want[i])
		}
	}
}

func TestSizeTracksGrowth(t *testing.T) {
	m := New()
	if m.ApproximateSize() != 0 {
		t.Errorf("initial size = %d, want 0", m.ApproximateSize())
	}
	m.Put([]byte("k"), []byte("v"), 1)
	if m.ApproximateSize() <= 0 {
		t.Errorf("size after put = %d, want > 0", m.ApproximateSize())
	}
}

func TestLenCountsVersionsNotKeys(t *testing.T) {
	m := New()
	m.Put([]byte("k"), []byte("v1"), 100)
	m.Put([]byte("k"), []byte("v2"), 200)
	m.Put([]byte("k"), []byte("v3"), 300)
	if got := m.Len(); got != 3 {
		t.Errorf("Len = %d, want 3 (each version is its own entry)", got)
	}
}
