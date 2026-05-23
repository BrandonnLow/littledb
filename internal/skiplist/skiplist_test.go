package skiplist

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"sort"
	"testing"
)

func TestEmpty(t *testing.T) {
	s := New()
	if _, ok := s.Get([]byte("x")); ok {
		t.Error("Get on empty: ok = true, want false")
	}
	if s.Len() != 0 {
		t.Errorf("Len() = %d, want 0", s.Len())
	}
	if s.Delete([]byte("x")) {
		t.Error("Delete on empty: returned true")
	}
}

func TestPutGet(t *testing.T) {
	s := New()
	s.Put([]byte("hello"), []byte("world"))

	v, ok := s.Get([]byte("hello"))
	if !ok {
		t.Fatal("Get: ok = false")
	}
	if !bytes.Equal(v, []byte("world")) {
		t.Errorf("Get: got %q, want world", v)
	}
	if s.Len() != 1 {
		t.Errorf("Len() = %d, want 1", s.Len())
	}
}

func TestPutGetMany(t *testing.T) {
	s := New()
	const n = 100
	for i := 0; i < n; i++ {
		s.Put([]byte(fmt.Sprintf("k%03d", i)), []byte(fmt.Sprintf("v%d", i)))
	}
	if s.Len() != n {
		t.Errorf("Len() = %d, want %d", s.Len(), n)
	}
	for i := 0; i < n; i++ {
		v, ok := s.Get([]byte(fmt.Sprintf("k%03d", i)))
		if !ok {
			t.Errorf("missing key %d", i)
			continue
		}
		if !bytes.Equal(v, []byte(fmt.Sprintf("v%d", i))) {
			t.Errorf("k%03d: got %q", i, v)
		}
	}
}

func TestUpdate(t *testing.T) {
	s := New()
	s.Put([]byte("k"), []byte("v1"))
	s.Put([]byte("k"), []byte("v2"))
	s.Put([]byte("k"), []byte("v3"))

	v, _ := s.Get([]byte("k"))
	if !bytes.Equal(v, []byte("v3")) {
		t.Errorf("got %q, want v3", v)
	}
	if s.Len() != 1 {
		t.Errorf("Len() = %d after 3 puts of same key, want 1", s.Len())
	}
}

func TestDelete(t *testing.T) {
	s := New()
	s.Put([]byte("a"), []byte("1"))
	s.Put([]byte("b"), []byte("2"))
	s.Put([]byte("c"), []byte("3"))

	if !s.Delete([]byte("b")) {
		t.Fatal("Delete: returned false for present key")
	}
	if _, ok := s.Get([]byte("b")); ok {
		t.Error("Get after Delete: still found")
	}
	if s.Len() != 2 {
		t.Errorf("Len() = %d, want 2", s.Len())
	}
	if v, ok := s.Get([]byte("a")); !ok || !bytes.Equal(v, []byte("1")) {
		t.Errorf("a: ok=%v v=%q", ok, v)
	}
	if v, ok := s.Get([]byte("c")); !ok || !bytes.Equal(v, []byte("3")) {
		t.Errorf("c: ok=%v v=%q", ok, v)
	}
}

func TestDeleteMissing(t *testing.T) {
	s := New()
	s.Put([]byte("a"), []byte("1"))
	if s.Delete([]byte("nope")) {
		t.Error("Delete on missing key: returned true")
	}
	if s.Len() != 1 {
		t.Errorf("Len() = %d, want 1", s.Len())
	}
}

func TestIterationSorted(t *testing.T) {
	s := New()
	keys := []string{"d", "a", "f", "c", "b", "e"}
	for _, k := range keys {
		s.Put([]byte(k), []byte("v"))
	}
	var got []string
	s.Iterate(func(k, v []byte) bool {
		got = append(got, string(k))
		return true
	})
	want := []string{"a", "b", "c", "d", "e", "f"}
	if len(got) != len(want) {
		t.Fatalf("iterated %d items, want %d: %v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("position %d: got %q, want %q", i, got[i], want[i])
		}
	}
}

func TestIterateEarlyStop(t *testing.T) {
	s := New()
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		s.Put([]byte(k), []byte("v"))
	}
	var got []string
	s.Iterate(func(k, v []byte) bool {
		got = append(got, string(k))
		return len(got) < 3
	})
	if len(got) != 3 {
		t.Errorf("iterated %d items, want exactly 3: %v", len(got), got)
	}
}

func TestIterateEmpty(t *testing.T) {
	s := New()
	called := false
	s.Iterate(func(k, v []byte) bool {
		called = true
		return true
	})
	if called {
		t.Error("Iterate on empty: called fn at least once")
	}
}

func TestEmptyKey(t *testing.T) {
	s := New()
	s.Put([]byte(""), []byte("v"))
	v, ok := s.Get([]byte(""))
	if !ok || !bytes.Equal(v, []byte("v")) {
		t.Errorf("empty key: ok=%v v=%q", ok, v)
	}
}

func TestCallerCanMutateInputs(t *testing.T) {
	s := New()
	key := []byte("k")
	val := []byte("original")
	s.Put(key, val)

	key[0] = 'X'
	val[0] = 'X'

	v, ok := s.Get([]byte("k"))
	if !ok {
		t.Fatal("Get: not found after caller mutated input slice")
	}
	if !bytes.Equal(v, []byte("original")) {
		t.Errorf("stored value mutated: got %q, want 'original'", v)
	}
}

func TestUpdateDoesNotInvalidateOldGet(t *testing.T) {
	s := New()
	s.Put([]byte("k"), []byte("first"))
	v1, _ := s.Get([]byte("k"))

	s.Put([]byte("k"), []byte("second"))

	if !bytes.Equal(v1, []byte("first")) {
		t.Errorf("old Get slice mutated: %q", v1)
	}
	v2, _ := s.Get([]byte("k"))
	if !bytes.Equal(v2, []byte("second")) {
		t.Errorf("new Get: got %q, want second", v2)
	}
}

// TestStressAgainstMap is the property-style test: do 10k random ops
// against the skiplist and a reference map, comparing results after each.
// Anything they disagree on is a bug.
func TestStressAgainstMap(t *testing.T) {
	s := New()
	ref := map[string]string{}

	r := rand.New(rand.NewPCG(42, 1337))
	keys := make([]string, 100)
	for i := range keys {
		keys[i] = fmt.Sprintf("k%03d", i)
	}

	const ops = 10_000
	for i := 0; i < ops; i++ {
		k := keys[r.IntN(len(keys))]
		switch r.IntN(3) {
		case 0:
			v := fmt.Sprintf("v%d", i)
			s.Put([]byte(k), []byte(v))
			ref[k] = v
		case 1:
			gotVal, gotOK := s.Get([]byte(k))
			wantVal, wantOK := ref[k]
			if gotOK != wantOK {
				t.Fatalf("op %d Get %s: ok=%v want %v", i, k, gotOK, wantOK)
			}
			if gotOK && !bytes.Equal(gotVal, []byte(wantVal)) {
				t.Fatalf("op %d Get %s: got %q want %q", i, k, gotVal, wantVal)
			}
		case 2:
			gotPresent := s.Delete([]byte(k))
			_, wantPresent := ref[k]
			if gotPresent != wantPresent {
				t.Fatalf("op %d Delete %s: ok=%v want %v", i, k, gotPresent, wantPresent)
			}
			delete(ref, k)
		}
		if s.Len() != len(ref) {
			t.Fatalf("op %d: Len()=%d, want %d", i, s.Len(), len(ref))
		}
	}

	wantKeys := make([]string, 0, len(ref))
	for k := range ref {
		wantKeys = append(wantKeys, k)
	}
	sort.Strings(wantKeys)

	gotKeys := []string{}
	s.Iterate(func(k, v []byte) bool {
		gotKeys = append(gotKeys, string(k))
		if !bytes.Equal(v, []byte(ref[string(k)])) {
			t.Errorf("iterate: %s value %q, want %q", k, v, ref[string(k)])
		}
		return true
	})
	if len(gotKeys) != len(wantKeys) {
		t.Fatalf("iterate yielded %d keys, want %d", len(gotKeys), len(wantKeys))
	}
	for i := range wantKeys {
		if gotKeys[i] != wantKeys[i] {
			t.Errorf("iterate pos %d: got %q want %q", i, gotKeys[i], wantKeys[i])
		}
	}
}

func TestLargeKeysAndValues(t *testing.T) {
	s := New()
	bigKey := bytes.Repeat([]byte("k"), 10_000)
	bigVal := bytes.Repeat([]byte("v"), 100_000)
	s.Put(bigKey, bigVal)

	v, ok := s.Get(bigKey)
	if !ok {
		t.Fatal("not found")
	}
	if !bytes.Equal(v, bigVal) {
		t.Errorf("value differs after roundtrip")
	}
}
