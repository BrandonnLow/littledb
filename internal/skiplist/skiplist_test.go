package skiplist

import (
	"bytes"
	"testing"
)

func TestSeekGEEmpty(t *testing.T) {
	s := New()
	if n := s.SeekGE([]byte("anything")); n != nil {
		t.Errorf("SeekGE on empty: got %v", n)
	}
}

func TestSeekGEExactMatch(t *testing.T) {
	s := New()
	s.Put([]byte("apple"), []byte("a"))
	s.Put([]byte("banana"), []byte("b"))
	s.Put([]byte("cherry"), []byte("c"))

	n := s.SeekGE([]byte("banana"))
	if n == nil || !bytes.Equal(n.Key(), []byte("banana")) {
		t.Errorf("exact: got %v", n)
	}
}

func TestSeekGEBetweenKeys(t *testing.T) {
	s := New()
	s.Put([]byte("apple"), []byte("a"))
	s.Put([]byte("cherry"), []byte("c"))

	n := s.SeekGE([]byte("banana"))
	if n == nil || !bytes.Equal(n.Key(), []byte("cherry")) {
		t.Errorf("between: got %v", n)
	}
}

func TestSeekGEBeforeFirst(t *testing.T) {
	s := New()
	s.Put([]byte("cherry"), []byte("c"))

	n := s.SeekGE([]byte("apple"))
	if n == nil || !bytes.Equal(n.Key(), []byte("cherry")) {
		t.Errorf("before: got %v", n)
	}
}

func TestSeekGEPastEnd(t *testing.T) {
	s := New()
	s.Put([]byte("apple"), []byte("a"))
	s.Put([]byte("banana"), []byte("b"))

	if n := s.SeekGE([]byte("zebra")); n != nil {
		t.Errorf("past end: got %v", n)
	}
}

func TestSeekGEThenNext(t *testing.T) {
	s := New()
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		s.Put([]byte(k), []byte(k))
	}

	got := []string{}
	for n := s.SeekGE([]byte("c")); n != nil; n = n.Next() {
		got = append(got, string(n.Key()))
	}
	want := []string{"c", "d", "e"}
	if len(got) != len(want) {
		t.Fatalf("len(got)=%d, want %d (%v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("[%d] got %q want %q", i, got[i], want[i])
		}
	}
}

func TestNextOnNilSafe(t *testing.T) {
	var n *Node
	if n.Next() != nil {
		t.Error("nil.Next() returned non-nil")
	}
}
