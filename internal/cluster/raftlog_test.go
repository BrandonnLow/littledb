package cluster

import (
	"bytes"
	"testing"
)

// term1 is an arbitrary fixed term; RaftLog itself is term-agnostic.
const term1 uint64 = 1

func TestRaftLogAppendAndRead(t *testing.T) {
	l := NewRaftLog()
	if l.lastIndex() != 0 {
		t.Fatalf("empty lastIndex = %d, want 0", l.lastIndex())
	}
	if l.term(0) != 0 {
		t.Errorf("term(0) = %d, want 0 (sentinel)", l.term(0))
	}
	if !l.has(0) {
		t.Error("has(0) = false, want true (empty-log sentinel)")
	}

	i1 := l.append(term1, []byte("one"))
	i2 := l.append(term1, []byte("two"))
	if i1 != 1 || i2 != 2 {
		t.Fatalf("append indexes = %d,%d, want 1,2", i1, i2)
	}
	if l.lastIndex() != 2 {
		t.Fatalf("lastIndex = %d, want 2", l.lastIndex())
	}
	if !bytes.Equal(l.entryAt(1), []byte("one")) || !bytes.Equal(l.entryAt(2), []byte("two")) {
		t.Errorf("entries = %q,%q, want one,two", l.entryAt(1), l.entryAt(2))
	}
	if l.term(1) != term1 || l.term(2) != term1 {
		t.Errorf("terms = %d,%d, want %d,%d", l.term(1), l.term(2), term1, term1)
	}
	if l.has(3) {
		t.Error("has(3) = true, want false")
	}
}

func TestRaftLogAppendCopiesBytes(t *testing.T) {
	l := NewRaftLog()
	buf := []byte("data")
	l.append(term1, buf)
	buf[0] = 'X' // mutate the caller's buffer
	if !bytes.Equal(l.entryAt(1), []byte("data")) {
		t.Errorf("entry = %q, want data (append must copy)", l.entryAt(1))
	}
}

func TestRaftLogMatchesPrev(t *testing.T) {
	l := NewRaftLog()
	if !l.matchesPrev(0, 0) {
		t.Error("matchesPrev(0,0) on empty = false, want true (start of log)")
	}
	l.append(term1, []byte("a"))
	l.append(term1, []byte("b"))

	if !l.matchesPrev(2, term1) {
		t.Error("matchesPrev(2, term1) = false, want true")
	}
	if l.matchesPrev(2, term1+1) {
		t.Error("matchesPrev with wrong term = true, want false")
	}
	if l.matchesPrev(3, term1) {
		t.Error("matchesPrev past end = true, want false")
	}
}

func TestRaftLogTruncateFrom(t *testing.T) {
	l := NewRaftLog()
	for i := 0; i < 4; i++ {
		l.append(term1, []byte{byte('0' + i)})
	}
	if l.lastIndex() != 4 {
		t.Fatalf("lastIndex = %d, want 4", l.lastIndex())
	}

	l.truncateFrom(5) // past end: no-op
	if l.lastIndex() != 4 {
		t.Errorf("after truncateFrom(5), lastIndex = %d, want 4", l.lastIndex())
	}

	l.truncateFrom(3) // drop indexes 3 and 4
	if l.lastIndex() != 2 {
		t.Fatalf("after truncateFrom(3), lastIndex = %d, want 2", l.lastIndex())
	}
	if !bytes.Equal(l.entryAt(1), []byte("0")) || !bytes.Equal(l.entryAt(2), []byte("1")) {
		t.Errorf("survivors = %q,%q, want 0,1", l.entryAt(1), l.entryAt(2))
	}

	// Can append after truncation; indexes resume from the new end.
	if idx := l.append(term1, []byte("new")); idx != 3 {
		t.Errorf("append after truncate = index %d, want 3", idx)
	}
}
