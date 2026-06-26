package cluster

import "testing"

// buildLog appends one entry per term, with data bytes 'a','b','c',... so each
// index is independently identifiable.
func buildLog(terms ...uint64) *RaftLog {
	l := NewRaftLog()
	for i, tm := range terms {
		l.append(tm, []byte{byte('a' + i)})
	}
	return l
}

func TestRaftLogCompactTo(t *testing.T) {
	l := buildLog(1, 1, 2, 2) // 1@1, 2@1, 3@2, 4@2
	l.compactTo(2, l.term(2)) // base 2 @ term 1; survivors 3@2, 4@2

	if l.baseIndex != 2 || l.baseTerm != 1 {
		t.Fatalf("base = (%d,%d), want (2,1)", l.baseIndex, l.baseTerm)
	}
	if l.lastIndex() != 4 || l.lastTerm() != 2 {
		t.Fatalf("lastIndex=%d lastTerm=%d, want 4/2", l.lastIndex(), l.lastTerm())
	}
	if l.term(2) != 1 {
		t.Errorf("term(baseIndex=2) = %d, want baseTerm 1", l.term(2))
	}
	if l.term(3) != 2 || string(l.entryAt(3)) != "c" || string(l.entryAt(4)) != "d" {
		t.Errorf("survivors wrong: term(3)=%d e3=%q e4=%q", l.term(3), l.entryAt(3), l.entryAt(4))
	}
	if !l.has(2) || !l.has(4) {
		t.Error("has(baseIndex) and has(lastIndex) must be true")
	}
	if l.has(1) || l.has(5) {
		t.Error("has(compacted) and has(past-end) must be false")
	}
	// The surviving suffix must live in a fresh backing array (old one released);
	// a fresh make has cap == len.
	if cap(l.entries) != len(l.entries) {
		t.Errorf("cap=%d len=%d; compactTo must copy to a fresh array to release the old one",
			cap(l.entries), len(l.entries))
	}
}

func TestRaftLogTermPanicsBelowBase(t *testing.T) {
	l := buildLog(1, 1, 2, 2)
	l.compactTo(2, l.term(2))
	defer func() {
		if recover() == nil {
			t.Fatal("term(idx < baseIndex) must panic (compacted region)")
		}
	}()
	_ = l.term(1)
}

func TestRaftLogMatchesPrevAcrossBase(t *testing.T) {
	l := buildLog(1, 1, 2, 2)
	l.compactTo(2, l.term(2)) // base 2 @ term 1

	if !l.matchesPrev(2, 1) {
		t.Error("matchesPrev(baseIndex, baseTerm) must match")
	}
	if l.matchesPrev(2, 9) {
		t.Error("matchesPrev(baseIndex, wrong term) must not match")
	}
	if !l.matchesPrev(1, 999) {
		t.Error("matchesPrev below base must be trusted true (committed)")
	}
	if !l.matchesPrev(3, 2) {
		t.Error("matchesPrev(3, 2) must match")
	}
	if l.matchesPrev(3, 1) || l.matchesPrev(5, 2) {
		t.Error("wrong-term and past-end matchesPrev must not match")
	}
}

func TestRaftLogTruncateFromAfterCompact(t *testing.T) {
	l := buildLog(1, 1, 2, 2)
	l.compactTo(2, l.term(2)) // base 2, entries 3,4

	l.truncateFrom(2) // <= base: no-op
	l.truncateFrom(1) // < base: no-op
	if l.lastIndex() != 4 {
		t.Fatalf("truncateFrom(<=base) mutated log: lastIndex=%d, want 4", l.lastIndex())
	}
	l.truncateFrom(4) // drop index 4, keep 3
	if l.lastIndex() != 3 || string(l.entryAt(3)) != "c" {
		t.Fatalf("after truncateFrom(4): lastIndex=%d e3=%q, want 3/c", l.lastIndex(), l.entryAt(3))
	}
	l.truncateFrom(3) // empty above base
	if l.lastIndex() != 2 || l.lastTerm() != 1 {
		t.Fatalf("after truncateFrom(3): lastIndex=%d lastTerm=%d, want 2/1 (back to base)", l.lastIndex(), l.lastTerm())
	}
	if idx := l.append(3, []byte("z")); idx != 3 {
		t.Fatalf("append after truncate-to-base returned %d, want 3", idx)
	}
}

func TestRaftLogCompactToNoOps(t *testing.T) {
	l := buildLog(1, 1, 2)
	l.compactTo(0, 0) // <= base
	l.compactTo(9, 9) // > lastIndex
	if l.baseIndex != 0 || l.lastIndex() != 3 {
		t.Fatalf("no-op compactions mutated log: base=%d lastIndex=%d", l.baseIndex, l.lastIndex())
	}
	l.compactTo(1, l.term(1))
	l.compactTo(1, l.term(1)) // == base now: no-op
	if l.baseIndex != 1 || l.lastIndex() != 3 {
		t.Fatalf("base=%d lastIndex=%d, want 1/3", l.baseIndex, l.lastIndex())
	}
}

func TestRaftLogEntriesAfter(t *testing.T) {
	l := buildLog(1, 1, 2, 2)
	l.compactTo(1, l.term(1)) // base 1
	got := l.entriesAfter(2)  // entries with index > 2: 3@2, 4@2
	if len(got) != 2 || got[0].term != 2 || string(got[0].data) != "c" || string(got[1].data) != "d" {
		t.Fatalf("entriesAfter(2) = %+v, want [3@2 c, 4@2 d]", got)
	}
	if all := l.entriesAfter(l.baseIndex); len(all) != 3 {
		t.Fatalf("entriesAfter(baseIndex) = %d entries, want 3 (the whole suffix)", len(all))
	}
}
