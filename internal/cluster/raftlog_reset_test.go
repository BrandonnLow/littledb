package cluster

import (
	"path/filepath"
	"testing"
)

func TestRaftLogResetToBaseDiscardsAll(t *testing.T) {
	l := NewRaftLog()
	l.append(1, []byte("a"))
	l.append(1, []byte("b"))
	l.append(2, []byte("c"))

	l.resetToBase(50, 7)

	if l.baseIndex != 50 || l.baseTerm != 7 {
		t.Fatalf("base = (%d,%d), want (50,7)", l.baseIndex, l.baseTerm)
	}
	if l.lastIndex() != 50 {
		t.Fatalf("lastIndex = %d, want 50 (empty above base)", l.lastIndex())
	}
	if len(l.entries) != 0 {
		t.Fatalf("entries len = %d, want 0", len(l.entries))
	}
	if l.term(50) != 7 {
		t.Fatalf("term(50) = %d, want 7", l.term(50))
	}
	// A leader replicating from the new base: prevLogIndex == baseIndex matches
	// iff prevLogTerm == baseTerm.
	if !l.matchesPrev(50, 7) {
		t.Fatalf("matchesPrev(50,7) = false, want true")
	}
	if l.matchesPrev(50, 6) {
		t.Fatalf("matchesPrev(50,6) = true, want false")
	}
	// Appends resume at base+1.
	if idx := l.append(7, []byte("d")); idx != 51 {
		t.Fatalf("append after reset returned %d, want 51", idx)
	}
}

func TestRaftLogFileResetToBaseSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, raftLogFileName)

	lf, _, err := openRaftLogFile(path, true)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := lf.append(1, []byte("x")); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := lf.append(1, []byte("y")); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := lf.resetToBase(100, 9); err != nil {
		t.Fatalf("resetToBase: %v", err)
	}
	if lf.baseIndex != 100 || lf.baseTerm != 9 {
		t.Fatalf("in-memory base = (%d,%d), want (100,9)", lf.baseIndex, lf.baseTerm)
	}
	// Append on top of the new base persists at index 101.
	if err := lf.append(9, []byte("z")); err != nil {
		t.Fatalf("append after reset: %v", err)
	}
	if err := lf.close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	lf2, entries, err := openRaftLogFile(path, true)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer lf2.close()
	if lf2.baseIndex != 100 || lf2.baseTerm != 9 {
		t.Fatalf("reopened base = (%d,%d), want (100,9)", lf2.baseIndex, lf2.baseTerm)
	}
	if len(entries) != 1 || string(entries[0].data) != "z" || entries[0].term != 9 {
		t.Fatalf("reopened entries = %+v, want one {9,z}", entries)
	}
}

func TestResetRaftLogFileToHelper(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, raftLogFileName)

	lf, _, err := openRaftLogFile(path, true)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	lf.append(3, []byte("old1"))
	lf.append(3, []byte("old2"))
	lf.close()

	// Completion-style reset without a live handle.
	if err := resetRaftLogFileTo(path, 200, 12, true); err != nil {
		t.Fatalf("resetRaftLogFileTo: %v", err)
	}
	// Idempotent: running it again is harmless.
	if err := resetRaftLogFileTo(path, 200, 12, true); err != nil {
		t.Fatalf("resetRaftLogFileTo (rerun): %v", err)
	}

	lf2, entries, err := openRaftLogFile(path, true)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer lf2.close()
	if lf2.baseIndex != 200 || lf2.baseTerm != 12 {
		t.Fatalf("base = (%d,%d), want (200,12)", lf2.baseIndex, lf2.baseTerm)
	}
	if len(entries) != 0 {
		t.Fatalf("entries len = %d, want 0", len(entries))
	}
}
