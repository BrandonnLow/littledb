package cluster

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func reopenRLF(t *testing.T, path string) (*raftLogFile, []persistedEntry) {
	t.Helper()
	lf, entries, err := openRaftLogFile(path, true)
	if err != nil {
		t.Fatalf("reopen raft log: %v", err)
	}
	return lf, entries
}

func TestRaftLogFileHeaderRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), raftLogFileName)
	lf, entries, err := openRaftLogFile(path, true)
	if err != nil {
		t.Fatal(err)
	}
	if lf.baseIndex != 0 || lf.baseTerm != 0 || len(entries) != 0 {
		t.Fatalf("fresh: base=(%d,%d) entries=%d, want (0,0)/0", lf.baseIndex, lf.baseTerm, len(entries))
	}
	for i := 0; i < 4; i++ {
		if err := lf.append(uint64(i+1), []byte{byte('a' + i)}); err != nil {
			t.Fatal(err)
		}
	}
	lf.close()

	lf2, entries2 := reopenRLF(t, path)
	defer lf2.close()
	if lf2.baseIndex != 0 || len(entries2) != 4 {
		t.Fatalf("reopen: base=%d entries=%d, want 0/4", lf2.baseIndex, len(entries2))
	}
	for i, pe := range entries2 {
		if pe.term != uint64(i+1) || pe.data[0] != byte('a'+i) {
			t.Errorf("entry %d = (t%d,%q)", i+1, pe.term, pe.data)
		}
	}
}

func TestRaftLogFileCompactAndReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), raftLogFileName)
	lf, _, err := openRaftLogFile(path, true)
	if err != nil {
		t.Fatal(err)
	}
	for i, tm := range []uint64{1, 1, 2, 2} { // a@1,b@1,c@2,d@2
		if err := lf.append(tm, []byte{byte('a' + i)}); err != nil {
			t.Fatal(err)
		}
	}
	// Compact to base 2 @ term 1; survivors are indices 3,4.
	if err := lf.compact(2, 1, []persistedEntry{{2, []byte("c")}, {2, []byte("d")}}); err != nil {
		t.Fatal(err)
	}
	if lf.baseIndex != 2 || lf.baseTerm != 1 {
		t.Fatalf("after compact base=(%d,%d), want (2,1)", lf.baseIndex, lf.baseTerm)
	}
	if err := lf.append(2, []byte("e")); err != nil { // index 5
		t.Fatal(err)
	}
	lf.close()

	lf2, entries := reopenRLF(t, path)
	defer lf2.close()
	if lf2.baseIndex != 2 || lf2.baseTerm != 1 {
		t.Fatalf("reopened base=(%d,%d), want (2,1)", lf2.baseIndex, lf2.baseTerm)
	}
	want := []string{"c", "d", "e"}
	if len(entries) != len(want) {
		t.Fatalf("got %d entries, want %d", len(entries), len(want))
	}
	for i, pe := range entries {
		if string(pe.data) != want[i] {
			t.Errorf("entry %d = %q, want %q", i, pe.data, want[i])
		}
	}
	// truncateFrom on a reopened compacted file maps index->offset across the
	// base: index 5 (e) is the 3rd surviving entry.
	if err := lf2.truncateFrom(5); err != nil {
		t.Fatal(err)
	}
	lf2.close()
	lf3, entries3 := reopenRLF(t, path)
	defer lf3.close()
	if len(entries3) != 2 || string(entries3[1].data) != "d" {
		t.Fatalf("after truncateFrom(5): %d entries, want 2 ending in d", len(entries3))
	}
}

func TestRaftLogFileRejectsPreSnapshotFormat(t *testing.T) {
	path := filepath.Join(t.TempDir(), raftLogFileName)
	// A pre-snapshot file: a raw frame {term=1, len=1, 'x'} with no base header.
	var hdr [raftEntryHeaderSize]byte
	binary.LittleEndian.PutUint64(hdr[0:], 1)
	binary.LittleEndian.PutUint32(hdr[8:], 1)
	if err := os.WriteFile(path, append(hdr[:], 'x'), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := openRaftLogFile(path, true); !errors.Is(err, ErrBadRaftLog) {
		t.Fatalf("open pre-snapshot file = %v, want ErrBadRaftLog (loud reject)", err)
	}
}

func TestRaftLogFileTornFrameAfterCompactedBase(t *testing.T) {
	path := filepath.Join(t.TempDir(), raftLogFileName)
	lf, _, err := openRaftLogFile(path, true)
	if err != nil {
		t.Fatal(err)
	}
	for i, tm := range []uint64{1, 1, 2, 2} {
		if err := lf.append(tm, []byte{byte('a' + i)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := lf.compact(2, 1, []persistedEntry{{2, []byte("c")}, {2, []byte("d")}}); err != nil {
		t.Fatal(err)
	}
	if err := lf.append(3, []byte("eeee")); err != nil { // index 5
		t.Fatal(err)
	}
	lf.close()

	// Chop the last 2 bytes (torn final-frame body) — simulates a crash mid-append.
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path, fi.Size()-2); err != nil {
		t.Fatal(err)
	}

	lf2, entries := reopenRLF(t, path)
	defer lf2.close()
	if lf2.baseIndex != 2 || lf2.baseTerm != 1 {
		t.Fatalf("base=(%d,%d), want (2,1) preserved across the torn tail", lf2.baseIndex, lf2.baseTerm)
	}
	if len(entries) != 2 || string(entries[0].data) != "c" || string(entries[1].data) != "d" {
		t.Fatalf("entries=%v, want [c d] (torn frame 5 dropped, survivors intact)", entries)
	}
}

func TestRaftLogFileTornHeaderReinit(t *testing.T) {
	path := filepath.Join(t.TempDir(), raftLogFileName)
	// Only the magic written (8 bytes): a torn initial header (< 24 bytes). No
	// frame can exist yet, so load reinitializes at base 0 rather than failing.
	var b [8]byte
	binary.LittleEndian.PutUint64(b[0:], raftLogMagic)
	if err := os.WriteFile(path, b[:], 0o644); err != nil {
		t.Fatal(err)
	}
	lf, entries, err := openRaftLogFile(path, true)
	if err != nil {
		t.Fatalf("torn header should reinitialize, got %v", err)
	}
	if lf.baseIndex != 0 || lf.baseTerm != 0 || len(entries) != 0 {
		t.Fatalf("reinit: base=(%d,%d) entries=%d, want (0,0)/0", lf.baseIndex, lf.baseTerm, len(entries))
	}
	if err := lf.append(1, []byte("a")); err != nil {
		t.Fatal(err)
	}
	lf.close()
	lf2, e2 := reopenRLF(t, path)
	defer lf2.close()
	if len(e2) != 1 || string(e2[0].data) != "a" {
		t.Fatalf("after reinit+append: %d entries, want 1", len(e2))
	}
}
