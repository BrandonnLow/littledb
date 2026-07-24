package db

import "testing"

// TestPrepareNoopAppliesAndRecovers pins the no-op log entry used by the
// read-index prerequisite: it must apply as a genuine no-op (advancing the
// applied index, changing no key) and its lone OpCommit must be counted by
// recovery, so the reconstructed applied index stays in lockstep with the Raft
// index across a restart. If recovery miscounted a no-op, a restarted node would
// re-drive a real entry — the duplicate-apply hazard the applied watermark exists
// to prevent.
func TestPrepareNoopAppliesAndRecovers(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.SyncOnWrite = false
	opts.DisableBackgroundCompaction = true

	db, err := OpenWith(dir, opts)
	if err != nil {
		t.Fatal(err)
	}

	// A committed write at Raft index 1, applied through the replicated path.
	tx := db.Begin()
	if err := tx.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	entry, writeTS, err := db.PrepareCommit(tx)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.ApplyEntry(1, entry); err != nil {
		t.Fatal(err)
	}

	// A no-op at index 2.
	noop, noopTS, err := db.PrepareNoop()
	if err != nil {
		t.Fatal(err)
	}
	if noopTS <= writeTS {
		t.Fatalf("no-op ts %d not greater than write ts %d", noopTS, writeTS)
	}
	if err := db.ApplyEntry(2, noop); err != nil {
		t.Fatalf("apply no-op: %v", err)
	}

	if ai := db.appliedIndex; ai != 2 {
		t.Fatalf("applied index after no-op = %d, want 2", ai)
	}
	if got, err := db.Get([]byte("k")); err != nil || string(got) != "v" {
		t.Fatalf("Get(k) after no-op = (%q,%v), want v (no-op must change no key)", got, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Recovery must count the no-op's OpCommit: applied index survives as 2.
	db2, err := OpenWith(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	if ra := db2.RecoveredAppliedIndex(); ra != 2 {
		t.Fatalf("recovered applied index = %d, want 2 (no-op OpCommit must be counted)", ra)
	}
	if got, err := db2.Get([]byte("k")); err != nil || string(got) != "v" {
		t.Fatalf("after restart Get(k) = (%q,%v), want v", got, err)
	}
}
