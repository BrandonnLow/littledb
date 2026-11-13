package db

import "testing"

// TestPrepareNoopAppliesAndRecovers pins the no-op log entry used by the
// read-index and catch-up barriers: it must apply as a genuine no-op (advancing
// the applied index, changing no key), and — although it leaves no data behind —
// a flush must record its timestamp, so the commit-timestamp counter never moves
// backwards across a restart. Recovering only the data's max timestamp (the
// SSTable footers) would let a restarted leader hand out the no-op's timestamp
// again.
func TestPrepareNoopAppliesAndRecovers(t *testing.T) {
	dir := t.TempDir()
	opts := replicaOpts()

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
	if err := db.FlushApplied(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2, err := OpenWith(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	if ra := db2.RecoveredAppliedIndex(); ra != 2 {
		t.Fatalf("recovered applied index = %d, want 2", ra)
	}
	if got, err := db2.Get([]byte("k")); err != nil || string(got) != "v" {
		t.Fatalf("after restart Get(k) = (%q,%v), want v", got, err)
	}
	if next := db2.NextTimestampForTesting(); next <= noopTS {
		t.Fatalf("nextTimestamp after restart = %d, want > no-op ts %d (only the data's ts %d is in an SSTable)",
			next, noopTS, writeTS)
	}
}
