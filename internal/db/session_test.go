package db

import "testing"

// applySessioned prepares and applies a sessioned write at Raft index idx.
func applySessioned(t *testing.T, db *DB, sess []byte, idx uint64, key, val string, seq uint64) {
	t.Helper()
	tx := db.Begin()
	tx.SetSession(sess, seq)
	if err := tx.Put([]byte(key), []byte(val)); err != nil {
		t.Fatal(err)
	}
	entry, _, err := db.PrepareCommit(tx)
	if err != nil {
		t.Fatalf("prepare seq %d: %v", seq, err)
	}
	if entry == nil {
		t.Fatalf("seq %d unexpectedly deduped at prepare", seq)
	}
	if err := db.ApplyEntry(idx, entry); err != nil {
		t.Fatalf("apply seq %d: %v", seq, err)
	}
}

// TestSessionDurableAcrossFlush pins Stage 2: the dedup table survives a WAL-
// truncating flush. A sequence applied before the flush (whose OpCommit the flush
// discards from the WAL) must still be recognized as a duplicate after a restart,
// which only holds if it was persisted to the sessions file.
func TestSessionDurableAcrossFlush(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.SyncOnWrite = false
	opts.DisableBackgroundCompaction = true

	db, err := OpenWith(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	sess := []byte("c")

	applySessioned(t, db, sess, 1, "k1", "v1", 1)
	applySessioned(t, db, sess, 2, "k2", "v2", 2)
	if err := db.FlushForTesting(); err != nil { // pre-flush sessions -> sessions file
		t.Fatal(err)
	}
	applySessioned(t, db, sess, 3, "k3", "v3", 3) // post-flush, in the new WAL
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2, err := OpenWith(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	// File (seq 2) + WAL replay (seq 3) reconstruct the full table.
	if got := db2.SessionLastSeq(sess); got != 3 {
		t.Fatalf("after flush+restart lastSeq = %d, want 3", got)
	}
	// The pre-flush seq 2 is still deduped — the durability guarantee.
	tx := db2.Begin()
	tx.SetSession(sess, 2)
	if err := tx.Put([]byte("k2"), []byte("stale-retry")); err != nil {
		t.Fatal(err)
	}
	if e, _, err := db2.PrepareCommit(tx); err != nil || e != nil {
		t.Fatalf("retry of pre-flush seq 2: entry=%v err=%v, want deduped (nil entry)", e, err)
	}
	if got, _ := db2.Get([]byte("k2")); string(got) != "v2" {
		t.Fatalf("k2 = %q after deduped retry, want v2", got)
	}
}

// TestSessionDedup pins exactly-once dedup at the DB layer: a retried commit with
// the same (session, seq) is recognized both at prepare time (leader short-circuit
// → nil entry) and at apply time (duplicate entry → no-op that still advances the
// applied index), and the session table survives a restart.
func TestSessionDedup(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.SyncOnWrite = false
	opts.DisableBackgroundCompaction = true

	db, err := OpenWith(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	sess := []byte("client-A")

	// commit prepares and applies a sessioned write at Raft index idx, returning
	// whether it was actually proposed (non-nil entry).
	commit := func(idx uint64, key, val string, seq uint64) bool {
		tx := db.Begin()
		tx.SetSession(sess, seq)
		if err := tx.Put([]byte(key), []byte(val)); err != nil {
			t.Fatal(err)
		}
		entry, _, err := db.PrepareCommit(tx)
		if err != nil {
			t.Fatalf("prepare seq %d: %v", seq, err)
		}
		if entry == nil {
			return false
		}
		if err := db.ApplyEntry(idx, entry); err != nil {
			t.Fatalf("apply seq %d: %v", seq, err)
		}
		return true
	}

	// seq 1 commits.
	if !commit(1, "k", "v1", 1) {
		t.Fatal("seq 1 should be proposed, not deduped")
	}
	if got := db.SessionLastSeq(sess); got != 1 {
		t.Fatalf("lastSeq = %d, want 1", got)
	}

	// A retry of seq 1 is deduped at prepare (leader short-circuit): no entry, and
	// the stored value is untouched (the retry's "vX" never applies).
	if commit(99, "k", "vX", 1) {
		t.Fatal("retry of seq 1 should be deduped at prepare (nil entry)")
	}
	if got, _ := db.Get([]byte("k")); string(got) != "v1" {
		t.Fatalf("k = %q after deduped retry, want v1", got)
	}

	// seq 2 commits; then re-apply its exact entry (as if the leader re-proposed it
	// before applying the original) — apply-time dedup must make it a no-op that
	// still advances the applied index, so the accounting stays in lockstep.
	tx := db.Begin()
	tx.SetSession(sess, 2)
	if err := tx.Put([]byte("k"), []byte("v2")); err != nil {
		t.Fatal(err)
	}
	entry2, _, err := db.PrepareCommit(tx)
	if err != nil || entry2 == nil {
		t.Fatalf("prepare seq 2: entry=%v err=%v", entry2, err)
	}
	if err := db.ApplyEntry(2, entry2); err != nil {
		t.Fatal(err)
	}
	if err := db.ApplyEntry(3, entry2); err != nil { // duplicate re-apply
		t.Fatal(err)
	}
	if got := db.SessionLastSeq(sess); got != 2 {
		t.Fatalf("lastSeq = %d, want 2", got)
	}
	if db.appliedIndex != 3 {
		t.Fatalf("appliedIndex = %d, want 3 (a duplicate still advances the index)", db.appliedIndex)
	}
	if got, _ := db.Get([]byte("k")); string(got) != "v2" {
		t.Fatalf("k = %q, want v2", got)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// The session table is reconstructed from the WAL on restart.
	db2, err := OpenWith(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	if got := db2.SessionLastSeq(sess); got != 2 {
		t.Fatalf("after restart lastSeq = %d, want 2", got)
	}
	if db2.RecoveredAppliedIndex() != 3 {
		t.Fatalf("after restart recovered applied index = %d, want 3", db2.RecoveredAppliedIndex())
	}
	if got, _ := db2.Get([]byte("k")); string(got) != "v2" {
		t.Fatalf("after restart k = %q, want v2", got)
	}
}
