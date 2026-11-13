package db

import "testing"

// applySessioned prepares and applies a sessioned write at Raft index idx,
// returning the entry so a test can replay it as the Raft log would.
func applySessioned(t *testing.T, db *DB, sess []byte, idx uint64, key, val string, seq uint64) []byte {
	t.Helper()
	entry := prepareEntry(t, db, sess, seq, map[string]string{key: val})
	if err := db.ApplyEntry(idx, entry); err != nil {
		t.Fatalf("apply seq %d: %v", seq, err)
	}
	return entry
}

// TestSessionDurableAcrossFlush pins the dedup table's durability: a flush makes
// its session state durable in the sessions file, so once the caller's log is
// compacted past the flush, a sequence applied before it is still recognized as
// a duplicate after a restart. Sequences applied after the flush come back from
// the caller replaying its log.
func TestSessionDurableAcrossFlush(t *testing.T) {
	dir := t.TempDir()
	opts := replicaOpts()

	db, err := OpenWith(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	sess := []byte("c")

	applySessioned(t, db, sess, 1, "k1", "v1", 1)
	applySessioned(t, db, sess, 2, "k2", "v2", 2)
	if err := db.FlushApplied(); err != nil { // sessions through index 2 -> sessions file
		t.Fatal(err)
	}
	entry3 := applySessioned(t, db, sess, 3, "k3", "v3", 3) // post-flush: only in the log
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2, err := OpenWith(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	// The store is back exactly as of the flush: sessions file through seq 2.
	if got := db2.RecoveredAppliedIndex(); got != 2 {
		t.Fatalf("RecoveredAppliedIndex = %d, want 2", got)
	}
	if got := db2.SessionLastSeq(sess); got != 2 {
		t.Fatalf("after restart lastSeq = %d, want 2 (from the sessions file)", got)
	}
	// The caller replays its log above the durable index.
	if err := db2.ApplyEntry(3, entry3); err != nil {
		t.Fatal(err)
	}
	if got := db2.SessionLastSeq(sess); got != 3 {
		t.Fatalf("after replay lastSeq = %d, want 3", got)
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
// applied index). Replaying the same log after a restart reproduces the same
// table and data, duplicate included — apply is deterministic.
func TestSessionDedup(t *testing.T) {
	dir := t.TempDir()
	opts := replicaOpts()

	db, err := OpenWith(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	sess := []byte("client-A")

	// commit prepares a sessioned write and, unless it is deduped at prepare,
	// applies it at Raft index idx, returning the entry (nil if deduped).
	commit := func(idx uint64, key, val string, seq uint64) []byte {
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
			return nil
		}
		if err := db.ApplyEntry(idx, entry); err != nil {
			t.Fatalf("apply seq %d: %v", seq, err)
		}
		return entry
	}

	// seq 1 commits.
	entry1 := commit(1, "k", "v1", 1)
	if entry1 == nil {
		t.Fatal("seq 1 should be proposed, not deduped")
	}
	if got := db.SessionLastSeq(sess); got != 1 {
		t.Fatalf("lastSeq = %d, want 1", got)
	}

	// A retry of seq 1 is deduped at prepare (leader short-circuit): no entry, and
	// the stored value is untouched (the retry's "vX" never applies).
	if commit(99, "k", "vX", 1) != nil {
		t.Fatal("retry of seq 1 should be deduped at prepare (nil entry)")
	}
	if got, _ := db.Get([]byte("k")); string(got) != "v1" {
		t.Fatalf("k = %q after deduped retry, want v1", got)
	}

	// seq 2 commits; then re-apply its exact entry (as if the leader re-proposed it
	// before applying the original) — apply-time dedup must make it a no-op that
	// still advances the applied index, keeping it in lockstep with the log.
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

	// Nothing was flushed, so the store comes back empty and the caller's replay
	// of the same three log entries must rebuild the same state.
	db2, err := OpenWith(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	if got := db2.RecoveredAppliedIndex(); got != 0 {
		t.Fatalf("after restart recovered applied index = %d, want 0 (no flush)", got)
	}
	for i, e := range [][]byte{entry1, entry2, entry2} {
		if err := db2.ApplyEntry(uint64(i+1), e); err != nil {
			t.Fatalf("replay %d: %v", i+1, err)
		}
	}
	if got := db2.SessionLastSeq(sess); got != 2 {
		t.Fatalf("after replay lastSeq = %d, want 2", got)
	}
	if db2.appliedIndex != 3 {
		t.Fatalf("after replay appliedIndex = %d, want 3", db2.appliedIndex)
	}
	if got, _ := db2.Get([]byte("k")); string(got) != "v2" {
		t.Fatalf("after replay k = %q, want v2", got)
	}
}
