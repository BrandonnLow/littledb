package cluster

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/BrandonnLow/littledb/internal/db"
)

// TestLeaderRMWSnapshotConflict deterministically pins the snapshot-isolation
// guarantee for read-modify-write txns on the leader under deferred apply.
//
// We park transaction A inside its in-flight window — after PrepareCommit has
// allocated its commit timestamp (and bumped nextTimestamp) but before the
// entry is applied — by holding the followers' acks so the leader's commit
// blocks waiting for a quorum. While A is parked, transaction B takes its read
// snapshot and stages a conflicting write to the same key. We then release A
// (it applies and commits) and commit B.
//
// B must observe a conflict: A committed the key after B's snapshot. If B's
// snapshot were taken from nextTimestamp (which already counts A's unapplied
// commit) instead of the applied watermark, B's conflict check would compute
// A_ts > B_readSnap as false and silently overwrite A — a lost update. The
// assertion is that B.Commit returns ErrConflict and A's write survives.
func TestLeaderRMWSnapshotConflict(t *testing.T) {
	const n = 3
	gate := newGateTransport(0, MsgAck) // hold acks destined for the leader
	gate.setHolding(false)              // ...but not during setup

	c, err := NewWithTransport(n, dirs(t, n), testOpts(), gate)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if err := c.Put([]byte("k"), []byte("0")); err != nil {
		t.Fatal(err)
	}
	if err := c.Quiesce(time.Second); err != nil {
		t.Fatal(err)
	}

	// From here, the leader's next commit will park waiting for acks.
	gate.setHolding(true)

	// A: a commit that gets stuck post-allocate, pre-apply.
	var aErr error
	var aWg sync.WaitGroup
	aWg.Add(1)
	go func() {
		defer aWg.Done()
		tx := c.Begin()
		if _, err := tx.Get([]byte("k")); err != nil {
			tx.Rollback()
			aErr = err
			return
		}
		tx.Put([]byte("k"), []byte("A"))
		aErr = tx.Commit() // blocks until the gate is released
	}()

	// Wait until A is parked: its followers have acked (and those acks are
	// being held), so A has allocated its timestamp and is waiting for quorum.
	waitFor(t, time.Second, func() bool { return gate.heldCount() >= 1 })

	// B: take a snapshot and stage a read-modify-write while A is in flight.
	txB := c.Begin()
	curB, err := txB.Get([]byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if string(curB) != "0" {
		t.Fatalf("B read %q, want \"0\" (A's write must not be visible yet)", curB)
	}
	txB.Put([]byte("k"), append(append([]byte(nil), curB...), 'B')) // "0B"

	// Release A: it now reaches quorum, applies "A", and commits.
	gate.release()
	aWg.Wait()
	if aErr != nil {
		t.Fatalf("A commit: %v", aErr)
	}

	// B must observe the conflict; committing "0B" here would lose A's write.
	if err := txB.Commit(); !errors.Is(err, db.ErrConflict) {
		t.Fatalf("B.Commit = %v, want ErrConflict (A committed k during B's snapshot; nil = lost update)", err)
	}

	if got, err := c.Get([]byte("k")); err != nil || string(got) != "A" {
		t.Errorf("k = (%q,%v), want \"A\" (A's write must survive)", got, err)
	}
}
