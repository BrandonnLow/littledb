package cluster

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/BrandonnLow/littledb/internal/db"
)

// retryMembership drives a membership operation to success under churn: leadership
// changes and in-flight config changes make these ops transiently fail, and all the
// membership operations are idempotent (re-removing an absent member, transferring
// to the current leader, promoting an already-caught-up learner all converge), so
// retrying until success — or the deadline — is safe.
func retryMembership(t *testing.T, timeout time.Duration, op func() error) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		err := op()
		if err == nil {
			return
		}
		switch {
		case errors.Is(err, ErrNotLeader), errors.Is(err, ErrMaybeCommitted),
			errors.Is(err, ErrConfigChangeInProgress), errors.Is(err, ErrTransferTimeout),
			errors.Is(err, ErrCatchupTimeout):
			// transient under churn; retry
		default:
			t.Fatalf("membership op failed non-transiently: %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("membership op did not succeed before timeout (last err %v)", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestMembershipUnderFaults is the Stage 6 capstone: membership changes run
// concurrently with the randomized workload AND partition fault injection, with
// compaction on so a follower that falls behind during the churn is caught up by an
// InstallSnapshot that must carry the new configuration. After healing, every node
// converges to identical committed state and the recorded history is linearizable —
// a configuration change, a leadership transfer, and a snapshot install are all
// invisible to the register.
//
// maxDown is 1 (conservative): the cluster shrinks from five voters to four during
// the run, and a four-voter cluster still tolerates one partition, so the workload
// stays available throughout rather than stalling on a lost quorum.
func TestMembershipUnderFaults(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping membership soak in -short mode")
	}
	const n = 5
	opts := db.Options{SyncOnWrite: true, DisableBackgroundCompaction: true}
	cfg := soakConfig()
	cfg.SnapshotThreshold = 8 // exercise compaction + InstallSnapshot under churn
	pt := newPartitionTransport()
	c, err := NewWithTransportConfig(n, dirs(t, n), opts, pt, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	stop := make(chan struct{})
	done := make(chan struct{})
	fi := &faultInjector{t: t, c: c, pt: pt, opts: opts, cfg: cfg, maxDown: 1}
	go fi.run(23, stop, done)

	// Membership churn concurrent with the workload+faults: shrink {0..4} -> {0,1,2,3}
	// and hand off leadership.
	var mwg sync.WaitGroup
	mwg.Add(1)
	go func() {
		defer mwg.Done()
		time.Sleep(200 * time.Millisecond)
		retryMembership(t, 15*time.Second, func() error { return c.RemoveServer(4) })
		time.Sleep(200 * time.Millisecond)
		retryMembership(t, 15*time.Second, func() error { return c.TransferLeadership(1) })
	}()

	h := runWorkload(c, 6, 50, 5, 77, true)
	mwg.Wait()

	close(stop)
	<-done // faults healed

	// Config settled to {0,1,2,3} on whatever leader now stands.
	if !within(10*time.Second, func() bool {
		got := c.Config()
		return len(got) == 4
	}) {
		t.Fatalf("config did not settle to 4 members: %v\n%s", c.Config(), dumpClusterState(c))
	}
	settleConvergeCheck(t, c, h, "membership+partitions")
}
