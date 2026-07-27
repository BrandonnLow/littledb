package cluster

import (
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"
)

// TestAddServerJoinsAndVotes is the core of Stage 4: a fresh server joins a
// running cluster as a non-voting learner, catches up, is promoted to a voter, and
// then participates in an election. A 3-node cluster grows to {0,1,2,3}; the new
// node holds the pre-join data; and when the leader is partitioned, the surviving
// three elect a successor — which under a 4-member majority of 3 requires the new
// node's vote (a candidate plus one old follower is only two).
func TestAddServerJoinsAndVotes(t *testing.T) {
	const n = 3
	pt := newPartitionTransport()
	c, err := NewWithTransportConfig(n, dirs(t, n), testOpts(), pt, electionConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	for i := 0; i < 3; i++ {
		if err := c.Put([]byte(fmt.Sprintf("k%d", i)), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("seed put %d: %v", i, err)
		}
	}
	if err := c.Quiesce(2 * time.Second); err != nil {
		t.Fatal(err)
	}
	oldLeader := c.Leader()

	// Add a fourth server. It joins as a learner, catches up, and is promoted.
	if err := c.AddServer(3, t.TempDir()); err != nil {
		t.Fatalf("AddServer(3): %v", err)
	}
	if got := c.Config(); !reflect.DeepEqual(got, []NodeID{0, 1, 2, 3}) {
		t.Fatalf("config after add = %v, want [0 1 2 3]", got)
	}
	if c.Size() != 4 {
		t.Fatalf("size = %d, want 4", c.Size())
	}

	// The new node converged on the pre-join data.
	if err := c.Quiesce(2 * time.Second); err != nil {
		t.Fatal(err)
	}
	assertConverged(t, c, []string{"k0", "k1", "k2"})

	// Prove the new node is a full voter: partition the (old) leader; with a
	// 4-member config the survivors need a 3-vote majority, and only three nodes are
	// reachable, so the new node MUST vote for the election to succeed.
	if oldLeader == 3 {
		t.Fatalf("unexpected: the just-added node %d was the pre-partition leader", oldLeader)
	}
	pt.disconnect(oldLeader)
	waitFor(t, 4*time.Second, func() bool {
		ld, ok := c.currentLeader()
		return ok && ld.id != oldLeader
	})
	newLeader, _ := c.currentLeader()
	t.Logf("added node 3, partitioned leader %d; %d now leads (node 3's vote was required)", oldLeader, newLeader.id)

	// The cluster (three of four reachable) still commits.
	if err := c.Put([]byte("after"), []byte("added")); err != nil {
		t.Fatalf("post-add put: %v", err)
	}
	pt.reconnect(oldLeader)
	if err := c.Quiesce(3 * time.Second); err != nil {
		t.Fatal(err)
	}
	assertConverged(t, c, []string{"k0", "k1", "k2", "after"})
}

// TestAddServerToSingleNode grows a one-node cluster to two — the case where the
// learner phase matters most: a 2-member majority needs BOTH nodes, so promoting an
// uncaught-up node would stall every subsequent commit. The learner catches up to
// the existing data before promotion, so the new majority is immediately met.
func TestAddServerToSingleNode(t *testing.T) {
	c, err := NewWithTransportConfig(1, dirs(t, 1), testOpts(), NewChannelTransport(), stableConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	for i := 0; i < 3; i++ {
		if err := c.Put([]byte(fmt.Sprintf("k%d", i)), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("seed put %d: %v", i, err)
		}
	}

	if err := c.AddServer(1, t.TempDir()); err != nil {
		t.Fatalf("AddServer(1): %v", err)
	}
	if got := c.Config(); !reflect.DeepEqual(got, []NodeID{0, 1}) {
		t.Fatalf("config = %v, want [0 1]", got)
	}

	// A commit now requires both nodes; it still succeeds, and node 1 holds the
	// pre-join data.
	if err := c.Put([]byte("after"), []byte("two")); err != nil {
		t.Fatalf("post-add put on {0,1}: %v", err)
	}
	if err := c.Quiesce(2 * time.Second); err != nil {
		t.Fatal(err)
	}
	assertConverged(t, c, []string{"k0", "k1", "k2", "after"})
}

// TestAddServerRejectsNonSequentialID pins the dense-id precondition: AddServer's
// id must be the next slot, so nodes[i] stays the node with id i.
func TestAddServerRejectsNonSequentialID(t *testing.T) {
	c := newCluster(t, 3)
	defer c.Close()
	if err := c.AddServer(5, t.TempDir()); err == nil {
		t.Fatalf("AddServer(5) on a 3-node cluster should reject a non-sequential id")
	}
	if c.Size() != 3 {
		t.Fatalf("size = %d, want 3 (no node should have been added)", c.Size())
	}
}

// TestClusterLinearizableUnderServerAdd runs the randomized concurrent workload
// (linearizable reads through the read-index barrier) while a server is added
// underneath it — {0,1,2} -> {0,1,2,3} — and checks the recorded history is
// linearizable. Growing the voting set must be invisible to the register.
func TestClusterLinearizableUnderServerAdd(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping membership soak in -short mode")
	}
	const n = 3
	opts := testOpts()
	cfg := soakConfig()
	c, err := NewWithTransportConfig(n, dirs(t, n), opts, NewChannelTransport(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	var h *history
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		h = runWorkload(c, 6, 60, 5, 4242, true)
	}()

	time.Sleep(150 * time.Millisecond)
	if err := c.AddServer(3, t.TempDir()); err != nil {
		t.Errorf("AddServer(3): %v", err)
	}
	wg.Wait()

	if got := c.Config(); !reflect.DeepEqual(got, []NodeID{0, 1, 2, 3}) {
		t.Fatalf("config after add = %v, want [0 1 2 3]", got)
	}
	settleConvergeCheck(t, c, h, "server-add")
}
