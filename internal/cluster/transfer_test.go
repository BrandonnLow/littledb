package cluster

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

// TestTransferLeadership pins Stage 5: the leader hands off to a chosen voting
// member, which takes over immediately — bypassing its randomized election timer
// and (via LeaderTransfer) the other voters' disruption-prevention rule — while the
// old leader steps down. Writes then flow through the new leader and all nodes
// converge.
func TestTransferLeadership(t *testing.T) {
	const n = 3
	c, err := NewWithTransportConfig(n, dirs(t, n), testOpts(), NewChannelTransport(), electionConfig())
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
	if c.Leader() != 0 {
		t.Fatalf("expected node 0 to lead initially, got %d", c.Leader())
	}

	// Hand leadership to node 2.
	start := time.Now()
	if err := c.TransferLeadership(2); err != nil {
		t.Fatalf("TransferLeadership(2): %v", err)
	}
	waitFor(t, 2*time.Second, func() bool { return c.Leader() == 2 })
	t.Logf("leadership transferred 0 -> 2 in %v", time.Since(start))

	if c.Node(0).roleValue() == Leader {
		t.Errorf("old leader (node 0) still reports Leader after transfer")
	}

	// Writes flow through the new leader; everyone converges.
	if err := c.Put([]byte("after"), []byte("transferred")); err != nil {
		t.Fatalf("post-transfer put: %v", err)
	}
	if err := c.Quiesce(2 * time.Second); err != nil {
		t.Fatal(err)
	}
	assertConverged(t, c, []string{"k0", "k1", "k2", "after"})
}

// TestTransferLeadershipToNonVoter rejects a transfer to a node that is not a
// voting member (it could never win the election).
func TestTransferLeadershipToNonVoter(t *testing.T) {
	c := newCluster(t, 3)
	defer c.Close()
	if err := c.TransferLeadership(5); !errors.Is(err, ErrTransferTarget) {
		t.Fatalf("transfer to a non-member = %v, want ErrTransferTarget", err)
	}
	// Leadership is unchanged.
	if c.Leader() != 0 {
		t.Fatalf("leader changed after a rejected transfer: %d", c.Leader())
	}
}

// TestTransferLeadershipToSelf is a no-op: transferring to the current leader
// leaves it in charge.
func TestTransferLeadershipToSelf(t *testing.T) {
	c := newCluster(t, 3)
	defer c.Close()
	if err := c.TransferLeadership(c.Leader()); err != nil {
		t.Fatalf("transfer to self = %v, want nil", err)
	}
	if c.Leader() != 0 {
		t.Fatalf("leader changed after a self-transfer: %d", c.Leader())
	}
}
