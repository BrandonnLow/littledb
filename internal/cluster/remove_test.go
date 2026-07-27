package cluster

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"
)

// TestRemoveServerShrinksQuorum is the core of Stage 3: removing voting members
// shrinks the quorum, so a cluster that would have halted under the old
// configuration keeps committing under the new one. A 5-node cluster is shrunk to
// {0,1,2} one server at a time; then only {0,1} is left reachable — a majority of
// {0,1,2} but NOT of the original five — and the cluster still commits.
func TestRemoveServerShrinksQuorum(t *testing.T) {
	const n = 5
	pt := newPartitionTransport()
	c, err := NewWithTransportConfig(n, dirs(t, n), testOpts(), pt, stableConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// Seed some state under the full 5-node configuration.
	for i := 0; i < 5; i++ {
		if err := c.Put([]byte(fmt.Sprintf("k%d", i)), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("seed put %d: %v", i, err)
		}
	}
	if err := c.Quiesce(2 * time.Second); err != nil {
		t.Fatal(err)
	}

	// Remove two followers, one at a time: {0,1,2,3,4} -> {0,1,2}. Each change
	// commits under its new majority before the next begins.
	if err := c.RemoveServer(4); err != nil {
		t.Fatalf("RemoveServer(4): %v", err)
	}
	if err := c.RemoveServer(3); err != nil {
		t.Fatalf("RemoveServer(3): %v", err)
	}
	if got := c.Config(); !reflect.DeepEqual(got, []NodeID{0, 1, 2}) {
		t.Fatalf("config after removals = %v, want [0 1 2]", got)
	}

	// Idempotent: re-removing an already-absent member is a no-op success.
	if err := c.RemoveServer(4); err != nil {
		t.Fatalf("re-RemoveServer(4) should be a no-op, got %v", err)
	}

	// The payoff of the shrink: disconnect node 2 (a voter) and the already-removed
	// 3 and 4. Only {0,1} is reachable — a majority of {0,1,2} but not of the
	// original five, so the write must still commit.
	pt.disconnect(2)
	pt.disconnect(3)
	pt.disconnect(4)
	if err := c.Put([]byte("after"), []byte("shrunk")); err != nil {
		t.Fatalf("post-shrink put with only {0,1} reachable: %v", err)
	}
	// The leader has committed+applied it immediately (the write reached a majority
	// of {0,1,2}); the surviving follower applies a moment later, once the advanced
	// commit index rides out on the next heartbeat.
	if got, err := c.Node(0).DB().Get([]byte("after")); err != nil || string(got) != "shrunk" {
		t.Errorf("leader node 0: after=(%q,%v), want shrunk", got, err)
	}
	waitFor(t, 2*time.Second, func() bool {
		v, err := c.Node(1).DB().Get([]byte("after"))
		return err == nil && string(v) == "shrunk"
	})

	// Heal and confirm every node (voters and the still-running removed nodes)
	// converges on every key.
	pt.reconnect(2)
	pt.reconnect(3)
	pt.reconnect(4)
	if err := c.Quiesce(3 * time.Second); err != nil {
		t.Fatal(err)
	}
	assertConverged(t, c, []string{"k0", "k1", "k2", "k3", "k4", "after"})
}

// TestClusterLinearizableUnderMembershipChange runs the randomized concurrent
// workload (linearizable reads through the read-index barrier) while two servers
// are removed underneath it — {0,1,2,3,4} -> {0,1,2} — and checks the recorded
// history is linearizable. A configuration change must be invisible to the
// register semantics, so shrinking the voting set mid-flight must not admit any
// non-linearizable outcome. (Membership changes concurrent with FAULT INJECTION
// are the heavier Stage 6 soak; this isolates the membership dimension.)
func TestClusterLinearizableUnderMembershipChange(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping membership soak in -short mode")
	}
	const n = 5
	opts := testOpts()
	cfg := soakConfig()
	c, err := NewWithTransportConfig(n, dirs(t, n), opts, NewChannelTransport(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// The bootstrap leader (node 0) stays leader (no faults, soak timers), driving
	// both client commits and the config changes through one serialized commit path.
	var h *history
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		h = runWorkload(c, 6, 60, 5, 2025, true)
	}()

	// Let the workload get going, then remove two followers while it runs. Each
	// removal commits before the next begins (the one-change-at-a-time gate), so
	// neither returns ErrConfigChangeInProgress.
	time.Sleep(150 * time.Millisecond)
	if err := c.RemoveServer(4); err != nil {
		t.Errorf("RemoveServer(4): %v", err)
	}
	if err := c.RemoveServer(3); err != nil {
		t.Errorf("RemoveServer(3): %v", err)
	}
	wg.Wait()

	if got := c.Config(); !reflect.DeepEqual(got, []NodeID{0, 1, 2}) {
		t.Fatalf("config after removals = %v, want [0 1 2]", got)
	}
	settleConvergeCheck(t, c, h, "membership-change")
}

// TestRemoveLeaderStepsDown pins the self-removal path (dissertation §4.2.2): the
// leader removes itself, commits C_new under the NEW majority (which excludes it),
// then relinquishes leadership so the remaining members elect a successor. The
// removed ex-leader never leads again (it is not in its own configuration).
func TestRemoveLeaderStepsDown(t *testing.T) {
	const n = 3
	pt := newPartitionTransport()
	c, err := NewWithTransportConfig(n, dirs(t, n), testOpts(), pt, electionConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if err := c.Put([]byte("x"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := c.Quiesce(2 * time.Second); err != nil {
		t.Fatal(err)
	}
	if c.Leader() != 0 {
		t.Fatalf("expected node 0 to lead initially, got %d", c.Leader())
	}

	// The leader removes itself: {0,1,2} -> {1,2}.
	if err := c.RemoveServer(0); err != nil {
		t.Fatalf("RemoveServer(0): %v", err)
	}

	// Node 0 stops leading; a leader emerges from within {1,2}.
	waitFor(t, 4*time.Second, func() bool {
		if c.Node(0).roleValue() == Leader {
			return false
		}
		ld, ok := c.currentLeader()
		return ok && (ld.id == 1 || ld.id == 2)
	})
	// Configuration settled to {1,2}.
	waitFor(t, 2*time.Second, func() bool {
		return reflect.DeepEqual(c.Config(), []NodeID{1, 2})
	})

	// The two-node cluster still commits, and node 0 (a non-voter still receiving
	// heartbeats) converges too.
	if err := c.Put([]byte("y"), []byte("2")); err != nil {
		t.Fatalf("post-removal put on {1,2}: %v", err)
	}
	if err := c.Quiesce(3 * time.Second); err != nil {
		t.Fatal(err)
	}
	assertConverged(t, c, []string{"x", "y"})

	if c.Node(0).roleValue() == Leader {
		t.Errorf("removed node 0 became leader again")
	}
}

// TestRemoveCannotRemoveLastMember rejects a removal that would empty the
// configuration, leaving no one able to form a quorum.
func TestRemoveCannotRemoveLastMember(t *testing.T) {
	c := newCluster(t, 1)
	defer c.Close()
	if err := c.RemoveServer(0); !errors.Is(err, ErrCannotRemove) {
		t.Fatalf("RemoveServer on a single-node cluster = %v, want ErrCannotRemove", err)
	}
}

// TestLatestConfigEntryCommittedGuard pins the one-change-at-a-time gate: a new
// membership change may start only once the newest config entry in the log has
// committed. A log with no config entry (only the base config) reads as ready.
func TestLatestConfigEntryCommittedGuard(t *testing.T) {
	l := NewRaftLog()
	l.append(1, EntryData, []byte("d"))                                       // 1
	l.append(1, EntryConfig, encodeConfig(map[NodeID]bool{0: true, 1: true})) // 2
	n := &Node{log: l, commitIndex: 1}
	if n.latestConfigEntryCommittedLocked() {
		t.Errorf("config entry at index 2 with commitIndex 1 must read as uncommitted")
	}
	n.commitIndex = 2
	if !n.latestConfigEntryCommittedLocked() {
		t.Errorf("config entry at index 2 with commitIndex 2 must read as committed")
	}

	l2 := NewRaftLog()
	l2.append(1, EntryData, []byte("d"))
	n2 := &Node{log: l2, commitIndex: 0}
	if !n2.latestConfigEntryCommittedLocked() {
		t.Errorf("a log with no config entry must read as committed (base config)")
	}
}

// makeStickyVoter builds a bare follower (no goroutines) wired enough to answer a
// RequestVote, with an election config and a settable last-leader-contact time, so
// the disruption-prevention (min-election-timeout) rule can be exercised directly.
func makeStickyVoter(t *testing.T, contact time.Time) (*Node, *ChannelTransport) {
	t.Helper()
	tr := NewChannelTransport()
	tr.Register(0) // voter
	tr.Register(9) // candidate, to capture the reply
	log := NewRaftLog()
	log.append(1, EntryData, []byte("e")) // one entry @ term 1
	nd := &Node{
		id: 0, transport: tr, log: log,
		role: Follower, currentTerm: 2, votedFor: noVote,
		cfg:               electionConfig(),
		lastLeaderContact: contact,
		electionResetCh:   make(chan struct{}, 1),
	}
	nd.appliedCond = sync.NewCond(&nd.raftMu)
	return nd, tr
}

// TestDisruptionPreventionIgnoresVote pins §4.2.3: a vote request arriving within
// the minimum election timeout of leader contact is ignored ENTIRELY — the voter
// neither grants nor even adopts the candidate's higher term — so a removed /
// partitioned server campaigning with ever-rising terms cannot unseat a live
// leader.
func TestDisruptionPreventionIgnoresVote(t *testing.T) {
	nd, tr := makeStickyVoter(t, time.Now())
	nd.handleRequestVote(Message{
		Type: MsgRequestVote, From: 9, Term: 5,
		CandidateID: 9, LastLogIndex: 1, LastLogTerm: 1,
	})
	if resp := <-tr.Inbox(9); resp.VoteGranted {
		t.Errorf("granted a vote within the leader-stickiness window")
	}
	if term := nd.termValue(); term != 2 {
		t.Errorf("adopted term %d; a sticky voter must not adopt a disruptor's higher term", term)
	}
	if r := nd.roleValue(); r != Follower {
		t.Errorf("role = %v, want follower (no step-down within stickiness)", r)
	}
}

// TestDisruptionPreventionExpiredAllowsVote confirms the rule is time-bounded:
// once the window has passed, the same higher-term request is honored normally.
func TestDisruptionPreventionExpiredAllowsVote(t *testing.T) {
	nd, tr := makeStickyVoter(t, time.Now().Add(-time.Second))
	nd.handleRequestVote(Message{
		Type: MsgRequestVote, From: 9, Term: 5,
		CandidateID: 9, LastLogIndex: 1, LastLogTerm: 1,
	})
	if resp := <-tr.Inbox(9); !resp.VoteGranted {
		t.Errorf("did not grant a vote after the stickiness window expired")
	}
	if term := nd.termValue(); term != 5 {
		t.Errorf("term = %d, want 5 (adopted after honoring the request)", term)
	}
}

// TestDisruptionPreventionLeaderTransferBypass confirms a leadership-transfer
// request bypasses the stickiness rule even inside the window — the mechanism
// Stage 5's TimeoutNow will rely on.
func TestDisruptionPreventionLeaderTransferBypass(t *testing.T) {
	nd, tr := makeStickyVoter(t, time.Now())
	nd.handleRequestVote(Message{
		Type: MsgRequestVote, From: 9, Term: 5,
		CandidateID: 9, LastLogIndex: 1, LastLogTerm: 1,
		LeaderTransfer: true,
	})
	if resp := <-tr.Inbox(9); !resp.VoteGranted {
		t.Errorf("leader-transfer vote request was not honored within the stickiness window")
	}
	if term := nd.termValue(); term != 5 {
		t.Errorf("term = %d, want 5 (leader-transfer bypasses stickiness)", term)
	}
}
