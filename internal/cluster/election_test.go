package cluster

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/BrandonnLow/littledb/internal/db"
)

// partitionTransport wraps a ChannelTransport and can isolate nodes: while a
// node is disconnected, every message to or from it is silently dropped, which
// models a network partition (the node keeps running and keeps believing
// whatever it last believed). Healing restores delivery.
type partitionTransport struct {
	inner *ChannelTransport
	mu    sync.Mutex
	down  map[NodeID]bool
}

func newPartitionTransport() *partitionTransport {
	return &partitionTransport{inner: NewChannelTransport(), down: make(map[NodeID]bool)}
}

func (p *partitionTransport) Register(id NodeID)               { p.inner.Register(id) }
func (p *partitionTransport) Inbox(self NodeID) <-chan Message { return p.inner.Inbox(self) }

func (p *partitionTransport) Send(to NodeID, msg Message) error {
	p.mu.Lock()
	blocked := p.down[to] || p.down[msg.From]
	p.mu.Unlock()
	if blocked {
		return nil // dropped by the partition
	}
	return p.inner.Send(to, msg)
}

func (p *partitionTransport) disconnect(id NodeID) {
	p.mu.Lock()
	p.down[id] = true
	p.mu.Unlock()
}

func (p *partitionTransport) reconnect(id NodeID) {
	p.mu.Lock()
	delete(p.down, id)
	p.mu.Unlock()
}

// electionConfig has timers fast enough to fail over within a test, yet with a
// heartbeat well under the election floor (30 <= 150/3) so a healthy leader is
// never voted out under -race.
func electionConfig() Config {
	return Config{ElectionMin: 150 * time.Millisecond, ElectionMax: 300 * time.Millisecond, Heartbeat: 30 * time.Millisecond}
}

// TestBootstrapLeader pins the bootstrap: node 0 starts as leader at term 1 and
// the rest as followers, with no election required. stableConfig keeps it that
// way for the assertion.
func TestBootstrapLeader(t *testing.T) {
	const n = 3
	c, err := NewWithTransportConfig(n, dirs(t, n), testOpts(), NewChannelTransport(), stableConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if r := c.Node(0).roleValue(); r != Leader {
		t.Errorf("node 0 role = %v, want leader", r)
	}
	if term := c.Node(0).termValue(); term != 1 {
		t.Errorf("node 0 term = %d, want 1", term)
	}
	for i := 1; i < n; i++ {
		if r := c.Node(i).roleValue(); r != Follower {
			t.Errorf("node %d role = %v, want follower", i, r)
		}
	}
	if c.Leader() != 0 {
		t.Errorf("Leader() = %d, want 0", c.Leader())
	}
}

// TestLeaderFailover is the end-to-end failover: commit under the bootstrap
// leader, partition it, a follower takes over at a higher term, a write goes
// through the new leader, then the old leader rejoins and steps down. All nodes
// converge on every key. Robust to which follower wins and to a spurious
// phase-1 re-election (it always partitions whoever currently leads).
func TestLeaderFailover(t *testing.T) {
	const n = 3
	pt := newPartitionTransport()
	c, err := NewWithTransportConfig(n, dirs(t, n), testOpts(), pt, electionConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	for i := 0; i < 3; i++ {
		if err := c.Put([]byte(fmt.Sprintf("k%d", i)), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("put k%d: %v", i, err)
		}
	}
	if err := c.Quiesce(2 * time.Second); err != nil {
		t.Fatal(err)
	}
	old, ok := c.currentLeader()
	if !ok {
		t.Fatal("no leader after initial writes")
	}
	oldID, oldTerm := old.id, old.termValue()

	// Partition the current leader; a connected follower must take over at a
	// higher term.
	pt.disconnect(oldID)
	waitFor(t, 4*time.Second, func() bool {
		ld, ok := c.currentLeader()
		return ok && ld.id != oldID && ld.termValue() > oldTerm
	})
	newLeader, _ := c.currentLeader()
	t.Logf("failover: node %d -> node %d (term %d -> %d)", oldID, newLeader.id, oldTerm, newLeader.termValue())

	// A write now goes through the new leader.
	if err := c.Put([]byte("after"), []byte("failover")); err != nil {
		t.Fatalf("post-failover put: %v", err)
	}

	// The new leader has applied the new term-N entry AND the prior-term tail
	// k2 (the prior tail commits because a current-term entry committed above
	// it). The deterministic pin for the commit rule is
	// TestCommitRulePriorTermViaCurrentTerm; this is the end-to-end echo of it.
	waitFor(t, 2*time.Second, func() bool {
		v, err := newLeader.store.Get([]byte("after"))
		if err != nil || string(v) != "failover" {
			return false
		}
		v2, err := newLeader.store.Get([]byte("k2"))
		return err == nil && string(v2) == "v2"
	})

	// Heal: the old leader rejoins, learns of the higher term, and steps down.
	pt.reconnect(oldID)
	waitFor(t, 4*time.Second, func() bool {
		return c.Node(int(oldID)).roleValue() == Follower &&
			c.Node(int(oldID)).termValue() >= newLeader.termValue()
	})

	if err := c.Quiesce(4 * time.Second); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"k0": "v0", "k1": "v1", "k2": "v2", "after": "failover"}
	for i := 0; i < n; i++ {
		for k, v := range want {
			got, err := c.Node(i).store.Get([]byte(k))
			if err != nil || string(got) != v {
				t.Errorf("node %d key %s = %q (err %v), want %q", i, k, got, err, v)
			}
		}
	}
	leaders := 0
	for i := 0; i < n; i++ {
		if c.Node(i).roleValue() == Leader {
			leaders++
		}
	}
	if leaders != 1 {
		t.Errorf("leader count after convergence = %d, want 1", leaders)
	}
}

// TestTermIncrementsSingleLeader checks that after a failover and healing, the
// cluster converges on exactly one leader and a single term >= 2 shared by all
// nodes (no split-brain, no stuck stale term).
func TestTermIncrementsSingleLeader(t *testing.T) {
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
	old, _ := c.currentLeader()
	oldID, oldTerm := old.id, old.termValue()

	pt.disconnect(oldID)
	waitFor(t, 4*time.Second, func() bool {
		ld, ok := c.currentLeader()
		return ok && ld.id != oldID && ld.termValue() > oldTerm
	})
	pt.reconnect(oldID)

	// Everyone settles on one leader at one term >= 2.
	waitFor(t, 5*time.Second, func() bool {
		leaders, lterm := 0, uint64(0)
		for i := 0; i < n; i++ {
			if c.Node(i).roleValue() == Leader {
				leaders++
				lterm = c.Node(i).termValue()
			}
		}
		if leaders != 1 || lterm < 2 {
			return false
		}
		for i := 0; i < n; i++ {
			if c.Node(i).termValue() != lterm {
				return false
			}
		}
		return true
	})
}

// makeVoter builds a bare follower at term 2 with a log of the given per-entry
// terms, wired enough to answer a RequestVote (no goroutines run).
func makeVoter(t *testing.T, logTerms []uint64) (*Node, *ChannelTransport) {
	t.Helper()
	tr := NewChannelTransport()
	tr.Register(0) // voter
	tr.Register(9) // candidate, to capture the reply
	log := NewRaftLog()
	for _, term := range logTerms {
		log.append(term, []byte("e"))
	}
	nd := &Node{
		id: 0, transport: tr, log: log,
		role: Follower, currentTerm: 2, votedFor: noVote,
		electionResetCh: make(chan struct{}, 1),
	}
	nd.appliedCond = sync.NewCond(&nd.raftMu)
	return nd, tr
}

func requestVote(nd *Node, tr *ChannelTransport, candIdx, candTerm uint64) bool {
	nd.handleRequestVote(Message{
		Type: MsgRequestVote, From: 9, Term: 2,
		CandidateID: 9, LastLogIndex: candIdx, LastLogTerm: candTerm,
	})
	return (<-tr.Inbox(9)).VoteGranted
}

// TestUpToDateVoteRestriction pins Raft §5.4.1: a vote is granted only when the
// candidate's log is at least as up-to-date as the voter's — a higher last term
// wins, otherwise an index at least as high at an equal last term.
func TestUpToDateVoteRestriction(t *testing.T) {
	cases := []struct {
		name              string
		voterLog          []uint64
		candIdx, candTerm uint64
		grant             bool
	}{
		{"shorter-log-denied", []uint64{1, 2}, 1, 1, false},
		{"equal-up-to-date-granted", []uint64{1, 2}, 2, 2, true},
		{"longer-same-term-granted", []uint64{1, 2}, 3, 2, true},
		{"lower-last-term-denied", []uint64{2}, 1, 1, false},
		{"higher-last-term-granted", []uint64{1}, 1, 2, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			nd, tr := makeVoter(t, tc.voterLog)
			if got := requestVote(nd, tr, tc.candIdx, tc.candTerm); got != tc.grant {
				t.Errorf("VoteGranted = %v, want %v", got, tc.grant)
			}
		})
	}
}

// TestStepDownOnHigherTerm pins step-down: a leader that receives any message
// carrying a higher term reverts to follower and adopts that term.
func TestStepDownOnHigherTerm(t *testing.T) {
	tr := NewChannelTransport()
	tr.Register(0) // leader under test
	tr.Register(1) // the higher-term leader (sender)
	store, err := db.OpenWith(t.TempDir(), testOpts())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	nd := &Node{
		id: 0, store: store, transport: tr, peers: []NodeID{1, 2},
		log:             NewRaftLog(),
		role:            Leader,
		currentTerm:     2,
		votedFor:        0,
		electionResetCh: make(chan struct{}, 1),
		applySignal:     make(chan struct{}, 1),
	}
	nd.appliedCond = sync.NewCond(&nd.raftMu)

	nd.handleAppendEntries(Message{
		Type: MsgAppendEntries, From: 1, Term: 5,
		PrevLogIndex: 0, PrevLogTerm: 0,
	})
	<-tr.Inbox(1) // drain the follower response

	if r := nd.roleValue(); r != Follower {
		t.Errorf("role = %v, want follower after higher-term message", r)
	}
	if term := nd.termValue(); term != 5 {
		t.Errorf("term = %d, want 5 (adopted)", term)
	}
}

// TestCommitRulePriorTermViaCurrentTerm is the deterministic pin for note 5
// (§5.4.2): a leader at term 2 must NOT commit a replicated term-1 entry by
// replica count, but the moment a term-2 entry commits, the term-1 tail below
// it commits too.
func TestCommitRulePriorTermViaCurrentTerm(t *testing.T) {
	log := NewRaftLog()
	log.append(1, []byte("prior")) // index 1 @ term 1 — the prior-term tail

	nd := &Node{
		peers:       []NodeID{1, 2},
		log:         log,
		role:        Leader,
		currentTerm: 2,
		matchIndex:  map[NodeID]uint64{1: 1, 2: 0}, // leader + peer 1 hold index 1
		nextIndex:   map[NodeID]uint64{1: 2, 2: 2},
		applySignal: make(chan struct{}, 1),
		replSignal:  map[NodeID]chan struct{}{1: make(chan struct{}, 1), 2: make(chan struct{}, 1)},
	}

	// A majority holds index 1, but it is a prior-term entry: must not commit.
	nd.raftMu.Lock()
	nd.maybeAdvanceCommitLocked()
	nd.raftMu.Unlock()
	if ci := nd.commitIndexValue(); ci != 0 {
		t.Fatalf("commitIndex = %d, want 0 (prior-term entry must not commit by count)", ci)
	}

	// Append a current-term entry and replicate it to a majority.
	nd.raftMu.Lock()
	nd.log.append(2, []byte("current")) // index 2 @ term 2
	nd.matchIndex[1] = 2
	nd.maybeAdvanceCommitLocked()
	ci := nd.commitIndex
	nd.raftMu.Unlock()

	// commitIndex jumps to 2, carrying the term-1 tail (index 1) with it.
	if ci != 2 {
		t.Fatalf("commitIndex = %d, want 2 (current-term commit carries the prior-term tail)", ci)
	}
}
