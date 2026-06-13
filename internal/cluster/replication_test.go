package cluster

import (
	"fmt"
	"testing"
	"time"

	"github.com/BrandonnLow/littledb/internal/db"
)

// TestReplicationOutOfOrderAcks holds one follower's responses while two
// writes commit on the leader-plus-other-follower majority. The leader does
// not wait for the held follower; the majority follower applies via the
// heartbeat that carries the advanced leaderCommit. When the held responses
// are finally released — after the commit index has already moved past them
// — they are absorbed idempotently and the straggler converges.
func TestReplicationOutOfOrderAcks(t *testing.T) {
	const n = 3
	gate := newGateTransport(func(to NodeID, m Message) bool {
		return to == 0 && m.From == 2 && m.Type == MsgAppendResponse // hold node 2's acks
	})
	c, err := NewWithTransport(n, dirs(t, n), testOpts(), gate)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if err := c.Put([]byte("k1"), []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if err := c.Put([]byte("k2"), []byte("v2")); err != nil {
		t.Fatal(err)
	}

	if got := c.Node(0).commitIndexValue(); got != 2 {
		t.Fatalf("leader commitIndex = %d, want 2 (committed via majority without node 2)", got)
	}
	// Majority follower applies via the leaderCommit heartbeat.
	waitFor(t, time.Second, func() bool { return c.Node(1).appliedIndex() >= 2 })
	if got, err := c.Node(1).DB().Get([]byte("k2")); err != nil || string(got) != "v2" {
		t.Fatalf("majority follower Get(k2) = (%q,%v), want v2", got, err)
	}

	// Release the straggler's acks: late, but idempotent — the commit index
	// must not move past 2, and node 2 converges.
	gate.release()
	waitFor(t, time.Second, func() bool { return c.Node(2).appliedIndex() >= 2 })
	if got := c.Node(0).commitIndexValue(); got != 2 {
		t.Errorf("after late acks, leader commitIndex = %d, want still 2 (idempotent)", got)
	}
	for _, kv := range []struct{ k, v string }{{"k1", "v1"}, {"k2", "v2"}} {
		if got, err := c.Node(2).DB().Get([]byte(kv.k)); err != nil || string(got) != kv.v {
			t.Errorf("node 2 after release: Get(%s) = (%q,%v), want %s", kv.k, got, err, kv.v)
		}
	}
}

// TestReplicationSlowFollowerCatchUp partitions one follower (it hears no
// AppendEntries) while several writes commit on the remaining majority, then
// reconnects it. It catches up via a batched AppendEntries carrying the
// backlog and converges.
func TestReplicationSlowFollowerCatchUp(t *testing.T) {
	const n = 3
	const writes = 5
	gate := newGateTransport(func(to NodeID, m Message) bool {
		return to == 2 && m.Type == MsgAppendEntries // node 2 hears nothing
	})
	c, err := NewWithTransport(n, dirs(t, n), testOpts(), gate)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	for i := 0; i < writes; i++ {
		if err := c.Put([]byte(fmt.Sprintf("k%d", i)), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	// Leader and node 1 are caught up; node 2 is partitioned and empty.
	waitFor(t, time.Second, func() bool {
		return c.Node(0).commitIndexValue() == writes && c.Node(1).appliedIndex() == writes
	})
	if got := c.Node(2).lastIndex(); got != 0 {
		t.Fatalf("slow follower lastIndex = %d, want 0 (partitioned)", got)
	}

	// Reconnect: node 2 catches up and converges.
	gate.release()
	waitFor(t, 2*time.Second, func() bool { return c.Node(2).appliedIndex() == writes })
	for i := 0; i < writes; i++ {
		k := fmt.Sprintf("k%d", i)
		if got, err := c.Node(2).DB().Get([]byte(k)); err != nil || string(got) != fmt.Sprintf("v%d", i) {
			t.Errorf("node 2 after catch-up: Get(%s) = (%q,%v), want v%d", k, got, err, i)
		}
	}
}

// TestAppendEntriesRejectsAheadPrevLog drives the follower's prevLog
// consistency check directly: an AppendEntries whose prevLogIndex is past the
// follower's log can't match, so the follower rejects with a back-up hint and
// touches neither its WAL nor its log. This is the consistency check that
// becomes load-bearing once elections produce real divergence.
func TestAppendEntriesRejectsAheadPrevLog(t *testing.T) {
	tr := NewChannelTransport()
	tr.Register(0) // captures the follower's response
	tr.Register(1)
	store, err := db.OpenWith(t.TempDir(), testOpts())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	f := &Node{
		id: 1, store: store, transport: tr, peers: []NodeID{0},
		inbox: tr.Inbox(1), quit: make(chan struct{}), log: NewRaftLog(),
	}

	// Empty follower log; prevLogIndex 3 cannot match.
	f.handleAppendEntries(Message{
		Type: MsgAppendEntries, From: 0, Term: currentTerm,
		PrevLogIndex: 3, PrevLogTerm: currentTerm,
	})

	resp := <-tr.Inbox(0)
	if resp.Type != MsgAppendResponse || resp.Success {
		t.Fatalf("resp = %+v, want a rejection", resp)
	}
	if resp.ConflictHint != 1 {
		t.Errorf("ConflictHint = %d, want 1 (lastIndex+1)", resp.ConflictHint)
	}
	if li := f.log.lastIndex(); li != 0 {
		t.Errorf("follower log grew to %d on reject, want 0 (untouched)", li)
	}
}

// TestLeaderNextIndexTransitions pins the leader's per-follower bookkeeping:
// success advances matchIndex/nextIndex; a stale, out-of-order success with a
// lower matchIndex must not regress (idempotent ack handling); a rejection
// backs nextIndex up to the hint, clamped to >= 1.
func TestLeaderNextIndexTransitions(t *testing.T) {
	n := &Node{
		peers:      []NodeID{1},
		nextIndex:  map[NodeID]uint64{1: 1},
		matchIndex: map[NodeID]uint64{1: 0},
	}
	n.raftMu.Lock()
	defer n.raftMu.Unlock()

	n.onAppendSuccessLocked(1, 3)
	if n.matchIndex[1] != 3 || n.nextIndex[1] != 4 {
		t.Fatalf("after success(3): match=%d next=%d, want 3/4", n.matchIndex[1], n.nextIndex[1])
	}
	n.onAppendSuccessLocked(1, 1) // stale/out-of-order
	if n.matchIndex[1] != 3 || n.nextIndex[1] != 4 {
		t.Fatalf("after stale success(1): match=%d next=%d, want unchanged 3/4", n.matchIndex[1], n.nextIndex[1])
	}
	n.onAppendRejectLocked(1, 2)
	if n.nextIndex[1] != 2 {
		t.Fatalf("after reject(hint=2): next=%d, want 2", n.nextIndex[1])
	}
	n.onAppendRejectLocked(1, 0) // hint below 1
	if n.nextIndex[1] != 1 {
		t.Fatalf("after reject(hint=0): next=%d, want 1 (clamped)", n.nextIndex[1])
	}
}
