package cluster

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/BrandonnLow/littledb/internal/db"
)

// TestReplicationOutOfOrderAcks holds one follower's responses while two
// writes commit on the leader-plus-other-follower majority. The leader does
// not wait for the held follower; the majority follower applies via the
// heartbeat that carries the advanced leaderCommit (the new path Option A
// introduces). When the held responses are finally released — after the commit
// index has already moved past them — they are absorbed idempotently and the
// straggler converges.
func TestReplicationOutOfOrderAcks(t *testing.T) {
	const n = 3
	gate := newGateTransport(func(to NodeID, m Message) bool {
		return to == 0 && m.From == 2 && m.Type == MsgAppendResponse // hold node 2's acks
	})
	c, err := NewWithTransportConfig(n, dirs(t, n), testOpts(), gate, stableConfig())
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
	c, err := NewWithTransportConfig(n, dirs(t, n), testOpts(), gate, stableConfig())
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
// becomes load-bearing once Week 3's elections produce real divergence.
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
		role: Follower, currentTerm: term1, votedFor: noVote,
		electionResetCh: make(chan struct{}, 1),
	}
	f.appliedCond = sync.NewCond(&f.raftMu)

	// Empty follower log; prevLogIndex 3 cannot match.
	f.handleAppendEntries(Message{
		Type: MsgAppendEntries, From: 0, Term: term1,
		PrevLogIndex: 3, PrevLogTerm: term1,
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
		log:        NewRaftLog(), // base 0: clamp leaves the assertions unchanged
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

// TestAppendEntriesRejectsPrevLogTermMismatch drives the prevLog consistency
// check at an index the follower HAS but at the wrong term (distinct from the
// "prevLogIndex past the end" case). The follower rejects with a back-up hint
// and leaves its log untouched.
func TestAppendEntriesRejectsPrevLogTermMismatch(t *testing.T) {
	tr := NewChannelTransport()
	tr.Register(0)
	tr.Register(1)
	log := NewRaftLog()
	log.append(1, []byte("a"))
	log.append(2, []byte("b"))
	f := &Node{
		id: 1, transport: tr, peers: []NodeID{0},
		inbox: tr.Inbox(1), quit: make(chan struct{}), log: log,
		role: Follower, currentTerm: 2, votedFor: noVote,
		electionResetCh: make(chan struct{}, 1),
	}
	f.appliedCond = sync.NewCond(&f.raftMu)

	f.handleAppendEntries(Message{
		Type: MsgAppendEntries, From: 0, Term: 2,
		PrevLogIndex: 2, PrevLogTerm: 99, // index 2 exists but at term 2, not 99
		Entries: []Entry{{2, []byte("c")}},
	})
	resp := <-tr.Inbox(0)
	if resp.Success {
		t.Fatalf("resp = %+v, want a rejection on prevLogTerm mismatch", resp)
	}
	if resp.ConflictHint != 3 {
		t.Errorf("ConflictHint = %d, want 3 (lastIndex+1)", resp.ConflictHint)
	}
	if li := f.log.lastIndex(); li != 2 {
		t.Errorf("log mutated to %d on reject, want 2 (untouched)", li)
	}
}

// TestAppendEntriesMixedSkipThenConflict exercises the per-entry loop with both
// branches in ONE message: the first entry is already present (idempotent skip)
// and the second conflicts (truncate + replace), then a third extends. The file
// must mirror memory after the repair.
func TestAppendEntriesMixedSkipThenConflict(t *testing.T) {
	dir := t.TempDir()
	tr := NewChannelTransport()
	tr.Register(0)
	tr.Register(1)
	logPath := filepath.Join(dir, raftLogFileName)
	lf, _, err := openRaftLogFile(logPath, true)
	if err != nil {
		t.Fatal(err)
	}
	log := NewRaftLog()
	for _, s := range []string{"a", "b", "c"} { // seed file + memory with [a@1,b@1,c@1]
		if err := lf.append(1, []byte(s)); err != nil {
			t.Fatal(err)
		}
		log.append(1, []byte(s))
	}
	f := &Node{
		id: 1, transport: tr, peers: []NodeID{0},
		inbox: tr.Inbox(1), quit: make(chan struct{}),
		log: log, logFile: lf,
		role: Follower, currentTerm: 2, votedFor: noVote,
		electionResetCh: make(chan struct{}, 1), applySignal: make(chan struct{}, 1),
	}
	f.appliedCond = sync.NewCond(&f.raftMu)
	defer f.logFile.close()

	// idx1 a@1 (skip), idx2 b2@2 (conflict: truncate b,c then append), idx3 c2@2.
	f.handleAppendEntries(Message{
		Type: MsgAppendEntries, From: 0, Term: 2, PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: []Entry{{1, []byte("a")}, {2, []byte("b2")}, {2, []byte("c2")}},
	})
	resp := <-tr.Inbox(0)
	if !resp.Success || resp.MatchIndex != 3 {
		t.Fatalf("resp = %+v, want success with MatchIndex 3", resp)
	}
	if f.log.lastIndex() != 3 ||
		string(f.log.entryAt(1)) != "a" || f.log.term(1) != 1 ||
		string(f.log.entryAt(2)) != "b2" || f.log.term(2) != 2 ||
		string(f.log.entryAt(3)) != "c2" || f.log.term(3) != 2 {
		t.Fatalf("log = [%q@%d,%q@%d,%q@%d], want [a@1,b2@2,c2@2]",
			f.log.entryAt(1), f.log.term(1), f.log.entryAt(2), f.log.term(2), f.log.entryAt(3), f.log.term(3))
	}
	assertFileMirrorsMemory(t, f, logPath)
}

// TestAppendEntriesRejectsStaleLowerTerm pins the stale-leader path: an
// AppendEntries whose term is below ours is rejected, and the reply carries OUR
// higher term so the stale leader steps down. The log is not mutated.
func TestAppendEntriesRejectsStaleLowerTerm(t *testing.T) {
	tr := NewChannelTransport()
	tr.Register(0)
	tr.Register(1)
	log := NewRaftLog()
	log.append(5, []byte("x"))
	f := &Node{
		id: 1, transport: tr, peers: []NodeID{0},
		inbox: tr.Inbox(1), quit: make(chan struct{}), log: log,
		role: Follower, currentTerm: 5, votedFor: noVote,
		electionResetCh: make(chan struct{}, 1),
	}
	f.appliedCond = sync.NewCond(&f.raftMu)

	f.handleAppendEntries(Message{
		Type: MsgAppendEntries, From: 0, Term: 3, // stale, below our 5
		PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: []Entry{{3, []byte("y")}},
	})
	resp := <-tr.Inbox(0)
	if resp.Success {
		t.Fatal("stale lower-term AppendEntries must be rejected")
	}
	if resp.Term != 5 {
		t.Errorf("reply Term = %d, want 5 (our higher term, so the stale leader steps down)", resp.Term)
	}
	if li := f.log.lastIndex(); li != 1 {
		t.Errorf("log mutated to %d, want 1 (untouched)", li)
	}
}
