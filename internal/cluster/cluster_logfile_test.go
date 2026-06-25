package cluster

import (
	"bytes"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/BrandonnLow/littledb/internal/db"
)

// assertFileMirrorsMemory reloads the on-disk Raft log file independently and
// asserts it holds EXACTLY the in-memory log: same length, terms, and bytes.
// This is the invariant finding #1 was about — the file must never lead the
// in-memory log (an orphan tail) nor duplicate a suffix.
func assertFileMirrorsMemory(t *testing.T, n *Node, logPath string) {
	t.Helper()
	lf2, entries, err := openRaftLogFile(logPath, false)
	if err != nil {
		t.Fatalf("reload raft log: %v", err)
	}
	defer lf2.close()
	if uint64(len(entries)) != n.log.lastIndex() {
		t.Fatalf("file has %d entries, in-memory log has %d (orphan or duplicate tail)",
			len(entries), n.log.lastIndex())
	}
	for i, pe := range entries {
		idx := uint64(i) + 1
		if pe.term != n.log.term(idx) {
			t.Errorf("entry %d: file term %d, memory term %d", idx, pe.term, n.log.term(idx))
		}
		if !bytes.Equal(pe.data, n.log.entryAt(idx)) {
			t.Errorf("entry %d: file data %q, memory data %q", idx, pe.data, n.log.entryAt(idx))
		}
	}
}

func drainAppendResp(t *testing.T, tr *ChannelTransport, inbox NodeID, wantSuccess bool) {
	t.Helper()
	resp := <-tr.Inbox(inbox)
	if resp.Type != MsgAppendResponse {
		t.Fatalf("resp type = %v, want AppendResponse", resp.Type)
	}
	if resp.Success != wantSuccess {
		t.Fatalf("resp.Success = %v, want %v", resp.Success, wantSuccess)
	}
}

// TestRaftLogFileMirrorsInMemoryLog drives a follower through an initial
// append, a conflicting suffix (truncate + replace), and an idempotent re-send,
// asserting after each that the persisted file is byte-identical to the
// in-memory log. Under the old split (append the file unlocked, then re-check
// and append memory) a step-down in the gap left the file a suffix ahead, which
// in-memory-length-keyed truncation could never reclaim, so a retry duplicated
// it. Mutating both logs together under raftMu makes that state unreachable.
func TestRaftLogFileMirrorsInMemoryLog(t *testing.T) {
	dir := t.TempDir()
	tr := NewChannelTransport()
	tr.Register(0) // sender; captures the follower's responses
	tr.Register(1) // follower under test
	store, err := db.OpenWith(t.TempDir(), testOpts())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	logPath := filepath.Join(dir, raftLogFileName)
	lf, _, err := openRaftLogFile(logPath, true)
	if err != nil {
		t.Fatal(err)
	}

	f := &Node{
		id: 1, store: store, transport: tr, peers: []NodeID{0},
		inbox: tr.Inbox(1), quit: make(chan struct{}),
		log: NewRaftLog(), logFile: lf,
		role: Follower, currentTerm: 1, votedFor: noVote,
		electionResetCh: make(chan struct{}, 1), applySignal: make(chan struct{}, 1),
	}
	f.appliedCond = sync.NewCond(&f.raftMu)
	defer f.logFile.close()

	ent := func(s string) []byte { return []byte(s) }

	// 1) Append three entries at term 1 onto an empty log.
	f.handleAppendEntries(Message{
		Type: MsgAppendEntries, From: 0, Term: 1, PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: []Entry{{1, ent("a")}, {1, ent("b")}, {1, ent("c")}},
	})
	drainAppendResp(t, tr, 0, true)
	if f.log.lastIndex() != 3 {
		t.Fatalf("after initial append, lastIndex = %d, want 3", f.log.lastIndex())
	}
	assertFileMirrorsMemory(t, f, logPath)

	// 2) Conflicting suffix at term 2 from index 2: truncate b,c@1 and append
	// b2,c2@2. This is exactly the divergence-repair path.
	f.handleAppendEntries(Message{
		Type: MsgAppendEntries, From: 0, Term: 2, PrevLogIndex: 1, PrevLogTerm: 1,
		Entries: []Entry{{2, ent("b2")}, {2, ent("c2")}},
	})
	drainAppendResp(t, tr, 0, true)
	if f.log.lastIndex() != 3 ||
		string(f.log.entryAt(2)) != "b2" || string(f.log.entryAt(3)) != "c2" {
		t.Fatalf("conflict not applied: len=%d e2=%q e3=%q",
			f.log.lastIndex(), f.log.entryAt(2), f.log.entryAt(3))
	}
	assertFileMirrorsMemory(t, f, logPath)

	// 3) Idempotent re-send of the same suffix: the log must not grow, and the
	// file must not gain a duplicate tail.
	f.handleAppendEntries(Message{
		Type: MsgAppendEntries, From: 0, Term: 2, PrevLogIndex: 1, PrevLogTerm: 1,
		Entries: []Entry{{2, ent("b2")}, {2, ent("c2")}},
	})
	drainAppendResp(t, tr, 0, true)
	if f.log.lastIndex() != 3 {
		t.Fatalf("after idempotent re-send, lastIndex = %d, want 3 (no growth)", f.log.lastIndex())
	}
	assertFileMirrorsMemory(t, f, logPath)

	// 4) Extend with a fresh entry at term 2 (prevLogIndex now matches the tail).
	f.handleAppendEntries(Message{
		Type: MsgAppendEntries, From: 0, Term: 2, PrevLogIndex: 3, PrevLogTerm: 2,
		Entries: []Entry{{2, ent("d2")}},
	})
	drainAppendResp(t, tr, 0, true)
	if f.log.lastIndex() != 4 || string(f.log.entryAt(4)) != "d2" {
		t.Fatalf("extend failed: len=%d e4=%q", f.log.lastIndex(), f.log.entryAt(4))
	}
	assertFileMirrorsMemory(t, f, logPath)
}

// assertFileMirrorsMemoryLive is the live-node analogue of
// assertFileMirrorsMemory: it snapshots the in-memory log under raftMu (so it
// does not race the node's goroutines) before independently reloading the file
// and comparing. The two must match exactly — no orphan or duplicate tail.
func assertFileMirrorsMemoryLive(t *testing.T, n *Node, logPath string) {
	t.Helper()
	n.raftMu.Lock()
	memLen := n.log.lastIndex()
	memTerms := make([]uint64, memLen)
	memData := make([][]byte, memLen)
	for i := uint64(1); i <= memLen; i++ {
		memTerms[i-1] = n.log.term(i)
		memData[i-1] = append([]byte(nil), n.log.entryAt(i)...)
	}
	n.raftMu.Unlock()

	lf, entries, err := openRaftLogFile(logPath, false)
	if err != nil {
		t.Fatalf("reload raft log: %v", err)
	}
	defer lf.close()
	if uint64(len(entries)) != memLen {
		t.Fatalf("file has %d entries, in-memory log has %d (orphan or duplicate tail)", len(entries), memLen)
	}
	for i, pe := range entries {
		if pe.term != memTerms[i] {
			t.Errorf("entry %d: file term %d, memory term %d", i+1, pe.term, memTerms[i])
		}
		if !bytes.Equal(pe.data, memData[i]) {
			t.Errorf("entry %d: file data %q, memory data %q", i+1, pe.data, memData[i])
		}
	}
}

// TestLeaderRaftLogMirrorsAcrossStepDown is the leader-side analogue of
// TestRaftLogFileMirrorsInMemoryLog. It parks a leader's commit() after it has
// appended the entry to the raft log (file + memory) but before quorum — by
// gating away the followers' acks so the commit index never advances — then
// forces a step-down with a higher-term message. The parked commit must return
// ErrNotLeader, and the raft log file must still mirror the in-memory log
// exactly: the leader-path append + a racing step-down leaves no orphan tail.
func TestLeaderRaftLogMirrorsAcrossStepDown(t *testing.T) {
	const n = 3
	ds := dirs(t, n)
	// Hold every append-response to the leader so it can never reach quorum.
	gate := newGateTransport(func(to NodeID, m Message) bool {
		return to == 0 && m.Type == MsgAppendResponse
	})
	c, err := NewWithTransportConfig(n, ds,
		db.Options{SyncOnWrite: true, DisableBackgroundCompaction: true}, gate, stableConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	waitFor(t, time.Second, func() bool { return c.Node(0).roleValue() == Leader })
	leaderTerm := c.Node(0).termValue()

	// Park a commit: it appends to the raft log, then blocks waiting for a quorum
	// the gate withholds forever.
	done := make(chan error, 1)
	go func() { done <- c.Node(0).store.Put([]byte("k"), []byte("v")) }()

	// Wait until the entry is durably appended (file + memory) — commit() is now
	// past the append and parked on its apply wait.
	waitFor(t, 2*time.Second, func() bool { return c.Node(0).lastIndex() >= 1 })

	// Deliver a higher-term AppendEntries (not held by the predicate). The
	// follower path steps the leader down, which broadcasts the apply cond; the
	// parked commit wakes, sees role != Leader, and returns ErrNotLeader.
	_ = gate.Send(0, Message{
		Type: MsgAppendEntries, From: 1, Term: leaderTerm + 5,
		PrevLogIndex: 0, PrevLogTerm: 0,
	})

	select {
	case err := <-done:
		if !errors.Is(err, ErrNotLeader) {
			t.Fatalf("parked Put returned %v, want ErrNotLeader", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("parked Put did not return after step-down")
	}
	waitFor(t, time.Second, func() bool { return c.Node(0).roleValue() == Follower })

	assertFileMirrorsMemoryLive(t, c.Node(0), filepath.Join(ds[0], "raft", raftLogFileName))
}
