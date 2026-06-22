package cluster

import (
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/BrandonnLow/littledb/internal/db"
)

// newVoterWithState builds a bare follower wired with a real durable state file
// at dir/state, enough to answer a RequestVote and persist its vote. No
// goroutines run; tests drive handleRequestVote directly.
func newVoterWithState(t *testing.T, dir string, id NodeID, term uint64, votedFor NodeID, logTerms []uint64, tr *ChannelTransport) *Node {
	t.Helper()
	sf, _, err := openRaftStateFile(filepath.Join(dir, raftStateFileName), true)
	if err != nil {
		t.Fatal(err)
	}
	log := NewRaftLog()
	for _, lt := range logTerms {
		log.append(lt, []byte("e"))
	}
	nd := &Node{
		id: id, transport: tr, log: log, stateFile: sf,
		role: Follower, currentTerm: term, votedFor: votedFor,
		electionResetCh: make(chan struct{}, 1),
	}
	nd.appliedCond = sync.NewCond(&nd.raftMu)
	return nd
}

// TestRaftStateFileRoundTrip pins the on-disk format: an absent file reports
// not-present, and save/reopen round-trips both currentTerm and votedFor,
// including the noVote sentinel and a real peer id.
func TestRaftStateFileRoundTrip(t *testing.T) {
	p := filepath.Join(t.TempDir(), raftStateFileName)

	sf, hard, err := openRaftStateFile(p, true)
	if err != nil {
		t.Fatal(err)
	}
	if hard.present {
		t.Fatal("absent state file reported present=true")
	}

	if err := sf.save(7, noVote); err != nil {
		t.Fatal(err)
	}
	if _, hard, _ = openRaftStateFile(p, true); !hard.present || hard.currentTerm != 7 || hard.votedFor != noVote {
		t.Fatalf("after save(7, noVote): %+v, want term 7 noVote present", hard)
	}

	if err := sf.save(9, NodeID(2)); err != nil {
		t.Fatal(err)
	}
	if _, hard, _ = openRaftStateFile(p, true); !hard.present || hard.currentTerm != 9 || hard.votedFor != NodeID(2) {
		t.Fatalf("after save(9, 2): %+v, want term 9 vote 2 present", hard)
	}
}

// TestNoDoubleVoteAcrossRestart is the core safety pin. A follower grants
// candidate 1 in term T, then "restarts" by reconstructing its hard state from
// the same dir. The restarted node must NOT grant a competing candidate 2 in the
// same term T (the double-vote / split-brain bug), but must still answer the
// original candidate idempotently. Voter logs are empty so every candidate is
// up-to-date — isolating the vote rule from the §5.4.1 log check.
func TestNoDoubleVoteAcrossRestart(t *testing.T) {
	const term uint64 = 5
	dir := t.TempDir()
	tr := NewChannelTransport()
	tr.Register(0) // voter
	tr.Register(1) // candidate 1
	tr.Register(2) // candidate 2

	f := newVoterWithState(t, dir, 0, term, noVote, nil, tr)
	f.handleRequestVote(Message{Type: MsgRequestVote, From: 1, Term: term, CandidateID: 1})
	if !(<-tr.Inbox(1)).VoteGranted {
		t.Fatal("candidate 1 should be granted in a fresh term")
	}

	// Restart: reconstruct hard state from disk. The vote must have survived.
	sf2, hard, err := openRaftStateFile(filepath.Join(dir, raftStateFileName), true)
	if err != nil {
		t.Fatal(err)
	}
	if !hard.present || hard.currentTerm != term || hard.votedFor != 1 {
		t.Fatalf("restored hard state = %+v, want term %d vote 1 present", hard, term)
	}
	f2 := &Node{
		id: 0, transport: tr, log: NewRaftLog(), stateFile: sf2,
		role: Follower, currentTerm: hard.currentTerm, votedFor: hard.votedFor,
		electionResetCh: make(chan struct{}, 1),
	}
	f2.appliedCond = sync.NewCond(&f2.raftMu)

	// Competing candidate 2 in the SAME term must be denied.
	f2.handleRequestVote(Message{Type: MsgRequestVote, From: 2, Term: term, CandidateID: 2})
	if (<-tr.Inbox(2)).VoteGranted {
		t.Fatal("double vote: candidate 2 granted in a term the node already voted in")
	}

	// The original candidate 1 is still granted (votedFor == candidate; idempotent).
	f2.handleRequestVote(Message{Type: MsgRequestVote, From: 1, Term: term, CandidateID: 1})
	if !(<-tr.Inbox(1)).VoteGranted {
		t.Fatal("idempotent re-vote to candidate 1 should be granted")
	}
}

// TestVoteDurableBeforeGrant asserts the ordering invariant: by the time
// handleRequestVote returns a grant, the vote is already on disk. An independent
// reopen of the state file must reflect it — the grant reply is gated on the
// persist, so a crash-after-reply-before-fsync window cannot exist.
func TestVoteDurableBeforeGrant(t *testing.T) {
	const term uint64 = 4
	dir := t.TempDir()
	tr := NewChannelTransport()
	tr.Register(0)
	tr.Register(1)

	f := newVoterWithState(t, dir, 0, term, noVote, nil, tr)
	f.handleRequestVote(Message{Type: MsgRequestVote, From: 1, Term: term, CandidateID: 1})
	if !(<-tr.Inbox(1)).VoteGranted {
		t.Fatal("expected grant")
	}

	_, hard, err := openRaftStateFile(filepath.Join(dir, raftStateFileName), true)
	if err != nil {
		t.Fatal(err)
	}
	if !hard.present || hard.currentTerm != term || hard.votedFor != 1 {
		t.Fatalf("state after grant = %+v, want term %d vote 1 (vote must be durable before the grant returns)", hard, term)
	}
}

// TestRestartMaxLoadTermReconciliation drives the restart rule through the real
// constructor over crafted raft/ files: currentTerm = max(state.term,
// log.lastTerm()), with votedFor reset only when the log term strictly wins. It
// also confirms the resolved value is persisted (the file matches memory).
func TestRestartMaxLoadTermReconciliation(t *testing.T) {
	cases := []struct {
		name      string
		logTerms  []uint64
		stateTerm uint64
		stateVote NodeID
		wantTerm  uint64
		wantVote  NodeID
	}{
		{"log-term-strictly-wins-resets-vote", []uint64{5, 5}, 3, 1, 5, noVote},
		{"state-term-wins-keeps-vote", []uint64{5}, 7, 1, 7, 1},
		{"tie-keeps-vote", []uint64{5, 5}, 5, 2, 5, 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			raftDir := filepath.Join(dir, "raft")
			if err := os.MkdirAll(raftDir, 0o755); err != nil {
				t.Fatal(err)
			}
			// Craft a raft.log at the given per-entry terms (payload is opaque to
			// the log file; commitIndex stays 0 so it is never decoded/applied).
			lf, _, err := openRaftLogFile(filepath.Join(raftDir, raftLogFileName), true)
			if err != nil {
				t.Fatal(err)
			}
			for _, lt := range tc.logTerms {
				if err := lf.append(lt, []byte("x")); err != nil {
					t.Fatal(err)
				}
			}
			if err := lf.close(); err != nil {
				t.Fatal(err)
			}
			// Craft the saved hard state.
			sf, _, err := openRaftStateFile(filepath.Join(raftDir, raftStateFileName), true)
			if err != nil {
				t.Fatal(err)
			}
			if err := sf.save(tc.stateTerm, tc.stateVote); err != nil {
				t.Fatal(err)
			}

			// stableConfig keeps the lone node from electing (and changing term)
			// before we observe it.
			c, err := NewWithTransportConfig(1, []string{dir},
				db.Options{SyncOnWrite: true, DisableBackgroundCompaction: true},
				NewChannelTransport(), stableConfig())
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()

			nd := c.Node(0)
			nd.raftMu.Lock()
			gotTerm, gotVote := nd.currentTerm, nd.votedFor
			nd.raftMu.Unlock()
			if gotTerm != tc.wantTerm || gotVote != tc.wantVote {
				t.Fatalf("reconciled (term,vote)=(%d,%d), want (%d,%d)", gotTerm, gotVote, tc.wantTerm, tc.wantVote)
			}

			// The repair/derivation is persisted, not just in memory.
			_, hard, err := openRaftStateFile(filepath.Join(raftDir, raftStateFileName), true)
			if err != nil {
				t.Fatal(err)
			}
			if hard.currentTerm != tc.wantTerm || hard.votedFor != tc.wantVote {
				t.Fatalf("persisted (term,vote)=(%d,%d), want (%d,%d)", hard.currentTerm, hard.votedFor, tc.wantTerm, tc.wantVote)
			}
		})
	}
}
