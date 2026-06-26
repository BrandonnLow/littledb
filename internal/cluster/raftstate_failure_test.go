package cluster

import (
	"errors"
	"sync"
	"testing"
)

// errInjected is the failure a fakePersister returns from save.
var errInjected = errors.New("cluster: injected persist failure")

// fakePersister is an injectable hardStatePersister. It records every save
// attempt (so a test can assert what was attempted, even on failure) and
// returns failErr. failErr is mutable so a test can fail then recover.
type fakePersister struct {
	failErr error
	saved   []hardState
}

func (f *fakePersister) save(currentTerm uint64, votedFor NodeID) error {
	f.saved = append(f.saved, hardState{currentTerm: currentTerm, votedFor: votedFor, present: true})
	return f.failErr
}

func (f *fakePersister) close() error { return nil }

// TestHandleRequestVotePersistFailureDeclines vote-cast rule: if persisting a
// granted vote fails, the node rolls votedFor back and replies NOT granted
// (rather than externalizing a vote it can't guarantee survived). It then
// confirms the rollback left clean state — once the persister recovers,the
// same candidate is granted normally.
func TestHandleRequestVotePersistFailureDeclines(t *testing.T) {
	tr := NewChannelTransport()
	tr.Register(0) // voter
	tr.Register(1) // candidate
	fp := &fakePersister{failErr: errInjected}
	f := &Node{
		id: 0, transport: tr, log: NewRaftLog(), stateFile: fp,
		role: Follower, currentTerm: 3, votedFor: noVote,
		electionResetCh: make(chan struct{}, 1),
	}
	f.appliedCond = sync.NewCond(&f.raftMu)

	f.handleRequestVote(Message{Type: MsgRequestVote, From: 1, Term: 3, CandidateID: 1})
	if (<-tr.Inbox(1)).VoteGranted {
		t.Fatal("vote granted despite persist failure; want declined")
	}
	f.raftMu.Lock()
	vf := f.votedFor
	f.raftMu.Unlock()
	if vf != noVote {
		t.Errorf("votedFor = %d after failed persist, want noVote (rolled back)", vf)
	}
	if len(fp.saved) != 1 || fp.saved[0].votedFor != 1 || fp.saved[0].currentTerm != 3 {
		t.Errorf("save attempts = %+v, want exactly one for (term 3, candidate 1)", fp.saved)
	}

	// Recovery: the persister works now, and the same candidate is granted — the
	// earlier rollback did not wedge the voter.
	fp.failErr = nil
	f.handleRequestVote(Message{Type: MsgRequestVote, From: 1, Term: 3, CandidateID: 1})
	if !(<-tr.Inbox(1)).VoteGranted {
		t.Fatal("after persister recovers, candidate should be granted (clean rollback)")
	}
	f.raftMu.Lock()
	vf = f.votedFor
	f.raftMu.Unlock()
	if vf != 1 {
		t.Errorf("votedFor = %d after successful grant, want 1", vf)
	}
}

// TestMaybeStartElectionPersistFailureAborts pins the candidate-side rollback:
// if persisting the self-vote fails, every in-memory mutation (term, role,
// vote, tally) is rolled back, the node stays Follower, and — the load-bearing
// externalization check — NO RequestVote is sent to any peer.
func TestMaybeStartElectionPersistFailureAborts(t *testing.T) {
	tr := NewChannelTransport()
	tr.Register(0) // candidate-to-be
	tr.Register(1) // peer
	tr.Register(2) // peer
	fp := &fakePersister{failErr: errInjected}
	n := &Node{
		id: 0, transport: tr, peers: []NodeID{1, 2}, log: NewRaftLog(), stateFile: fp,
		role: Follower, currentTerm: 4, votedFor: noVote,
		electionResetCh: make(chan struct{}, 1),
	}
	n.appliedCond = sync.NewCond(&n.raftMu)

	n.maybeStartElection()

	n.raftMu.Lock()
	gotTerm, gotRole, gotVote, gotVotes := n.currentTerm, n.role, n.votedFor, n.votesReceived
	n.raftMu.Unlock()
	if gotTerm != 4 || gotRole != Follower || gotVote != noVote || gotVotes != 0 {
		t.Errorf("after aborted election: term=%d role=%v votedFor=%d votes=%d, want 4/follower/noVote/0",
			gotTerm, gotRole, gotVote, gotVotes)
	}
	if len(fp.saved) != 1 || fp.saved[0].currentTerm != 5 || fp.saved[0].votedFor != 0 {
		t.Errorf("save attempts = %+v, want exactly one for (term 5, self 0)", fp.saved)
	}
	for _, p := range []NodeID{1, 2} {
		select {
		case m := <-tr.Inbox(p):
			t.Errorf("peer %d received %+v; want nothing (election aborted before soliciting)", p, m)
		default:
		}
	}
}

// TestStepDownPersistFailureProceeds asymmetry: a term adoption whose persist
// fails still proceeds in memory (adopt term, revert to follower) rather than
// panicking or rolling back. This path only ever externalizes an idempotent
// ack, so the degraded (memory term > durable term) window is the bounded risk
// the design accepts. A vote response carrying a higher term is the cleanest
// trigger (step-down without a following vote).
func TestStepDownPersistFailureProceeds(t *testing.T) {
	fp := &fakePersister{failErr: errInjected}
	n := &Node{
		id: 0, log: NewRaftLog(), stateFile: fp,
		role: Candidate, currentTerm: 4, votedFor: 0, // candidate, voted self
		electionResetCh: make(chan struct{}, 1),
	}
	n.appliedCond = sync.NewCond(&n.raftMu)

	n.handleVoteResponse(Message{Type: MsgRequestVoteResponse, From: 1, Term: 9, VoteGranted: false})

	n.raftMu.Lock()
	gotTerm, gotRole, gotVote := n.currentTerm, n.role, n.votedFor
	n.raftMu.Unlock()
	if gotTerm != 9 || gotRole != Follower || gotVote != noVote {
		t.Errorf("after step-down with failing persist: term=%d role=%v votedFor=%d, want 9/follower/noVote",
			gotTerm, gotRole, gotVote)
	}
	if len(fp.saved) != 1 || fp.saved[0].currentTerm != 9 {
		t.Errorf("save attempts = %+v, want exactly one attempt at term 9 (attempted, not skipped)", fp.saved)
	}
}
