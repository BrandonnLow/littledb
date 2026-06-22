package cluster

import (
	"math/rand"
	"time"
)

// Role is a node's current Raft role.
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "unknown"
	}
}

// noVote is the votedFor sentinel meaning "have not voted this term".
const noVote NodeID = -1

func (n *Node) majority() int { return (len(n.peers)+1)/2 + 1 }

func (n *Node) randomElectionTimeout() time.Duration {
	span := n.cfg.ElectionMax - n.cfg.ElectionMin
	return n.cfg.ElectionMin + time.Duration(rand.Int63n(int64(span)+1))
}

// resetElectionTimer pokes the election-timer goroutine to restart its clock.
// Called when we hear from the current leader, grant a vote, or start an
// election. Non-blocking and coalescing.
func (n *Node) resetElectionTimer() {
	select {
	case n.electionResetCh <- struct{}{}:
	default:
	}
}

// becomeLeaderLocked promotes this node to leader for its current term:
// it (re)initializes per-follower replication state, and fires an immediate
// heartbeat so followers reset their election timers before any of them can
// time out and start a competing election. Must hold raftMu.
func (n *Node) becomeLeaderLocked() {
	n.role = Leader
	last := n.log.lastIndex()
	for _, p := range n.peers {
		n.nextIndex[p] = last + 1
		n.matchIndex[p] = 0
	}
	n.signalReplicators() // immediate heartbeat: assert leadership now
}

// persistHardStateLocked durably records (currentTerm, votedFor). A nil
// stateFile (bare-Node white-box tests) is a no-op. Must hold raftMu, paired
// with the in-memory mutation so disk never lags the field the node acts on.
func (n *Node) persistHardStateLocked() error {
	if n.stateFile == nil {
		return nil
	}
	return n.stateFile.save(n.currentTerm, n.votedFor)
}

// stepDownLocked adopts a higher term and reverts to follower. It wakes any
// commit waiter (a commit in flight can no longer complete here) and resets
// the election timer. Must hold raftMu.
func (n *Node) stepDownLocked(term uint64) {
	n.currentTerm = term
	n.role = Follower
	n.votedFor = noVote
	// Persist the adopted term + reset vote before the node acts on the new
	// term. A failure here degrades to under-approximation (in-memory term may
	// exceed the durable term); it never externalizes a vote, only an idempotent
	// ack, so we proceed rather than panic. A vote cast afterwards re-persists
	// the full hard state and IS gated on success.
	_ = n.persistHardStateLocked()
	n.appliedCond.Broadcast()
	n.resetElectionTimer()
}

// electionTimer runs on every node. While not leader, it starts an election
// when the timeout elapses without a reset (no heartbeat from a live leader).
// It idles by blocking on the timer / reset / quit — never spins on role.
func (n *Node) electionTimer() {
	defer n.wg.Done()
	timer := time.NewTimer(n.randomElectionTimeout())
	defer timer.Stop()
	for {
		select {
		case <-n.quit:
			return
		case <-n.electionResetCh:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(n.randomElectionTimeout())
		case <-timer.C:
			n.maybeStartElection()
			timer.Reset(n.randomElectionTimeout())
		}
	}
}

// maybeStartElection becomes a candidate for a new term and solicits votes,
// unless this node is already the leader. The RequestVotes are sent outside
// raftMu (Send may block on a slow inbox).
func (n *Node) maybeStartElection() {
	n.raftMu.Lock()
	if n.role == Leader {
		n.raftMu.Unlock()
		return
	}
	prevTerm, prevRole, prevVote, prevVotes := n.currentTerm, n.role, n.votedFor, n.votesReceived
	n.currentTerm++
	n.role = Candidate
	n.votedFor = n.id
	n.votesReceived = 1 // vote for self
	// Our own vote must be durable before we solicit others' — the same rule as
	// a granted vote. On persist failure, roll back every in-memory mutation so
	// the term cannot run ahead of disk, stay Follower, and retry next timeout.
	if err := n.persistHardStateLocked(); err != nil {
		n.currentTerm, n.role, n.votedFor, n.votesReceived = prevTerm, prevRole, prevVote, prevVotes
		n.raftMu.Unlock()
		return
	}
	term := n.currentTerm
	lastIndex := n.log.lastIndex()
	lastTerm := n.log.lastTerm()
	peers := n.peers
	n.raftMu.Unlock()

	n.resetElectionTimer() // give this election a fresh window

	for _, p := range peers {
		_ = n.transport.Send(p, Message{
			Type: MsgRequestVote, From: n.id, Term: term,
			CandidateID: n.id, LastLogIndex: lastIndex, LastLogTerm: lastTerm,
		})
	}
}

// handleRequestVote is the voter side. It adopts a higher term (stepping
// down), rejects a stale term, and otherwise grants its vote at most once per
// term and only to a candidate whose log is at least as up-to-date as its own
// (Raft §5.4.1). Runs in the inbox goroutine.
func (n *Node) handleRequestVote(m Message) {
	n.raftMu.Lock()
	if m.Term > n.currentTerm {
		n.stepDownLocked(m.Term)
	}
	grant := false
	if m.Term == n.currentTerm &&
		(n.votedFor == noVote || n.votedFor == m.CandidateID) &&
		n.candidateUpToDateLocked(m.LastLogIndex, m.LastLogTerm) {
		prevVote := n.votedFor
		n.votedFor = m.CandidateID
		// The vote must be durable before the grant is sent: a crash between
		// reply and fsync would re-open the double-vote window. On persist
		// failure, roll the vote back and decline rather than grant something we
		// cannot guarantee survived.
		if err := n.persistHardStateLocked(); err != nil {
			n.votedFor = prevVote
		} else {
			grant = true
			n.resetElectionTimer()
		}
	}
	term := n.currentTerm
	n.raftMu.Unlock()

	_ = n.transport.Send(m.From, Message{
		Type: MsgRequestVoteResponse, From: n.id, Term: term, VoteGranted: grant,
	})
}

// candidateUpToDateLocked reports whether a candidate's (lastIndex, lastTerm)
// is at least as up-to-date as our log: a higher last term wins, or an equal
// last term with an index at least as high. Must hold raftMu.
func (n *Node) candidateUpToDateLocked(candIndex, candTerm uint64) bool {
	myTerm := n.log.lastTerm()
	if candTerm != myTerm {
		return candTerm > myTerm
	}
	return candIndex >= n.log.lastIndex()
}

// handleVoteResponse tallies a vote for the current election. On a majority
// the candidate becomes leader; a higher term in the reply makes it step down.
// Runs in the inbox goroutine.
func (n *Node) handleVoteResponse(m Message) {
	n.raftMu.Lock()
	defer n.raftMu.Unlock()
	if m.Term > n.currentTerm {
		n.stepDownLocked(m.Term)
		return
	}
	if n.role != Candidate || m.Term != n.currentTerm {
		return // stale: from an old election or we already moved on
	}
	if m.VoteGranted {
		n.votesReceived++
		if n.votesReceived >= n.majority() {
			n.becomeLeaderLocked()
		}
	}
}

// heartbeatTicker drives the leader's periodic heartbeat: every interval, if
// we are the leader, wake the replicators to push a (possibly empty)
// AppendEntries so followers keep their election timers reset. Idles by
// blocking on the ticker / quit.
func (n *Node) heartbeatTicker() {
	defer n.wg.Done()
	t := time.NewTicker(n.cfg.Heartbeat)
	defer t.Stop()
	for {
		select {
		case <-n.quit:
			return
		case <-t.C:
			n.raftMu.Lock()
			isLeader := n.role == Leader
			n.raftMu.Unlock()
			if isLeader {
				n.signalReplicators()
			}
		}
	}
}
