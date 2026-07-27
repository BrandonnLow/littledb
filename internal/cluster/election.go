package cluster

import (
	"math/rand"
	"sort"
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

// majority is the quorum size of the current configuration (a strict majority of
// the voting members, self included). A nil config falls back to the fixed peer
// set, for bare-Node white-box tests. Must hold raftMu (reads config).
func (n *Node) majority() int {
	if n.config == nil {
		return (len(n.peers)+1)/2 + 1
	}
	return len(n.config)/2 + 1
}

// votingPeers returns the current voting members other than this node, in id
// order. Derived from config, so it shrinks and grows as membership changes,
// unlike the fixed peers slice (which remains the transport-level universe of
// nodes this process knows how to reach). A nil config falls back to the peer
// set. Must hold raftMu.
func (n *Node) votingPeers() []NodeID {
	if n.config == nil {
		return n.peers
	}
	out := make([]NodeID, 0, len(n.config))
	for p := range n.config {
		if p != n.id {
			out = append(out, p)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// withinLeaderStickinessLocked reports whether we heard from a current leader too
// recently to entertain a vote request — within one minimum election timeout. It
// is the predicate behind disruption prevention (§4.2.3). A zero lastLeaderContact
// (never heard a leader) or a zero ElectionMin (bare-Node white-box tests) makes
// it false, so those paths behave exactly as before. Must hold raftMu.
func (n *Node) withinLeaderStickinessLocked() bool {
	if n.lastLeaderContact.IsZero() || n.cfg.ElectionMin <= 0 {
		return false
	}
	return time.Since(n.lastLeaderContact) < n.cfg.ElectionMin
}

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
	// Ensure a replicator exists for every voting member, including any added since
	// this node was constructed (its replication state and goroutine may not exist
	// yet), before initializing their nextIndex/matchIndex.
	n.reconcilePeersLocked()
	last := n.log.lastIndex()
	for _, p := range n.votingPeers() {
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
	if n.installing {
		// Mid-install: we are deliberately behind and catching up via a snapshot.
		// Standing for election now would be wrong (and our log is in flux), so
		// defer — the install resets the timer on completion.
		n.raftMu.Unlock()
		return
	}
	if !n.inConfigLocked() {
		// We are not a voting member of our own configuration (e.g. we were removed,
		// or we are a leader that removed itself). Standing for election would let a
		// removed server win leadership back, so we stay a follower and go quiet. The
		// min-election-timeout rule (handleRequestVote) is the companion defense for a
		// removed server that has NOT yet learned it was removed.
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
	peers := n.votingPeers() // solicit only current voting members
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
	// Disruption prevention (Raft dissertation §4.2.3). If we heard from a current
	// leader within the minimum election timeout, a vote request is almost
	// certainly from a server that has been removed from the configuration — or
	// partitioned — and is campaigning with an ever-rising term. We IGNORE it
	// entirely: not merely declining the vote, but declining even to adopt its
	// (higher) term, so it cannot force the live leader to step down and thrash the
	// cluster. A leadership-transfer request (Stage 5) carries LeaderTransfer to
	// bypass this, since the current leader deliberately initiated it. We reply with
	// our current term (a stale candidate still steps down on it); a higher-term
	// disruptor simply gets no grant and keeps failing to win.
	if !m.LeaderTransfer && n.withinLeaderStickinessLocked() {
		term := n.currentTerm
		n.raftMu.Unlock()
		_ = n.transport.Send(m.From, Message{
			Type: MsgRequestVoteResponse, From: n.id, Term: term, VoteGranted: false,
		})
		return
	}
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
