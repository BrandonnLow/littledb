package cluster

import (
	"sort"
	"time"
)

// appendResponseTimeout bounds how long a replication round waits for a
// follower's reply. A lost reply (e.g. a partitioned follower) is abandoned and
// retried on the next heartbeat rather than stalling the replicator forever.
// Far longer than a healthy in-process round trip, so it only fires on loss.
const appendResponseTimeout = 250 * time.Millisecond

// signalReplicators wakes every follower's replication goroutine. Non-blocking
// and coalescing. Called when a new entry is appended, the commit index
// advances, or this node becomes leader (immediate heartbeat).
func (n *Node) signalReplicators() {
	for _, ch := range n.replSignal {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// replicateTo is the per-follower replication loop, running on every node but
// active only while this node is the leader. It idles by blocking on its
// signal / quit — never spins on role.
func (n *Node) replicateTo(p NodeID) {
	defer n.wg.Done()
	for {
		select {
		case <-n.quit:
			return
		case <-n.replSignal[p]:
		}
		if !n.sendLoop(p) {
			return // quit observed mid-send
		}
	}
}

// sendLoop pushes AppendEntries to p until p is caught up (then returns true to
// await the next signal). Returns immediately if we are not the leader, so a
// follower's replicator just idles. Returns false only on quit.
func (n *Node) sendLoop(p NodeID) bool {
	for {
		n.raftMu.Lock()
		if n.role != Leader {
			n.raftMu.Unlock()
			return true // not leader: idle until signalled again
		}
		term := n.currentTerm
		next := n.nextIndex[p]
		last := n.log.lastIndex()
		prevLogIndex := next - 1
		prevLogTerm := n.log.term(prevLogIndex)
		var entries []Entry
		for i := next; i <= last; i++ {
			entries = append(entries, Entry{Term: n.log.term(i), Data: n.log.entryAt(i)})
		}
		leaderCommit := n.commitIndex
		n.raftMu.Unlock()

		msg := Message{
			Type: MsgAppendEntries, From: n.id, Term: term,
			PrevLogIndex: prevLogIndex, PrevLogTerm: prevLogTerm,
			Entries: entries, LeaderCommit: leaderCommit,
		}
		if err := n.transport.Send(p, msg); err != nil {
			return true // peer unreachable; wait for the next signal to retry
		}

		var resp Message
		select {
		case resp = <-n.respCh[p]:
		case <-time.After(appendResponseTimeout):
			return true // reply lost (e.g. partition); retry on the next signal
		case <-n.quit:
			return false
		}

		n.raftMu.Lock()
		if resp.Term > n.currentTerm {
			n.stepDownLocked(resp.Term)
			n.raftMu.Unlock()
			return true
		}
		if n.role != Leader || n.currentTerm != term {
			n.raftMu.Unlock()
			return true // stepped down / term changed during the round trip
		}
		if resp.Success {
			n.onAppendSuccessLocked(p, prevLogIndex+uint64(len(entries)))
			n.maybeAdvanceCommitLocked()
			caughtUp := n.nextIndex[p] > n.log.lastIndex()
			n.raftMu.Unlock()
			if caughtUp {
				return true
			}
			// More entries appended during the round trip — keep going.
		} else {
			n.onAppendRejectLocked(p, resp.ConflictHint)
			n.raftMu.Unlock()
			// Backed up; retry immediately from the lower nextIndex.
		}
	}
}

// onAppendSuccessLocked records that p has replicated through matchIndex and
// advances its nextIndex. matchIndex only moves forward, so a stale or
// out-of-order success carrying an older value is absorbed without regressing.
// Must hold raftMu.
func (n *Node) onAppendSuccessLocked(p NodeID, matchIndex uint64) {
	if matchIndex > n.matchIndex[p] {
		n.matchIndex[p] = matchIndex
	}
	n.nextIndex[p] = n.matchIndex[p] + 1
}

// onAppendRejectLocked backs p's nextIndex up to the follower's hint (clamped
// to >= 1) after a prevLog mismatch. Must hold raftMu.
func (n *Node) onAppendRejectLocked(p NodeID, hint uint64) {
	if hint < 1 {
		hint = 1
	}
	n.nextIndex[p] = hint
}

// maybeAdvanceCommitLocked recomputes the commit index as the highest log
// index replicated on a majority, and if it advances, wakes the apply loop and
// re-signals the replicators (so a heartbeat carries the new leaderCommit out
// to followers — including the majority follower that just acked — and they
// apply too). Must hold raftMu.
//
// A leader may only directly commit an entry from its CURRENT term; a candidate
// index in an earlier term is not committed by replica count. Earlier-term entries
// below it commit indirectly, the moment a current-term entry does (committing
// index N commits everything <= N). The leader's own match is its lastIndex (it
// holds every entry it appended).
func (n *Node) maybeAdvanceCommitLocked() {
	total := len(n.peers) + 1
	vals := make([]uint64, 0, total)
	vals = append(vals, n.log.lastIndex())
	for _, p := range n.peers {
		vals = append(vals, n.matchIndex[p])
	}
	sort.Slice(vals, func(i, j int) bool { return vals[i] > vals[j] })
	cand := vals[total/2] // highest index a majority (incl. leader) holds

	if cand > n.commitIndex && n.log.term(cand) == n.currentTerm {
		n.commitIndex = cand
		n.signalApply()
		n.signalReplicators()
	}
}
