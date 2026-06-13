package cluster

import "sort"

// signalReplicators wakes every follower's replication goroutine. Non-blocking
// and coalescing: a goroutine that is mid-send picks up the latest state on
// its next iteration regardless. Called when a new entry is appended or the
// commit index advances.
func (n *Node) signalReplicators() {
	for _, ch := range n.replSignal {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// replicateTo is the leader's per-follower replication loop. It sleeps until
// signalled (new entry, or advanced commit index), then pushes whatever the
// follower is missing — and, when there is nothing new to send, a heartbeat
// that still carries the current leaderCommit so the follower can apply.
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

// sendLoop sends AppendEntries to p until p is caught up on a successful
// response (then it returns true to await the next signal). On a rejection it
// backs nextIndex up to the follower's hint and retries immediately. Returns
// false if the node is stopping.
func (n *Node) sendLoop(p NodeID) bool {
	for {
		n.raftMu.Lock()
		next := n.nextIndex[p]
		last := n.log.lastIndex()
		prevLogIndex := next - 1
		prevLogTerm := n.log.term(prevLogIndex)
		var entries [][]byte
		for i := next; i <= last; i++ {
			entries = append(entries, n.log.entryAt(i)) // read-only; immutable once appended
		}
		leaderCommit := n.commitIndex
		n.raftMu.Unlock()

		msg := Message{
			Type: MsgAppendEntries, From: n.id, Term: currentTerm,
			PrevLogIndex: prevLogIndex, PrevLogTerm: prevLogTerm,
			Entries: entries, LeaderCommit: leaderCommit,
		}
		if err := n.transport.Send(p, msg); err != nil {
			return true // peer unreachable; wait for the next signal to retry
		}

		var resp Message
		select {
		case resp = <-n.respCh[p]:
		case <-n.quit:
			return false
		}

		n.raftMu.Lock()
		if resp.Success {
			n.onAppendSuccessLocked(p, prevLogIndex+uint64(len(entries)))
			n.maybeAdvanceCommitLocked()
			caughtUp := n.nextIndex[p] > n.log.lastIndex()
			n.raftMu.Unlock()
			if caughtUp {
				return true // nothing more to push; await the next signal
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
// to >= 1) after a prevLog mismatch, so the next AppendEntries probes from
// further back. Must hold raftMu.
func (n *Node) onAppendRejectLocked(p NodeID, hint uint64) {
	if hint < 1 {
		hint = 1
	}
	n.nextIndex[p] = hint
}

// maybeAdvanceCommitLocked recomputes the commit index as the highest log
// index replicated on a majority, and if it advances, wakes the apply loop AND
// re-signals the replicators. The re-signal is essential: it is what pushes a
// heartbeat carrying the new leaderCommit out to the followers (including the
// majority follower that just acked), so they apply too — without it the last
// committed entry would never apply anywhere but the leader. Must hold raftMu.
//
// The leader's own match is its lastIndex (it holds every entry it appended).
//
// A leader may only directly commit an entry from its CURRENT term, so guard
// the candidate with n.log.term(cand) == currentTerm before advancing.
// Unreachable today since every entry is currentTerm.
func (n *Node) maybeAdvanceCommitLocked() {
	total := len(n.peers) + 1
	vals := make([]uint64, 0, total)
	vals = append(vals, n.log.lastIndex())
	for _, p := range n.peers {
		vals = append(vals, n.matchIndex[p])
	}
	sort.Slice(vals, func(i, j int) bool { return vals[i] > vals[j] })
	cand := vals[total/2] // highest index a majority (incl. leader) holds

	if cand > n.commitIndex {
		n.commitIndex = cand
		n.signalApply()
		n.signalReplicators()
	}
}
