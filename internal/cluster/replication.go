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
// advances, or this node becomes leader (immediate heartbeat). replSignal may
// gain keys concurrently (a server being added), so iterate under replMu.
func (n *Node) signalReplicators() {
	n.replMu.RLock()
	defer n.replMu.RUnlock()
	for _, ch := range n.replSignal {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// replicateTo is the per-follower replication loop, running on every node but
// active only while this node is the leader. It idles by blocking on its
// signal / quit — never spins on role. The signal channel is captured once (it is
// published before this goroutine starts and never changes), so the loop never
// re-reads the replSignal map and cannot race a concurrent peer addition.
func (n *Node) replicateTo(p NodeID) {
	defer n.wg.Done()
	// Capture p's signal and response channels once. Both are published before this
	// goroutine starts and never change, so reading them once avoids re-reading the
	// respCh/replSignal maps — which a concurrent peer addition (ensurePeerLocked)
	// may be writing — on every round.
	n.replMu.RLock()
	sig := n.replSignal[p]
	rc := n.respCh[p]
	n.replMu.RUnlock()
	for {
		select {
		case <-n.quit:
			return
		case <-sig:
		}
		if !n.sendLoop(p, rc) {
			return // quit observed mid-send
		}
	}
}

// sendLoop pushes AppendEntries to p until p is caught up (then returns true to
// await the next signal). Returns immediately if we are not the leader, so a
// follower's replicator just idles. Returns false only on quit. rc is p's captured
// response channel (see replicateTo).
func (n *Node) sendLoop(p NodeID, rc chan Message) bool {
	for {
		n.raftMu.Lock()
		if n.role != Leader {
			n.raftMu.Unlock()
			return true // not leader: idle until signalled again
		}
		term := n.currentTerm
		next := n.nextIndex[p]
		base := n.log.baseIndex
		if next-1 < base {
			// p's nextIndex has fallen into our compacted prefix — we no longer
			// hold the entry to continue from. Catch it up with a snapshot instead
			// of AppendEntries (term(prevLogIndex) below would index the compacted
			// prefix and panic).
			n.raftMu.Unlock()
			if !n.sendSnapshot(p, term, rc) {
				return false
			}
			n.raftMu.Lock()
			caughtUp := n.nextIndex[p] > n.log.lastIndex() || n.role != Leader
			n.raftMu.Unlock()
			if caughtUp {
				return true
			}
			continue
		}
		last := n.log.lastIndex()
		prevLogIndex := next - 1
		prevLogTerm := n.log.term(prevLogIndex)
		var entries []Entry
		for i := next; i <= last; i++ {
			entries = append(entries, Entry{Term: n.log.term(i), Kind: n.log.kindAt(i), Data: n.log.entryAt(i)})
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
		case resp = <-rc:
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
			n.maybeCompactLocked()
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
// to >= 1) after a prevLog mismatch. If the hint lands in our compacted prefix,
// sendLoop detects nextIndex-1 < baseIndex on the next round and switches to
// InstallSnapshot — the hint is no longer clamped up to the base here. Must
// hold raftMu.
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
	// Over the current voting configuration only. Self counts only if it is itself
	// a voting member — its match is its lastIndex, since it holds every entry it
	// appended. A leader that removed ITSELF from the configuration still drives
	// C_new to commitment but is no longer part of the majority that commits it, so
	// it must not count its own log toward the tally (else it would commit C_new one
	// ack early, under a majority that does not exist).
	peers := n.votingPeers()
	vals := make([]uint64, 0, len(peers)+1)
	if n.inConfigLocked() {
		vals = append(vals, n.log.lastIndex())
	}
	for _, p := range peers {
		vals = append(vals, n.matchIndex[p])
	}
	if len(vals) == 0 {
		return // no voters to form a quorum (degenerate); nothing can commit
	}
	sort.Slice(vals, func(i, j int) bool { return vals[i] > vals[j] })
	cand := vals[len(vals)/2] // highest index a strict majority of voters holds

	// cand >= baseIndex whenever term(cand) is evaluated: the guard requires
	// cand > commitIndex, and commitIndex >= baseIndex always (we only compact
	// up to safe <= lastApplied <= commitIndex). After a leadership change
	// resets matchIndex to 0, cand can be < baseIndex, but then cand <=
	// commitIndex short-circuits the term() call. So term(cand) never indexes
	// the compacted prefix.
	if cand > n.commitIndex && n.log.term(cand) == n.currentTerm {
		n.commitIndex = cand
		n.signalApply()
		n.signalReplicators()
	}
}

// maybeCompactLocked compacts the Raft log up to lastApplied once the in-memory
// suffix reaches SnapshotThreshold. It is no longer bounded by the slowest
// follower's matchIndex: a peer that falls below the new base is caught up via
// InstallSnapshot (sendLoop switches to a snapshot when its nextIndex enters the
// compacted prefix), so discarding entries a lagging follower still needs is
// safe. Runs on every node — the leader (from the replication path) and
// followers (from the apply loop) — each compacting its own applied prefix. The
// in-memory compactTo and the file rewrite happen together under raftMu; on a
// file error the file is unchanged and memory is left uncompacted, so the two
// stay mirrored. Must hold raftMu.
func (n *Node) maybeCompactLocked() {
	if n.cfg.SnapshotThreshold <= 0 || len(n.log.entries) < n.cfg.SnapshotThreshold {
		return
	}
	safe := n.lastApplied
	if safe <= n.log.baseIndex {
		return
	}
	newBaseTerm := n.log.term(safe)
	// Fold any configuration entry in the prefix we are about to discard into the
	// durable base config, so a membership change below the new base survives a
	// restart — after compaction the log no longer holds the entry to re-derive it
	// from. The config as of `safe` is the latest config entry at index <= safe, or
	// the existing base if none. Persist it BEFORE compacting: if we crash in
	// between, the base file reflects `safe` while the (still-full) log's latest
	// config entry re-derives the same configuration, so either order recovers
	// correctly; persisting first also means a persist failure aborts the compaction
	// with nothing lost.
	if n.configPath != "" {
		newBaseConfig := n.configAsOfLocked(safe)
		if !configEqual(newBaseConfig, n.baseConfig) {
			if err := writeBaseConfigFile(n.configPath, newBaseConfig, n.opts.SyncOnWrite); err != nil {
				return // couldn't persist the base config; don't compact past it
			}
			n.baseConfig = newBaseConfig
		}
	}
	if n.logFile != nil {
		if err := n.logFile.compact(safe, newBaseTerm, n.log.entriesAfter(safe)); err != nil {
			return // file unchanged; leave memory uncompacted to stay mirrored
		}
	}
	n.log.compactTo(safe, newBaseTerm)
}

// configAsOfLocked returns the configuration in effect at log index idx: the
// payload of the latest config entry at index <= idx (and above the compaction
// base), or a copy of the current base config if none survive in that range. Must
// hold raftMu.
func (n *Node) configAsOfLocked(idx uint64) map[NodeID]bool {
	for i := idx; i > n.log.baseIndex; i-- {
		if n.log.kindAt(i) == EntryConfig {
			return decodeConfig(n.log.entryAt(i))
		}
	}
	return copyConfig(n.baseConfig)
}
