package cluster

// currentTerm is the fixed Raft term: with a single fixed leader and no
// election, every entry is created in term 1. Real per-entry terms and their
// persistence (introduced alongside leader election) live behind this
// constant, so the seam is in one place.
const currentTerm uint64 = 1

// logEntry is one Raft log entry: a committed transaction's encoded records
// (data records terminated by an OpCommit marker, all at one timestamp),
// tagged with the term in which the leader created it.
type logEntry struct {
	term  uint64
	bytes []byte
}

// RaftLog is an in-memory, 1-based Raft log. Index 0 is the empty sentinel
// (no entry, term 0); the first real entry is index 1. It is not internally
// synchronized — the owning Node guards it with raftMu, matching the rest of
// the Raft state.
type RaftLog struct {
	entries []logEntry // entries[i] holds log index i+1
}

// NewRaftLog returns an empty log (lastIndex 0).
func NewRaftLog() *RaftLog { return &RaftLog{} }

// lastIndex returns the highest index present, or 0 if the log is empty.
func (l *RaftLog) lastIndex() uint64 { return uint64(len(l.entries)) }

// has reports whether idx refers to a present entry (1..lastIndex). Index 0,
// the empty-log sentinel used by prevLogIndex checks, is always "present".
func (l *RaftLog) has(idx uint64) bool { return idx <= l.lastIndex() }

// term returns the term of the entry at idx. term(0) is 0 (the sentinel).
// Callers must ensure idx <= lastIndex; out-of-range indexes panic.
func (l *RaftLog) term(idx uint64) uint64 {
	if idx == 0 {
		return 0
	}
	return l.entries[idx-1].term
}

// entryAt returns the encoded bytes of the entry at idx (1-based). The slice
// is owned by the log; callers must not mutate it. Out-of-range indexes panic.
func (l *RaftLog) entryAt(idx uint64) []byte { return l.entries[idx-1].bytes }

// append adds bytes as a new entry in the given term and returns its index.
// The bytes are copied, so the caller may reuse the buffer.
func (l *RaftLog) append(term uint64, bytes []byte) uint64 {
	l.entries = append(l.entries, logEntry{term: term, bytes: append([]byte(nil), bytes...)})
	return l.lastIndex()
}

// truncateFrom drops every entry with index >= idx (idx >= 1), discarding a
// conflicting suffix. truncateFrom(lastIndex+1) and idx past the end are
// no-ops. No divergence occurs under a single fixed leader, but suffix
// truncation is part of the figure-2 AppendEntries contract.
func (l *RaftLog) truncateFrom(idx uint64) {
	if idx == 0 {
		idx = 1
	}
	if idx <= l.lastIndex() {
		l.entries = l.entries[:idx-1]
	}
}

// matchesPrev reports whether the log is consistent with a leader's
// prevLogIndex/prevLogTerm: prevLogIndex 0 always matches (start of log),
// otherwise the entry at prevLogIndex must exist and carry prevLogTerm.
func (l *RaftLog) matchesPrev(prevLogIndex, prevLogTerm uint64) bool {
	if prevLogIndex == 0 {
		return true
	}
	return l.has(prevLogIndex) && l.term(prevLogIndex) == prevLogTerm
}
