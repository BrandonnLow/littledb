package cluster

// logEntry is one Raft log entry: a committed transaction's encoded records
// (data records terminated by an OpCommit marker, all at one timestamp),
// tagged with the term in which the leader created it.
type logEntry struct {
	term  uint64
	bytes []byte
}

// RaftLog is an in-memory, 1-based Raft log with a compaction base. Entries
// with index <= baseIndex have been compacted away (they are committed and
// durable in the data WAL); baseIndex is the new sentinel, replacing the old
// index-0 sentinel. entries[i] holds log index baseIndex+1+i. A fresh log has
// baseIndex 0 and baseTerm 0, so index 0 is the empty-log sentinel as before.
// It is not internally synchronized — the owning Node guards it with raftMu.
type RaftLog struct {
	baseIndex uint64
	baseTerm  uint64
	entries   []logEntry // entries[i] holds log index baseIndex+1+i
}

// NewRaftLog returns an empty log (baseIndex 0, lastIndex 0).
func NewRaftLog() *RaftLog { return &RaftLog{} }

// NewRaftLogWithBase returns an empty log whose first real entry is baseIndex+1,
// with baseTerm the term of the (compacted) entry at baseIndex. Used at restart
// to reconstruct a previously-compacted log before its surviving suffix is
// appended on top.
func NewRaftLogWithBase(baseIndex, baseTerm uint64) *RaftLog {
	return &RaftLog{baseIndex: baseIndex, baseTerm: baseTerm}
}

// lastIndex returns the highest index present, or baseIndex if no entries
// remain above the base (0 for a fresh log).
func (l *RaftLog) lastIndex() uint64 { return l.baseIndex + uint64(len(l.entries)) }

// lastTerm returns the term of the last entry, or baseTerm if the log holds no
// entries above the base (0 for a fresh log).
func (l *RaftLog) lastTerm() uint64 { return l.term(l.lastIndex()) }

// has reports whether idx is a present, queryable index: baseIndex (the
// sentinel) through lastIndex. Compacted indices (< baseIndex) and indices past
// the end are absent.
func (l *RaftLog) has(idx uint64) bool { return idx >= l.baseIndex && idx <= l.lastIndex() }

// term returns the term of the entry at idx. term(baseIndex) is baseTerm (for a
// fresh log, term(0)=0, the old sentinel). idx < baseIndex panics: those entries
// are compacted and callers must not reach them (the commit rule and the
// replication bounds guarantee this). idx > lastIndex panics (out of range).
func (l *RaftLog) term(idx uint64) uint64 {
	if idx == l.baseIndex {
		return l.baseTerm
	}
	if idx < l.baseIndex {
		panic("raftlog: term of compacted index")
	}
	return l.entries[idx-l.baseIndex-1].term
}

// entryAt returns the encoded bytes of the entry at idx (must be > baseIndex and
// <= lastIndex). The slice is owned by the log; callers must not mutate it.
func (l *RaftLog) entryAt(idx uint64) []byte { return l.entries[idx-l.baseIndex-1].bytes }

// append adds bytes as a new entry in the given term and returns its index. The
// bytes are copied, so the caller may reuse the buffer.
func (l *RaftLog) append(term uint64, bytes []byte) uint64 {
	l.entries = append(l.entries, logEntry{term: term, bytes: append([]byte(nil), bytes...)})
	return l.lastIndex()
}

// truncateFrom drops every entry with index >= idx, discarding a conflicting
// suffix. idx <= baseIndex is a no-op (compacted/committed entries are never
// truncated); idx past the end is a no-op. Conflict repair only ever targets
// uncommitted entries (index > commitIndex >= baseIndex), so the guard is
// defensive.
func (l *RaftLog) truncateFrom(idx uint64) {
	if idx <= l.baseIndex {
		return
	}
	if idx <= l.lastIndex() {
		l.entries = l.entries[:idx-l.baseIndex-1]
	}
}

// matchesPrev reports whether the log is consistent with a leader's
// prevLogIndex/prevLogTerm. prevLogIndex < baseIndex is trusted as a match
// (those entries are committed, so agreement is guaranteed — reachable only
// across leadership changes once snapshots install a high base in stage 2).
// prevLogIndex == baseIndex matches iff prevLogTerm == baseTerm. Otherwise the
// entry at prevLogIndex must exist and carry prevLogTerm.
func (l *RaftLog) matchesPrev(prevLogIndex, prevLogTerm uint64) bool {
	if prevLogIndex < l.baseIndex {
		return true
	}
	if prevLogIndex == l.baseIndex {
		return prevLogTerm == l.baseTerm
	}
	return prevLogIndex <= l.lastIndex() && l.term(prevLogIndex) == prevLogTerm
}

// compactTo discards every entry with index <= newBaseIndex and sets the new
// base. newBaseTerm must be the term of the entry at newBaseIndex (the caller
// reads it via term() before compacting). The surviving suffix is copied into a
// fresh backing array so the old (larger) array is released — a bare reslice
// would keep it alive and defeat the point. newBaseIndex outside (baseIndex,
// lastIndex] is a no-op.
func (l *RaftLog) compactTo(newBaseIndex, newBaseTerm uint64) {
	if newBaseIndex <= l.baseIndex || newBaseIndex > l.lastIndex() {
		return
	}
	keep := l.entries[newBaseIndex-l.baseIndex:]
	fresh := make([]logEntry, len(keep))
	copy(fresh, keep)
	l.entries = fresh
	l.baseIndex = newBaseIndex
	l.baseTerm = newBaseTerm
}

// resetToBase discards ALL entries and sets the base to (newBaseIndex,
// newBaseTerm) — an index this log need not currently hold. It is the snapshot
// analogue of compactTo: compactTo trims to an index the log has (reading its
// term from the surviving entry), whereas resetToBase installs a base learned
// from a leader's snapshot, where the node has none of the prefix. After it the
// log is empty above the new base and the next AppendEntries appends from
// newBaseIndex+1. Used only on the InstallSnapshot path.
func (l *RaftLog) resetToBase(newBaseIndex, newBaseTerm uint64) {
	l.entries = nil
	l.baseIndex = newBaseIndex
	l.baseTerm = newBaseTerm
}

// entriesAfter returns the entries with index > idx (idx in [baseIndex,
// lastIndex]) as persistedEntry values, in index order — for rewriting the
// compacted Raft log file. The data slices are shared (not copied); the caller
// only reads them.
func (l *RaftLog) entriesAfter(idx uint64) []persistedEntry {
	start := idx - l.baseIndex
	out := make([]persistedEntry, 0, uint64(len(l.entries))-start)
	for i := start; i < uint64(len(l.entries)); i++ {
		out = append(out, persistedEntry{term: l.entries[i].term, data: l.entries[i].bytes})
	}
	return out
}
