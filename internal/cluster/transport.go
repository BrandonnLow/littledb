// Package cluster replicates a db.DB across N in-process nodes using Raft.
// A leader appends each committed transaction to a Raft-style log, replicates
// it to followers via AppendEntries, and advances a commit index once a quorum
// has it. Appending to the log and applying to the state machine (the
// memtable) are separate: an entry is appended on receipt but applied only
// once known committed (carried by a later AppendEntries' leaderCommit).
// Leadership is elected: a randomized election timeout makes a follower stand
// for election, terms order leadership, and a node steps down on seeing a
// higher term. Node 0 bootstraps as leader at term 1.
package cluster

import (
	"fmt"
	"sync"
)

// NodeID identifies a node within a cluster.
type NodeID int

// MsgType tags a cluster message.
type MsgType int

const (
	// MsgAppendEntries is the leader's replication / heartbeat RPC: zero or
	// more entries to append after (PrevLogIndex, PrevLogTerm), plus the
	// leader's commit index. Zero entries makes it a pure heartbeat.
	MsgAppendEntries MsgType = iota
	// MsgAppendResponse is a follower's reply: Success with the MatchIndex it
	// now holds, or a rejection with a ConflictHint, or a Term higher than the
	// leader's (telling the leader to step down).
	MsgAppendResponse
	// MsgRequestVote is a candidate's request for a vote.
	MsgRequestVote
	// MsgRequestVoteResponse is a voter's reply: VoteGranted or not, plus its
	// term so a stale candidate steps down.
	MsgRequestVoteResponse
	// MsgInstallSnapshot carries a leader's snapshot to a follower whose
	// nextIndex has fallen into the leader's compacted prefix (so plain
	// AppendEntries can no longer reach it). It ships the complete logical state
	// as of (LastIncludedIndex, LastIncludedTerm), every pair stamped SnapshotTS.
	// Single-message in this stage; chunking is a later refinement.
	MsgInstallSnapshot
	// MsgInstallSnapshotResponse is the follower's ack: Success once the snapshot
	// is durably installed (MatchIndex = LastIncludedIndex), or a higher Term so
	// a stale leader steps down.
	MsgInstallSnapshotResponse
)

// KVPair is one live key/value pair on the wire in an InstallSnapshot. Keys are
// in ascending order; the follower copies what it keeps (the channel transport
// shares slices in-process).
type KVPair struct {
	Key   []byte
	Value []byte
}

// EntryKind tags what a Raft log entry carries. Most entries are data (an encoded
// transaction); a config entry carries a membership change, which the Raft layer
// interprets itself (adopting it on append) rather than applying to the state
// machine.
type EntryKind uint8

const (
	EntryData   EntryKind = iota // an encoded transaction: data records + OpCommit
	EntryConfig                  // a membership configuration (encoded voter set)
)

// Entry is one log entry on the wire: its kind, the payload bytes, and the term in
// which its leader created it. The term must travel with the entry so a follower
// can store it correctly and detect a conflicting (same-index, different-term)
// entry during divergence repair; the kind must travel so a follower adopts a
// config entry on receipt rather than trying to apply it as a transaction.
type Entry struct {
	Term uint64
	Kind EntryKind
	Data []byte
}

// Message is a single inter-node message. Fields are grouped by message type;
// only those relevant to Type are set. Term is the sender's currentTerm on
// every message and drives election / step-down.
type Message struct {
	Type MsgType
	From NodeID
	Term uint64

	// MsgAppendEntries (leader -> follower):
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []Entry // read-only; the follower copies what it keeps
	LeaderCommit uint64

	// MsgAppendResponse (follower -> leader):
	Success      bool
	MatchIndex   uint64 // on success: highest index the follower now holds
	ConflictHint uint64 // on reject: index the leader should set nextIndex to

	// MsgRequestVote (candidate -> peers):
	CandidateID  NodeID
	LastLogIndex uint64
	LastLogTerm  uint64

	// MsgRequestVoteResponse (voter -> candidate):
	VoteGranted bool

	// MsgInstallSnapshot (leader -> far-behind follower):
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	SnapshotTS        uint64
	Snapshot          []KVPair // read-only; the follower copies what it keeps
}

// Transport delivers messages between nodes. The Raft invariants are
// independent of transport, so the in-process channel implementation is a
// real implementation, not a mock; a network transport is a future drop-in.
type Transport interface {
	Register(id NodeID)
	Send(to NodeID, msg Message) error
	Inbox(self NodeID) <-chan Message
}

const inboxBuffer = 256

// ChannelTransport is an in-process Transport backed by one buffered channel
// per node. Per-node FIFO ordering guarantees followers see entries in log
// order.
type ChannelTransport struct {
	mu      sync.Mutex
	inboxes map[NodeID]chan Message
}

// NewChannelTransport returns an empty transport.
func NewChannelTransport() *ChannelTransport {
	return &ChannelTransport{inboxes: make(map[NodeID]chan Message)}
}

// Register creates an inbox for id if one does not already exist.
func (t *ChannelTransport) Register(id NodeID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.inboxes[id]; !ok {
		t.inboxes[id] = make(chan Message, inboxBuffer)
	}
}

// Send delivers msg to the target's inbox.
func (t *ChannelTransport) Send(to NodeID, msg Message) error {
	t.mu.Lock()
	ch, ok := t.inboxes[to]
	t.mu.Unlock()
	if !ok {
		return fmt.Errorf("cluster: no inbox registered for node %d", to)
	}
	ch <- msg
	return nil
}

// Inbox returns the receive end of self's inbox.
func (t *ChannelTransport) Inbox(self NodeID) <-chan Message {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.inboxes[self]
}
