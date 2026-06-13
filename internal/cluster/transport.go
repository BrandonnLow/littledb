// Package cluster replicates a db.DB across N in-process nodes: a single
// fixed leader appends each committed transaction to a Raft-style log,
// replicates it to followers via AppendEntries, and advances a commit index
// once a quorum has it. Appending to the log and applying to the state machine
// (the memtable) are separate steps: an entry is appended on receipt but
// applied only once known committed (carried by a later AppendEntries'
// leaderCommit). Fixed leader, no election.
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
	// MsgAppendEntries is the leader's replication RPC: it carries zero or
	// more log entries to append after (PrevLogIndex, PrevLogTerm), plus the
	// leader's commit index. Zero entries makes it a heartbeat that only
	// propagates LeaderCommit.
	MsgAppendEntries MsgType = iota
	// MsgAppendResponse is a follower's reply: Success with the MatchIndex it
	// now holds, or a rejection with a ConflictHint telling the leader where
	// to back up.
	MsgAppendResponse
)

// Message is a single inter-node message. Fields are grouped by direction;
// only those relevant to Type are set.
type Message struct {
	Type MsgType
	From NodeID
	Term uint64 // currentTerm; carried for election readiness

	// MsgAppendEntries (leader -> follower):
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      [][]byte // read-only; the follower copies what it keeps
	LeaderCommit uint64

	// MsgAppendResponse (follower -> leader):
	Success      bool
	MatchIndex   uint64 // on success: highest index the follower now holds
	ConflictHint uint64 // on reject: index the leader should set nextIndex to
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
