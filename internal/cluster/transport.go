// Package cluster replicates a db.DB across N in-process nodes: a single
// fixed leader streams each committed transaction to followers and waits for
// a quorum before acknowledging to the client. Fixed leader, no election,
// no failover. "RPC" is a channel send.
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
	// MsgReplicate carries one txn's encoded records (data + OpCommit) from
	// the leader to a follower.
	MsgReplicate MsgType = iota
	// MsgAck is a follower's acknowledgement that it applied a replicate.
	MsgAck
)

// Message is a single inter-node message.
type Message struct {
	Type     MsgType
	From     NodeID
	CommitTS uint64
	Entry    []byte // MsgReplicate: the txn's encoded records
	OK       bool   // MsgAck: whether the apply succeeded
}

// Transport delivers messages between nodes. The Raft invariants are
// independent of transport, so the in-process channel implementation here is
// a real implementation, not a mock; a network transport is a future drop-in.
type Transport interface {
	Send(to NodeID, msg Message) error
	Inbox(self NodeID) <-chan Message
}

const inboxBuffer = 256

// ChannelTransport is an in-process Transport backed by one buffered channel
// per node. Per-node FIFO ordering is what guarantees followers apply
// replicated entries in commit order.
type ChannelTransport struct {
	mu      sync.Mutex
	inboxes map[NodeID]chan Message
}

// NewChannelTransport returns an empty transport. Register each node before
// sending to it.
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

// Send delivers msg to the target's inbox. It blocks only if that inbox's
// buffer is full, which under serialized commits does not happen.
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
