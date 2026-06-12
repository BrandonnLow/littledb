package cluster

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/BrandonnLow/littledb/internal/db"
)

// Node wraps a db.DB with a Raft-style log and a transport endpoint. Every
// node maintains an in-memory log (1-based index), a commit index (highest
// entry known committed), and a last-applied index (highest entry applied to
// the memtable). Appending to the log and applying to the memtable are
// separate steps: a follower appends an entry on receipt (MsgReplicate) and
// applies it only once the leader's commit index covers it (MsgCommit). A
// per-node apply loop performs the apply. Roles are fixed at construction
// (node 0 is the leader).
type Node struct {
	id        NodeID
	store     *db.DB
	transport Transport
	peers     []NodeID
	isLeader  bool

	inbox <-chan Message
	ackCh chan Message // leader: append-acks routed here for the commit loop
	quit  chan struct{}
	wg    sync.WaitGroup

	// Raft state, guarded by raftMu. raftMu and the store's db.mu are never
	// held simultaneously: applies happen outside raftMu.
	raftMu      sync.Mutex
	log         *RaftLog // 1-based Raft log (entry bytes + term)
	commitIndex uint64
	lastApplied uint64
	appliedCond *sync.Cond // broadcast when lastApplied advances

	applySignal chan struct{} // coalescing wake for the apply loop

	// commitMu serializes the leader's commits end-to-end, so each commit is
	// fully applied before the next one's conflict check (in PrepareCommit,
	// which reads the memtable) runs. Leader-only.
	commitMu sync.Mutex
}

// ID returns the node's id.
func (n *Node) ID() NodeID { return n.id }

// DB returns the node's underlying store (for inspection and tests).
func (n *Node) DB() *db.DB { return n.store }

func (n *Node) lastIndex() uint64 {
	n.raftMu.Lock()
	defer n.raftMu.Unlock()
	return n.log.lastIndex()
}

func (n *Node) appliedIndex() uint64 {
	n.raftMu.Lock()
	defer n.raftMu.Unlock()
	return n.lastApplied
}

func (n *Node) commitIndexValue() uint64 {
	n.raftMu.Lock()
	defer n.raftMu.Unlock()
	return n.commitIndex
}

func (n *Node) start() {
	n.appliedCond = sync.NewCond(&n.raftMu)
	n.applySignal = make(chan struct{}, 1)
	n.wg.Add(2)
	go n.run()
	go n.applyLoop()
}

func (n *Node) stop() {
	close(n.quit)
	n.wg.Wait()
}

func (n *Node) signalApply() {
	select {
	case n.applySignal <- struct{}{}:
	default:
	}
}

// run drains the inbox. A follower appends replicate requests and advances
// its commit index on commit messages; the leader routes acks to its commit
// loop.
func (n *Node) run() {
	defer n.wg.Done()
	for {
		select {
		case <-n.quit:
			return
		case m := <-n.inbox:
			switch m.Type {
			case MsgReplicate:
				n.handleReplicate(m)
			case MsgCommit:
				n.handleCommit(m)
			case MsgAck:
				// Non-blocking: acks arriving between commits (when nothing is
				// draining) are dropped; the active commit is always draining.
				select {
				case n.ackCh <- m:
				default:
				}
			}
		}
	}
}

// handleReplicate (follower) appends the entry to the WAL and the in-memory
// log, then acks. It does NOT apply — apply waits for a MsgCommit that
// advances the commit index past this entry.
func (n *Node) handleReplicate(m Message) {
	if err := n.store.AppendToLog(m.Entry); err != nil {
		_ = n.transport.Send(m.From, Message{Type: MsgAck, From: n.id, Index: m.Index, OK: false})
		return
	}
	n.raftMu.Lock()
	// FIFO transport + fixed leader means entries arrive in order
	// with no gaps, so the appended index is lastIndex()+1 == m.Index.
	idx := n.log.append(currentTerm, m.Entry)
	n.raftMu.Unlock()
	_ = n.transport.Send(m.From, Message{Type: MsgAck, From: n.id, Index: idx, OK: true})
}

// handleCommit (follower) advances the commit index (bounded by what it has
// logged) and wakes the apply loop.
func (n *Node) handleCommit(m Message) {
	n.raftMu.Lock()
	ci := m.CommitIndex
	if last := n.log.lastIndex(); ci > last {
		ci = last
	}
	if ci > n.commitIndex {
		n.commitIndex = ci
	}
	n.raftMu.Unlock()
	n.signalApply()
}

// applyLoop applies committed-but-unapplied entries to the memtable.
func (n *Node) applyLoop() {
	defer n.wg.Done()
	for {
		select {
		case <-n.quit:
			return
		case <-n.applySignal:
			n.applyCommitted()
		}
	}
}

func (n *Node) applyCommitted() {
	for {
		n.raftMu.Lock()
		if n.lastApplied >= n.commitIndex {
			n.raftMu.Unlock()
			return
		}
		idx := n.lastApplied + 1
		entry := n.log.entryAt(idx)
		n.raftMu.Unlock()

		// Apply outside raftMu (ApplyEntry takes db.mu).
		if err := n.store.ApplyEntry(entry); err != nil {
			panic(fmt.Sprintf("cluster: node %d apply entry %d: %v", n.id, idx, err))
		}

		n.raftMu.Lock()
		n.lastApplied = idx
		n.appliedCond.Broadcast()
		n.raftMu.Unlock()
	}
}

// commit is the leader's commitOverride. It prepares the txn, appends the
// entry to its log, replicates it, advances the commit index once a quorum
// has appended it, and waits for its own apply before returning. commitMu
// serializes commits so PrepareCommit always sees the previous commit applied.
func (n *Node) commit(t *db.Txn) error {
	n.commitMu.Lock()
	defer n.commitMu.Unlock()

	entry, _, err := n.store.PrepareCommit(t)
	if err != nil {
		return err
	}
	if entry == nil {
		return nil // empty txn: nothing to replicate
	}

	// Append to the leader's own log (WAL + in-memory), assigning index idx.
	if err := n.store.AppendToLog(entry); err != nil {
		return fmt.Errorf("cluster: leader append to log: %w", err)
	}
	n.raftMu.Lock()
	idx := n.log.append(currentTerm, entry)
	n.raftMu.Unlock()

	// Replicate and wait for a quorum to append. Majority of N is
	// floor(N/2)+1; the leader's own copy counts as one, so it needs
	// floor((peers+1)/2) follower acks.
	for _, p := range n.peers {
		if err := n.transport.Send(p, Message{Type: MsgReplicate, From: n.id, Index: idx, Entry: entry}); err != nil {
			return fmt.Errorf("cluster: replicate to %d: %w", p, err)
		}
	}
	need := (len(n.peers) + 1) / 2
	for got := 0; got < need; {
		select {
		case m := <-n.ackCh:
			if m.Type == MsgAck && m.Index == idx && m.OK {
				got++
			}
		case <-n.quit:
			return errors.New("cluster: node stopped during replication")
		}
	}

	// Quorum has the entry. Advance the commit index, wake our own apply
	// loop, and tell followers to apply.
	n.raftMu.Lock()
	if idx > n.commitIndex {
		n.commitIndex = idx
	}
	n.raftMu.Unlock()
	n.signalApply()

	for _, p := range n.peers {
		_ = n.transport.Send(p, Message{Type: MsgCommit, From: n.id, CommitIndex: idx})
	}

	// Wait until our apply loop has applied this entry, so the write is
	// visible to subsequent reads on the leader before Commit returns.
	n.raftMu.Lock()
	for n.lastApplied < idx {
		n.appliedCond.Wait()
	}
	n.raftMu.Unlock()
	return nil
}

// Cluster is a fixed-leader replication group over a transport.
type Cluster struct {
	transport Transport
	nodes     []*Node
	leader    NodeID
}

// New creates an n-node cluster with node 0 as the fixed leader, over an
// in-process channel transport. dirs must have length n.
func New(n int, dirs []string, opts db.Options) (*Cluster, error) {
	return NewWithTransport(n, dirs, opts, NewChannelTransport())
}

// NewWithTransport is New with a caller-supplied transport, allowing tests to
// interpose on message delivery (and, later, a network transport).
func NewWithTransport(n int, dirs []string, opts db.Options, tr Transport) (*Cluster, error) {
	if n < 1 {
		return nil, errors.New("cluster: need at least one node")
	}
	if len(dirs) != n {
		return nil, fmt.Errorf("cluster: need %d dirs, got %d", n, len(dirs))
	}

	for i := 0; i < n; i++ {
		tr.Register(NodeID(i))
	}

	c := &Cluster{transport: tr, leader: 0}
	for i := 0; i < n; i++ {
		store, err := db.OpenWith(dirs[i], opts)
		if err != nil {
			c.Close()
			return nil, fmt.Errorf("cluster: open node %d: %w", i, err)
		}
		var peers []NodeID
		for j := 0; j < n; j++ {
			if j != i {
				peers = append(peers, NodeID(j))
			}
		}
		c.nodes = append(c.nodes, &Node{
			id:        NodeID(i),
			store:     store,
			transport: tr,
			peers:     peers,
			isLeader:  NodeID(i) == c.leader,
			inbox:     tr.Inbox(NodeID(i)),
			ackCh:     make(chan Message, inboxBuffer),
			quit:      make(chan struct{}),
			log:       NewRaftLog(),
		})
	}

	// Install the leader's commit orchestration, then start every node.
	leaderNode := c.nodes[c.leader]
	leaderNode.store.SetCommitOverride(leaderNode.commit)
	for _, nd := range c.nodes {
		nd.start()
	}
	return c, nil
}

func (c *Cluster) leaderNode() *Node { return c.nodes[c.leader] }

// Put routes a write to the leader, which replicates it before returning.
func (c *Cluster) Put(key, value []byte) error { return c.leaderNode().store.Put(key, value) }

// Delete routes a delete to the leader.
func (c *Cluster) Delete(key []byte) error { return c.leaderNode().store.Delete(key) }

// Get reads from the leader (linearizable). Follower reads are a future add.
func (c *Cluster) Get(key []byte) ([]byte, error) { return c.leaderNode().store.Get(key) }

// Begin starts a transaction on the leader; its Commit drives replication.
func (c *Cluster) Begin() *db.Txn { return c.leaderNode().store.Begin() }

// Node returns the i-th node, for inspection and tests.
func (c *Cluster) Node(i int) *Node { return c.nodes[i] }

// Size returns the node count.
func (c *Cluster) Size() int { return len(c.nodes) }

// Leader returns the leader's id.
func (c *Cluster) Leader() NodeID { return c.leader }

// Quiesce waits until every node has applied through the leader's commit
// index, or the timeout elapses. The leader returns to the client once a
// majority has the entry logged and it has applied locally, so minority
// followers may still be draining their inboxes.
func (c *Cluster) Quiesce(timeout time.Duration) error {
	target := c.leaderNode().commitIndexValue()
	deadline := time.Now().Add(timeout)
	for {
		caughtUp := true
		for _, nd := range c.nodes {
			if nd.appliedIndex() < target {
				caughtUp = false
				break
			}
		}
		if caughtUp {
			return nil
		}
		if time.Now().After(deadline) {
			return errors.New("cluster: quiesce timed out")
		}
		time.Sleep(time.Millisecond)
	}
}

// Close stops all node loops and closes their stores. Call once.
func (c *Cluster) Close() error {
	for _, nd := range c.nodes {
		nd.stop()
	}
	var firstErr error
	for _, nd := range c.nodes {
		if err := nd.store.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
