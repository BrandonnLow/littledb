package cluster

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/BrandonnLow/littledb/internal/db"
)

// Node wraps a db.DB with a Raft-style log and a transport endpoint. Every
// node keeps an in-memory log (1-based index), a commit index (highest entry
// known committed), and a last-applied index. Appending to the log and
// applying to the memtable are separate: a follower appends entries on receipt
// (AppendEntries) and applies them only once leaderCommit covers them. A
// per-node apply loop performs the apply. The leader additionally runs one
// replication goroutine per follower. Roles are fixed at construction (node 0
// is the leader).
type Node struct {
	id        NodeID
	store     *db.DB
	transport Transport
	peers     []NodeID
	isLeader  bool

	inbox <-chan Message
	quit  chan struct{}
	wg    sync.WaitGroup

	// Raft state, guarded by raftMu. raftMu and the store's db.mu are never
	// held simultaneously: applies and WAL appends happen outside raftMu.
	raftMu      sync.Mutex
	log         *RaftLog // 1-based Raft log (entry bytes + term)
	commitIndex uint64
	lastApplied uint64
	appliedCond *sync.Cond // broadcast when lastApplied advances

	// Leader-only replication state, guarded by raftMu. nextIndex/matchIndex
	// track each follower; respCh routes that follower's responses to its
	// replication goroutine; replSignal wakes the goroutine when there is new
	// work (an appended entry or an advanced commit index).
	nextIndex  map[NodeID]uint64
	matchIndex map[NodeID]uint64
	respCh     map[NodeID]chan Message
	replSignal map[NodeID]chan struct{}

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
	if n.isLeader {
		n.wg.Add(2 + len(n.peers))
		go n.run()
		go n.applyLoop()
		for _, p := range n.peers {
			go n.replicateTo(p)
		}
	} else {
		n.wg.Add(2)
		go n.run()
		go n.applyLoop()
	}
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

// run drains the inbox. A follower handles AppendEntries; the leader routes
// each AppendResponse to the owning follower's replication goroutine.
func (n *Node) run() {
	defer n.wg.Done()
	for {
		select {
		case <-n.quit:
			return
		case m := <-n.inbox:
			switch m.Type {
			case MsgAppendEntries:
				n.handleAppendEntries(m)
			case MsgAppendResponse:
				// Route to the follower's replication goroutine. Reliable
				// (no drop): with one AppendEntries in flight per follower,
				// dropping a response would stall that follower forever.
				ch := n.respCh[m.From]
				if ch == nil {
					continue
				}
				select {
				case ch <- m:
				case <-n.quit:
					return
				}
			}
		}
	}
}

// handleAppendEntries is the follower side of replication. On a prev-log
// mismatch it rejects with a back-up hint and touches neither the WAL nor the
// log. On a match it durably appends only the genuinely-new suffix (so WALs
// stay byte-identical across nodes), advances its commit index, and wakes its
// apply loop — apply itself stays deferred to the apply loop.
func (n *Node) handleAppendEntries(m Message) {
	n.raftMu.Lock()
	if !n.log.matchesPrev(m.PrevLogIndex, m.PrevLogTerm) {
		hint := n.log.lastIndex() + 1
		n.raftMu.Unlock()
		_ = n.transport.Send(m.From, Message{
			Type: MsgAppendResponse, From: n.id, Term: currentTerm,
			Success: false, ConflictHint: hint,
		})
		return
	}
	// Entries cover indices PrevLogIndex+1, +2, ... Keep only those past our
	// current end; ones we already hold are skipped (idempotent — under a
	// fixed leader an existing index always carries the same term).
	last := n.log.lastIndex()
	var toAppend [][]byte
	for i, e := range m.Entries {
		if m.PrevLogIndex+uint64(i)+1 > last {
			toAppend = append(toAppend, e)
		}
	}
	matchIndex := m.PrevLogIndex + uint64(len(m.Entries))
	n.raftMu.Unlock()

	// Durably append the new suffix (each call takes db.mu) outside raftMu.
	for _, e := range toAppend {
		if err := n.store.AppendToLog(e); err != nil {
			_ = n.transport.Send(m.From, Message{
				Type: MsgAppendResponse, From: n.id, Term: currentTerm,
				Success: false, ConflictHint: n.lastIndex() + 1,
			})
			return
		}
	}

	n.raftMu.Lock()
	for _, e := range toAppend {
		n.log.append(currentTerm, e)
	}
	ci := m.LeaderCommit
	if li := n.log.lastIndex(); ci > li {
		ci = li
	}
	if ci > n.commitIndex {
		n.commitIndex = ci
	}
	n.raftMu.Unlock()
	n.signalApply()

	_ = n.transport.Send(m.From, Message{
		Type: MsgAppendResponse, From: n.id, Term: currentTerm,
		Success: true, MatchIndex: matchIndex,
	})
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
// entry to its log, wakes the replication goroutines, and waits for its own
// apply before returning. commitMu serializes commits so PrepareCommit always
// sees the previous commit applied.
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

	if err := n.store.AppendToLog(entry); err != nil {
		return fmt.Errorf("cluster: leader append to log: %w", err)
	}

	n.raftMu.Lock()
	idx := n.log.append(currentTerm, entry)
	// Drive the commit index ourselves: for n=1 there are no replication
	// goroutines to advance it, so this commits immediately; for n>1 the
	// followers' matchIndex are still behind, so this is a no-op until their
	// responses arrive.
	n.maybeAdvanceCommitLocked()
	n.raftMu.Unlock()

	n.signalReplicators()

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
		nd := &Node{
			id:        NodeID(i),
			store:     store,
			transport: tr,
			peers:     peers,
			isLeader:  NodeID(i) == c.leader,
			inbox:     tr.Inbox(NodeID(i)),
			quit:      make(chan struct{}),
			log:       NewRaftLog(),
		}
		if nd.isLeader {
			nd.nextIndex = make(map[NodeID]uint64, len(peers))
			nd.matchIndex = make(map[NodeID]uint64, len(peers))
			nd.respCh = make(map[NodeID]chan Message, len(peers))
			nd.replSignal = make(map[NodeID]chan struct{}, len(peers))
			for _, p := range peers {
				nd.nextIndex[p] = nd.log.lastIndex() + 1 // empty log => 1
				nd.matchIndex[p] = 0
				nd.respCh[p] = make(chan Message, 1)
				nd.replSignal[p] = make(chan struct{}, 1)
			}
		}
		c.nodes = append(c.nodes, nd)
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
