package cluster

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/BrandonnLow/littledb/internal/db"
)

// Node wraps a db.DB with a transport endpoint. Each node runs one goroutine
// draining its inbox: a follower applies replicate requests and acks them; a
// leader collects acks. Roles are fixed at construction.
type Node struct {
	id        NodeID
	store     *db.DB
	transport Transport
	peers     []NodeID // every other node
	isLeader  bool

	inbox <-chan Message
	ackCh chan Message
	quit  chan struct{}
	wg    sync.WaitGroup
}

// ID returns the node's id.
func (n *Node) ID() NodeID { return n.id }

// DB returns the node's underlying store (for inspection and tests).
func (n *Node) DB() *db.DB { return n.store }

func (n *Node) start() {
	n.wg.Add(1)
	go n.run()
}

func (n *Node) run() {
	defer n.wg.Done()
	for {
		select {
		case <-n.quit:
			return
		case m := <-n.inbox:
			switch m.Type {
			case MsgReplicate:
				// Follower: apply the leader's bytes, then acknowledge.
				ok := n.store.ApplyReplicated(m.Entry) == nil
				_ = n.transport.Send(m.From, Message{
					Type: MsgAck, From: n.id, CommitTS: m.CommitTS, OK: ok,
				})
			case MsgAck:
				// Leader: hand the ack to a waiting Replicate. A
				// non-blocking send drops stale acks that arrive between
				// commits (no Replicate is draining then); current acks are
				// always received because Replicate is actively waiting.
				select {
				case n.ackCh <- m:
				default:
				}
			}
		}
	}
}

func (n *Node) stop() {
	close(n.quit)
	n.wg.Wait()
}

// Replicate implements db.Replicator. It is called by the leader's Txn.Commit
// while holding db.mu, so at most one replication is in flight; acks are
// filtered by commitTS so a late ack from a previous commit is ignored.
func (n *Node) Replicate(entry []byte, commitTS uint64) error {
	for _, p := range n.peers {
		msg := Message{Type: MsgReplicate, From: n.id, CommitTS: commitTS, Entry: entry}
		if err := n.transport.Send(p, msg); err != nil {
			return err
		}
	}
	// Majority of N = floor(N/2)+1; the leader's own copy counts as one, so
	// it needs floor((peers+1)/2) follower acks.
	need := (len(n.peers) + 1) / 2
	got := 0
	for got < need {
		select {
		case m := <-n.ackCh:
			if m.Type == MsgAck && m.CommitTS == commitTS && m.OK {
				got++
			}
		case <-n.quit:
			return errors.New("cluster: node stopped during replication")
		}
	}
	return nil
}

// Cluster is a fixed-leader replication group over an in-process transport.
type Cluster struct {
	transport *ChannelTransport
	nodes     []*Node
	leader    NodeID
}

// New creates an n-node cluster with node 0 as the fixed leader. dirs must
// have length n; each node gets its own data directory and opts.
func New(n int, dirs []string, opts db.Options) (*Cluster, error) {
	if n < 1 {
		return nil, errors.New("cluster: need at least one node")
	}
	if len(dirs) != n {
		return nil, fmt.Errorf("cluster: need %d dirs, got %d", n, len(dirs))
	}

	tr := NewChannelTransport()
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
		})
	}

	// Wire the leader's replication hook, then start every node's loop.
	leaderNode := c.nodes[c.leader]
	leaderNode.store.SetReplicator(leaderNode)
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

// Get reads from the leader (linearizable). Bounded-staleness follower reads
// are a future addition.
func (c *Cluster) Get(key []byte) ([]byte, error) { return c.leaderNode().store.Get(key) }

// Begin starts a transaction on the leader. Its Commit triggers the
// replication round; followers apply the whole txn on the OpCommit.
func (c *Cluster) Begin() *db.Txn { return c.leaderNode().store.Begin() }

// Node returns the i-th node, for inspection and tests.
func (c *Cluster) Node(i int) *Node { return c.nodes[i] }

// Size returns the node count.
func (c *Cluster) Size() int { return len(c.nodes) }

// Leader returns the leader's id.
func (c *Cluster) Leader() NodeID { return c.leader }

// Quiesce waits until every node has applied through the leader's latest
// commit, or the timeout elapses. The leader returns to the client after only
// a majority acks, so a minority follower may still be draining its inbox.
func (c *Cluster) Quiesce(timeout time.Duration) error {
	target := c.leaderNode().store.LastAppliedTS()
	deadline := time.Now().Add(timeout)
	for {
		caughtUp := true
		for _, nd := range c.nodes {
			if nd.store.LastAppliedTS() < target {
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
