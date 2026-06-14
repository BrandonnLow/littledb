package cluster

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/BrandonnLow/littledb/internal/db"
)

// ErrNotLeader is returned by a commit routed to a node that is not currently
// the leader (or that stepped down mid-commit). Clients retry on the new
// leader; the cluster's own Put/Delete/Get do this automatically.
var ErrNotLeader = errors.New("cluster: not leader")

const bootstrapLeader NodeID = 0

// routeTimeout bounds how long Put/Delete/Get retry to find a leader (e.g.
// across an election) before giving up.
const routeTimeout = 2 * time.Second

// Config tunes the election and heartbeat timers. Heartbeat must be well below
// ElectionMin (a few heartbeats per election window) or healthy leaders get
// voted out.
type Config struct {
	ElectionMin time.Duration
	ElectionMax time.Duration
	Heartbeat   time.Duration
}

// DefaultConfig returns conservative timers: heartbeat 50ms, election
// 250–500ms (50 <= 250/3), safe against spurious elections under -race.
func DefaultConfig() Config {
	return Config{ElectionMin: 250 * time.Millisecond, ElectionMax: 500 * time.Millisecond, Heartbeat: 50 * time.Millisecond}
}

// Node wraps a db.DB with a Raft log, an election state machine, and a
// transport endpoint. Every node runs the same goroutine set; its role gates
// behavior. Appending to the log and applying to the memtable are separate: an
// entry is appended on receipt and applied only once leaderCommit covers it.
type Node struct {
	id        NodeID
	store     *db.DB
	transport Transport
	peers     []NodeID
	cfg       Config

	inbox <-chan Message
	quit  chan struct{}
	wg    sync.WaitGroup

	// Raft state, guarded by raftMu. raftMu and the store's db.mu are never
	// held simultaneously: applies and WAL appends happen outside raftMu.
	raftMu      sync.Mutex
	role        Role
	currentTerm uint64
	votedFor    NodeID // noVote if none this term

	log         *RaftLog
	commitIndex uint64
	lastApplied uint64
	appliedCond *sync.Cond // broadcast when lastApplied advances or we step down

	votesReceived int // tally for the current election (candidate only)

	// Leader replication state (per peer), guarded by raftMu. Present on every
	// node since any node can become leader; only acted on while role==Leader.
	nextIndex  map[NodeID]uint64
	matchIndex map[NodeID]uint64
	respCh     map[NodeID]chan Message
	replSignal map[NodeID]chan struct{}

	applySignal     chan struct{} // coalescing wake for the apply loop
	electionResetCh chan struct{} // coalescing reset for the election timer

	// commitMu serializes the leader's commits end-to-end so each commit is
	// fully applied before the next one's conflict check runs.
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

func (n *Node) roleValue() Role {
	n.raftMu.Lock()
	defer n.raftMu.Unlock()
	return n.role
}

func (n *Node) termValue() uint64 {
	n.raftMu.Lock()
	defer n.raftMu.Unlock()
	return n.currentTerm
}

func (n *Node) start() {
	n.appliedCond = sync.NewCond(&n.raftMu)
	n.applySignal = make(chan struct{}, 1)
	n.electionResetCh = make(chan struct{}, 1)

	n.wg.Add(4 + len(n.peers))
	go n.run()
	go n.applyLoop()
	go n.electionTimer()
	go n.heartbeatTicker()
	for _, p := range n.peers {
		go n.replicateTo(p)
	}

	// The bootstrap leader asserts leadership immediately (note: fires the
	// first heartbeat so followers don't time out at term 2).
	n.raftMu.Lock()
	if n.role == Leader {
		n.becomeLeaderLocked()
	}
	n.raftMu.Unlock()
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

// run drains the inbox and dispatches by message type. Every node handles
// AppendEntries and RequestVote (to follow / vote/ step down); a leader also
// routes AppendedResponses to the owning follower's replicator; a candidate
// tallies vote responses.
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
			case MsgRequestVote:
				n.handleRequestVote(m)
			case MsgRequestVoteResponse:
				n.handleVoteResponse(m)
			case MsgAppendResponse:
				// Route to the follower's replicator. Reliable (no drop): with
				// one AppendEntries in flight per follower, dropping a response
				// would stall it forever.
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

// handleAppendEntries is the follower side of replication. It adopts a higher
// term (stepping down), rejects a stale leader (so that leader steps down),
// reverts a candidate to follower, and resets its election timer. On a prev-log
// match it durably appends the genuinely-new suffix — truncating a conflicting
// suffix first — advances its commit index, and wakes its apply loop.
func (n *Node) handleAppendEntries(m Message) {
	n.raftMu.Lock()
	if m.Term < n.currentTerm {
		term := n.currentTerm
		n.raftMu.Unlock()
		_ = n.transport.Send(m.From, Message{
			Type: MsgAppendResponse, From: n.id, Term: term, Success: false,
		})
		return
	}
	if m.Term > n.currentTerm {
		n.stepDownLocked(m.Term)
	}
	n.role = Follower // a current-term leader exists; defer to it
	n.resetElectionTimer()
	term := n.currentTerm

	if !n.log.matchesPrev(m.PrevLogIndex, m.PrevLogTerm) {
		hint := n.log.lastIndex() + 1
		n.raftMu.Unlock()
		_ = n.transport.Send(m.From, Message{
			Type: MsgAppendResponse, From: n.id, Term: term,
			Success: false, ConflictHint: hint,
		})
		return
	}

	// Genuinely-new suffix, truncating a conflicting one first. Truncation only
	// ever removes UNCOMMITTED (hence unapplied) entries: the up-to-date vote
	// rule guarantees a committed entry sits in every future leader's log and
	// so is never the loser of a conflict. (The on-disk WAL does not yet reflect
	// truncation; the in-memory log we serve and replicate from is correct.)
	var toAppend []Entry
	for i, e := range m.Entries {
		idx := m.PrevLogIndex + uint64(i) + 1
		if idx <= n.log.lastIndex() {
			if n.log.term(idx) == e.Term {
				continue // already have this exact entry (idempotent)
			}
			n.log.truncateFrom(idx) // conflict: drop this and everything after
		}
		toAppend = append(toAppend, e)
	}
	matchIndex := m.PrevLogIndex + uint64(len(m.Entries))
	n.raftMu.Unlock()

	// Durably append the new suffix (each call takes db.mu) outside raftMu.
	// A mid-batch failure here (after a conflict truncation) leaves the
	// in-memory log shorter than the WAL and missing the suffix; we reject so
	// the leader retries from the hint and re-sends the whole suffix, healing
	// both. Safe: only uncommitted entries are ever in flight here. (TODO:
	// On-disk reconciliation of the short-vs-long mismatch.)
	for _, e := range toAppend {
		if err := n.store.AppendToLog(e.Data); err != nil {
			_ = n.transport.Send(m.From, Message{
				Type: MsgAppendResponse, From: n.id, Term: term,
				Success: false, ConflictHint: m.PrevLogIndex + 1,
			})
			return
		}
	}

	n.raftMu.Lock()
	// A concurrent election can have bumped our term/role between the WAL
	// append above and here (the election timer may fire the same instant we
	// reset it). Don't apply under a changed term: reject with our current term
	// so the now-stale sender steps down. The WAL-appended suffix becomes an
	// orphan until commitIndex-bounded recovery, exactly as in commit().
	if n.role != Follower || n.currentTerm != term {
		hint := n.log.lastIndex() + 1
		cur := n.currentTerm
		n.raftMu.Unlock()
		_ = n.transport.Send(m.From, Message{
			Type: MsgAppendResponse, From: n.id, Term: cur,
			Success: false, ConflictHint: hint,
		})
		return
	}
	for _, e := range toAppend {
		n.log.append(e.Term, e.Data)
	}
	if ci := m.LeaderCommit; ci > n.commitIndex {
		if li := n.log.lastIndex(); ci > li {
			ci = li
		}
		if ci > n.commitIndex {
			n.commitIndex = ci
		}
	}
	n.raftMu.Unlock()
	n.signalApply()

	_ = n.transport.Send(m.From, Message{
		Type: MsgAppendResponse, From: n.id, Term: term,
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

		if err := n.store.ApplyEntry(entry); err != nil {
			panic(fmt.Sprintf("cluster: node %d apply entry %d: %v", n.id, idx, err))
		}

		n.raftMu.Lock()
		n.lastApplied = idx
		n.appliedCond.Broadcast()
		n.raftMu.Unlock()
	}
}

// commit is the leader's commitOverride (installed on every node, so a write
// to a non-leader fails cleanly rather than taking the single-node path). It
// prepares the txn, appends to its log, wakes the replicators, and waits for
// its own apply. It returns ErrNotLeader if this node is not the leader or
// stepped down before the entry committed.
func (n *Node) commit(t *db.Txn) error {
	n.commitMu.Lock()
	defer n.commitMu.Unlock()

	n.raftMu.Lock()
	if n.role != Leader {
		n.raftMu.Unlock()
		return ErrNotLeader
	}
	term := n.currentTerm
	n.raftMu.Unlock()

	entry, _, err := n.store.PrepareCommit(t)
	if err != nil {
		return err
	}
	if entry == nil {
		return nil // empty txn
	}

	if err := n.store.AppendToLog(entry); err != nil {
		return fmt.Errorf("cluster: leader append to log: %w", err)
	}

	n.raftMu.Lock()
	// Re-check leadership: we may have stepped down during PrepareCommit /
	// AppendToLog. The entry is now an orphan in our WAL — durable but absent
	// from the in-memory log and unreplicated. Harmless while running, but the
	// current plain-replay db.Open WOULD resurrect it on restart (it is a
	// complete txn with OpCommit); TODO: recovery must be commitIndex-bounded
	// to discard it.
	if n.role != Leader || n.currentTerm != term {
		n.raftMu.Unlock()
		return ErrNotLeader
	}
	idx := n.log.append(term, entry)
	n.maybeAdvanceCommitLocked() // commits immediately at n=1; no-op otherwise
	n.raftMu.Unlock()

	n.signalReplicators()

	n.raftMu.Lock()
	defer n.raftMu.Unlock()
	for {
		if n.lastApplied >= idx {
			// Quorum'd and applied — a real commit, even if we step down next.
			return nil
		}
		if n.role != Leader {
			return ErrNotLeader
		}
		n.appliedCond.Wait()
	}
}

// Cluster is a Raft replication group over a transport.
type Cluster struct {
	transport Transport
	nodes     []*Node
}

// New creates an n-node cluster with default timers over an in-process channel
// transport. dirs must have length n.
func New(n int, dirs []string, opts db.Options) (*Cluster, error) {
	return NewWithTransportConfig(n, dirs, opts, NewChannelTransport(), DefaultConfig())
}

// NewWithTransport is New with a caller-supplied transport and default timers.
func NewWithTransport(n int, dirs []string, opts db.Options, tr Transport) (*Cluster, error) {
	return NewWithTransportConfig(n, dirs, opts, tr, DefaultConfig())
}

// NewWithTransportConfig is the full constructor: caller-supplied transport and
// timer config. Node 0 bootstraps as leader at term 1; the others start as
// followers and elect a successor if it fails.
func NewWithTransportConfig(n int, dirs []string, opts db.Options, tr Transport, cfg Config) (*Cluster, error) {
	if n < 1 {
		return nil, errors.New("cluster: need at least one node")
	}
	if len(dirs) != n {
		return nil, fmt.Errorf("cluster: need %d dirs, got %d", n, len(dirs))
	}

	for i := 0; i < n; i++ {
		tr.Register(NodeID(i))
	}

	c := &Cluster{transport: tr}
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
			id:          NodeID(i),
			store:       store,
			transport:   tr,
			peers:       peers,
			cfg:         cfg,
			inbox:       tr.Inbox(NodeID(i)),
			quit:        make(chan struct{}),
			log:         NewRaftLog(),
			role:        Follower,
			currentTerm: 1,
			votedFor:    noVote,
			nextIndex:   make(map[NodeID]uint64, len(peers)),
			matchIndex:  make(map[NodeID]uint64, len(peers)),
			respCh:      make(map[NodeID]chan Message, len(peers)),
			replSignal:  make(map[NodeID]chan struct{}, len(peers)),
		}
		for _, p := range peers {
			nd.nextIndex[p] = 1
			nd.matchIndex[p] = 0
			nd.respCh[p] = make(chan Message, 1)
			nd.replSignal[p] = make(chan struct{}, 1)
		}
		if NodeID(i) == bootstrapLeader {
			nd.role = Leader
			nd.votedFor = bootstrapLeader // term-1 self-vote
		}
		// Every node's DB delegates Txn.Commit to its node, which checks
		// leadership — so a write to a non-leader fails with ErrNotLeader rather
		// than silently committing locally.
		nd.store.SetCommitOverride(nd.commit)
		c.nodes = append(c.nodes, nd)
	}

	for _, nd := range c.nodes {
		nd.start()
	}
	return c, nil
}

// currentLeader returns the node that currently believes it is leader at the
// highest term, if any.
func (c *Cluster) currentLeader() (*Node, bool) {
	var best *Node
	var bestTerm uint64
	for _, nd := range c.nodes {
		nd.raftMu.Lock()
		isLeader := nd.role == Leader
		term := nd.currentTerm
		nd.raftMu.Unlock()
		if isLeader && (best == nil || term > bestTerm) {
			best, bestTerm = nd, term
		}
	}
	return best, best != nil
}

func (c *Cluster) leaderNode() *Node {
	if ld, ok := c.currentLeader(); ok {
		return ld
	}
	return c.nodes[0]
}

// withLeader runs fn against the current leader, retrying (through an election)
// while there is no leader or fn reports ErrNotLeader, up to routeTimeout.
func (c *Cluster) withLeader(fn func(*Node) error) error {
	deadline := time.Now().Add(routeTimeout)
	for {
		if ld, ok := c.currentLeader(); ok {
			if err := fn(ld); !errors.Is(err, ErrNotLeader) {
				return err
			}
		}
		if time.Now().After(deadline) {
			return ErrNotLeader
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// Put routes a write to the current leader, retrying across leadership changes.
func (c *Cluster) Put(key, value []byte) error {
	return c.withLeader(func(ld *Node) error { return ld.store.Put(key, value) })
}

// Delete routes a delete to the current leader, retrying across changes.
func (c *Cluster) Delete(key []byte) error {
	return c.withLeader(func(ld *Node) error { return ld.store.Delete(key) })
}

// Get reads from the current leader, retrying across leadership changes.
//
// Not linearizable: currentLeader() inspects every node's role directly, so the
// in-process harness always finds the true leader — but a real client routed to
// a partitioned ex-leader (still role==Leader at its stale term until it hears a
// higher one) could read stale data. Linearizable reads (read-index / leader
// lease) are future work.
func (c *Cluster) Get(key []byte) ([]byte, error) {
	var out []byte
	err := c.withLeader(func(ld *Node) error {
		v, e := ld.store.Get(key)
		if e != nil {
			return e
		}
		out = v
		return nil
	})
	return out, err
}

// Begin starts a transaction on the current leader. A user-held Txn that spans
// a leadership change will get ErrNotLeader from Commit (no transparent retry —
// the snapshot is tied to the node it began on).
func (c *Cluster) Begin() *db.Txn { return c.leaderNode().store.Begin() }

// Node returns the i-th node, for inspection and tests.
func (c *Cluster) Node(i int) *Node { return c.nodes[i] }

// Size returns the node count.
func (c *Cluster) Size() int { return len(c.nodes) }

// Leader returns the current leader's id, or the bootstrap node if none.
func (c *Cluster) Leader() NodeID {
	if ld, ok := c.currentLeader(); ok {
		return ld.id
	}
	return bootstrapLeader
}

// Quiesce waits until every node has applied through the current leader's
// commit index, or the timeout elapses.
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
