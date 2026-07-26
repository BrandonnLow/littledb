package cluster

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/BrandonnLow/littledb/internal/db"
)

// ErrNotLeader is returned by a commit routed to a node that is not currently
// the leader, or that stepped down BEFORE appending the entry (so it definitely
// did not commit). Clients retry on the new leader; the cluster's own
// Put/Delete/Get do this automatically.
var ErrNotLeader = errors.New("cluster: not leader")

// ErrMaybeCommitted is returned by a write whose entry was appended and
// replicated but whose leader stepped down before confirming it applied. The
// write may or may not have committed: a later leader may commit the replicated
// entry, or truncate it as an uncommitted tail. Without exactly-once client
// sessions the caller cannot tell, and a naive retry may self-conflict with (or
// double-apply) the earlier entry — so this is a distinct, honest outcome rather
// than a plain ErrNotLeader. Deduplicating retries is the client-sessions track.
var ErrMaybeCommitted = errors.New("cluster: write may or may not have committed")

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
	// SnapshotThreshold is the in-memory Raft-log entry count above which the
	// leader compacts (leader-only, bounded by the slowest follower's
	// matchIndex). <= 0 disables compaction.
	SnapshotThreshold int
}

// DefaultConfig returns conservative timers: heartbeat 50ms, election
// 250–500ms (50 <= 250/3), safe against spurious elections under -race.
func DefaultConfig() Config {
	return Config{ElectionMin: 250 * time.Millisecond, ElectionMax: 500 * time.Millisecond, Heartbeat: 50 * time.Millisecond, SnapshotThreshold: 1024}
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

	// dir is the node's DB data dir; raftDir (dir/raft) holds the Raft log,
	// state, and InstallSnapshot staging/marker. opts reopens the store after a
	// snapshot install. storeMu guards store-call sites against the install swap
	// (RLock to use the store, exclusive Lock to replace it); it is strictly
	// OUTER to raftMu.
	dir     string
	raftDir string
	opts    db.Options
	storeMu sync.RWMutex

	// installing (raftMu) is set while an InstallSnapshot is being applied: the
	// apply loop parks, and the node defers elections, until the swap completes.
	// installsApplied counts completed installs, for tests.
	installing      bool
	installsApplied atomic.Uint64

	inbox <-chan Message
	quit  chan struct{}
	wg    sync.WaitGroup

	// Raft state, guarded by raftMu. raftMu and the store's db.mu are never
	// held simultaneously: applies and WAL appends happen outside raftMu.
	raftMu      sync.Mutex
	role        Role
	currentTerm uint64
	votedFor    NodeID // noVote if none this term

	// config is the current voting membership (node ids, including self). Quorum
	// size, the vote-solicitation set, and the commit-index computation all derive
	// from it, so the cluster is no longer fixed-size: a membership change rewrites
	// it. Bootstrapped to every node at construction, so a cluster with no
	// membership changes behaves exactly as a fixed peer set. Guarded by raftMu.
	config map[NodeID]bool

	log         *RaftLog
	logFile     *raftLogFile       // durable Raft log; nil for bare-Node white-box tests
	stateFile   hardStatePersister // durable (currentTerm, votedFor); nil for bare-Node tests
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
func (n *Node) DB() *db.DB {
	n.storeMu.RLock()
	defer n.storeMu.RUnlock()
	return n.store
}

func (n *Node) lastIndex() uint64 {
	n.raftMu.Lock()
	defer n.raftMu.Unlock()
	return n.log.lastIndex()
}

// baseIndexValue returns the Raft log's compaction base under raftMu.
func (n *Node) baseIndexValue() uint64 {
	n.raftMu.Lock()
	defer n.raftMu.Unlock()
	return n.log.baseIndex
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

// InstallsForTesting reports how many InstallSnapshot installs this node has
// completed.
func (n *Node) InstallsForTesting() uint64 { return n.installsApplied.Load() }

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
// AppendEntries and RequestVote (to follow / vote / step down); a leader also
// routes AppendResponses to the owning follower's replicator; a candidate
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
			case MsgInstallSnapshot:
				n.handleInstallSnapshot(m)
			case MsgAppendResponse, MsgInstallSnapshotResponse:
				// Route to the follower's replicator. Reliable (no drop): with
				// one AppendEntries / InstallSnapshot in flight per follower,
				// dropping a response would stall it forever.
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

	// Append the genuinely-new suffix, truncating a conflicting one first. The
	// Raft log file and the in-memory log are mutated TOGETHER under raftMu, so
	// the file can never lead the in-memory log: no orphan tail accumulates and a
	// retry can't duplicate one — the file's contents are always exactly the
	// in-memory log. (The earlier design appended the file unlocked, then
	// re-checked and appended memory; a step-down in that gap left the file one
	// suffix ahead, and truncation keyed on the in-memory length could never
	// reach it, so a retry re-appended and the file grew [..4,5,4,5].) The
	// append/truncate fsync eagerly, so an entry is durable before the commit
	// index can advance over it and apply it. Truncation only removes UNCOMMITTED
	// (unapplied) entries: §5.4.1's up-to-date vote rule keeps every committed
	// entry in every future leader's log, so it never loses a conflict; the data
	// WAL is untouched, holding only applied entries that never truncate.
	for i, e := range m.Entries {
		idx := m.PrevLogIndex + uint64(i) + 1
		if idx <= n.log.baseIndex {
			continue // already compacted (committed); we have it durably
		}
		if idx <= n.log.lastIndex() {
			if n.log.term(idx) == e.Term {
				continue // already have this exact entry (idempotent)
			}
			// Conflict: drop this index and everything after from both logs, the
			// file first so a failure leaves it no shorter than the in-memory log.
			if n.logFile != nil {
				if err := n.logFile.truncateFrom(idx); err != nil {
					cur := n.currentTerm
					n.raftMu.Unlock()
					_ = n.transport.Send(m.From, Message{
						Type: MsgAppendResponse, From: n.id, Term: cur,
						Success: false, ConflictHint: m.PrevLogIndex + 1,
					})
					return
				}
			}
			n.log.truncateFrom(idx)
		}
		if n.logFile != nil {
			if err := n.logFile.append(e.Term, e.Data); err != nil {
				cur := n.currentTerm
				n.raftMu.Unlock()
				_ = n.transport.Send(m.From, Message{
					Type: MsgAppendResponse, From: n.id, Term: cur,
					Success: false, ConflictHint: m.PrevLogIndex + 1,
				})
				return
			}
		}
		n.log.append(e.Term, e.Data)
	}
	matchIndex := m.PrevLogIndex + uint64(len(m.Entries))
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
		// storeMu (shared) is held across the whole iteration so an install swap
		// (exclusive) can only interpose BETWEEN iterations, never mid-apply, and
		// never re-drives an entry the swapped-in snapshot already covers. It is
		// strictly outer to raftMu.
		n.storeMu.RLock()
		n.raftMu.Lock()
		if n.installing || n.lastApplied >= n.commitIndex {
			n.raftMu.Unlock()
			n.storeMu.RUnlock()
			return
		}
		idx := n.lastApplied + 1
		entry := n.log.entryAt(idx)
		n.raftMu.Unlock()

		if err := n.store.ApplyEntry(idx, entry); err != nil {
			n.storeMu.RUnlock()
			panic(fmt.Sprintf("cluster: node %d apply entry %d: %v", n.id, idx, err))
		}

		n.raftMu.Lock()
		n.lastApplied = idx
		n.appliedCond.Broadcast()
		// Followers self-compact once their applied suffix is long enough; safe
		// now that a far-behind peer is caught up via InstallSnapshot rather than
		// AppendEntries. The leader compacts on the replication path too; this
		// call is harmless there (it recomputes the same safe point).
		n.maybeCompactLocked()
		n.raftMu.Unlock()
		n.storeMu.RUnlock()
	}
}

// commit is the leader's commitOverride (installed on every node, so a write
// to a non-leader fails cleanly rather than taking the single-node path). It
// prepares the txn, then replicates and applies the entry via proposeAndAwait.
// It returns ErrNotLeader if this node is not the leader or stepped down before
// the entry committed.
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

	n.storeMu.RLock()
	entry, _, err := n.store.PrepareCommit(t)
	n.storeMu.RUnlock()
	if err != nil {
		return err
	}
	if entry == nil {
		return nil // empty txn
	}
	return n.proposeAndAwait(term, entry)
}

// readBarrier drives a no-op entry through the ordinary quorum-commit path and
// blocks until the leader has applied it. Committing a current-term entry
// requires a majority to replicate it at this term, so a successful return is
// itself proof that this node is still the leader as of an index that post-dates
// the call — the read-index guarantee. A partitioned ex-leader cannot commit the
// no-op (no quorum) and blocks until it steps down or the caller's deadline
// fires, so it can never serve a stale read. After a nil return, a local read of
// the store reflects every write committed before the barrier, and is
// linearizable.
//
// This is the simple ReadIndex variant — one no-op per read. The heartbeat-only
// optimization (confirm leadership with an empty AppendEntries round instead of a
// logged entry) is deferred; it trades this scheme's log growth and quorum
// round-trip for extra bookkeeping.
func (n *Node) readBarrier() error {
	n.commitMu.Lock()
	defer n.commitMu.Unlock()

	n.raftMu.Lock()
	if n.role != Leader {
		n.raftMu.Unlock()
		return ErrNotLeader
	}
	term := n.currentTerm
	n.raftMu.Unlock()

	n.storeMu.RLock()
	entry, _, err := n.store.PrepareNoop()
	n.storeMu.RUnlock()
	if err != nil {
		return err
	}
	// A no-op that "maybe committed" is irrelevant — it changes nothing — but it
	// means we lost leadership before confirming, so the read did not clear the
	// barrier here. Report ErrNotLeader so the caller cleanly redirects.
	if err := n.proposeAndAwait(term, entry); err != nil {
		if errors.Is(err, ErrMaybeCommitted) {
			return ErrNotLeader
		}
		return err
	}
	return nil
}

// proposeAndAwait appends entry (created in `term`) to the leader's log,
// replicates it, and waits until the leader has applied it. It is the shared core
// of client commits and read barriers. The caller holds commitMu, has verified
// leadership at `term`, and has built the entry outside raftMu. Returns
// ErrNotLeader if leadership was lost before the entry committed.
func (n *Node) proposeAndAwait(term uint64, entry []byte) error {
	n.raftMu.Lock()
	// Re-check leadership: we may have stepped down while building the entry. Append
	// to the Raft log file and the in-memory log TOGETHER under raftMu, so the file
	// never leads the in-memory log — there is no orphan tail for a later commit
	// (after a re-election) to blind-append onto and misalign. The append fsyncs
	// eagerly, so the entry is durable before maybeAdvanceCommit can count it toward
	// the commit index and apply it. If we stepped down we bail before touching
	// either log; the entry is simply never created here.
	if n.role != Leader || n.currentTerm != term {
		n.raftMu.Unlock()
		return ErrNotLeader
	}
	if n.logFile != nil {
		if err := n.logFile.append(term, entry); err != nil {
			n.raftMu.Unlock()
			return fmt.Errorf("cluster: leader append to raft log: %w", err)
		}
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
			// The entry was already appended and replicated; a later leader may yet
			// commit it or truncate it. Its fate is unknown, which is materially
			// different from never having been proposed (ErrNotLeader).
			return ErrMaybeCommitted
		}
		n.appliedCond.Wait()
	}
}

// Cluster is a Raft replication group over a transport.
type Cluster struct {
	transport Transport

	// nodesMu guards the nodes slice's contents. Its length is fixed at
	// construction, but an element is swapped when a node is rebuilt after a
	// simulated crash (see the linearizability harness), so readers snapshot the
	// slice header under RLock and the swap takes the write lock.
	nodesMu sync.RWMutex
	nodes   []*Node
}

// snapshotNodes returns a copy of the nodes slice header under RLock, so callers
// can iterate a stable set of node pointers while a crash-rebuild may be swapping
// an element. The node structs themselves are internally synchronized.
func (c *Cluster) snapshotNodes() []*Node {
	c.nodesMu.RLock()
	defer c.nodesMu.RUnlock()
	out := make([]*Node, len(c.nodes))
	copy(out, c.nodes)
	return out
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
		nd, err := buildNode(NodeID(i), n, dirs[i], opts, tr, cfg)
		if err != nil {
			c.Close()
			return nil, err
		}
		c.nodes = append(c.nodes, nd)
	}

	for _, nd := range c.nodes {
		nd.start()
	}
	return c, nil
}

// buildNode constructs — but does not start — a single node with id over dir,
// reconstructing its durable Raft state (log, hard state, applied watermark) and
// resolving its startup role. size is the cluster's node count (for the peer
// set). It is used both at cluster creation and to rebuild a node over the same
// dir after a simulated crash, so it must be a faithful cold-start: the durable
// files alone decide fresh-bootstrap vs. restart-and-re-elect. On any error it
// releases whatever it had already opened. The caller registers the id with the
// transport and starts the node.
func buildNode(id NodeID, size int, dir string, opts db.Options, tr Transport, cfg Config) (*Node, error) {
	// Per-node Raft files live in a raft/ subdir, isolated from the DB's own dir.
	// Create it first so install completion (below) can find a pending marker, and
	// so the freshness stat further down sees a stable dir.
	raftDir := filepath.Join(dir, "raft")
	if err := os.MkdirAll(raftDir, 0o755); err != nil {
		return nil, fmt.Errorf("cluster: mkdir raft dir node %d: %w", id, err)
	}
	// Finish any interrupted InstallSnapshot BEFORE opening the store and BEFORE
	// the recovered-vs-base guard: a mid-install crash leaves the data dir possibly
	// mid-swap and the recovered applied index below the snapshot base until the
	// swap lands. Completion is idempotent (re-runs from the staged dir until the
	// marker clears).
	if err := completeInstallIfPending(dir, raftDir, opts.SyncOnWrite); err != nil {
		return nil, fmt.Errorf("cluster: complete pending install node %d: %w", id, err)
	}

	store, err := db.OpenWith(dir, opts)
	if err != nil {
		return nil, fmt.Errorf("cluster: open node %d: %w", id, err)
	}

	// The log file's prior existence distinguishes a fresh start (bootstrap) from a
	// restart (reconstruct + re-elect).
	logPath := filepath.Join(raftDir, raftLogFileName)
	statePath := filepath.Join(raftDir, raftStateFileName)
	// Stat BOTH durable raft files before opening either: openRaftLogFile's
	// O_CREATE would otherwise erase the freshness signal. A node is fresh
	// (eligible to bootstrap) only when neither file exists — a node can persist a
	// vote (writing raft/state) before ever appending a log entry, so keying
	// freshness on raft.log alone would wrongly bootstrap such a node and discard
	// its durable vote.
	_, logStatErr := os.Stat(logPath)
	_, stateStatErr := os.Stat(statePath)
	fresh := errors.Is(logStatErr, os.ErrNotExist) && errors.Is(stateStatErr, os.ErrNotExist)

	logFile, persisted, err := openRaftLogFile(logPath, opts.SyncOnWrite)
	if err != nil {
		store.Close()
		return nil, fmt.Errorf("cluster: open raft log node %d: %w", id, err)
	}
	stateFile, hard, err := openRaftStateFile(statePath, opts.SyncOnWrite)
	if err != nil {
		logFile.close()
		store.Close()
		return nil, fmt.Errorf("cluster: open raft state node %d: %w", id, err)
	}

	rlog := NewRaftLogWithBase(logFile.baseIndex, logFile.baseTerm)
	for _, pe := range persisted {
		rlog.append(pe.term, pe.data)
	}
	recovered := store.RecoveredAppliedIndex()
	// Everything at-or-below baseIndex was applied and made durable in the data WAL
	// before we compacted past it, so recovery must reconstruct an applied index at
	// least baseIndex. A lower value means the compacted prefix outran durable
	// applied state — corruption; refuse to start rather than silently apply into a
	// compacted region.
	if recovered < logFile.baseIndex {
		logFile.close()
		stateFile.close()
		store.Close()
		return nil, fmt.Errorf("cluster: node %d recovered applied index %d below raft log base %d (corruption)", id, recovered, logFile.baseIndex)
	}

	var peers []NodeID
	config := make(map[NodeID]bool, size)
	for j := 0; j < size; j++ {
		config[NodeID(j)] = true // bootstrap config: every node is a voting member
		if NodeID(j) != id {
			peers = append(peers, NodeID(j))
		}
	}
	nd := &Node{
		id:          id,
		store:       store,
		transport:   tr,
		peers:       peers,
		config:      config,
		cfg:         cfg,
		dir:         dir,
		raftDir:     raftDir,
		opts:        opts,
		inbox:       tr.Inbox(id),
		quit:        make(chan struct{}),
		log:         rlog,
		logFile:     logFile,
		stateFile:   stateFile,
		commitIndex: recovered,
		lastApplied: recovered,
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
	switch {
	case fresh && id == bootstrapLeader:
		nd.role = Leader
		nd.votedFor = bootstrapLeader // term-1 self-vote
	case fresh:
		// Follower@1, already set.
	default:
		// Restart: never bootstrap. Start as follower and re-elect.
		// currentTerm is the durable hard state reconciled against the log:
		// max(state.term, log.lastTerm()). The log term can exceed the saved term
		// in the degraded window where a higher term was adopted and its entries
		// were logged but the hard-state fsync was lost; a node holding a term-T
		// entry has been in term >= T, so we must resume at least there. votedFor is
		// kept only when the saved term wins or ties; if the log term strictly wins
		// we are moving to a higher term than the saved vote belongs to, so that
		// vote is stale and resets. Absent state reads as (0, noVote), so this same
		// rule is the legacy fallback. commitIndex/lastApplied come from the applied
		// watermark, so committed entries are not re-applied and uncommitted tails
		// wait.
		logTerm := rlog.lastTerm()
		if logTerm > hard.currentTerm {
			nd.currentTerm = logTerm
			nd.votedFor = noVote
		} else {
			nd.currentTerm = hard.currentTerm
			nd.votedFor = hard.votedFor
		}
	}
	// Persist the resolved hard state at construction so disk reflects any max-load
	// repair or legacy derivation, and so raft/state exists from the first run
	// (keeping the freshness signal stable across restarts). The node's goroutines
	// have not started, so no lock is needed and we save directly rather than
	// through persistHardStateLocked.
	if err := nd.stateFile.save(nd.currentTerm, nd.votedFor); err != nil {
		logFile.close()
		stateFile.close()
		store.Close()
		return nil, fmt.Errorf("cluster: persist raft state node %d: %w", id, err)
	}
	// Every node's DB delegates Txn.Commit to its node, which checks leadership — so
	// a write to a non-leader fails with ErrNotLeader rather than silently
	// committing locally.
	nd.store.SetCommitOverride(nd.commit)
	return nd, nil
}

// currentLeader returns the node that currently believes it is leader at the
// highest term, if any.
func (c *Cluster) currentLeader() (*Node, bool) {
	var best *Node
	var bestTerm uint64
	for _, nd := range c.snapshotNodes() {
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
	return c.Node(0)
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
	return c.withLeader(func(ld *Node) error { return ld.storePut(key, value) })
}

// Delete routes a delete to the current leader, retrying across changes.
func (c *Cluster) Delete(key []byte) error {
	return c.withLeader(func(ld *Node) error { return ld.storeDelete(key) })
}

// Get performs a linearizable read on the current leader, retrying across
// leadership changes. It routes through the leader's read-index barrier
// (linearizableGet), so the result reflects every write that completed before the
// call: a partitioned ex-leader cannot confirm leadership and returns ErrNotLeader
// (or blocks until it steps down), so this never observes stale data. The barrier
// costs a quorum round-trip per read; the heartbeat-only optimization is deferred.
func (c *Cluster) Get(key []byte) ([]byte, error) {
	var out []byte
	err := c.withLeader(func(ld *Node) error {
		v, e := ld.linearizableGet(key)
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
func (c *Cluster) Begin() *db.Txn { return c.leaderNode().storeBegin() }

// storePut/storeDelete/storeGet/storeBegin reach the node's store under storeMu
// (shared), so an in-flight InstallSnapshot swap (exclusive) never tears a read
// or write across the store replacement.
func (n *Node) storePut(key, value []byte) error {
	n.storeMu.RLock()
	defer n.storeMu.RUnlock()
	return n.store.Put(key, value)
}

func (n *Node) storeDelete(key []byte) error {
	n.storeMu.RLock()
	defer n.storeMu.RUnlock()
	return n.store.Delete(key)
}

func (n *Node) storeGet(key []byte) ([]byte, error) {
	n.storeMu.RLock()
	defer n.storeMu.RUnlock()
	return n.store.Get(key)
}

func (n *Node) storeBegin() *db.Txn {
	n.storeMu.RLock()
	defer n.storeMu.RUnlock()
	return n.store.Begin()
}

// linearizableGet runs a read-index barrier and then reads the key from the local
// store, so the result reflects every write that completed before the call. It
// returns ErrNotLeader if this node cannot confirm leadership (e.g. a partitioned
// ex-leader), so the caller redirects rather than reading stale state;
// db.ErrKeyNotFound is returned for an absent key.
func (n *Node) linearizableGet(key []byte) ([]byte, error) {
	if err := n.readBarrier(); err != nil {
		return nil, err
	}
	return n.storeGet(key)
}

// commitSessioned runs a transaction stamped with (session, seq) — typically a
// read-modify-write — behind a read-index barrier, giving exactly-once semantics
// on retry. The barrier does double duty: it confirms leadership AND ensures this
// node has applied everything committed, so the txn reads current state (not a
// freshly-elected leader's lagging view) and PrepareCommit's dedup table is
// current. A retry with the same seq is deduped to a single application. The apply
// callback runs under storeMu (a consistent read view); the commit is issued
// without holding storeMu, since commit() re-acquires it for PrepareCommit.
// Returns ErrNotLeader / ErrMaybeCommitted like a plain commit; db errors
// (ErrConflict, ...) surface from Commit.
func (n *Node) commitSessioned(session []byte, seq uint64, apply func(*db.Txn) error) error {
	if err := n.readBarrier(); err != nil {
		return err
	}
	n.storeMu.RLock()
	tx := n.store.Begin()
	tx.SetSession(session, seq)
	err := apply(tx)
	n.storeMu.RUnlock()
	if err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

// Node returns the i-th node, for inspection and tests.
func (c *Cluster) Node(i int) *Node {
	c.nodesMu.RLock()
	defer c.nodesMu.RUnlock()
	return c.nodes[i]
}

// Size returns the node count. The count is fixed at construction.
func (c *Cluster) Size() int {
	c.nodesMu.RLock()
	defer c.nodesMu.RUnlock()
	return len(c.nodes)
}

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
		for _, nd := range c.snapshotNodes() {
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
	nodes := c.snapshotNodes()
	for _, nd := range nodes {
		nd.stop()
	}
	var firstErr error
	for _, nd := range nodes {
		if err := nd.store.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		if nd.logFile != nil {
			if err := nd.logFile.close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		if nd.stateFile != nil {
			if err := nd.stateFile.close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}
