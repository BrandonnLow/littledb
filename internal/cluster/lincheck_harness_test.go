package cluster

import (
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/BrandonnLow/littledb/internal/db"
	"github.com/BrandonnLow/littledb/internal/lincheck"
)

// This file drives a real cluster with concurrent clients under fault injection
// and checks the resulting history for linearizability (Stage 2/3 of the
// linearizability-checker work). The generic checker lives in internal/lincheck;
// here we produce the histories it judges.
//
// The clients route the way a real network client must, NOT the way the
// package's own Cluster.Put/Get helpers do. Those helpers call currentLeader(),
// which inspects every node's role directly — omniscient routing that is
// impossible over a wire and, critically, masks stale reads: it always finds the
// true leader, so a partitioned ex-leader is never contacted. A realistic client
// instead holds a *believed* leader, sends there, and is redirected on
// ErrNotLeader. That is what lets a read land on a partitioned ex-leader (still
// role==Leader at its stale term) and observe stale data — the very anomaly the
// checker exists to catch.

// Client-side timeouts. A write to a partitioned leader blocks in commit()
// forever (it never reaches quorum and never hears a higher term to step down),
// so each attempt runs under a deadline, exactly like an RPC timeout; on expiry
// the write's fate is unknown and it is recorded with an Infinity return.
const (
	writeAttemptTimeout = 300 * time.Millisecond
	writeOverallTimeout = 3 * time.Second
)

// history accumulates a global-order operation log. A single atomic tick counter
// shared by every client stamps each call and return, so the ticks impose the
// real-time partial order the checker needs across clients.
type history struct {
	mu   sync.Mutex
	tick atomic.Int64
	ops  []lincheck.Op
}

func (h *history) now() int64 { return h.tick.Add(1) }

func (h *history) record(op lincheck.Op) {
	h.mu.Lock()
	h.ops = append(h.ops, op)
	h.mu.Unlock()
}

func (h *history) snapshot() []lincheck.Op {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]lincheck.Op, len(h.ops))
	copy(out, h.ops)
	return out
}

// client is one logical client: it remembers a believed leader, redirects on
// ErrNotLeader, and records every operation's real-time window and outcome.
type client struct {
	id       int
	c        *Cluster
	believed NodeID
	rng      *rand.Rand
	h        *history
}

// order returns the node ids to try, believed leader first, then the rest in a
// rotating order so a redirect eventually reaches whoever leads.
func (cl *client) order() []NodeID {
	n := cl.c.Size()
	ids := make([]NodeID, 0, n)
	ids = append(ids, cl.believed)
	for off := 1; off < n; off++ {
		ids = append(ids, NodeID((int(cl.believed)+off)%n))
	}
	return ids
}

// writeOutcome is the outcome of one write attempt against one node.
type writeOutcome int

const (
	wroteOK       writeOutcome = iota // committed: must linearize (finite return)
	wroteNotHere                      // ErrNotLeader: try elsewhere
	wroteConflict                     // ErrConflict: definitely did not apply
	wroteTimeout                      // blocked past the deadline: fate unknown
	wroteOther                        // some other error (e.g. closed): fate unknown
)

// tryWrite runs a single put/delete against node id under a deadline. A
// partitioned leader blocks in commit(), so the call runs in its own goroutine
// and is abandoned on timeout (it unblocks and exits once the partition heals).
func (cl *client) tryWrite(id NodeID, apply func(*Node) error) writeOutcome {
	done := make(chan error, 1)
	go func() { done <- apply(cl.c.Node(int(id))) }()
	select {
	case err := <-done:
		switch {
		case err == nil:
			return wroteOK
		case errors.Is(err, ErrNotLeader):
			return wroteNotHere
		case errors.Is(err, db.ErrConflict):
			return wroteConflict
		default:
			return wroteOther
		}
	case <-time.After(writeAttemptTimeout):
		return wroteTimeout
	}
}

// write performs a put or delete as one logical operation: it stamps the call,
// probes nodes until one commits (updating the believed leader) or the overall
// deadline passes, stamps the return, and records the operation. A committed
// write gets a finite return (must linearize); a conflict is dropped (it did not
// happen); an uncertain outcome gets an Infinity return.
func (cl *client) write(op lincheck.OpKind, key, value string, apply func(*Node) error) {
	call := cl.h.now()
	deadline := time.Now().Add(writeOverallTimeout)
	uncertain := false
	for time.Now().Before(deadline) {
		progressed := false
		for _, id := range cl.order() {
			switch cl.tryWrite(id, apply) {
			case wroteOK:
				cl.believed = id
				cl.record(op, key, value, false, call, cl.h.now())
				return
			case wroteConflict:
				// First-committer-wins rejected it: a definite no-op. Drop it from
				// the history entirely — an operation with no effect constrains
				// nothing.
				cl.h.now() // consume a return tick for ordering hygiene
				return
			case wroteNotHere:
				progressed = true // redirect and keep probing
			case wroteTimeout, wroteOther:
				uncertain = true
			}
		}
		if !progressed {
			time.Sleep(5 * time.Millisecond) // mid-election: back off, retry
		}
	}
	// Never got a definite commit. If any attempt blocked or errored, the write
	// may have taken effect; record it with an Infinity return so it can justify a
	// later read but never forces a violation on its own.
	if uncertain {
		cl.recordUncertain(op, key, value, call)
	} else {
		cl.h.now()
	}
}

func (cl *client) record(op lincheck.OpKind, key, value string, found bool, call, ret int64) {
	cl.h.record(lincheck.Op{Client: cl.id, Kind: op, Key: key, Value: value, Found: found, Call: call, Return: ret})
}

func (cl *client) recordUncertain(op lincheck.OpKind, key, value string, call int64) {
	cl.h.record(lincheck.Op{Client: cl.id, Kind: op, Key: key, Value: value, Call: call, Return: lincheck.Infinity})
}

func (cl *client) put(key, value string) {
	cl.write(lincheck.Put, key, value, func(nd *Node) error {
		return nd.storePut([]byte(key), []byte(value))
	})
}

func (cl *client) del(key string) {
	cl.write(lincheck.Delete, key, "", func(nd *Node) error {
		return nd.storeDelete([]byte(key))
	})
}

// get reads from the believed leader and records the observation. Reads do not
// redirect: a real read-heavy client stays pinned to the server it believes is
// leader, so when that server is a partitioned ex-leader the read observes stale
// state — which is exactly the anomaly to catch. A plain local read never
// blocks, so no timeout is needed. ErrKeyNotFound is a valid observation
// (absent), not a failure; any other error is dropped as unobserved.
func (cl *client) get(key string) {
	call := cl.h.now()
	v, err := cl.c.Node(int(cl.believed)).storeGet([]byte(key))
	ret := cl.h.now()
	switch {
	case err == nil:
		cl.record(lincheck.Get, key, string(v), true, call, ret)
	case errors.Is(err, db.ErrKeyNotFound):
		cl.record(lincheck.Get, key, "", false, call, ret)
	default:
		// Unobserved (e.g. store closed at teardown): constrains nothing.
	}
}

// runWorkload spawns clients that each issue opsPerClient random operations over
// a small key space, values unique per (client, sequence) so a read pins which
// write it observed. It returns the recorded history.
func runWorkload(c *Cluster, clients, opsPerClient, numKeys int, seed int64) *history {
	h := &history{}
	var wg sync.WaitGroup
	for ci := 0; ci < clients; ci++ {
		wg.Add(1)
		go func(ci int) {
			defer wg.Done()
			cl := &client{id: ci, c: c, believed: bootstrapLeader, rng: rand.New(rand.NewSource(seed + int64(ci)))}
			cl.h = h
			for seq := 0; seq < opsPerClient; seq++ {
				key := fmt.Sprintf("k%d", cl.rng.Intn(numKeys))
				switch cl.rng.Intn(10) {
				case 0, 1, 2, 3: // 40% reads
					cl.get(key)
				case 4: // 10% deletes
					cl.del(key)
				default: // 50% writes
					cl.put(key, fmt.Sprintf("c%d-%d", ci, seq))
				}
			}
		}(ci)
	}
	wg.Wait()
	return h
}

// TestClusterLinearizableNoFaults is the plumbing baseline: concurrent clients
// on a healthy cluster with no partitions. A correct single-leader Raft serves
// every operation from the true leader (believed converges to it), so the
// history must be linearizable. A failure here means a harness or checker
// integration bug, not a consensus bug.
func TestClusterLinearizableNoFaults(t *testing.T) {
	const n = 3
	c, err := NewWithTransportConfig(n, dirs(t, n), testOpts(), NewChannelTransport(), electionConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	h := runWorkload(c, 6, 150, 5, 42)
	hist := h.snapshot()
	t.Logf("recorded %d operations", len(hist))

	if res := lincheck.Check(hist); !res.Linearizable {
		t.Fatalf("healthy-cluster history is NOT linearizable (offending key %q) — harness/checker bug", res.Key)
	}
}

// crashRestart simulates a crash of node i and its restart over the same dir: it
// stops the node's goroutines, drops the store, then rebuilds a fresh node (which
// reconstructs Raft state from disk and re-elects) and swaps it in. All volatile
// in-memory state — role, commit index, in-flight votes — is lost, so this is what
// actually exercises the durable-log / hard-state / applied-watermark recovery
// paths that partitions leave untouched.
//
// The teardown order matters because an in-process node shares its address space
// with the client goroutines still calling it:
//   - stop() ends the node's own goroutines.
//   - Stepping the old node down releases any client commit parked in its wait
//     loop (it returns ErrNotLeader, freeing the storeMu read-lock it holds) and
//     ensures no late storePut can append to the raft log we are about to close.
//   - Closing the store and files under storeMu (exclusive) — the same fence the
//     InstallSnapshot swap uses — serialises against any in-flight read/write, so
//     none of them race the close; a call arriving afterward returns cleanly
//     (errClosed / not-leader) and the client treats the node as unreachable.
//
// The store and files are fully closed before the replacement opens the same dir,
// so there is only ever one writer to the raft log. The slice swap is guarded by
// the cluster's nodesMu.
func crashRestart(c *Cluster, i int, opts db.Options, cfg Config) error {
	c.nodesMu.RLock()
	old := c.nodes[i]
	size := len(c.nodes)
	c.nodesMu.RUnlock()

	old.stop()

	old.raftMu.Lock()
	old.role = Follower
	old.appliedCond.Broadcast()
	old.raftMu.Unlock()

	old.storeMu.Lock()
	old.store.Close()
	if old.logFile != nil {
		old.logFile.close()
	}
	if old.stateFile != nil {
		old.stateFile.close()
	}
	old.storeMu.Unlock()

	nd, err := buildNode(old.id, size, old.dir, opts, c.transport, cfg)
	if err != nil {
		return err
	}
	c.nodesMu.Lock()
	c.nodes[i] = nd
	c.nodesMu.Unlock()
	nd.start()
	return nil
}

// faultInjector churns a cluster until stopped, on a deterministic seed. It
// alternates minority partitions (isolate 1..maxDown nodes, heal) with, when
// crashes is set, crash+restart of a single node isolated for its downtime.
// maxDown is kept below a majority so a quorum always survives and the cluster
// keeps making progress. It heals everything on stop.
type faultInjector struct {
	t       *testing.T
	c       *Cluster
	pt      *partitionTransport
	opts    db.Options
	cfg     Config
	maxDown int
	crashes bool
}

func (fi *faultInjector) run(seed int64, stop <-chan struct{}, done chan<- struct{}) {
	rng := rand.New(rand.NewSource(seed))
	n := fi.c.Size()
	var down []NodeID
	heal := func() {
		for _, id := range down {
			fi.pt.reconnect(id)
		}
		down = nil
	}
	defer func() { heal(); close(done) }()
	for {
		select {
		case <-stop:
			return
		default:
		}
		if fi.crashes && rng.Intn(3) == 0 {
			// Crash one node and bring it back, isolated for the downtime so it
			// rebuilds and rejoins cleanly rather than racing stale messages.
			target := rng.Intn(n)
			fi.pt.disconnect(NodeID(target))
			if err := crashRestart(fi.c, target, fi.opts, fi.cfg); err != nil {
				fi.t.Errorf("crash/restart node %d: %v", target, err)
				fi.pt.reconnect(NodeID(target))
				return
			}
			time.Sleep(time.Duration(50+rng.Intn(150)) * time.Millisecond)
			fi.pt.reconnect(NodeID(target))
		} else {
			k := 1 + rng.Intn(fi.maxDown)
			for _, p := range rng.Perm(n)[:k] {
				fi.pt.disconnect(NodeID(p))
				down = append(down, NodeID(p))
			}
			time.Sleep(time.Duration(50+rng.Intn(200)) * time.Millisecond) // partitioned
			heal()
		}
		time.Sleep(time.Duration(50+rng.Intn(150)) * time.Millisecond) // healthy gap
	}
}

// TestStaleReadIsCaught is the deterministic demonstration that the checker
// catches the read anomaly the project documents as open: a client pinned to a
// partitioned ex-leader reads stale data. It orchestrates the exact scenario —
// write x=1, isolate the leader, write x=2 through the new leader, then read x
// from the old (still role==Leader) node — and asserts the read observes the
// stale "1" AND that lincheck flags the history as non-linearizable.
//
// This is a characterization test of the CURRENT behavior. When linearizable
// reads (ReadIndex / leader lease) land, the pinned read will instead redirect
// or block until confirmed, the stale observation disappears, and this test
// flips to asserting linearizable. Until then it pins the gap precisely.
func TestStaleReadIsCaught(t *testing.T) {
	const n = 3
	pt := newPartitionTransport()
	c, err := NewWithTransportConfig(n, dirs(t, n), testOpts(), pt, electionConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	h := &history{}
	oldLeaderID := c.Leader()

	// x = 1, committed and applied on the (soon-to-be ex-) leader.
	w0 := &client{id: 0, c: c, believed: oldLeaderID, h: h}
	w0.put("x", "1")
	if err := c.Quiesce(2 * time.Second); err != nil {
		t.Fatal(err)
	}

	// Isolate the leader; the majority elects a successor at a higher term. Capture
	// the successor inside the poll: re-querying afterward can race a further
	// leadership change and return nil.
	pt.disconnect(oldLeaderID)
	var newLeader *Node
	waitFor(t, 4*time.Second, func() bool {
		ld, ok := c.currentLeader()
		if ok && ld.id != oldLeaderID {
			newLeader = ld
			return true
		}
		return false
	})

	// x = 2 through the new leader; this write completes (majority side).
	w1 := &client{id: 1, c: c, believed: newLeader.id, h: h}
	w1.put("x", "2")

	// The pinned reader still believes the old leader, which is partitioned but
	// still role==Leader and happily serves its local (now stale) state.
	r := &client{id: 2, c: c, believed: oldLeaderID, h: h}
	before := len(h.snapshot())
	r.get("x")
	obs := h.snapshot()[before:]
	if len(obs) != 1 || obs[0].Kind != lincheck.Get || obs[0].Found != true || obs[0].Value != "1" {
		t.Fatalf("expected a stale read of x=\"1\" from the ex-leader, got %+v", obs)
	}
	t.Logf("pinned read from ex-leader %d observed stale x=%q while new leader %d holds x=\"2\"",
		oldLeaderID, obs[0].Value, newLeader.id)

	if res := lincheck.Check(h.snapshot()); res.Linearizable {
		t.Fatalf("history with a stale read reported linearizable — the checker missed the anomaly")
	}
	t.Log("lincheck correctly flagged the stale read as non-linearizable")
}

// soakConfig uses timers relaxed enough to stay stable under the CPU contention
// of `go test ./... -race` — a scheduling-delayed heartbeat must not trip a
// follower's election, or the cluster thrashes leaders and never makes progress —
// while still failing over well within a client's write deadline.
func soakConfig() Config {
	return Config{ElectionMin: 300 * time.Millisecond, ElectionMax: 600 * time.Millisecond, Heartbeat: 50 * time.Millisecond}
}

// within polls cond until it holds or the timeout elapses, without failing the
// test (unlike waitFor). Returns whether cond became true.
func within(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(2 * time.Millisecond)
	}
	return true
}

// settleConvergeCheck drives a chaos-tested cluster to a clean, well-defined
// convergence point and asserts it. After faults have healed it first commits one
// fresh write through the recovered cluster: a current-term entry whose commit
// pulls forward the commit of any prior-term tail (Raft §5.4.2), giving every node
// a definite index to apply up to — without it, a leader elected after the last
// write could sit on uncommitted prior-term entries indefinitely. It then quiesces,
// checks all nodes hold identical committed state for the workload keys, and runs
// the recorded history through lincheck (logging the verdict — believed-leader
// reads can be stale until linearizable reads land).
func settleConvergeCheck(t *testing.T, c *Cluster, h *history, label string) {
	t.Helper()
	if !within(20*time.Second, func() bool {
		return c.Put([]byte("__sync__"), []byte("1")) == nil
	}) {
		t.Fatalf("%s: no leader accepted a sync write after heal\n%s", label, dumpClusterState(c))
	}
	if err := c.Quiesce(30 * time.Second); err != nil {
		t.Fatalf("%s: quiesce after heal: %v\n%s", label, err, dumpClusterState(c))
	}

	keys := make([]string, 5)
	for i := range keys {
		keys[i] = fmt.Sprintf("k%d", i)
	}
	assertConverged(t, c, keys)

	hist := h.snapshot()
	res := lincheck.Check(hist)
	t.Logf("%s: recorded %d operations; lincheck linearizable=%v (offending key %q)",
		label, len(hist), res.Linearizable, res.Key)
}

// TestClusterConvergesUnderPartitions runs the randomized workload while faults
// churn the cluster, then heals, settles, and checks that every node converges to
// identical committed state — the write-path guarantee under churn. It also runs
// the full history through lincheck and logs the verdict: with believed-leader
// reads, stale reads from ex-leaders are expected, so the verdict is reported for
// visibility rather than asserted (it becomes an assertion once reads are made
// linearizable). Exercising the checker on a large real history also stress-tests
// it for performance.
func TestClusterConvergesUnderPartitions(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping partition soak in -short mode")
	}
	const n = 5
	opts := testOpts()
	cfg := soakConfig()
	pt := newPartitionTransport()
	c, err := NewWithTransportConfig(n, dirs(t, n), opts, pt, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	stop := make(chan struct{})
	done := make(chan struct{})
	fi := &faultInjector{t: t, c: c, pt: pt, opts: opts, cfg: cfg, maxDown: (n - 1) / 2}
	go fi.run(7, stop, done)

	h := runWorkload(c, 6, 50, 5, 99)

	close(stop)
	<-done // faults healed

	settleConvergeCheck(t, c, h, "partitions")
}

// dumpClusterState renders each node's Raft bookkeeping, for diagnosing a cluster
// that failed to resettle.
func dumpClusterState(c *Cluster) string {
	var b strings.Builder
	if ld, ok := c.currentLeader(); ok {
		fmt.Fprintf(&b, "leader=%d commitIndex=%d\n", ld.id, ld.commitIndexValue())
	} else {
		fmt.Fprintf(&b, "no leader\n")
	}
	for i := 0; i < c.Size(); i++ {
		nd := c.Node(i)
		fmt.Fprintf(&b, "  node %d: role=%s term=%d commit=%d applied=%d last=%d base=%d\n",
			nd.id, nd.roleValue(), nd.termValue(), nd.commitIndexValue(),
			nd.appliedIndex(), nd.lastIndex(), nd.baseIndexValue())
	}
	return b.String()
}

// TestClusterConvergesUnderCrashes is the crash/restart counterpart: alongside
// partitions, the injector repeatedly crashes a node and restarts it over its
// dir, dropping volatile Raft state and forcing reconstruction from the durable
// log, hard state, and applied watermark. After the chaos heals and the cluster
// resettles, every node must converge to identical committed state — no committed
// write lost, none re-applied — which is the real test of the durability and
// recovery machinery. The lincheck verdict is logged (believed-leader reads can
// still be stale until linearizable reads land).
//
// opts must be durable here: an abrupt-restart model only preserves acknowledged
// writes if they were fsynced, so unlike the other harness tests this one turns
// SyncOnWrite on.
func TestClusterConvergesUnderCrashes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping crash soak in -short mode")
	}
	const n = 5
	opts := db.Options{SyncOnWrite: true, DisableBackgroundCompaction: true}
	cfg := soakConfig()
	pt := newPartitionTransport()
	c, err := NewWithTransportConfig(n, dirs(t, n), opts, pt, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	stop := make(chan struct{})
	done := make(chan struct{})
	fi := &faultInjector{t: t, c: c, pt: pt, opts: opts, cfg: cfg, maxDown: (n - 1) / 2, crashes: true}
	go fi.run(11, stop, done)

	h := runWorkload(c, 6, 40, 5, 123)

	close(stop)
	<-done // faults healed, all nodes restarted-and-reconnected

	settleConvergeCheck(t, c, h, "crashes+partitions")
}
