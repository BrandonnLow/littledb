package cluster

import (
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/BrandonnLow/littledb/internal/db"
)

// sessClient is a session-aware client: a unique session id and a monotonic seq
// counter. It retries each operation — reusing the SAME seq — across leader
// changes and blocked attempts, relying on the state machine's (session, seq)
// dedup to make a committed-but-unacked retry apply exactly once.
type sessClient struct {
	c        *Cluster
	session  []byte
	seq      uint64
	believed NodeID
}

func (cl *sessClient) order() []NodeID {
	n := cl.c.Size()
	ids := make([]NodeID, 0, n)
	ids = append(ids, cl.believed)
	for off := 1; off < n; off++ {
		ids = append(ids, NodeID((int(cl.believed)+off)%n))
	}
	return ids
}

// increment applies +1 to key as one exactly-once logical operation. It assigns a
// fresh seq, then retries that seq across nodes until a definitive success (nil) or
// the deadline. A retry after the write already committed is deduped by the state
// machine and returns nil, so the loop terminates once the cluster stabilizes.
// Returns whether it succeeded.
func (cl *sessClient) increment(key string, deadline time.Time) bool {
	cl.seq++
	seq := cl.seq
	apply := func(tx *db.Txn) error {
		n := int64(0)
		v, err := tx.Get([]byte(key))
		if err == nil {
			n, _ = strconv.ParseInt(string(v), 10, 64)
		} else if !errors.Is(err, db.ErrKeyNotFound) {
			return err
		}
		return tx.Put([]byte(key), []byte(strconv.FormatInt(n+1, 10)))
	}
	for time.Now().Before(deadline) {
		for _, id := range cl.order() {
			node := cl.c.Node(int(id))
			done := make(chan error, 1)
			go func() { done <- node.commitSessioned(cl.session, seq, apply) }()
			select {
			case err := <-done:
				if err == nil {
					cl.believed = id
					return true
				}
				// Any error (not-leader, maybe-committed, conflict, closed) → retry
				// the SAME seq elsewhere; dedup guarantees at-most-once apply.
			case <-time.After(writeAttemptTimeout):
				// Blocked (e.g. barrier on a partitioned leader): abandon and try the
				// next node; the leaked goroutine resolves once the partition heals.
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}

// TestExactlyOnceCounterUnderCrashes is the exactly-once payoff. A single
// sequential client increments a counter N times under crash + partition churn,
// retrying each increment (reusing its seq) until a definitive success. Because a
// read-modify-write is NOT idempotent, a committed-but-unacked increment that is
// retried would double-count without dedup — so the final counter equalling
// exactly N is the proof that (session, seq) dedup delivers exactly-once. Reads and
// the RMW go through the read-index barrier, so an increment never builds on a
// stale (freshly-elected, apply-lagging) leader's view.
//
// Compaction is disabled: InstallSnapshot does not yet carry the session table
// (a documented v1 limitation), so a snapshot install would drop dedup state.
func TestExactlyOnceCounterUnderCrashes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping exactly-once crash soak in -short mode")
	}
	const (
		n = 5
		N = 25
	)
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
	go fi.run(31, stop, done)

	cl := &sessClient{c: c, session: []byte("counter-client"), believed: bootstrapLeader}
	for i := 0; i < N; i++ {
		if !cl.increment("counter", time.Now().Add(20*time.Second)) {
			close(stop)
			<-done
			t.Fatalf("increment %d did not succeed within deadline\n%s", i, dumpClusterState(c))
		}
	}

	close(stop)
	<-done // faults healed

	// Settle on one leader, then read the counter linearizably.
	if !within(30*time.Second, func() bool { return c.Put([]byte("__sync__"), []byte("1")) == nil }) {
		t.Fatalf("no leader accepted a sync write after heal\n%s", dumpClusterState(c))
	}
	if err := c.Quiesce(30 * time.Second); err != nil {
		t.Fatalf("quiesce after heal: %v\n%s", err, dumpClusterState(c))
	}

	got, err := c.Get([]byte("counter"))
	if err != nil {
		t.Fatalf("read counter: %v", err)
	}
	v, err := strconv.ParseInt(string(got), 10, 64)
	if err != nil {
		t.Fatalf("counter value %q not an integer: %v", got, err)
	}
	if v != N {
		t.Fatalf("counter = %d, want %d — each of %d increments must apply exactly once", v, N, N)
	}
	t.Logf("counter = %d after %d sessioned increments under crashes+partitions — exactly once", v, N)
}
