package cluster

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/BrandonnLow/littledb/internal/db"
)

// nodeTerm reads a node's current term under its lock.
func nodeTerm(nd *Node) uint64 {
	nd.raftMu.Lock()
	defer nd.raftMu.Unlock()
	return nd.currentTerm
}

// countSSTables returns how many flushed SSTables sit in a node's data dir.
func countSSTables(t *testing.T, dir string) int {
	t.Helper()
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, e := range ents {
		if filepath.Ext(e.Name()) == ".sst" {
			n++
		}
	}
	return n
}

// TestClusterFullRestartPreservesData restarts an entire cluster over the same
// directories. With no bootstrap (state is on disk), every node restarts as a
// follower, reconstructs its Raft log from raft.log, and the cluster re-elects
// at a higher term. Nothing was flushed, so every store comes back EMPTY — the
// Raft log is the only log, and no data WAL holds the writes — and the
// committed state must come back by replay: the new leader commits a no-op in
// its own term (Raft §8), which commits the whole prior-term log, and every node
// re-applies it. All committed keys must then be on every node, with no client
// traffic needed to trigger it.
func TestClusterFullRestartPreservesData(t *testing.T) {
	const n = 3
	ds := dirs(t, n)
	opts := db.Options{SyncOnWrite: true, DisableBackgroundCompaction: true}

	c1, err := NewWithTransportConfig(n, ds, opts, NewChannelTransport(), electionConfig())
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{}
	for i := 0; i < 12; i++ {
		k := fmt.Sprintf("k%02d", i)
		v := fmt.Sprintf("v%02d", i)
		if err := c1.Put([]byte(k), []byte(v)); err != nil {
			t.Fatal(err)
		}
		want[k] = v
	}
	if err := c1.Quiesce(2 * time.Second); err != nil {
		t.Fatal(err)
	}
	term1 := nodeTerm(c1.Node(int(c1.Leader())))
	if err := c1.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen the SAME dirs. No bootstrap: nodes restart as followers and elect.
	c2, err := NewWithTransportConfig(n, ds, opts, NewChannelTransport(), electionConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()

	// Capture the leader inside the poll: re-querying after the wait can race a
	// leadership change under the tight election timers and return nil.
	var ld *Node
	waitFor(t, 3*time.Second, func() bool {
		l, ok := c2.currentLeader()
		if ok {
			ld = l
		}
		return ok
	})

	if got := nodeTerm(ld); got <= term1 {
		t.Errorf("re-election term %d, want > %d (restart must not bootstrap a leader)", got, term1)
	}
	for i := 0; i < n; i++ {
		if got := c2.Node(i).DB().RecoveredAppliedIndex(); got != 0 {
			t.Fatalf("node %d recovered applied index %d, want 0 (test expects no flush)", i, got)
		}
	}

	// Every node re-applied every committed key from its own raft.log.
	waitFor(t, 5*time.Second, func() bool { return allNodesHave(c2, want) })
	for i := 0; i < n; i++ {
		d := c2.Node(i).DB()
		for k, v := range want {
			got, err := d.Get([]byte(k))
			if err != nil || string(got) != v {
				t.Errorf("node %d after restart: %s=(%q,%v), want %q", i, k, got, err, v)
			}
		}
	}
}

// allNodesHave reports whether every node's store holds every key in want with
// the expected value (read directly, not through the leader).
func allNodesHave(c *Cluster, want map[string]string) bool {
	for _, nd := range c.snapshotNodes() {
		d := nd.DB()
		for k, v := range want {
			if got, err := d.Get([]byte(k)); err != nil || string(got) != v {
				return false
			}
		}
	}
	return true
}

// TestRestartedLeaderDoesNotLoseOverwrite is the regression pin for the catch-up
// barrier. After a full restart with nothing flushed, every store comes back
// empty and the key's whole history lives only in raft.log. Whichever node wins
// the election therefore starts with a commit-timestamp counter far behind the
// timestamps already in its log. If it prepared a write before applying that
// log, the overwrite would get a LOWER timestamp than the versions it replaces
// and be shadowed by them — acknowledged to the client, then lost. The leader
// must apply its log first (the term-start no-op, or the catch-up in the write
// path, whichever gets there first), so the overwrite is what every node reads.
func TestRestartedLeaderDoesNotLoseOverwrite(t *testing.T) {
	const n = 3
	ds := dirs(t, n)
	opts := db.Options{SyncOnWrite: true, DisableBackgroundCompaction: true}

	c1, err := NewWithTransportConfig(n, ds, opts, NewChannelTransport(), electionConfig())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if err := c1.Put([]byte("k"), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := c1.Quiesce(2 * time.Second); err != nil {
		t.Fatal(err)
	}
	if err := c1.Close(); err != nil {
		t.Fatal(err)
	}

	c2, err := NewWithTransportConfig(n, ds, opts, NewChannelTransport(), electionConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	for i := 0; i < n; i++ {
		if got := c2.Node(i).DB().RecoveredAppliedIndex(); got != 0 {
			t.Fatalf("node %d recovered applied index %d, want 0 (the hazard needs an unflushed restart)", i, got)
		}
	}

	// Overwrite as soon as a leader exists — racing its term-start no-op. Elections
	// right after a full restart can churn (a second timer fires just after the
	// first leader wins), so an attempt may come back ErrMaybeCommitted; like any
	// client, retry it. The retry writes the same value, so it is idempotent, and
	// the check below still catches a shadowed overwrite whichever attempt lands.
	for attempt := 1; ; attempt++ {
		err := c2.Put([]byte("k"), []byte("after"))
		if err == nil {
			break
		}
		if !errors.Is(err, ErrMaybeCommitted) || attempt == 5 {
			t.Fatalf("overwrite after restart (attempt %d): %v", attempt, err)
		}
	}
	if v, err := c2.Get([]byte("k")); err != nil || string(v) != "after" {
		t.Fatalf("linearizable read after overwrite = (%q,%v), want \"after\" (lost write)", v, err)
	}
	waitFor(t, 5*time.Second, func() bool { return allNodesHave(c2, map[string]string{"k": "after"}) })
}

// TestClusterFlushBeforeRestart forces memtable flushes mid-operation, then
// restarts, so each store comes back with a durable prefix in SSTables and an
// unflushed suffix that exists only in raft.log. Recovery must stitch the two
// together exactly at the flush point: the store reports the last flush as its
// recovered applied index, the node resumes applying from the next entry, and
// every key — flushed or not — ends up on every node, with no entry skipped or
// applied twice (ApplyEntry rejects both).
func TestClusterFlushBeforeRestart(t *testing.T) {
	const n = 3
	const writes = 60
	ds := dirs(t, n)
	// Tiny memtable so a handful of writes forces at least one flush per node.
	opts := db.Options{SyncOnWrite: true, MemtableSizeMax: 512, DisableBackgroundCompaction: true}

	c1, err := NewWithTransportConfig(n, ds, opts, NewChannelTransport(), electionConfig())
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{}
	for i := 0; i < writes; i++ {
		k := fmt.Sprintf("key%04d", i)
		v := fmt.Sprintf("val%04d", i)
		if err := c1.Put([]byte(k), []byte(v)); err != nil {
			t.Fatal(err)
		}
		want[k] = v
	}
	if err := c1.Quiesce(2 * time.Second); err != nil {
		t.Fatal(err)
	}
	// The test is only meaningful if a flush actually happened.
	for i := 0; i < n; i++ {
		if got := countSSTables(t, ds[i]); got == 0 {
			t.Fatalf("node %d: no flush occurred; raise writes or lower MemtableSizeMax", i)
		}
	}
	if err := c1.Close(); err != nil {
		t.Fatal(err)
	}

	c2, err := NewWithTransportConfig(n, ds, opts, NewChannelTransport(), electionConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()

	for i := 0; i < n; i++ {
		// The store came back as of its last flush: a durable prefix, no more.
		if got := c2.Node(i).DB().RecoveredAppliedIndex(); got == 0 || got > writes {
			t.Errorf("node %d recovered applied index %d, want the last flush point in (0, %d]", i, got, writes)
		}
	}
	waitFor(t, 5*time.Second, func() bool { return allNodesHave(c2, want) })
	for i := 0; i < n; i++ {
		nd := c2.Node(i)
		if got := nd.appliedIndex(); got < writes {
			t.Errorf("node %d applied index %d after convergence, want >= %d", i, got, writes)
		}
		for k, v := range want {
			got, err := nd.DB().Get([]byte(k))
			if err != nil || string(got) != v {
				t.Errorf("node %d after flush+restart: %s=(%q,%v), want %q", i, k, got, err, v)
			}
		}
	}
}
