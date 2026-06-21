package cluster

import (
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
// follower, reconstructs its Raft log from raft.log, recovers its committed
// state from the data WAL, and the cluster re-elects at a higher term. All
// committed keys must survive on every node.
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

	waitFor(t, 3*time.Second, func() bool {
		_, ok := c2.currentLeader()
		return ok
	})

	ld, _ := c2.currentLeader()
	if got := nodeTerm(ld); got <= term1 {
		t.Errorf("re-election term %d, want > %d (restart must not bootstrap a leader)", got, term1)
	}

	// Every node recovered every committed key from its own data WAL.
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

// TestClusterFlushBeforeRestart forces a memtable flush mid-operation, then
// restarts. This is the case the naive "count OpCommits in the WAL" derivation
// of lastApplied gets wrong: the flush moves entries into an SSTable and resets
// the WAL, so the post-restart WAL under-counts. The applied watermark
// (SSTable footer appliedIndex + WAL base) must reconstruct the exact index, so
// recovered == total writes on every node, with no re-apply or data loss.
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

	waitFor(t, 3*time.Second, func() bool {
		_, ok := c2.currentLeader()
		return ok
	})

	for i := 0; i < n; i++ {
		d := c2.Node(i).DB()
		// The watermark survived the flush: exact applied index, no under-count.
		if got := d.RecoveredAppliedIndex(); got != writes {
			t.Errorf("node %d recovered appliedIndex %d, want %d (watermark lost across flush?)", i, got, writes)
		}
		for k, v := range want {
			got, err := d.Get([]byte(k))
			if err != nil || string(got) != v {
				t.Errorf("node %d after flush+restart: %s=(%q,%v), want %q", i, k, got, err, v)
			}
		}
	}
}
