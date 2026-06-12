package cluster

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/BrandonnLow/littledb/internal/db"
)

func dirs(t *testing.T, n int) []string {
	t.Helper()
	out := make([]string, n)
	for i := range out {
		out[i] = t.TempDir()
	}
	return out
}

func testOpts() db.Options {
	return db.Options{SyncOnWrite: false, DisableBackgroundCompaction: true}
}

func newCluster(t *testing.T, n int) *Cluster {
	t.Helper()
	c, err := New(n, dirs(t, n), testOpts())
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// assertConverged checks every node holds identical state for the given keys.
func assertConverged(t *testing.T, c *Cluster, keys []string) {
	t.Helper()
	leader := c.Node(int(c.Leader())).DB()
	for _, k := range keys {
		want, werr := leader.Get([]byte(k))
		for i := 0; i < c.Size(); i++ {
			got, gerr := c.Node(i).DB().Get([]byte(k))
			leaderMissing := errors.Is(werr, db.ErrKeyNotFound)
			nodeMissing := errors.Is(gerr, db.ErrKeyNotFound)
			if leaderMissing != nodeMissing || (!leaderMissing && !bytes.Equal(got, want)) {
				t.Errorf("key %q: node %d=(%q,%v) leader=(%q,%v)", k, i, got, gerr, want, werr)
			}
		}
	}
}

func TestClusterConverges3(t *testing.T) { testConverges(t, 3) }
func TestClusterConverges5(t *testing.T) { testConverges(t, 5) }

func testConverges(t *testing.T, n int) {
	c := newCluster(t, n)
	defer c.Close()

	const distinct = 50
	const writes = 300
	for i := 0; i < writes; i++ {
		k := fmt.Sprintf("k%04d", i%distinct) // repeated keys => overwrites
		v := fmt.Sprintf("v%d", i)
		if err := c.Put([]byte(k), []byte(v)); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	if err := c.Quiesce(2 * time.Second); err != nil {
		t.Fatal(err)
	}

	keys := make([]string, distinct)
	for i := range keys {
		keys[i] = fmt.Sprintf("k%04d", i)
	}
	assertConverged(t, c, keys)

	// Last writer for each key must win uniformly.
	for i := 0; i < distinct; i++ {
		k := fmt.Sprintf("k%04d", i)
		// The last write to key i was at iteration (largest j < writes with j%distinct==i).
		lastJ := i
		for j := i; j < writes; j += distinct {
			lastJ = j
		}
		want := fmt.Sprintf("v%d", lastJ)
		got, err := c.Get([]byte(k))
		if err != nil || string(got) != want {
			t.Errorf("key %s: leader got (%q,%v), want %q", k, got, err, want)
		}
	}
}

func TestClusterTxnReplicates(t *testing.T) {
	c := newCluster(t, 3)
	defer c.Close()

	tx := c.Begin()
	tx.Put([]byte("a"), []byte("1"))
	tx.Put([]byte("b"), []byte("2"))
	tx.Put([]byte("c"), []byte("3"))
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := c.Quiesce(time.Second); err != nil {
		t.Fatal(err)
	}
	assertConverged(t, c, []string{"a", "b", "c"})
	for i := 0; i < c.Size(); i++ {
		got, err := c.Node(i).DB().Get([]byte("b"))
		if err != nil || string(got) != "2" {
			t.Errorf("node %d: b=(%q,%v), want 2", i, got, err)
		}
	}
}

func TestClusterDeleteReplicates(t *testing.T) {
	c := newCluster(t, 5)
	defer c.Close()

	c.Put([]byte("k"), []byte("v"))
	c.Delete([]byte("k"))
	if err := c.Quiesce(time.Second); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < c.Size(); i++ {
		if _, err := c.Node(i).DB().Get([]byte("k")); !errors.Is(err, db.ErrKeyNotFound) {
			t.Errorf("node %d: expected ErrKeyNotFound, got %v", i, err)
		}
	}
}

// TestClusterCommitWaitsForMajority verifies the Raft guarantee on return.
// Under deferred apply, followers append (and ack) an entry on receipt but
// apply it only once the leader's commit index reaches them (a later
// MsgCommit), so the count of nodes that have *applied* the write right after
// Put returns may be only the leader. What a quorum is guaranteed to hold on
// return is the entry in its *log*; the leader additionally has it applied.
func TestClusterCommitWaitsForMajority(t *testing.T) {
	const n = 5
	c := newCluster(t, n)
	defer c.Close()

	if err := c.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	majority := n/2 + 1

	// The leader has applied: the write is readable on it immediately.
	if got, err := c.Get([]byte("k")); err != nil || string(got) != "v" {
		t.Fatalf("leader Get right after commit = (%q,%v), want v", got, err)
	}

	// A majority has the entry durably in its log (leader + acking quorum).
	logged := 0
	for i := 0; i < n; i++ {
		if c.Node(i).lastIndex() >= 1 {
			logged++
		}
	}
	if logged < majority {
		t.Errorf("right after commit, %d/%d nodes have the entry logged, want >= %d (majority)", logged, n, majority)
	}
}

func TestClusterSingleNode(t *testing.T) {
	// n=1: no peers, no replication needed; behaves like a plain DB.
	c := newCluster(t, 1)
	defer c.Close()
	c.Put([]byte("k"), []byte("v"))
	if err := c.Quiesce(time.Second); err != nil {
		t.Fatal(err)
	}
	got, err := c.Get([]byte("k"))
	if err != nil || string(got) != "v" {
		t.Errorf("got (%q,%v), want v", got, err)
	}
}
