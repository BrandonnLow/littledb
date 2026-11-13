package cluster

import (
	"fmt"
	"testing"
	"time"

	"github.com/BrandonnLow/littledb/internal/db"
)

func snapshotOpts() db.Options {
	return db.Options{SyncOnWrite: true, DisableBackgroundCompaction: true}
}

// TestLaggingFollowerGetsSnapshot pins decision 6 (aggressive compaction) and
// the InstallSnapshot path. With the min-matchIndex bound dropped, a leader
// compacts to its own lastApplied even while a follower is partitioned — past
// that follower's matchIndex. On reconnect the follower's nextIndex sits in the
// compacted prefix, so the leader catches it up with a snapshot (not
// AppendEntries), after which it holds every key.
func TestLaggingFollowerGetsSnapshot(t *testing.T) {
	const n = 3
	ds := dirs(t, n)
	pt := newPartitionTransport()
	cfg := stableConfig() // no spurious elections while a follower is partitioned
	cfg.SnapshotThreshold = 4
	c, err := NewWithTransportConfig(n, ds, snapshotOpts(), pt, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	waitFor(t, time.Second, func() bool { return c.Node(0).roleValue() == Leader })

	// Partition follower 2 so its matchIndex stays 0, then write past the
	// threshold so the leader compacts past it.
	pt.disconnect(2)
	for i := 0; i < 8; i++ {
		if err := c.Put([]byte(fmt.Sprintf("k%d", i)), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	// Aggressive compaction: the leader's base advances past follower 2's
	// matchIndex (0) — the exact opposite of the old min-matchIndex bound.
	waitFor(t, 3*time.Second, func() bool { return c.Node(0).baseIndexValue() > 0 })
	leaderBase := c.Node(0).baseIndexValue()
	t.Logf("leader compacted to base %d while follower 2 partitioned", leaderBase)

	// Reconnect follower 2: its nextIndex is in the compacted prefix, so it must
	// be caught up by InstallSnapshot.
	pt.reconnect(2)
	// Generous: a synchronous install (rebuild + swap + reopen) can exceed a few
	// seconds under the CPU contention of the full -race suite.
	waitFor(t, 15*time.Second, func() bool { return c.Node(2).InstallsForTesting() >= 1 })

	// After install, follower 2's base equals the snapshot's lastIncludedIndex
	// (> 0) and it holds every key. Use DB() (storeMu-guarded): the install
	// swapped the store out from under the node.
	if base := c.Node(2).baseIndexValue(); base == 0 {
		t.Fatalf("follower 2 base still 0 after install")
	}
	waitFor(t, 2*time.Second, func() bool {
		v, err := c.Node(2).DB().Get([]byte("k7"))
		return err == nil && string(v) == "v7"
	})
	for i := 0; i < 8; i++ {
		key := fmt.Sprintf("k%d", i)
		want := fmt.Sprintf("v%d", i)
		v, err := c.Node(2).DB().Get([]byte(key))
		if err != nil || string(v) != want {
			t.Errorf("follower 2 Get(%s) = (%q,%v), want %s", key, v, err, want)
		}
	}
	t.Logf("follower 2 caught up via InstallSnapshot (installs=%d, base=%d)",
		c.Node(2).InstallsForTesting(), c.Node(2).baseIndexValue())
}

// TestRestartAfterCompaction pins decision 9: a compacted raft.log persists its
// base across a full cluster restart, the in-memory log is reconstructed as
// base+suffix, and all applied data survives.
func TestRestartAfterCompaction(t *testing.T) {
	const n = 3
	ds := dirs(t, n)
	cfg := electionConfig() // fast re-election after restart (a restarted node starts as a follower)
	cfg.SnapshotThreshold = 4
	opts := snapshotOpts()

	c, err := NewWithTransportConfig(n, ds, opts, NewChannelTransport(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if err := c.Put([]byte(fmt.Sprintf("k%02d", i)), []byte(fmt.Sprintf("v%02d", i))); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	if err := c.Quiesce(2 * time.Second); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 3*time.Second, func() bool { return c.Node(0).baseIndexValue() > 0 })
	baseBefore := c.Node(0).baseIndexValue()
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}

	c2, err := NewWithTransportConfig(n, ds, opts, NewChannelTransport(), cfg)
	if err != nil {
		t.Fatalf("restart: %v", err)
	}
	defer c2.Close()
	if got := c2.Node(0).baseIndexValue(); got != baseBefore {
		t.Fatalf("restarted node 0 base = %d, want %d (compaction must persist)", got, baseBefore)
	}
	if li := c2.Node(0).lastIndex(); li < baseBefore {
		t.Fatalf("restarted lastIndex %d below base %d", li, baseBefore)
	}
	waitFor(t, 3*time.Second, func() bool { _, ok := c2.currentLeader(); return ok })
	for i := 0; i < 10; i++ {
		want := fmt.Sprintf("v%02d", i)
		if v, err := c2.Get([]byte(fmt.Sprintf("k%02d", i))); err != nil || string(v) != want {
			t.Errorf("after restart Get(k%02d) = (%q,%v), want %s", i, v, err, want)
		}
	}
}

// TestLeadershipChangeAfterCompaction is the across-elections correctness pin: a
// leader compacts (base > 0), then fails; a full-log follower takes over and,
// when the ex-leader rejoins, brings it current. With aggressive compaction the
// ex-leader may now reconverge either by AppendEntries from the compaction
// boundary OR by InstallSnapshot if its nextIndex has fallen into the new
// leader's compacted prefix; the test asserts convergence and data integrity
// either way (it no longer requires the snapshot-free path). No node is stranded
// below another's base; all converge.
func TestLeadershipChangeAfterCompaction(t *testing.T) {
	const n = 3
	ds := dirs(t, n)
	pt := newPartitionTransport()
	cfg := electionConfig()
	cfg.SnapshotThreshold = 4
	c, err := NewWithTransportConfig(n, ds, snapshotOpts(), pt, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	for i := 0; i < 8; i++ {
		if err := c.Put([]byte(fmt.Sprintf("k%d", i)), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	if err := c.Quiesce(2 * time.Second); err != nil {
		t.Fatal(err)
	}
	old, ok := c.currentLeader()
	if !ok {
		t.Fatal("no leader after initial writes")
	}
	waitFor(t, 3*time.Second, func() bool { return old.baseIndexValue() > 0 })
	oldID, oldTerm := old.id, old.termValue()
	t.Logf("leader %d compacted to base %d before failover", oldID, old.baseIndexValue())

	// Fail the compacted leader; a connected full-log follower must take over.
	pt.disconnect(oldID)
	waitFor(t, 5*time.Second, func() bool {
		ld, ok := c.currentLeader()
		return ok && ld.id != oldID && ld.termValue() > oldTerm
	})
	newLeader, _ := c.currentLeader()
	t.Logf("failover: node %d -> node %d", oldID, newLeader.id)

	if err := c.Put([]byte("after"), []byte("compact")); err != nil {
		t.Fatalf("post-failover put: %v", err)
	}

	// Reconnect the ex-leader: it converges on both new and pre-compaction data
	// (via AppendEntries or InstallSnapshot). Read through DB() since an install
	// would swap the store.
	pt.reconnect(oldID)
	waitFor(t, 6*time.Second, func() bool {
		v, err := c.Node(int(oldID)).DB().Get([]byte("after"))
		if err != nil || string(v) != "compact" {
			return false
		}
		v7, err := c.Node(int(oldID)).DB().Get([]byte("k7"))
		return err == nil && string(v7) == "v7"
	})
	for _, nd := range []int{0, 1, 2} {
		if v, err := c.Node(nd).DB().Get([]byte("k0")); err != nil || string(v) != "v0" {
			t.Errorf("node %d Get(k0) = (%q,%v), want v0", nd, v, err)
		}
	}
}

// TestCompactionStopsAtDurableIndex pins the compaction bound in isolation. With
// the Raft log as the store's only write-ahead log, an entry that is applied but
// not yet flushed exists on disk nowhere but raft.log, so compaction must stop
// at the store's durable index even when more has been applied.
func TestCompactionStopsAtDurableIndex(t *testing.T) {
	l := NewRaftLog()
	for i := 0; i < 6; i++ {
		l.append(1, EntryData, []byte(fmt.Sprintf("d%d", i)))
	}
	n := &Node{
		log:          l,
		cfg:          Config{SnapshotThreshold: 1},
		lastApplied:  5,
		durableIndex: 3, // 4 and 5 are applied but live only in the memtable
	}
	n.raftMu.Lock()
	n.maybeCompactLocked()
	n.raftMu.Unlock()
	if n.log.baseIndex != 3 {
		t.Fatalf("compacted to base %d, want 3 (the durable index, not lastApplied 5)", n.log.baseIndex)
	}
	if got := n.log.lastIndex(); got != 6 {
		t.Fatalf("lastIndex = %d after compaction, want 6", got)
	}
}

// TestCompactionBoundedByDurableIndex is the cluster-level counterpart. Small
// writes never fill a 4 MB memtable, so with SnapshotThreshold set the nodes
// compact only because the apply loop flushes their stores once enough applied
// entries are not yet durable. On every node the log base must sit at or below
// the store's durable index, and a restart — every store back exactly at its
// last flush, everything after it replayed from raft.log — must find every entry
// it needs and converge.
func TestCompactionBoundedByDurableIndex(t *testing.T) {
	const n = 3
	ds := dirs(t, n)
	cfg := electionConfig()
	cfg.SnapshotThreshold = 4
	opts := snapshotOpts()

	c, err := NewWithTransportConfig(n, ds, opts, NewChannelTransport(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{}
	for i := 0; i < 11; i++ {
		k, v := fmt.Sprintf("k%02d", i), fmt.Sprintf("v%02d", i)
		if err := c.Put([]byte(k), []byte(v)); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
		want[k] = v
	}
	if err := c.Quiesce(2 * time.Second); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		nd := c.Node(i)
		waitFor(t, 3*time.Second, func() bool { return nd.baseIndexValue() > 0 })
		base, durable := nd.baseIndexValue(), nd.DB().DurableAppliedIndex()
		if base > durable {
			t.Fatalf("node %d compacted to base %d past its durable index %d", i, base, durable)
		}
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}

	c2, err := NewWithTransportConfig(n, ds, opts, NewChannelTransport(), cfg)
	if err != nil {
		t.Fatalf("restart: %v", err)
	}
	defer c2.Close()
	waitFor(t, 5*time.Second, func() bool { return allNodesHave(c2, want) })
}
