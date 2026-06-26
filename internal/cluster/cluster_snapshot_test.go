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

// TestLeaderCompactionBoundedByMinMatchIndex: a leader only compacts up to
// the slowest follower's matchIndex. A partitioned follower pins safe at 0
// (no compaction), and on reconnect it catches up via plain AppendEntries
// — no InstallSnapshot — after which compaction proceeds.
func TestLeaderCompactionBoundedByMinMatchIndex(t *testing.T) {
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

	// Partition follower 2 so its matchIndex stays 0.
	pt.disconnect(2)
	for i := 0; i < 8; i++ {
		if err := c.Put([]byte(fmt.Sprintf("k%d", i)), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	waitFor(t, 2*time.Second, func() bool { return c.Node(0).lastIndex() >= 8 })
	// Every successful ack from follower 1 ran maybeCompactLocked with
	// safe=min(lastApplied, matchIndex[1], matchIndex[2]=0)=0, so the base must
	// still be 0.
	time.Sleep(150 * time.Millisecond)
	if base := c.Node(0).baseIndexValue(); base != 0 {
		t.Fatalf("leader compacted to base %d with follower 2 partitioned; want 0 (minMatchIndex bound)", base)
	}

	// Reconnect follower 2: it catches up by ordinary replication, matchIndex[2]
	// rises, and the leader can finally compact.
	pt.reconnect(2)
	waitFor(t, 5*time.Second, func() bool { return c.Node(0).baseIndexValue() > 0 })
	waitFor(t, 2*time.Second, func() bool {
		v, err := c.Node(2).store.Get([]byte("k7"))
		return err == nil && string(v) == "v7"
	})
	t.Logf("after reconnect: leader base=%d, follower 2 caught up via AppendEntries", c.Node(0).baseIndexValue())
}

// TestRestartAfterCompaction: a compacted raft.log persists its base across a
// full cluster restart, the in-memory log is reconstructed as base+suffix, and
// all applied data survives.
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
// when the ex-leader rejoins, serves it from the compaction boundary
// (prevLogIndex == ex-leader.baseIndex, matched against baseTerm) via plain
// AppendEntries. No node is stranded below another's base; all converge.
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
	// without ever receiving a snapshot RPC.
	pt.reconnect(oldID)
	waitFor(t, 6*time.Second, func() bool {
		v, err := c.Node(int(oldID)).store.Get([]byte("after"))
		if err != nil || string(v) != "compact" {
			return false
		}
		v7, err := c.Node(int(oldID)).store.Get([]byte("k7"))
		return err == nil && string(v7) == "v7"
	})
	for _, nd := range []int{0, 1, 2} {
		if v, err := c.Node(nd).store.Get([]byte("k0")); err != nil || string(v) != "v0" {
			t.Errorf("node %d Get(k0) = (%q,%v), want v0", nd, v, err)
		}
	}
}
