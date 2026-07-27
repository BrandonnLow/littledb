package cluster

import (
	"fmt"
	"testing"
	"time"
)

// TestSnapshotCarriesConfigAndSessions pins Stage 6b end-to-end: an InstallSnapshot
// carries the membership configuration AND the session dedup table, so a follower
// caught up by a snapshot does not lose either.
//
// Setup: a 4-node cluster records a sessioned write (so the leader has session
// state to ship), then follower 3 is partitioned and removed from the config on the
// {0,1,2} side. Writes past the snapshot threshold make the leader compact past
// follower 3's matchIndex. On reconnect, follower 3's nextIndex is in the compacted
// prefix, so it is caught up by InstallSnapshot — and the snapshot must teach it the
// new config ({0,1,2}, which it never saw the log entry for) and the session table.
func TestSnapshotCarriesConfigAndSessions(t *testing.T) {
	const n = 4
	pt := newPartitionTransport()
	cfg := stableConfig() // node 0 stays leader; no elections while 3 is partitioned
	cfg.SnapshotThreshold = 4
	c, err := NewWithTransportConfig(n, dirs(t, n), snapshotOpts(), pt, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	waitFor(t, time.Second, func() bool { return c.Node(0).roleValue() == Leader })

	// A sessioned write, so the leader's session table has an entry to ship.
	tx := c.Begin()
	tx.SetSession([]byte("clientA"), 7)
	tx.Put([]byte("s"), []byte("v"))
	if err := tx.Commit(); err != nil {
		t.Fatalf("sessioned commit: %v", err)
	}

	// Partition follower 3, then remove it from the config (committed on {0,1,2}).
	pt.disconnect(3)
	if err := c.RemoveServer(3); err != nil {
		t.Fatalf("RemoveServer(3): %v", err)
	}

	// Write past the threshold so the leader compacts past follower 3's matchIndex.
	for i := 0; i < 8; i++ {
		if err := c.Put([]byte(fmt.Sprintf("k%d", i)), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	waitFor(t, 3*time.Second, func() bool { return c.Node(0).baseIndexValue() > 0 })

	// Reconnect follower 3: it is caught up via InstallSnapshot.
	pt.reconnect(3)
	waitFor(t, 15*time.Second, func() bool { return c.Node(3).InstallsForTesting() >= 1 })

	// The snapshot carried the configuration: follower 3 now holds {0,1,2} — it
	// learns it was removed, though it never received the config log entry. (Without
	// config-in-snapshot it would keep its stale bootstrap view {0,1,2,3}.)
	n3 := c.Node(3)
	n3.raftMu.Lock()
	got := copyConfig(n3.config)
	n3.raftMu.Unlock()
	if !configEqual(got, map[NodeID]bool{0: true, 1: true, 2: true}) {
		t.Fatalf("follower 3 config after install = %v, want {0,1,2} (snapshot must carry config)", got)
	}

	// The snapshot carried the session table: exactly-once dedup survives the install.
	waitFor(t, 2*time.Second, func() bool {
		return c.Node(3).DB().SessionLastSeq([]byte("clientA")) == 7
	})
	if seq := c.Node(3).DB().SessionLastSeq([]byte("clientA")); seq != 7 {
		t.Fatalf("follower 3 SessionLastSeq(clientA) after install = %d, want 7 (snapshot must carry sessions)", seq)
	}
	t.Logf("follower 3 caught up via InstallSnapshot with config %v and session clientA=7", got)
}
