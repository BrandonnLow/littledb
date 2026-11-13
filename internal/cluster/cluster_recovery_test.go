package cluster

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/BrandonnLow/littledb/internal/db"
)

const walFile = "littledb.log"

// TestRaftLogIsTheOnlyLog pins the single-log design: a replica's store keeps no
// data WAL, so a committed write is made durable exactly once — by the raft.log
// append before it commits — and nothing applied since the last flush is durable
// anywhere else (DurableAppliedIndex stays 0 with no flush). Every node logs the
// same entries in the same order, so with a stable leader the one log is
// byte-for-byte identical across nodes.
func TestRaftLogIsTheOnlyLog(t *testing.T) {
	const n = 3
	ds := dirs(t, n)
	c, err := NewWithTransportConfig(n, ds, testOpts(), NewChannelTransport(), stableConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	for i := 0; i < 20; i++ {
		if err := c.Put([]byte("k"), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	tx := c.Begin()
	tx.Put([]byte("a"), []byte("1"))
	tx.Put([]byte("b"), []byte("2"))
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete([]byte("a")); err != nil {
		t.Fatal(err)
	}
	if err := c.Quiesce(time.Second); err != nil {
		t.Fatal(err)
	}

	leaderLog, err := os.ReadFile(filepath.Join(ds[int(c.Leader())], "raft", raftLogFileName))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		if _, err := os.Stat(filepath.Join(ds[i], walFile)); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("node %d has a data WAL (stat err=%v); the Raft log should be its only log", i, err)
		}
		if got := c.Node(i).DB().DurableAppliedIndex(); got != 0 {
			t.Errorf("node %d DurableAppliedIndex = %d, want 0 (nothing flushed: raft.log is the only durable copy)", i, got)
		}
		log, err := os.ReadFile(filepath.Join(ds[i], "raft", raftLogFileName))
		if err != nil {
			t.Fatalf("node %d: %v", i, err)
		}
		if !bytes.Equal(log, leaderLog) {
			t.Errorf("node %d raft.log differs from leader's (%d vs %d bytes)", i, len(log), len(leaderLog))
		}
	}
}

// TestRestartedFollowerReplaysRaftLog crashes and restarts a follower whose store
// never flushed. Its store comes back empty (recovered applied index 0) — there
// is no data WAL to replay — yet it must converge to every committed key, by
// re-applying its own raft.log once the leader's next heartbeat tells it the
// commit index. No InstallSnapshot is involved: the follower's own log suffices.
func TestRestartedFollowerReplaysRaftLog(t *testing.T) {
	const n = 3
	ds := dirs(t, n)
	opts := db.Options{SyncOnWrite: true, DisableBackgroundCompaction: true}
	cfg := stableConfig() // node 0 stays leader; the restart needs no election
	c, err := NewWithTransportConfig(n, ds, opts, NewChannelTransport(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	want := map[string]string{"a": "1", "b": "2", "c": "3"}
	for k, v := range want {
		if err := c.Put([]byte(k), []byte(v)); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Quiesce(time.Second); err != nil {
		t.Fatal(err)
	}

	const follower = 1
	if err := crashRestart(c, follower, opts, cfg); err != nil {
		t.Fatal(err)
	}
	nd := c.Node(follower)
	if got := nd.DB().RecoveredAppliedIndex(); got != 0 {
		t.Fatalf("restarted follower recovered applied index %d, want 0 (nothing was flushed)", got)
	}
	waitFor(t, 3*time.Second, func() bool {
		for k, v := range want {
			if got, err := nd.DB().Get([]byte(k)); err != nil || string(got) != v {
				return false
			}
		}
		return true
	})
	if got := nd.InstallsForTesting(); got != 0 {
		t.Fatalf("restarted follower needed %d snapshot installs, want 0 (its own raft.log has the entries)", got)
	}
}
