package cluster

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/BrandonnLow/littledb/internal/db"
)

// TestCompleteInstallIfPendingSwapsAndResets exercises the crash-recovery path
// directly: a marker + staged dir planted on disk (as a mid-install crash would
// leave them) must, on the next Open, swap the staged DB into place, reset the
// raft log to the snapshot base, and clear the marker — and re-running it is a
// no-op. This is the completion-before-guard machinery in isolation.
func TestCompleteInstallIfPendingSwapsAndResets(t *testing.T) {
	dir := t.TempDir()
	raftDir := filepath.Join(dir, "raft")
	if err := os.MkdirAll(raftDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// Stale data dir: an "old" DB the follower held before the install.
	old, err := db.OpenWith(dir, db.Options{SyncOnWrite: true, DisableBackgroundCompaction: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := old.Put([]byte("stale"), []byte("old")); err != nil {
		t.Fatal(err)
	}
	if err := old.FlushForTesting(); err != nil {
		t.Fatal(err)
	}
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}

	// Stale raft log with a low base and a couple of entries.
	lf, _, err := openRaftLogFile(filepath.Join(raftDir, raftLogFileName), true)
	if err != nil {
		t.Fatal(err)
	}
	lf.append(1, []byte("e1"))
	lf.append(1, []byte("e2"))
	lf.close()

	// Staged snapshot DB with fresh keys at a high index/ts.
	const lii, lit, ts = 100, 7, 50
	stagedDir := stagedDBPath(raftDir)
	if err := os.MkdirAll(stagedDir, 0o755); err != nil {
		t.Fatal(err)
	}
	kvs := []db.KV{
		{Key: []byte("a"), Value: []byte("1")},
		{Key: []byte("b"), Value: []byte("2")},
	}
	if err := db.BuildSnapshotDB(stagedDir, kvs, lii, ts); err != nil {
		t.Fatal(err)
	}
	if err := writeInstallMarker(raftDir, installMarker{lastIncludedIndex: lii, lastIncludedTerm: lit, snapshotTS: ts}, true); err != nil {
		t.Fatal(err)
	}

	// Complete, as the constructor does before opening the store.
	if err := completeInstallIfPending(dir, raftDir, true); err != nil {
		t.Fatalf("completeInstallIfPending: %v", err)
	}

	// Marker and staged dir gone.
	if _, _, ok := mustReadMarker(t, raftDir); ok {
		t.Fatalf("marker still present after completion")
	}
	if _, err := os.Stat(stagedDir); !os.IsNotExist(err) {
		t.Fatalf("staged dir still present after completion")
	}

	// Raft log reset to the snapshot base, no entries.
	lf2, entries, err := openRaftLogFile(filepath.Join(raftDir, raftLogFileName), true)
	if err != nil {
		t.Fatal(err)
	}
	if lf2.baseIndex != lii || lf2.baseTerm != lit {
		t.Fatalf("raft log base = (%d,%d), want (%d,%d)", lf2.baseIndex, lf2.baseTerm, lii, lit)
	}
	if len(entries) != 0 {
		t.Fatalf("raft log has %d entries after reset, want 0", len(entries))
	}
	lf2.close()

	// Data dir now holds the snapshot, not the stale data.
	newDB, err := db.OpenWith(dir, db.Options{SyncOnWrite: true, DisableBackgroundCompaction: true})
	if err != nil {
		t.Fatal(err)
	}
	defer newDB.Close()
	if got := newDB.RecoveredAppliedIndex(); got != lii {
		t.Fatalf("RecoveredAppliedIndex = %d, want %d", got, lii)
	}
	if v, err := newDB.Get([]byte("a")); err != nil || string(v) != "1" {
		t.Fatalf("Get(a) = (%q,%v), want 1", v, err)
	}
	if _, err := newDB.Get([]byte("stale")); err != db.ErrKeyNotFound {
		t.Fatalf("stale key survived the swap: err=%v", err)
	}

	// Idempotent: re-running with no marker is a clean no-op.
	if err := completeInstallIfPending(dir, raftDir, true); err != nil {
		t.Fatalf("completeInstallIfPending (rerun): %v", err)
	}
}

func mustReadMarker(t *testing.T, raftDir string) (uint64, uint64, bool) {
	t.Helper()
	m, ok, err := readInstallMarker(raftDir)
	if err != nil {
		t.Fatalf("readInstallMarker: %v", err)
	}
	return m.lastIncludedIndex, m.lastIncludedTerm, ok
}

// TestInstallWipesStaleLiveKey is the wipe-and-rebuild correctness pin: a
// follower that held a live key before falling behind must LOSE that key after
// an install whose snapshot was taken once the key had been deleted. An in-place
// merge would leave the stale live version (deletes are absent from the stream);
// only a wipe-and-rebuild is correct. It also checks timestamp continuity — a
// post-install write applies cleanly on the rebuilt follower.
func TestInstallWipesStaleLiveKey(t *testing.T) {
	const n = 3
	ds := dirs(t, n)
	pt := newPartitionTransport()
	cfg := stableConfig()
	cfg.SnapshotThreshold = 4
	c, err := NewWithTransportConfig(n, ds, snapshotOpts(), pt, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	waitFor(t, time.Second, func() bool { return c.Node(0).roleValue() == Leader })

	// Everyone (incl. follower 2) gets a live "gone"=here.
	if err := c.Put([]byte("gone"), []byte("here")); err != nil {
		t.Fatal(err)
	}
	if err := c.Quiesce(2 * time.Second); err != nil {
		t.Fatal(err)
	}
	if v, err := c.Node(2).DB().Get([]byte("gone")); err != nil || string(v) != "here" {
		t.Fatalf("follower 2 should hold gone=here before partition: (%q,%v)", v, err)
	}

	// Partition follower 2, delete "gone", then write past the threshold so the
	// snapshot is taken after the delete and the leader compacts past follower 2.
	pt.disconnect(2)
	if err := c.Delete([]byte("gone")); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 8; i++ {
		if err := c.Put([]byte(fmt.Sprintf("k%d", i)), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	waitFor(t, 3*time.Second, func() bool { return c.Node(0).baseIndexValue() > 0 })

	// Reconnect: follower 2 installs the snapshot (which omits the deleted key).
	pt.reconnect(2)
	// Generous: the install is a synchronous DB rebuild + swap + reopen, which can
	// run well past a few seconds under the CPU contention of the full -race suite.
	waitFor(t, 15*time.Second, func() bool { return c.Node(2).InstallsForTesting() >= 1 })
	waitFor(t, 2*time.Second, func() bool {
		v, err := c.Node(2).DB().Get([]byte("k7"))
		return err == nil && string(v) == "v7"
	})

	// The stale live key must be GONE — the install wiped it, it was not merged.
	if v, err := c.Node(2).DB().Get([]byte("gone")); err != db.ErrKeyNotFound {
		t.Fatalf("stale live key survived install (merge bug): Get(gone) = (%q,%v), want ErrKeyNotFound", v, err)
	}

	// Timestamp continuity: a post-install write replicates and applies on the
	// rebuilt follower (would fail if nextTimestamp regressed below the snapshot).
	if err := c.Put([]byte("post"), []byte("install")); err != nil {
		t.Fatalf("post-install put: %v", err)
	}
	waitFor(t, 3*time.Second, func() bool {
		v, err := c.Node(2).DB().Get([]byte("post"))
		return err == nil && string(v) == "install"
	})
}
