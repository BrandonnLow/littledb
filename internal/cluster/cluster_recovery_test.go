package cluster

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/BrandonnLow/littledb/internal/db"
)

const walFile = "littledb.log"

func TestClusterFollowerWALByteIdentical(t *testing.T) {
	const n = 3
	ds := dirs(t, n)
	c, err := New(n, ds, testOpts())
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
	tx.Commit()
	c.Delete([]byte("a"))
	if err := c.Quiesce(time.Second); err != nil {
		t.Fatal(err)
	}

	// The leader appends data+OpCommit per txn; followers append the same
	// bytes via ApplyReplicated. No flush was triggered, so the whole log is
	// on disk and must be byte-for-byte identical across nodes.
	leaderLog, err := os.ReadFile(filepath.Join(ds[int(c.Leader())], walFile))
	if err != nil {
		t.Fatal(err)
	}
	if len(leaderLog) == 0 {
		t.Fatal("leader WAL is empty")
	}
	for i := 0; i < n; i++ {
		log, err := os.ReadFile(filepath.Join(ds[i], walFile))
		if err != nil {
			t.Fatalf("node %d: %v", i, err)
		}
		if !bytes.Equal(log, leaderLog) {
			t.Errorf("node %d WAL differs from leader (%d vs %d bytes)", i, len(log), len(leaderLog))
		}
	}
}

func TestClusterNodeRecoversFromDisk(t *testing.T) {
	const n = 3
	ds := dirs(t, n)
	c, err := New(n, ds, db.Options{SyncOnWrite: true, DisableBackgroundCompaction: true})
	if err != nil {
		t.Fatal(err)
	}

	for _, kv := range []struct{ k, v string }{{"a", "1"}, {"b", "2"}, {"c", "3"}} {
		if err := c.Put([]byte(kv.k), []byte(kv.v)); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Quiesce(time.Second); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen each node's directory as a plain DB and confirm the replicated
	// state survived: each follower replayed the entries its WAL received.
	for i := 0; i < n; i++ {
		d, err := db.Open(ds[i])
		if err != nil {
			t.Fatalf("reopen node %d: %v", i, err)
		}
		for _, kv := range []struct{ k, v string }{{"a", "1"}, {"b", "2"}, {"c", "3"}} {
			got, err := d.Get([]byte(kv.k))
			if err != nil || string(got) != kv.v {
				t.Errorf("node %d after reopen: %s=(%q,%v), want %q", i, kv.k, got, err, kv.v)
			}
		}
		d.Close()
	}
}
