package cluster

import (
	"testing"

	"github.com/BrandonnLow/littledb/internal/db"
)

// BenchmarkClusterPut measures replicated write throughput on a 3-node in-process
// cluster with SyncOnWrite on, so every fsync is real. A Put commits one Raft
// entry: the leader and each follower fsync it to raft.log, and applying it
// writes nothing further (the Raft log is the store's only write-ahead log).
// Run with -benchtime to taste; the number is dominated by the device's fsync
// latency, which is the point.
func BenchmarkClusterPut(b *testing.B) {
	ds := make([]string, 3)
	for i := range ds {
		ds[i] = b.TempDir()
	}
	c, err := NewWithTransportConfig(3, ds,
		db.Options{SyncOnWrite: true, DisableBackgroundCompaction: true},
		NewChannelTransport(), stableConfig())
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()

	key := []byte("bench-key")
	val := make([]byte, 100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := c.Put(key, val); err != nil {
			b.Fatal(err)
		}
	}
}
