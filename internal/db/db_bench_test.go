package db

import (
	"fmt"
	"testing"
)

// Benchmark_PutSync measures Put throughput with fsync on every write
// (the default and safe mode). Expect hundreds to low-thousands of ops/sec
// on consumer SSD; this is the cost of durability.
func Benchmark_PutSync(b *testing.B) {
	d, err := OpenWith(b.TempDir(), Options{SyncOnWrite: true})
	if err != nil {
		b.Fatal(err)
	}
	defer d.Close()

	value := make([]byte, 100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("k%08d", i))
		if err := d.Put(key, value); err != nil {
			b.Fatal(err)
		}
	}
}

// Benchmark_PutNoSync measures Put throughput without fsync. The OS still
// buffers writes in the page cache; data is safe across process crashes
// but not power loss. Expect 100x or more vs sync mode.
func Benchmark_PutNoSync(b *testing.B) {
	d, err := OpenWith(b.TempDir(), Options{SyncOnWrite: false})
	if err != nil {
		b.Fatal(err)
	}
	defer d.Close()

	value := make([]byte, 100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("k%08d", i))
		if err := d.Put(key, value); err != nil {
			b.Fatal(err)
		}
	}
}

// Benchmark_Get measures read throughput against a pre-populated DB.
// Each Get is: map lookup + one pread + decode. No fsync overhead.
func Benchmark_Get(b *testing.B) {
	d, err := OpenWith(b.TempDir(), Options{SyncOnWrite: false})
	if err != nil {
		b.Fatal(err)
	}
	defer d.Close()

	const n = 10_000
	value := make([]byte, 100)
	keys := make([][]byte, n)
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("k%08d", i))
		keys[i] = k
		if err := d.Put(k, value); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := d.Get(keys[i%n]); err != nil {
			b.Fatal(err)
		}
	}
}

// Benchmark_OpenReplay measures cold-start time for a populated DB.
// This is what users wait through on startup: replay every record to
// rebuild the in-memory index.
func Benchmark_OpenReplay(b *testing.B) {
	dir := b.TempDir()

	d, err := OpenWith(dir, Options{SyncOnWrite: false})
	if err != nil {
		b.Fatal(err)
	}
	const n = 10_000
	value := make([]byte, 100)
	for i := 0; i < n; i++ {
		if err := d.Put([]byte(fmt.Sprintf("k%08d", i)), value); err != nil {
			b.Fatal(err)
		}
	}
	d.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d2, err := Open(dir)
		if err != nil {
			b.Fatal(err)
		}
		d2.Close()
	}
}
