package sstable

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"path/filepath"
	"testing"

	"github.com/BrandonnLow/littledb/internal/record"
)

// Benchmark_Get measures point-lookup latency in a populated SSTable.
// With the sparse index it should be near-constant regardless of file
// size; the linear-scan version this replaces was O(file size).
func Benchmark_Get(b *testing.B) {
	const n = 50_000
	path := filepath.Join(b.TempDir(), "bench.sst")
	w, err := NewWriter(path)
	if err != nil {
		b.Fatal(err)
	}
	value := bytes.Repeat([]byte("v"), 100)
	keys := make([][]byte, n)
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("k%08d", i))
		keys[i] = k
		if err := w.Add(record.OpPut, k, value); err != nil {
			b.Fatal(err)
		}
	}
	if err := w.Finish(); err != nil {
		b.Fatal(err)
	}

	r, err := OpenReader(path)
	if err != nil {
		b.Fatal(err)
	}
	defer r.Close()

	b.Logf("file has %d blocks", r.NumBlocks())

	rng := rand.New(rand.NewPCG(1, 2))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _, _ = r.Get(keys[rng.IntN(n)])
	}
}
