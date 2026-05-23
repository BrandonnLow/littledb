package skiplist

import (
	"fmt"
	"math/rand/v2"
	"testing"
)

func Benchmark_Put(b *testing.B) {
	s := New()
	value := make([]byte, 100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("k%08d", i))
		s.Put(key, value)
	}
}

func Benchmark_Get(b *testing.B) {
	s := New()
	const n = 100_000
	value := make([]byte, 100)
	keys := make([][]byte, n)
	for i := 0; i < n; i++ {
		keys[i] = []byte(fmt.Sprintf("k%08d", i))
		s.Put(keys[i], value)
	}
	r := rand.New(rand.NewPCG(1, 2))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Get(keys[r.IntN(n)])
	}
}
