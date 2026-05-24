package memtable

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"testing"
)

func TestEmptyGet(t *testing.T) {
	m := New()
	v, op, ok := m.Get([]byte("x"))
	if ok || op != 0 || v != nil {
		t.Errorf("Get on empty: got (%q, %d, %v)", v, op, ok)
	}
	if m.Len() != 0 {
		t.Errorf("Len = %d, want 0", m.Len())
	}
}

func TestPutGet(t *testing.T) {
	m := New()
	if err := m.Put([]byte("hello"), []byte("world")); err != nil {
		t.Fatal(err)
	}
	v, op, ok := m.Get([]byte("hello"))
	if !ok || op != OpPut || !bytes.Equal(v, []byte("world")) {
		t.Errorf("got (%q, %d, %v), want (world, OpPut, true)", v, op, ok)
	}
}

func TestDeleteWritesTombstone(t *testing.T) {
	m := New()
	m.Put([]byte("k"), []byte("v"))
	if err := m.Delete([]byte("k")); err != nil {
		t.Fatal(err)
	}
	v, op, ok := m.Get([]byte("k"))
	if !ok {
		t.Error("Get after Delete: ok=false, want true (tombstone should be visible)")
	}
	if op != OpDelete {
		t.Errorf("op = %d, want OpDelete", op)
	}
	if v != nil {
		t.Errorf("tombstone value = %q, want nil", v)
	}
}

func TestDeleteOnMissingKeyStillWritesTombstone(t *testing.T) {
	// Important: tombstones must be written even for keys we don't see,
	// because the key may exist in an older SSTable that we need to mask.
	m := New()
	if err := m.Delete([]byte("nope")); err != nil {
		t.Fatal(err)
	}
	_, op, ok := m.Get([]byte("nope"))
	if !ok || op != OpDelete {
		t.Errorf("got (op=%d, ok=%v), want (OpDelete, true)", op, ok)
	}
}

func TestPutAfterDelete(t *testing.T) {
	m := New()
	m.Put([]byte("k"), []byte("v1"))
	m.Delete([]byte("k"))
	m.Put([]byte("k"), []byte("v2"))

	v, op, ok := m.Get([]byte("k"))
	if !ok || op != OpPut || !bytes.Equal(v, []byte("v2")) {
		t.Errorf("got (%q, %d, %v), want (v2, OpPut, true)", v, op, ok)
	}
}

func TestEmptyValue(t *testing.T) {
	// Empty value must be distinguishable from a tombstone. The contract
	// is that op == OpPut for a stored empty value (regardless of whether
	// the returned slice is nil or a zero-length non-nil slice — Go's
	// append([]byte(nil), x...) of an empty x returns nil, which is fine).
	m := New()
	m.Put([]byte("k"), []byte{})

	v, op, ok := m.Get([]byte("k"))
	if !ok {
		t.Fatal("Get: ok = false, want true")
	}
	if op != OpPut {
		t.Errorf("op = %d, want OpPut", op)
	}
	if len(v) != 0 {
		t.Errorf("value = %v, want empty", v)
	}
}

func TestFreezeBlocksWrites(t *testing.T) {
	m := New()
	m.Put([]byte("k"), []byte("v"))
	m.Freeze()
	if !m.IsFrozen() {
		t.Error("IsFrozen = false after Freeze")
	}

	if err := m.Put([]byte("k2"), []byte("v")); !errors.Is(err, ErrFrozen) {
		t.Errorf("Put after Freeze: err = %v, want ErrFrozen", err)
	}
	if err := m.Delete([]byte("k")); !errors.Is(err, ErrFrozen) {
		t.Errorf("Delete after Freeze: err = %v, want ErrFrozen", err)
	}

	v, op, ok := m.Get([]byte("k"))
	if !ok || op != OpPut || !bytes.Equal(v, []byte("v")) {
		t.Errorf("Get after Freeze: got (%q, %d, %v)", v, op, ok)
	}
}

func TestApproximateSize(t *testing.T) {
	m := New()
	if m.ApproximateSize() != 0 {
		t.Errorf("ApproximateSize on empty = %d, want 0", m.ApproximateSize())
	}
	m.Put([]byte("k"), []byte("v"))
	if m.ApproximateSize() == 0 {
		t.Error("ApproximateSize after Put = 0, want > 0")
	}
	before := m.ApproximateSize()

	m.Put([]byte("k"), []byte("v2"))
	after := m.ApproximateSize()
	if after < before-100 || after > before+100 {
		t.Errorf("update size delta too large: before=%d after=%d", before, after)
	}
}

func TestIterateSortedIncludesTombstones(t *testing.T) {
	m := New()
	m.Put([]byte("c"), []byte("3"))
	m.Put([]byte("a"), []byte("1"))
	m.Delete([]byte("b"))
	m.Put([]byte("d"), []byte("4"))

	type entry struct {
		key string
		val string
		op  Op
	}
	var got []entry
	m.Iterate(func(k, v []byte, op Op) bool {
		got = append(got, entry{string(k), string(v), op})
		return true
	})

	want := []entry{
		{"a", "1", OpPut},
		{"b", "", OpDelete},
		{"c", "3", OpPut},
		{"d", "4", OpPut},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d entries, want %d: %+v", len(got), len(want), got)
	}
	for i := range want {
		if got[i].key != want[i].key || got[i].op != want[i].op {
			t.Errorf("[%d] got %+v, want %+v", i, got[i], want[i])
		}
		if want[i].op == OpPut && got[i].val != want[i].val {
			t.Errorf("[%d] value: got %q, want %q", i, got[i].val, want[i].val)
		}
	}
}

func TestIterateEarlyStop(t *testing.T) {
	m := New()
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		m.Put([]byte(k), []byte("v"))
	}
	count := 0
	m.Iterate(func(k, v []byte, op Op) bool {
		count++
		return count < 3
	})
	if count != 3 {
		t.Errorf("iterated %d entries, want 3", count)
	}
}

func TestCallerCanMutateInputs(t *testing.T) {
	m := New()
	key := []byte("k")
	val := []byte("original")
	m.Put(key, val)

	key[0] = 'X'
	val[0] = 'X'

	v, _, ok := m.Get([]byte("k"))
	if !ok {
		t.Fatal("Get: not found after caller mutated input")
	}
	if !bytes.Equal(v, []byte("original")) {
		t.Errorf("stored value mutated: %q", v)
	}
}

// TestConcurrentReadersAndWriter verifies the RWMutex usage: many
// readers can run alongside a single writer without races.
func TestConcurrentReadersAndWriter(t *testing.T) {
	m := New()
	const n = 100
	for i := 0; i < n; i++ {
		m.Put([]byte(fmt.Sprintf("k%d", i)), []byte(fmt.Sprintf("v%d-init", i)))
	}

	const readers = 4
	const writes = 500
	stop := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(readers + 1)
	errCh := make(chan error, readers+1)

	for r := 0; r < readers; r++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				for i := 0; i < n; i++ {
					_, _, _ = m.Get([]byte(fmt.Sprintf("k%d", i)))
				}
				_ = m.Len()
				_ = m.ApproximateSize()
			}
		}()
	}

	go func() {
		defer wg.Done()
		for w := 0; w < writes; w++ {
			k := []byte(fmt.Sprintf("k%d", w%n))
			v := []byte(fmt.Sprintf("v%d-up%d", w%n, w))
			if err := m.Put(k, v); err != nil {
				errCh <- fmt.Errorf("put: %w", err)
				return
			}
		}
		close(stop)
	}()

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
}
