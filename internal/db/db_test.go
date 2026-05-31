package db

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestPutGet(t *testing.T) {
	d, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	if err := d.Put([]byte("hello"), []byte("world")); err != nil {
		t.Fatal(err)
	}
	got, err := d.Get([]byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, []byte("world")) {
		t.Errorf("got %q, want world", got)
	}
}

func TestGetMissing(t *testing.T) {
	d, _ := Open(t.TempDir())
	defer d.Close()
	if _, err := d.Get([]byte("nope")); !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("err = %v, want ErrKeyNotFound", err)
	}
}

func TestOverwrite(t *testing.T) {
	d, _ := Open(t.TempDir())
	defer d.Close()

	d.Put([]byte("k"), []byte("v1"))
	d.Put([]byte("k"), []byte("v2"))
	d.Put([]byte("k"), []byte("v3"))

	got, _ := d.Get([]byte("k"))
	if !bytes.Equal(got, []byte("v3")) {
		t.Errorf("got %q, want v3", got)
	}
}

func TestDelete(t *testing.T) {
	d, _ := Open(t.TempDir())
	defer d.Close()
	d.Put([]byte("k"), []byte("v"))
	d.Delete([]byte("k"))
	if _, err := d.Get([]byte("k")); !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("after delete: err = %v, want ErrKeyNotFound", err)
	}
}

func TestPersistence(t *testing.T) {
	dir := t.TempDir()
	d, _ := Open(dir)
	d.Put([]byte("a"), []byte("1"))
	d.Put([]byte("b"), []byte("2"))
	d.Close()

	d2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer d2.Close()
	for _, c := range []struct{ k, v string }{{"a", "1"}, {"b", "2"}} {
		got, err := d2.Get([]byte(c.k))
		if err != nil {
			t.Errorf("%s: %v", c.k, err)
			continue
		}
		if !bytes.Equal(got, []byte(c.v)) {
			t.Errorf("%s: got %q want %q", c.k, got, c.v)
		}
	}
}

func TestPersistenceWithDeletes(t *testing.T) {
	dir := t.TempDir()
	d, _ := Open(dir)
	d.Put([]byte("keep"), []byte("yes"))
	d.Put([]byte("gone"), []byte("no"))
	d.Delete([]byte("gone"))
	d.Close()

	d2, _ := Open(dir)
	defer d2.Close()

	if got, err := d2.Get([]byte("keep")); err != nil || !bytes.Equal(got, []byte("yes")) {
		t.Errorf("keep: %q err=%v", got, err)
	}
	if _, err := d2.Get([]byte("gone")); !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("gone: err = %v, want ErrKeyNotFound", err)
	}
}

func TestFlushTriggersAtThreshold(t *testing.T) {
	dir := t.TempDir()
	d, err := OpenWith(dir, Options{
		SyncOnWrite:     false,
		MemtableSizeMax: 4 * 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	value := make([]byte, 100)
	for i := 0; i < 200; i++ {
		if err := d.Put([]byte(fmt.Sprintf("k%04d", i)), value); err != nil {
			t.Fatal(err)
		}
	}
	if d.NumSSTablesForTesting() == 0 {
		t.Error("expected at least one SSTable to have been produced")
	}

	for _, i := range []int{0, 50, 100, 150, 199} {
		k := []byte(fmt.Sprintf("k%04d", i))
		got, err := d.Get(k)
		if err != nil {
			t.Errorf("Get %q: %v", k, err)
		}
		if !bytes.Equal(got, value) {
			t.Errorf("Get %q: wrong value", k)
		}
	}
}

func TestReadAcrossMemtableAndSSTable(t *testing.T) {
	d, _ := OpenWith(t.TempDir(), Options{SyncOnWrite: false})
	defer d.Close()

	d.Put([]byte("flushed"), []byte("a"))
	d.FlushForTesting()
	d.Put([]byte("active"), []byte("b"))

	if d.NumSSTablesForTesting() != 1 {
		t.Errorf("NumSSTables = %d, want 1", d.NumSSTablesForTesting())
	}

	if got, _ := d.Get([]byte("flushed")); !bytes.Equal(got, []byte("a")) {
		t.Errorf("flushed: got %q", got)
	}
	if got, _ := d.Get([]byte("active")); !bytes.Equal(got, []byte("b")) {
		t.Errorf("active: got %q", got)
	}
}

func TestTombstoneShadowsSSTable(t *testing.T) {
	d, _ := OpenWith(t.TempDir(), Options{SyncOnWrite: false})
	defer d.Close()

	d.Put([]byte("k"), []byte("v"))
	d.FlushForTesting()
	d.Delete([]byte("k"))

	if _, err := d.Get([]byte("k")); !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("after delete-of-flushed-key: err = %v, want ErrKeyNotFound", err)
	}
}

func TestOverwriteAcrossSSTables(t *testing.T) {
	d, _ := OpenWith(t.TempDir(), Options{SyncOnWrite: false})
	defer d.Close()

	d.Put([]byte("k"), []byte("v1"))
	d.FlushForTesting()
	d.Put([]byte("k"), []byte("v2"))
	d.FlushForTesting()
	d.Put([]byte("k"), []byte("v3"))

	got, _ := d.Get([]byte("k"))
	if !bytes.Equal(got, []byte("v3")) {
		t.Errorf("got %q, want v3", got)
	}
	if d.NumSSTablesForTesting() != 2 {
		t.Errorf("NumSSTables = %d, want 2", d.NumSSTablesForTesting())
	}
}

func TestMultipleFlushesPersist(t *testing.T) {
	dir := t.TempDir()
	d, _ := OpenWith(dir, Options{SyncOnWrite: false})

	keys := []string{"a", "b", "c", "d", "e"}
	for i, k := range keys {
		d.Put([]byte(k), []byte(fmt.Sprintf("v%d", i)))
		d.FlushForTesting()
	}
	if d.NumSSTablesForTesting() != len(keys) {
		t.Errorf("NumSSTables = %d, want %d", d.NumSSTablesForTesting(), len(keys))
	}
	d.Close()

	d2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer d2.Close()
	if d2.NumSSTablesForTesting() != len(keys) {
		t.Errorf("after reopen NumSSTables = %d, want %d", d2.NumSSTablesForTesting(), len(keys))
	}
	for i, k := range keys {
		got, err := d2.Get([]byte(k))
		if err != nil {
			t.Errorf("%s: %v", k, err)
			continue
		}
		want := []byte(fmt.Sprintf("v%d", i))
		if !bytes.Equal(got, want) {
			t.Errorf("%s: got %q want %q", k, got, want)
		}
	}
}

func TestRecoveryWithWALAndSSTables(t *testing.T) {
	dir := t.TempDir()
	d, _ := OpenWith(dir, Options{SyncOnWrite: true})

	d.Put([]byte("flushed-1"), []byte("f1"))
	d.Put([]byte("flushed-2"), []byte("f2"))
	d.FlushForTesting()

	d.Put([]byte("active-1"), []byte("a1"))
	d.Put([]byte("active-2"), []byte("a2"))

	// "Crash" — drop without Close. WAL is already fsynced.
	_ = d

	d2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer d2.Close()

	for _, c := range []struct{ k, v string }{
		{"flushed-1", "f1"},
		{"flushed-2", "f2"},
		{"active-1", "a1"},
		{"active-2", "a2"},
	} {
		got, err := d2.Get([]byte(c.k))
		if err != nil {
			t.Errorf("%s: %v", c.k, err)
			continue
		}
		if !bytes.Equal(got, []byte(c.v)) {
			t.Errorf("%s: got %q want %q", c.k, got, c.v)
		}
	}
}

func TestWALTruncatedAfterFlush(t *testing.T) {
	dir := t.TempDir()
	d, _ := OpenWith(dir, Options{SyncOnWrite: false})
	defer d.Close()

	for i := 0; i < 100; i++ {
		d.Put([]byte(fmt.Sprintf("k%d", i)), []byte("v"))
	}
	if err := d.FlushForTesting(); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(dir, walFilename))
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != 0 {
		t.Errorf("WAL size after flush = %d, want 0", info.Size())
	}
}

func TestConcurrentReadersAndWriter(t *testing.T) {
	d, _ := OpenWith(t.TempDir(), Options{SyncOnWrite: false})
	defer d.Close()

	const n = 50
	for i := 0; i < n; i++ {
		d.Put([]byte(fmt.Sprintf("k%d", i)), []byte(fmt.Sprintf("v%d-init", i)))
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
					if _, err := d.Get([]byte(fmt.Sprintf("k%d", i))); err != nil {
						errCh <- fmt.Errorf("reader: %w", err)
						return
					}
				}
			}
		}()
	}

	go func() {
		defer wg.Done()
		for w := 0; w < writes; w++ {
			k := []byte(fmt.Sprintf("k%d", w%n))
			v := []byte(fmt.Sprintf("v%d-up%d", w%n, w))
			if err := d.Put(k, v); err != nil {
				errCh <- fmt.Errorf("writer: %w", err)
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

func TestCloseIdempotent(t *testing.T) {
	d, _ := Open(t.TempDir())
	if err := d.Close(); err != nil {
		t.Error(err)
	}
	if err := d.Close(); err != nil {
		t.Error(err)
	}
}
