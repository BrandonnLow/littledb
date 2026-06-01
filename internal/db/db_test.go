package db

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestPutGet(t *testing.T) {
	d, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	d.Put([]byte("hello"), []byte("world"))
	got, err := d.Get([]byte("hello"))
	if err != nil || !bytes.Equal(got, []byte("world")) {
		t.Errorf("got %q err=%v", got, err)
	}
}

func TestGetMissing(t *testing.T) {
	d, _ := Open(t.TempDir())
	defer d.Close()
	if _, err := d.Get([]byte("nope")); !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("err = %v", err)
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
		t.Errorf("got %q", got)
	}
}

func TestDelete(t *testing.T) {
	d, _ := Open(t.TempDir())
	defer d.Close()
	d.Put([]byte("k"), []byte("v"))
	d.Delete([]byte("k"))
	if _, err := d.Get([]byte("k")); !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("err = %v", err)
	}
}

func TestPersistence(t *testing.T) {
	dir := t.TempDir()
	d, _ := Open(dir)
	d.Put([]byte("a"), []byte("1"))
	d.Put([]byte("b"), []byte("2"))
	d.Close()
	d2, _ := Open(dir)
	defer d2.Close()
	for _, c := range []struct{ k, v string }{{"a", "1"}, {"b", "2"}} {
		got, err := d2.Get([]byte(c.k))
		if err != nil || !bytes.Equal(got, []byte(c.v)) {
			t.Errorf("%s: got %q err=%v", c.k, got, err)
		}
	}
}

func TestFlushTriggersAtThreshold(t *testing.T) {
	dir := t.TempDir()
	d, _ := OpenWith(dir, Options{SyncOnWrite: false, MemtableSizeMax: 4 * 1024})
	defer d.Close()
	value := make([]byte, 100)
	for i := 0; i < 200; i++ {
		d.Put([]byte(fmt.Sprintf("k%04d", i)), value)
	}
	if d.NumSSTablesForTesting() == 0 {
		t.Error("expected at least one SSTable")
	}
	for _, i := range []int{0, 50, 100, 150, 199} {
		got, err := d.Get([]byte(fmt.Sprintf("k%04d", i)))
		if err != nil || !bytes.Equal(got, value) {
			t.Errorf("i=%d err=%v", i, err)
		}
	}
}

func TestReadAcrossMemtableAndSSTable(t *testing.T) {
	d, _ := OpenWith(t.TempDir(), Options{SyncOnWrite: false})
	defer d.Close()
	d.Put([]byte("flushed"), []byte("a"))
	d.FlushForTesting()
	d.Put([]byte("active"), []byte("b"))
	if got, _ := d.Get([]byte("flushed")); !bytes.Equal(got, []byte("a")) {
		t.Errorf("flushed: %q", got)
	}
	if got, _ := d.Get([]byte("active")); !bytes.Equal(got, []byte("b")) {
		t.Errorf("active: %q", got)
	}
}

func TestTombstoneShadowsSSTable(t *testing.T) {
	d, _ := OpenWith(t.TempDir(), Options{SyncOnWrite: false})
	defer d.Close()
	d.Put([]byte("k"), []byte("v"))
	d.FlushForTesting()
	d.Delete([]byte("k"))
	if _, err := d.Get([]byte("k")); !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("err = %v", err)
	}
}

func TestRecoveryWithWALAndSSTables(t *testing.T) {
	dir := t.TempDir()
	d, _ := OpenWith(dir, Options{SyncOnWrite: true})
	d.Put([]byte("flushed-1"), []byte("f1"))
	d.FlushForTesting()
	d.Put([]byte("active-1"), []byte("a1"))
	d.Close()

	d2, _ := Open(dir)
	defer d2.Close()
	for _, c := range []struct{ k, v string }{{"flushed-1", "f1"}, {"active-1", "a1"}} {
		got, err := d2.Get([]byte(c.k))
		if err != nil || !bytes.Equal(got, []byte(c.v)) {
			t.Errorf("%s: got %q err=%v", c.k, got, err)
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
	d.FlushForTesting()
	info, _ := os.Stat(filepath.Join(dir, walFilename))
	if info.Size() != 0 {
		t.Errorf("WAL size = %d, want 0", info.Size())
	}
}

func TestConcurrentReadersAndWriter(t *testing.T) {
	d, _ := OpenWith(t.TempDir(), Options{SyncOnWrite: false})
	defer d.Close()
	const n = 50
	for i := 0; i < n; i++ {
		d.Put([]byte(fmt.Sprintf("k%d", i)), []byte(fmt.Sprintf("v%d", i)))
	}
	const readers = 4
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
						errCh <- err
						return
					}
				}
			}
		}()
	}
	go func() {
		defer wg.Done()
		for w := 0; w < 500; w++ {
			d.Put([]byte(fmt.Sprintf("k%d", w%n)), []byte(fmt.Sprintf("v%d", w)))
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
	d.Close()
	if err := d.Close(); err != nil {
		t.Error(err)
	}
}

func TestCompactionMergesOldSSTables(t *testing.T) {
	d, _ := OpenWith(t.TempDir(), Options{
		SyncOnWrite:       false,
		CompactionTrigger: 4,
	})
	defer d.Close()

	for i := 0; i < 4; i++ {
		d.Put([]byte(fmt.Sprintf("k%d", i)), []byte("v"))
		d.FlushForTesting()
	}
	if d.NumSSTablesForTesting() != 4 {
		t.Fatalf("pre-compact NumSSTables = %d, want 4", d.NumSSTablesForTesting())
	}

	if err := d.CompactForTesting(); err != nil {
		t.Fatal(err)
	}
	if d.NumSSTablesForTesting() != 1 {
		t.Errorf("post-compact NumSSTables = %d, want 1", d.NumSSTablesForTesting())
	}

	for i := 0; i < 4; i++ {
		got, err := d.Get([]byte(fmt.Sprintf("k%d", i)))
		if err != nil || !bytes.Equal(got, []byte("v")) {
			t.Errorf("k%d: got %q err=%v", i, got, err)
		}
	}
}

func TestCompactionCollapsesOverwrites(t *testing.T) {
	d, _ := OpenWith(t.TempDir(), Options{
		SyncOnWrite:       false,
		CompactionTrigger: 4,
	})
	defer d.Close()

	for i := 0; i < 4; i++ {
		d.Put([]byte("k"), []byte(fmt.Sprintf("v%d", i)))
		d.FlushForTesting()
	}
	if err := d.CompactForTesting(); err != nil {
		t.Fatal(err)
	}
	got, _ := d.Get([]byte("k"))
	if !bytes.Equal(got, []byte("v3")) {
		t.Errorf("got %q, want v3", got)
	}
}

func TestCompactionDropsTombstonesAtBottom(t *testing.T) {
	d, _ := OpenWith(t.TempDir(), Options{
		SyncOnWrite:       false,
		CompactionTrigger: 2,
	})
	defer d.Close()

	d.Put([]byte("k"), []byte("v"))
	d.FlushForTesting()
	d.Delete([]byte("k"))
	d.FlushForTesting()

	if d.NumSSTablesForTesting() != 2 {
		t.Fatalf("pre NumSSTables = %d", d.NumSSTablesForTesting())
	}
	if err := d.CompactForTesting(); err != nil {
		t.Fatal(err)
	}
	if d.NumSSTablesForTesting() != 1 {
		t.Errorf("post NumSSTables = %d, want 1", d.NumSSTablesForTesting())
	}
	if _, err := d.Get([]byte("k")); !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("err = %v, want ErrKeyNotFound", err)
	}
}

func TestRecoveryAfterCompaction(t *testing.T) {
	dir := t.TempDir()
	d, _ := OpenWith(dir, Options{
		SyncOnWrite:       true,
		CompactionTrigger: 2,
	})

	d.Put([]byte("a"), []byte("1"))
	d.FlushForTesting()
	d.Put([]byte("b"), []byte("2"))
	d.FlushForTesting()
	d.CompactForTesting()
	if d.NumSSTablesForTesting() != 1 {
		t.Fatalf("pre-reopen NumSSTables = %d", d.NumSSTablesForTesting())
	}
	d.Close()

	d2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer d2.Close()
	if d2.NumSSTablesForTesting() != 1 {
		t.Errorf("post-reopen NumSSTables = %d", d2.NumSSTablesForTesting())
	}
	for _, c := range []struct{ k, v string }{{"a", "1"}, {"b", "2"}} {
		got, err := d2.Get([]byte(c.k))
		if err != nil || !bytes.Equal(got, []byte(c.v)) {
			t.Errorf("%s: got %q err=%v", c.k, got, err)
		}
	}
}

func TestCompactionDeletesOldFiles(t *testing.T) {
	dir := t.TempDir()
	d, _ := OpenWith(dir, Options{
		SyncOnWrite:                 false,
		CompactionTrigger:           2,
		DisableBackgroundCompaction: true,
	})
	defer d.Close()

	d.Put([]byte("a"), []byte("1"))
	d.FlushForTesting()
	d.Put([]byte("b"), []byte("2"))
	d.FlushForTesting()

	if _, err := os.Stat(filepath.Join(dir, "000001.sst")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "000002.sst")); err != nil {
		t.Fatal(err)
	}

	d.CompactForTesting()

	// The merged output reuses the max input ID (2). 000001.sst was
	// the older input and is now unlinked; 000002.sst was the newer
	// input and has been atomically replaced by the merged output.
	if _, err := os.Stat(filepath.Join(dir, "000001.sst")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("000001.sst still present: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "000002.sst")); err != nil {
		t.Errorf("000002.sst missing (should be the merged output): %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "000003.sst")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("000003.sst unexpectedly exists (no fresh ID should have been allocated): %v", err)
	}
}

func TestCompactionConcurrentWithReads(t *testing.T) {
	d, _ := OpenWith(t.TempDir(), Options{
		SyncOnWrite:       false,
		CompactionTrigger: 2,
	})
	defer d.Close()

	const n = 100
	for i := 0; i < n; i++ {
		d.Put([]byte(fmt.Sprintf("k%03d", i)), []byte("v"))
	}
	d.FlushForTesting()
	for i := 0; i < n; i++ {
		d.Put([]byte(fmt.Sprintf("k%03d", i)), []byte("v2"))
	}
	d.FlushForTesting()

	stop := make(chan struct{})
	var wg sync.WaitGroup
	errCh := make(chan error, 8)
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				for i := 0; i < n; i++ {
					if _, err := d.Get([]byte(fmt.Sprintf("k%03d", i))); err != nil {
						errCh <- err
						return
					}
				}
			}
		}()
	}

	if err := d.CompactForTesting(); err != nil {
		t.Fatal(err)
	}
	close(stop)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
}

func TestCompactionNoTrigger(t *testing.T) {
	d, _ := OpenWith(t.TempDir(), Options{
		SyncOnWrite:       false,
		CompactionTrigger: 4,
	})
	defer d.Close()
	d.Put([]byte("k"), []byte("v"))
	d.FlushForTesting()
	if err := d.CompactForTesting(); err != nil {
		t.Fatal(err)
	}
	if d.NumSSTablesForTesting() != 1 {
		t.Errorf("NumSSTables = %d", d.NumSSTablesForTesting())
	}
}

// TestCompactionPreservesNewerSSTableAfterReopen catches the bug
// where the merged output was given a fresh ID (higher than the
// untouched newer SSTable), making on-disk ID order disagree with
// in-memory recency order. In-process Get returned the right value;
// reopening returned a stale one.
func TestCompactionPreservesNewerSSTableAfterReopen(t *testing.T) {
	dir := t.TempDir()
	d, _ := OpenWith(dir, Options{
		SyncOnWrite:                 false,
		CompactionTrigger:           4,
		DisableBackgroundCompaction: true,
	})

	d.Put([]byte("k"), []byte("v1"))
	d.FlushForTesting() // 000001
	d.Put([]byte("x1"), []byte("x"))
	d.FlushForTesting() // 000002
	d.Put([]byte("x2"), []byte("x"))
	d.FlushForTesting() // 000003
	d.Put([]byte("x3"), []byte("x"))
	d.FlushForTesting() // 000004
	d.Put([]byte("k"), []byte("v2"))
	d.FlushForTesting() // 000005 — newest, must remain newest

	if d.NumSSTablesForTesting() != 5 {
		t.Fatalf("pre-compact NumSSTables = %d, want 5", d.NumSSTablesForTesting())
	}
	if err := d.CompactForTesting(); err != nil {
		t.Fatal(err)
	}
	if d.NumSSTablesForTesting() != 2 {
		t.Fatalf("post-compact NumSSTables = %d, want 2", d.NumSSTablesForTesting())
	}

	got, _ := d.Get([]byte("k"))
	if !bytes.Equal(got, []byte("v2")) {
		t.Errorf("in-process Get(k) = %q, want v2", got)
	}
	d.Close()

	d2, _ := OpenWith(dir, Options{DisableBackgroundCompaction: true})
	defer d2.Close()
	got, err := d2.Get([]byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, []byte("v2")) {
		t.Errorf("post-reopen Get(k) = %q, want v2 (compaction ID/recency bug)", got)
	}
}

func TestBackgroundCompactionTriggers(t *testing.T) {
	d, _ := OpenWith(t.TempDir(), Options{
		SyncOnWrite:       false,
		CompactionTrigger: 2,
	})
	defer d.Close()

	d.Put([]byte("a"), []byte("1"))
	d.FlushForTesting()
	d.Put([]byte("b"), []byte("2"))
	d.FlushForTesting()

	for i := 0; i < 200; i++ {
		if d.NumSSTablesForTesting() == 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Errorf("background compaction never ran; NumSSTables = %d", d.NumSSTablesForTesting())
}
