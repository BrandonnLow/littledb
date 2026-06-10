package db

import (
	"fmt"
	"sync"
	"testing"
)

type kv struct{ k, v string }

func collectScan(t *testing.T, it *Iterator) []kv {
	t.Helper()
	var out []kv
	for it.Next() {
		out = append(out, kv{string(it.Key()), string(it.Value())})
	}
	if err := it.Err(); err != nil {
		t.Fatalf("scan err: %v", err)
	}
	it.Close()
	return out
}

func wantKV(t *testing.T, got []kv, want ...kv) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d kvs %v, want %d %v", len(got), got, len(want), want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("[%d] got %v want %v", i, got[i], want[i])
		}
	}
}

func scanNowKV(t *testing.T, d *DB, start, end []byte) []kv {
	t.Helper()
	it, err := d.ScanNow(start, end)
	if err != nil {
		t.Fatalf("ScanNow: %v", err)
	}
	return collectScan(t, it)
}

func openScanDB(t *testing.T) *DB {
	t.Helper()
	d, err := OpenWith(t.TempDir(), Options{SyncOnWrite: false, DisableBackgroundCompaction: true})
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func TestScanEmptyDB(t *testing.T) {
	d := openScanDB(t)
	defer d.Close()
	wantKV(t, scanNowKV(t, d, nil, nil))
}

func TestScanSingleMemtableSorted(t *testing.T) {
	d := openScanDB(t)
	defer d.Close()
	d.Put([]byte("c"), []byte("3"))
	d.Put([]byte("a"), []byte("1"))
	d.Put([]byte("b"), []byte("2"))
	wantKV(t, scanNowKV(t, d, nil, nil), kv{"a", "1"}, kv{"b", "2"}, kv{"c", "3"})
}

func TestScanBounds(t *testing.T) {
	d := openScanDB(t)
	defer d.Close()
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		d.Put([]byte(k), []byte(k))
	}
	// [b, d): start inclusive, end exclusive.
	wantKV(t, scanNowKV(t, d, []byte("b"), []byte("d")), kv{"b", "b"}, kv{"c", "c"})
	// nil end = through the last.
	wantKV(t, scanNowKV(t, d, []byte("d"), nil), kv{"d", "d"}, kv{"e", "e"})
	// nil start = from the first.
	wantKV(t, scanNowKV(t, d, nil, []byte("c")), kv{"a", "a"}, kv{"b", "b"})
}

func TestScanAcrossLayers(t *testing.T) {
	d := openScanDB(t)
	defer d.Close()
	d.Put([]byte("a"), []byte("1"))
	d.Put([]byte("c"), []byte("3"))
	d.FlushForTesting() // a,c -> sstable
	d.Put([]byte("b"), []byte("2"))
	d.Put([]byte("d"), []byte("4"))
	d.FlushForTesting() // b,d -> newer sstable
	d.Put([]byte("e"), []byte("5"))
	if d.NumSSTablesForTesting() != 2 {
		t.Fatalf("expected 2 sstables, got %d", d.NumSSTablesForTesting())
	}
	wantKV(t, scanNowKV(t, d, nil, nil),
		kv{"a", "1"}, kv{"b", "2"}, kv{"c", "3"}, kv{"d", "4"}, kv{"e", "5"})
}

func TestScanNewestVersionWins(t *testing.T) {
	d := openScanDB(t)
	defer d.Close()
	d.Put([]byte("k"), []byte("v1"))
	d.FlushForTesting()
	d.Put([]byte("k"), []byte("v2"))
	d.FlushForTesting()
	d.Put([]byte("k"), []byte("v3")) // in memtable
	wantKV(t, scanNowKV(t, d, nil, nil), kv{"k", "v3"})
}

func TestScanSnapshotSeesOldValue(t *testing.T) {
	d := openScanDB(t)
	defer d.Close()
	d.Put([]byte("k"), []byte("v1"))
	snap := d.NextTimestampForTesting() - 1
	d.Put([]byte("k"), []byte("v2"))

	it, err := d.Scan(nil, nil, snap)
	if err != nil {
		t.Fatal(err)
	}
	wantKV(t, collectScan(t, it), kv{"k", "v1"})
	// And "now" sees v2.
	wantKV(t, scanNowKV(t, d, nil, nil), kv{"k", "v2"})
}

func TestScanSkipsTombstone(t *testing.T) {
	d := openScanDB(t)
	defer d.Close()
	d.Put([]byte("a"), []byte("1"))
	d.Put([]byte("b"), []byte("2"))
	d.Put([]byte("c"), []byte("3"))
	snapBeforeDelete := d.NextTimestampForTesting() - 1
	d.Delete([]byte("b"))

	// Now: b is gone.
	wantKV(t, scanNowKV(t, d, nil, nil), kv{"a", "1"}, kv{"c", "3"})
	// At the earlier snapshot: b is still present.
	it, err := d.Scan(nil, nil, snapBeforeDelete)
	if err != nil {
		t.Fatal(err)
	}
	wantKV(t, collectScan(t, it), kv{"a", "1"}, kv{"b", "2"}, kv{"c", "3"})
}

func TestScanTombstoneAcrossLayers(t *testing.T) {
	// Tombstone in the memtable must shadow a value flushed to an SSTable.
	d := openScanDB(t)
	defer d.Close()
	d.Put([]byte("k"), []byte("v"))
	d.FlushForTesting()
	d.Delete([]byte("k")) // tombstone in memtable
	wantKV(t, scanNowKV(t, d, nil, nil))
}

func TestScanSnapshotConsistentVsConcurrentWrite(t *testing.T) {
	d := openScanDB(t)
	defer d.Close()
	d.Put([]byte("a"), []byte("1"))
	d.Put([]byte("b"), []byte("2"))

	it, err := d.ScanNow(nil, nil) // captures snapshot + layers now
	if err != nil {
		t.Fatal(err)
	}
	// A write committed after the scan started must be invisible to it.
	d.Put([]byte("c"), []byte("3"))
	d.Put([]byte("a"), []byte("1-new"))
	wantKV(t, collectScan(t, it), kv{"a", "1"}, kv{"b", "2"})
}

func TestScanSurvivesFlushBoundary(t *testing.T) {
	d := openScanDB(t)
	defer d.Close()
	d.Put([]byte("k"), []byte("v1"))
	it, err := d.ScanNow(nil, nil) // memtable snapshot has k@v1, no sstables yet
	if err != nil {
		t.Fatal(err)
	}
	d.FlushForTesting()              // k@v1 moves to sstable, memtable cleared
	d.Put([]byte("k"), []byte("v2")) // newer version, higher ts
	wantKV(t, collectScan(t, it), kv{"k", "v1"})
}

func TestScanCloseIdempotent(t *testing.T) {
	d := openScanDB(t)
	defer d.Close()
	d.Put([]byte("k"), []byte("v"))
	it, err := d.ScanNow(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := it.Close(); err != nil {
		t.Fatal(err)
	}
	if err := it.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
	if it.Next() {
		t.Error("Next after Close returned true")
	}
}

func TestScanMultiBlockSubRange(t *testing.T) {
	d, err := OpenWith(t.TempDir(), Options{
		SyncOnWrite: false, DisableBackgroundCompaction: true, MemtableSizeMax: 8 * 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	value := make([]byte, 80)
	for i := 0; i < 500; i++ {
		d.Put([]byte(fmt.Sprintf("k%04d", i)), value)
	}
	d.FlushForTesting()

	got := scanNowKV(t, d, []byte("k0100"), []byte("k0150"))
	if len(got) != 50 {
		t.Fatalf("sub-range got %d keys, want 50", len(got))
	}
	if got[0].k != "k0100" || got[len(got)-1].k != "k0149" {
		t.Errorf("range ends: first=%s last=%s", got[0].k, got[len(got)-1].k)
	}
	// Ascending order.
	for i := 1; i < len(got); i++ {
		if got[i-1].k >= got[i].k {
			t.Fatalf("not ascending at %d: %s then %s", i, got[i-1].k, got[i].k)
		}
	}
}

func TestScanConcurrentWithWrites(t *testing.T) {
	d, err := OpenWith(t.TempDir(), Options{SyncOnWrite: false, CompactionTrigger: 3})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	const n = 100
	for i := 0; i < n; i++ {
		d.Put([]byte(fmt.Sprintf("k%04d", i)), []byte("seed"))
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// Writer: keeps mutating and flushing while scans run.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for w := 0; ; w++ {
			select {
			case <-stop:
				return
			default:
			}
			d.Put([]byte(fmt.Sprintf("k%04d", w%n)), []byte(fmt.Sprintf("v%d", w)))
			if w%50 == 0 {
				d.FlushForTesting()
			}
		}
	}()

	// Scanners: full scans must always succeed and stay sorted.
	errCh := make(chan error, 4)
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				it, err := d.ScanNow(nil, nil)
				if err != nil {
					errCh <- err
					return
				}
				var last string
				for it.Next() {
					k := string(it.Key())
					if last != "" && k <= last {
						errCh <- fmt.Errorf("scan out of order: %q then %q", last, k)
						it.Close()
						return
					}
					last = k
				}
				if err := it.Err(); err != nil {
					errCh <- err
					it.Close()
					return
				}
				it.Close()
			}
		}()
	}

	// Let it run a bit, then stop.
	for i := 0; i < 20; i++ {
		d.Put([]byte("trigger"), []byte("x"))
	}
	close(stop)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
}
