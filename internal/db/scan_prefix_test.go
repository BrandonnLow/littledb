package db

import (
	"testing"
)

// scanKeys returns just the keys a scan over [start, end) yields, in order.
// The prefix tests care about presence and ordering, not values.
func scanKeys(t *testing.T, d *DB, start, end []byte) []string {
	t.Helper()
	pairs := scanNowKV(t, d, start, end)
	out := make([]string, 0, len(pairs))
	for _, p := range pairs {
		out = append(out, p.k)
	}
	return out
}

func sameKeys(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestScanWithPrefixKeys is the regression guard for the MVCC key-ordering bug
// (see mvcckey.Compare). A key that is a strict prefix of another used to sort
// AFTER it, because a raw byte comparison read the shorter key's timestamp
// suffix as key bytes. Two symptoms followed, and both are asserted here:
//
//  1. A full scan returned keys out of order — "cats" before "cat" — breaking
//     Iterator's documented ascending-order contract.
//  2. A bounded scan could silently OMIT a live key. The cursors stop at the
//     first userKey at-or-past the end bound; with the ordering inverted,
//     "cats" (>= "catq") was reached before "cat" (< "catq"), so the scan
//     exited early and never yielded "cat" at all. Silent omission, not an
//     error — the caller had no way to tell.
//
// Both the memtable path (pre-flush) and the SSTable path (post-flush) are
// covered, since the two layers reach the ordering through different code.
func TestScanWithPrefixKeys(t *testing.T) {
	d := openScanDB(t)

	// "cat" is a strict prefix of "cats"; "car"/"cow" bracket them.
	for _, k := range []string{"car", "cat", "cats", "cow"} {
		if err := d.Put([]byte(k), []byte("v-"+k)); err != nil {
			t.Fatalf("put %q: %v", k, err)
		}
	}

	checks := []struct {
		name       string
		start, end []byte
		want       []string
	}{
		{"full scan is ascending", nil, nil, []string{"car", "cat", "cats", "cow"}},
		// The bound that used to lose a key: "cat" < "catq" <= "cats", so only
		// "cats" is out of range. Stopping on it must not hide "cat".
		{"end bound between a key and its extension", nil, []byte("catq"), []string{"car", "cat"}},
		{"range covering both prefix keys", []byte("cat"), []byte("cau"), []string{"cat", "cats"}},
		{"start bound on the longer key", []byte("cats"), nil, []string{"cats", "cow"}},
		{"empty range", []byte("cau"), []byte("cav"), nil},
	}

	for _, layer := range []string{"memtable", "sstable"} {
		if layer == "sstable" {
			if err := d.FlushForTesting(); err != nil {
				t.Fatalf("flush: %v", err)
			}
			if n := d.NumSSTablesForTesting(); n != 1 {
				t.Fatalf("expected 1 sstable after flush, got %d", n)
			}
		}
		for _, c := range checks {
			if got := scanKeys(t, d, c.start, c.end); !sameKeys(got, c.want) {
				t.Errorf("[%s] %s: Scan(%q, %q) = %v, want %v",
					layer, c.name, c.start, c.end, got, c.want)
			}
		}
	}
}

// TestGetWithPrefixKeys pins that point lookups keep working across the
// ordering change. They were correct even under the old raw-byte ordering —
// every version of a userKey still clustered contiguously, so SeekGE plus a
// userKey-match check landed correctly — which is exactly why the bug hid for
// so long: the primary API was unaffected and only range scans exposed it.
func TestGetWithPrefixKeys(t *testing.T) {
	d := openScanDB(t)

	keys := []string{"a", "ab", "abc", "car", "cat", "cats", "cow"}
	for _, k := range keys {
		if err := d.Put([]byte(k), []byte("v-"+k)); err != nil {
			t.Fatalf("put %q: %v", k, err)
		}
	}
	// Overwrite one prefix key so it carries multiple versions.
	if err := d.Put([]byte("cat"), []byte("v-cat-2")); err != nil {
		t.Fatal(err)
	}

	for _, layer := range []string{"memtable", "sstable"} {
		if layer == "sstable" {
			if err := d.FlushForTesting(); err != nil {
				t.Fatalf("flush: %v", err)
			}
		}
		for _, k := range keys {
			want := "v-" + k
			if k == "cat" {
				want = "v-cat-2"
			}
			got, err := d.Get([]byte(k))
			if err != nil {
				t.Errorf("[%s] Get(%q): %v", layer, k, err)
				continue
			}
			if string(got) != want {
				t.Errorf("[%s] Get(%q) = %q, want %q", layer, k, got, want)
			}
		}
		if _, err := d.Get([]byte("ca")); err == nil {
			t.Errorf("[%s] Get(%q) succeeded; that key was never written", layer, "ca")
		}
	}
}

// TestCompactionWithPrefixKeys covers the third path that orders encoded keys:
// the compaction merge heap. Several SSTables holding prefix pairs must merge
// into one whose records are still correctly ordered — sstable.Writer.Add
// rejects an out-of-order key outright, so a mis-ordered merge surfaces as a
// compaction error rather than a silently corrupt file.
func TestCompactionWithPrefixKeys(t *testing.T) {
	dir := t.TempDir()
	d, err := OpenWith(dir, Options{
		SyncOnWrite:                 false,
		CompactionTrigger:           4,
		DisableBackgroundCompaction: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.Close() })

	// One key per flush, ordered so prefix pairs land in different files.
	for _, k := range []string{"cats", "cat", "cow", "car"} {
		if err := d.Put([]byte(k), []byte("v-"+k)); err != nil {
			t.Fatalf("put %q: %v", k, err)
		}
		if err := d.FlushForTesting(); err != nil {
			t.Fatalf("flush: %v", err)
		}
	}
	if n := d.NumSSTablesForTesting(); n != 4 {
		t.Fatalf("expected 4 sstables, got %d", n)
	}

	if err := d.CompactForTesting(); err != nil {
		t.Fatalf("compaction failed (a mis-ordered merge is rejected by Writer.Add): %v", err)
	}
	if n := d.NumSSTablesForTesting(); n != 1 {
		t.Fatalf("expected 1 sstable after compaction, got %d", n)
	}

	want := []string{"car", "cat", "cats", "cow"}
	if got := scanKeys(t, d, nil, nil); !sameKeys(got, want) {
		t.Errorf("scan after compaction = %v, want %v", got, want)
	}
	for _, k := range want {
		got, err := d.Get([]byte(k))
		if err != nil || string(got) != "v-"+k {
			t.Errorf("after compaction Get(%q) = (%q, %v), want %q", k, got, err, "v-"+k)
		}
	}
}
