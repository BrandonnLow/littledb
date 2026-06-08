package db

import (
	"path/filepath"
	"testing"

	"github.com/BrandonnLow/littledb/internal/record"
	"github.com/BrandonnLow/littledb/internal/sstable"
)

// These tests exercise compactSSTables directly with bottomOfLSM=false —
// the path no live database configuration currently produces (today's
// size-tiered strategy always merges the oldest tail, so the real call site
// always passes true). A future mid-layer compaction strategy would pass
// false, and without these guards the version-GC branches would silently
// drop records that lower SSTables still depend on, exposing stale data.
//
// Each test pairs the bottomOfLSM=false case (must KEEP) with a
// bottomOfLSM=true control (must DROP), so the two polarities are pinned.

// version is one (op, userKey, value, ts) tuple for building a test SSTable.
type version struct {
	op    record.Op
	key   string
	value string
	ts    uint64
}

// buildSSTable writes versions to a fresh SSTable at path and returns an
// opened reader. Versions must already be in ascending encoded-key order:
// userKey ascending, then timestamp descending within a userKey.
func buildSSTable(t *testing.T, path string, versions []version) *sstable.Reader {
	t.Helper()
	w, err := sstable.NewWriter(path, len(versions))
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	for _, v := range versions {
		if err := w.Add(v.op, []byte(v.key), []byte(v.value), v.ts); err != nil {
			t.Fatalf("Add(%s@%d): %v", v.key, v.ts, err)
		}
	}
	if err := w.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}
	r, err := sstable.OpenReader(path)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	return r
}

// runCompaction compacts a single input SSTable (built from versions) at the
// given watermark and bottomOfLSM flag, returning an opened reader over the
// merged output. A single input still drives the full merge + per-userKey GC
// machinery; the k-way merge across multiple inputs is covered elsewhere.
func runCompaction(t *testing.T, versions []version, watermark uint64, bottomOfLSM bool) *sstable.Reader {
	t.Helper()
	dir := t.TempDir()
	in := buildSSTable(t, filepath.Join(dir, "000001.sst"), versions)
	defer in.Close()

	out := filepath.Join(dir, "000009.sst")
	if err := compactSSTables([]*sstable.Reader{in}, out, watermark, bottomOfLSM); err != nil {
		t.Fatalf("compactSSTables(bottomOfLSM=%v): %v", bottomOfLSM, err)
	}
	r, err := sstable.OpenReader(out)
	if err != nil {
		t.Fatalf("OpenReader(output): %v", err)
	}
	return r
}

// TestGCMidLayerKeepsTombstone: a head tombstone with ts ≤ watermark is the
// classic "drop the whole key" candidate at the bottom of the LSM. Mid-layer
// it must survive — an older SSTable below could hold versions this tombstone
// is masking, and dropping it would resurrect them.
func TestGCMidLayerKeepsTombstone(t *testing.T) {
	versions := []version{
		{record.OpDelete, "k", "", 10}, // newest
		{record.OpPut, "k", "v", 5},    // older, masked by the tombstone
	}

	// Mid-layer: nothing may be dropped.
	r := runCompaction(t, versions, 100, false)
	if got := r.VersionCountForTesting([]byte("k")); got != 2 {
		t.Errorf("bottomOfLSM=false: kept %d versions of k, want 2 (tombstone + put preserved)", got)
	}
	r.Close()

	// Control at the bottom: head tombstone (ts ≤ watermark) drops, and the
	// cascade drops the older put too — the whole key vanishes.
	r = runCompaction(t, versions, 100, true)
	if got := r.VersionCountForTesting([]byte("k")); got != 0 {
		t.Errorf("bottomOfLSM=true: kept %d versions of k, want 0 (whole key GC'd)", got)
	}
	r.Close()
}

// TestGCMidLayerKeepsOlderVersion guards the exact latent bug from Phase 3:
// the older-version-drop branch originally lacked the bottomOfLSM guard.
// Here the newest version's ts ≤ watermark, so at the bottom the older
// version is unreachable and correctly GC'd — but mid-layer it must be kept,
// because lower SSTables may rely on it for reads in its visibility window.
func TestGCMidLayerKeepsOlderVersion(t *testing.T) {
	versions := []version{
		{record.OpPut, "k", "new", 10}, // newest
		{record.OpPut, "k", "old", 5},  // older
	}

	// Mid-layer: both versions kept. (Buggy pre-fix code dropped "old" here.)
	r := runCompaction(t, versions, 100, false)
	if got := r.VersionCountForTesting([]byte("k")); got != 2 {
		t.Errorf("bottomOfLSM=false: kept %d versions of k, want 2 (older version preserved)", got)
	}
	r.Close()

	// Control at the bottom: the older version is dropped, newest survives.
	r = runCompaction(t, versions, 100, true)
	if got := r.VersionCountForTesting([]byte("k")); got != 1 {
		t.Errorf("bottomOfLSM=true: kept %d versions of k, want 1 (newest only)", got)
	}
	r.Close()
}
