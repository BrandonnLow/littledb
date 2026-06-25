package db

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/BrandonnLow/littledb/internal/record"
)

// TestAppliedWatermarkSurvivesFlush guards the recovery seed of appliedTS.
// After a flush empties the WAL, committed data lives only in SSTables, so the
// recovered applied watermark must be seeded from maxTS (which folds in the
// SSTable footers), not just the highest WAL commit. If it were seeded from
// the WAL alone it would recover as 0, and the first overwrite of any
// pre-existing key would fail the conflict check forever (existingTS > 0),
// wedging the workload.
func TestAppliedWatermarkSurvivesFlush(t *testing.T) {
	dir := t.TempDir()
	opts := Options{SyncOnWrite: false, DisableBackgroundCompaction: true}

	d, err := OpenWith(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := d.FlushForTesting(); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen: WAL is empty, k lives in an SSTable. Overwriting k must not
	// spuriously conflict against a watermark that forgot the SSTable.
	d2, err := OpenWith(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer d2.Close()

	if err := d2.Put([]byte("k"), []byte("v2")); err != nil {
		t.Fatalf("overwrite after flush+reopen = %v, want nil (watermark must cover SSTable data)", err)
	}
	if got, err := d2.Get([]byte("k")); err != nil || string(got) != "v2" {
		t.Errorf("Get(k) = (%q,%v), want v2", got, err)
	}
}

// TestRecoveryAppliedIndexSurvivesLostBase pins the crash-window invariant for
// the applied watermark: after a flush stamps appliedIndex=K into the SSTable
// footer, a lost-or-torn applied.base must not under-count lastApplied, because
// recovery takes max(footer, base+walCommits). The flush ordering guarantees a
// missing base co-occurs with an empty post-flush WAL, so this is exactly the
// recoverable window — max() resolves to the footer (K). The complementary
// base-present-with-post-flush-commits path is covered by the cluster's
// TestClusterFlushBeforeRestart.
func TestRecoveryAppliedIndexSurvivesLostBase(t *testing.T) {
	cases := []struct {
		name   string
		damage func(t *testing.T, basePath string)
	}{
		{"base-deleted", func(t *testing.T, p string) {
			if err := os.Remove(p); err != nil {
				t.Fatal(err)
			}
		}},
		{"base-truncated", func(t *testing.T, p string) {
			if err := os.Truncate(p, 3); err != nil { // <8 bytes => treated as absent
				t.Fatal(err)
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			opts := Options{SyncOnWrite: true, DisableBackgroundCompaction: true}

			d, err := OpenWith(dir, opts)
			if err != nil {
				t.Fatal(err)
			}
			const k = 5
			for i := 1; i <= k; i++ {
				key := []byte(fmt.Sprintf("key%02d", i))
				val := []byte(fmt.Sprintf("val%02d", i))
				entry := append(
					record.Encode(&record.Record{Op: record.OpPut, Timestamp: uint64(i), Key: key, Value: val}),
					record.Encode(&record.Record{Op: record.OpCommit, Timestamp: uint64(i)})...)
				if err := d.ApplyEntry(uint64(i), entry); err != nil {
					t.Fatalf("ApplyEntry %d: %v", i, err)
				}
			}
			// Flush: footer stamps appliedIndex=k, applied.base written, WAL reset.
			if err := d.FlushForTesting(); err != nil {
				t.Fatal(err)
			}
			if err := d.Close(); err != nil {
				t.Fatal(err)
			}

			tc.damage(t, filepath.Join(dir, appliedBaseFilename))

			d2, err := OpenWith(dir, opts)
			if err != nil {
				t.Fatal(err)
			}
			defer d2.Close()
			if got := d2.RecoveredAppliedIndex(); got != k {
				t.Errorf("RecoveredAppliedIndex = %d, want %d (footer must cover a lost applied.base)", got, k)
			}
			for i := 1; i <= k; i++ {
				key := []byte(fmt.Sprintf("key%02d", i))
				want := fmt.Sprintf("val%02d", i)
				if got, err := d2.Get(key); err != nil || string(got) != want {
					t.Errorf("Get(%s) = (%q,%v), want %s", key, got, err, want)
				}
			}
		})
	}
}
