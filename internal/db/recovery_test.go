package db

import "testing"

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
