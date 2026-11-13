package db

import (
	"os"
	"path/filepath"
	"testing"
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

// TestRecoveryReplaysFromAppliedBase pins the ExternalLog flush's crash contract
// (flushExternalLocked). A flush writes the SSTable, then the sessions file, then
// applied.base — the commit point. A crash after the SSTable (and perhaps the
// sessions file) landed but before applied.base did must recover to the PREVIOUS
// flush, leaving the caller to replay its log from there. That replay re-applies
// entries whose data is already in the new SSTable and, in the second case,
// whose sessions are already in the new sessions file; both must converge to
// exactly the state of a clean run: same values, same session table, a
// session-deduped retry still deduped, and the same state again after the next
// flush and restart.
func TestRecoveryReplaysFromAppliedBase(t *testing.T) {
	cases := []struct {
		name         string
		sessionsLost bool // crash before the sessions file was replaced too
	}{
		{"crash-after-sstable", true},
		{"crash-after-sstable-and-sessions", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			opts := replicaOpts()
			sess := []byte("client")

			d, err := OpenWith(dir, opts)
			if err != nil {
				t.Fatal(err)
			}
			apply := func(idx uint64, entry []byte) {
				t.Helper()
				if err := d.ApplyEntry(idx, entry); err != nil {
					t.Fatalf("ApplyEntry %d: %v", idx, err)
				}
			}
			// First flush covers 1..3 — the state the crash falls back to.
			apply(1, prepareEntry(t, d, nil, 0, map[string]string{"k1": "a"}))
			apply(2, prepareEntry(t, d, sess, 1, map[string]string{"k2": "b"}))
			apply(3, prepareEntry(t, d, nil, 0, map[string]string{"k1": "c"}))
			if err := d.FlushApplied(); err != nil {
				t.Fatal(err)
			}
			oldBase := readFile(t, filepath.Join(dir, appliedBaseFilename))
			oldSessions := readFile(t, filepath.Join(dir, sessionsFilename))

			// Second flush covers 4..6 and is the one the crash interrupts.
			tail := [][]byte{
				prepareEntry(t, d, sess, 2, map[string]string{"k2": "d"}),
				nil, // placeholder: prepared after 4 is applied (see below)
				nil,
			}
			apply(4, tail[0])
			tail[1] = prepareEntry(t, d, nil, 0, nil, "k1")
			apply(5, tail[1])
			tail[2] = prepareEntry(t, d, sess, 3, map[string]string{"k3": "e"})
			apply(6, tail[2])
			if err := d.FlushApplied(); err != nil {
				t.Fatal(err)
			}
			if err := d.Close(); err != nil {
				t.Fatal(err)
			}

			// Rewind to the crash point: the new SSTable stays; applied.base (and in
			// the first case the sessions file) is still the first flush's.
			writeFile(t, filepath.Join(dir, appliedBaseFilename), oldBase)
			if tc.sessionsLost {
				writeFile(t, filepath.Join(dir, sessionsFilename), oldSessions)
			}

			d, err = OpenWith(dir, opts)
			if err != nil {
				t.Fatal(err)
			}
			if got := d.RecoveredAppliedIndex(); got != 3 {
				t.Fatalf("RecoveredAppliedIndex = %d, want 3 (applied.base, not the newer SSTable footer)", got)
			}
			for i, e := range tail {
				apply(uint64(4+i), e) // the caller's replay of its log
			}

			check := func(label string) {
				t.Helper()
				if _, err := d.Get([]byte("k1")); err != ErrKeyNotFound {
					t.Errorf("%s: k1 err = %v, want ErrKeyNotFound (deleted at 5)", label, err)
				}
				for k, want := range map[string]string{"k2": "d", "k3": "e"} {
					if got, err := d.Get([]byte(k)); err != nil || string(got) != want {
						t.Errorf("%s: %s = (%q,%v), want %q", label, k, got, err, want)
					}
				}
				if got := d.SessionLastSeq(sess); got != 3 {
					t.Errorf("%s: SessionLastSeq = %d, want 3", label, got)
				}
				// A retry of seq 2 (applied at 4, inside the replayed window) is deduped.
				tx := d.Begin()
				tx.SetSession(sess, 2)
				if err := tx.Put([]byte("k2"), []byte("stale-retry")); err != nil {
					t.Fatal(err)
				}
				if e, _, err := d.PrepareCommit(tx); err != nil || e != nil {
					t.Errorf("%s: retry of seq 2: entry=%v err=%v, want deduped", label, e, err)
				}
			}
			check("after replay")

			// The duplicated versions collapse, and the next flush + restart agrees.
			if err := d.FlushApplied(); err != nil {
				t.Fatal(err)
			}
			if err := d.Close(); err != nil {
				t.Fatal(err)
			}
			d, err = OpenWith(dir, opts)
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close()
			if got := d.RecoveredAppliedIndex(); got != 6 {
				t.Fatalf("RecoveredAppliedIndex after re-flush = %d, want 6", got)
			}
			check("after re-flush and restart")
		})
	}
}

func readFile(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func writeFile(t *testing.T, path string, b []byte) {
	t.Helper()
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
}
