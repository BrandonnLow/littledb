package db

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// replicaOpts are the options a cluster replica opens its store with: the Raft
// log is the write-ahead log (ExternalLog), so the store keeps no data WAL.
func replicaOpts() Options {
	return Options{SyncOnWrite: false, DisableBackgroundCompaction: true, ExternalLog: true}
}

// prepareEntry builds a committed-entry payload through the real leader path
// (PrepareCommit), optionally stamped with (session, seq), for a txn that puts
// each key in puts and deletes each key in dels.
func prepareEntry(t *testing.T, d *DB, session []byte, seq uint64, puts map[string]string, dels ...string) []byte {
	t.Helper()
	tx := d.Begin()
	if session != nil {
		tx.SetSession(session, seq)
	}
	for k, v := range puts {
		if err := tx.Put([]byte(k), []byte(v)); err != nil {
			t.Fatal(err)
		}
	}
	for _, k := range dels {
		if err := tx.Delete([]byte(k)); err != nil {
			t.Fatal(err)
		}
	}
	entry, _, err := d.PrepareCommit(tx)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if entry == nil {
		t.Fatalf("prepare: unexpectedly deduped or empty")
	}
	return entry
}

// TestExternalLogKeepsNoWAL pins the point of ExternalLog: applying entries
// writes no data WAL at all, and nothing about them is durable in the store
// until a flush — DurableAppliedIndex stays put until then, which is what tells
// the caller it must keep its own log. The single-node commit path, which would
// need a WAL, is refused rather than silently non-durable.
func TestExternalLogKeepsNoWAL(t *testing.T) {
	dir := t.TempDir()
	d, err := OpenWith(dir, replicaOpts())
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	for i := uint64(1); i <= 3; i++ {
		if err := d.ApplyEntry(i, prepareEntry(t, d, nil, 0, map[string]string{"k": "v"})); err != nil {
			t.Fatalf("apply %d: %v", i, err)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, walFilename)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("data WAL exists in an ExternalLog store (stat err=%v)", err)
	}
	if got := d.DurableAppliedIndex(); got != 0 {
		t.Fatalf("DurableAppliedIndex before any flush = %d, want 0", got)
	}
	if err := d.FlushApplied(); err != nil {
		t.Fatal(err)
	}
	if got := d.DurableAppliedIndex(); got != 3 {
		t.Fatalf("DurableAppliedIndex after flush = %d, want 3", got)
	}

	// No override installed: a plain commit has no log to make it durable.
	if err := d.Put([]byte("x"), []byte("y")); !errors.Is(err, errNoCommitPath) {
		t.Fatalf("single-node Put on an ExternalLog store = %v, want errNoCommitPath", err)
	}
	if _, err := d.Get([]byte("x")); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("refused Put left key x visible (err=%v)", err)
	}
}

// TestExternalLogRefusesDataWAL pins the clean cut from the two-log layout: a
// store with a non-empty data WAL (standalone, or a replica written before the
// Raft log became the only log) holds writes no replicated log knows about, so
// opening it as a replica must fail loudly rather than drop them. An empty WAL
// holds nothing and is accepted.
func TestExternalLogRefusesDataWAL(t *testing.T) {
	walMode := Options{SyncOnWrite: false, DisableBackgroundCompaction: true}

	dir := t.TempDir()
	w, err := OpenWith(dir, walMode)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if d, err := OpenWith(dir, replicaOpts()); !errors.Is(err, ErrUnexpectedWAL) {
		if d != nil {
			d.Close()
		}
		t.Fatalf("OpenWith(ExternalLog) over a non-empty WAL = %v, want ErrUnexpectedWAL", err)
	}

	empty := t.TempDir()
	w2, err := OpenWith(empty, walMode) // creates an empty WAL file
	if err != nil {
		t.Fatal(err)
	}
	if err := w2.Close(); err != nil {
		t.Fatal(err)
	}
	d, err := OpenWith(empty, replicaOpts())
	if err != nil {
		t.Fatalf("OpenWith(ExternalLog) over an empty WAL: %v", err)
	}
	d.Close()
}

// TestApplyEntryOrdering pins the replicated-apply contract: ApplyEntry and
// ApplyControlEntry need ExternalLog, and accept only the very next index, so a
// skipped or repeated entry is an error instead of silent divergence between the
// store's applied index and the caller's lastApplied.
func TestApplyEntryOrdering(t *testing.T) {
	walStore, err := OpenWith(t.TempDir(), Options{SyncOnWrite: false, DisableBackgroundCompaction: true})
	if err != nil {
		t.Fatal(err)
	}
	defer walStore.Close()
	noop, _, err := walStore.PrepareNoop()
	if err != nil {
		t.Fatal(err)
	}
	if err := walStore.ApplyEntry(1, noop); !errors.Is(err, errNotExternalLog) {
		t.Fatalf("ApplyEntry on a WAL-mode store = %v, want errNotExternalLog", err)
	}
	if err := walStore.ApplyControlEntry(1); !errors.Is(err, errNotExternalLog) {
		t.Fatalf("ApplyControlEntry on a WAL-mode store = %v, want errNotExternalLog", err)
	}

	d, err := OpenWith(t.TempDir(), replicaOpts())
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	noop, _, err = d.PrepareNoop()
	if err != nil {
		t.Fatal(err)
	}
	if err := d.ApplyEntry(2, noop); err == nil {
		t.Fatal("ApplyEntry(2) on a fresh store succeeded, want out-of-order error")
	}
	if err := d.ApplyEntry(1, noop); err != nil {
		t.Fatalf("ApplyEntry(1): %v", err)
	}
	if err := d.ApplyEntry(1, noop); err == nil {
		t.Fatal("re-applying index 1 succeeded, want out-of-order error")
	}
	if err := d.ApplyControlEntry(3); err == nil {
		t.Fatal("ApplyControlEntry(3) after 1 succeeded, want out-of-order error")
	}
	if err := d.ApplyControlEntry(2); err != nil {
		t.Fatalf("ApplyControlEntry(2): %v", err)
	}
	if d.appliedIndex != 2 {
		t.Fatalf("appliedIndex = %d, want 2", d.appliedIndex)
	}
}

// TestFlushAppliedWithoutData pins the empty-memtable flush: entries that leave
// no data behind (a read-index no-op, a membership change) still advance the
// applied index, and the log can only be compacted past them once that index is
// durable. FlushApplied must therefore record it even with nothing to write to
// an SSTable — and write no empty SSTable.
func TestFlushAppliedWithoutData(t *testing.T) {
	dir := t.TempDir()
	d, err := OpenWith(dir, replicaOpts())
	if err != nil {
		t.Fatal(err)
	}
	noop, noopTS, err := d.PrepareNoop()
	if err != nil {
		t.Fatal(err)
	}
	if err := d.ApplyEntry(1, noop); err != nil {
		t.Fatal(err)
	}
	if err := d.ApplyControlEntry(2); err != nil {
		t.Fatal(err)
	}
	if err := d.FlushApplied(); err != nil {
		t.Fatal(err)
	}
	if got := d.DurableAppliedIndex(); got != 2 {
		t.Fatalf("DurableAppliedIndex = %d, want 2", got)
	}
	if n := d.NumSSTablesForTesting(); n != 0 {
		t.Fatalf("flush with no data wrote %d SSTables, want 0", n)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}

	d2, err := OpenWith(dir, replicaOpts())
	if err != nil {
		t.Fatal(err)
	}
	defer d2.Close()
	if got := d2.RecoveredAppliedIndex(); got != 2 {
		t.Fatalf("RecoveredAppliedIndex = %d, want 2", got)
	}
	if got := d2.NextTimestampForTesting(); got <= noopTS {
		t.Fatalf("nextTimestamp after restart = %d, want > no-op ts %d", got, noopTS)
	}
}
