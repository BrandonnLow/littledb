package db

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/BrandonnLow/littledb/internal/record"
	"github.com/BrandonnLow/littledb/internal/wal"
)

func TestTxnPutCommitVisible(t *testing.T) {
	d, _ := OpenWith(t.TempDir(), Options{SyncOnWrite: false})
	defer d.Close()

	tx := d.Begin()
	tx.Put([]byte("k"), []byte("v"))
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	got, err := d.Get([]byte("k"))
	if err != nil || !bytes.Equal(got, []byte("v")) {
		t.Errorf("got %q err=%v", got, err)
	}
}

func TestTxnRollbackDiscardsWrites(t *testing.T) {
	d, _ := OpenWith(t.TempDir(), Options{SyncOnWrite: false})
	defer d.Close()

	tx := d.Begin()
	tx.Put([]byte("k"), []byte("v"))
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	if _, err := d.Get([]byte("k")); !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("err = %v, want ErrKeyNotFound", err)
	}
}

func TestTxnReadYourOwnWrites(t *testing.T) {
	d, _ := OpenWith(t.TempDir(), Options{SyncOnWrite: false})
	defer d.Close()

	tx := d.Begin()
	tx.Put([]byte("k"), []byte("v"))
	got, err := tx.Get([]byte("k"))
	if err != nil || !bytes.Equal(got, []byte("v")) {
		t.Errorf("read-your-own-writes: got %q err=%v", got, err)
	}

	tx.Delete([]byte("k"))
	if _, err := tx.Get([]byte("k")); !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("read-your-own-delete: err = %v, want ErrKeyNotFound", err)
	}
	tx.Rollback()
}

func TestTxnSnapshotIsolation(t *testing.T) {
	d, _ := OpenWith(t.TempDir(), Options{SyncOnWrite: false})
	defer d.Close()
	d.Put([]byte("k"), []byte("v1"))

	txA := d.Begin()

	// Another writer commits while txA is open.
	d.Put([]byte("k"), []byte("v2"))

	got, err := txA.Get([]byte("k"))
	if err != nil || !bytes.Equal(got, []byte("v1")) {
		t.Errorf("txA: got %q err=%v, want v1 (snapshot at Begin)", got, err)
	}

	got, _ = d.Get([]byte("k"))
	if !bytes.Equal(got, []byte("v2")) {
		t.Errorf("outside: got %q, want v2", got)
	}
	txA.Rollback()
}

func TestTxnEmptyCommitNoOp(t *testing.T) {
	d, _ := OpenWith(t.TempDir(), Options{SyncOnWrite: false})
	defer d.Close()
	tx := d.Begin()
	tsBeforeCommit := d.NextTimestampForTesting()
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if got := d.NextTimestampForTesting(); got != tsBeforeCommit {
		t.Errorf("nextTimestamp = %d, want unchanged at %d", got, tsBeforeCommit)
	}
}

func TestTxnAlreadyFinished(t *testing.T) {
	d, _ := OpenWith(t.TempDir(), Options{SyncOnWrite: false})
	defer d.Close()
	tx := d.Begin()
	tx.Commit()
	if err := tx.Put([]byte("k"), []byte("v")); !errors.Is(err, ErrTxnFinished) {
		t.Errorf("Put after Commit: err = %v", err)
	}
	if err := tx.Commit(); !errors.Is(err, ErrTxnFinished) {
		t.Errorf("Commit twice: err = %v", err)
	}
	if err := tx.Rollback(); !errors.Is(err, ErrTxnFinished) {
		t.Errorf("Rollback after Commit: err = %v", err)
	}
}

func TestTxnMultiKeyAtomicVisibility(t *testing.T) {
	d, _ := OpenWith(t.TempDir(), Options{SyncOnWrite: false})
	defer d.Close()

	tx := d.Begin()
	tx.Put([]byte("a"), []byte("1"))
	tx.Put([]byte("b"), []byte("2"))
	tx.Put([]byte("c"), []byte("3"))

	for _, k := range []string{"a", "b", "c"} {
		if _, err := d.Get([]byte(k)); !errors.Is(err, ErrKeyNotFound) {
			t.Errorf("pre-commit Get(%q): err = %v", k, err)
		}
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	for _, c := range []struct{ k, v string }{{"a", "1"}, {"b", "2"}, {"c", "3"}} {
		got, err := d.Get([]byte(c.k))
		if err != nil || !bytes.Equal(got, []byte(c.v)) {
			t.Errorf("post-commit Get(%q): got %q err=%v", c.k, got, err)
		}
	}
}

func TestTxnMultiKeySharesSingleTimestamp(t *testing.T) {
	d, _ := OpenWith(t.TempDir(), Options{SyncOnWrite: false})
	defer d.Close()

	tsBefore := d.NextTimestampForTesting() - 1
	tx := d.Begin()
	tx.Put([]byte("a"), []byte("1"))
	tx.Put([]byte("b"), []byte("2"))
	tx.Commit()
	tsAfter := d.NextTimestampForTesting() - 1

	if tsAfter != tsBefore+1 {
		t.Errorf("expected one timestamp consumed; before=%d after=%d", tsBefore, tsAfter)
	}

	for _, k := range []string{"a", "b"} {
		if _, err := d.GetAsOf([]byte(k), tsBefore); !errors.Is(err, ErrKeyNotFound) {
			t.Errorf("GetAsOf(%q, tsBefore): err = %v", k, err)
		}
	}
	for _, c := range []struct{ k, v string }{{"a", "1"}, {"b", "2"}} {
		got, err := d.GetAsOf([]byte(c.k), tsAfter)
		if err != nil || !bytes.Equal(got, []byte(c.v)) {
			t.Errorf("GetAsOf(%q, tsAfter): got %q err=%v", c.k, got, err)
		}
	}
}

// ---------- Concurrent txns ----------

func TestConcurrentTxnsIndependent(t *testing.T) {
	d, _ := OpenWith(t.TempDir(), Options{SyncOnWrite: false})
	defer d.Close()

	const numTxns = 8
	const writesPerTxn = 5
	var wg sync.WaitGroup
	errCh := make(chan error, numTxns)

	for i := 0; i < numTxns; i++ {
		wg.Add(1)
		go func(txnID int) {
			defer wg.Done()
			tx := d.Begin()
			for j := 0; j < writesPerTxn; j++ {
				k := fmt.Sprintf("txn%d-k%d", txnID, j)
				v := fmt.Sprintf("v%d", j)
				if err := tx.Put([]byte(k), []byte(v)); err != nil {
					errCh <- err
					return
				}
			}
			if err := tx.Commit(); err != nil {
				errCh <- err
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}

	for i := 0; i < numTxns; i++ {
		for j := 0; j < writesPerTxn; j++ {
			k := fmt.Sprintf("txn%d-k%d", i, j)
			v := fmt.Sprintf("v%d", j)
			got, err := d.Get([]byte(k))
			if err != nil || !bytes.Equal(got, []byte(v)) {
				t.Errorf("Get(%q): got %q err=%v", k, got, err)
			}
		}
	}
}

// TestConcurrentTxnsLastWriterWins documents the previous conflict
// semantics: two concurrent txns can write the same key, and the
// later committer's value wins. We will replace this with
// first-committer-wins (returning ErrConflict to the loser).
func TestConcurrentTxnsLastWriterWins(t *testing.T) {
	d, _ := OpenWith(t.TempDir(), Options{SyncOnWrite: false})
	defer d.Close()

	txA := d.Begin()
	txB := d.Begin()
	txA.Put([]byte("k"), []byte("from-A"))
	txB.Put([]byte("k"), []byte("from-B"))

	if err := txA.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := txB.Commit(); err != nil {
		t.Fatal(err)
	}

	got, _ := d.Get([]byte("k"))
	if !bytes.Equal(got, []byte("from-B")) {
		t.Errorf("got %q, want from-B (last writer wins)", got)
	}
}

// ---------- Recovery / crash safety ----------

func TestTxnPersistsAcrossClose(t *testing.T) {
	dir := t.TempDir()
	d, _ := OpenWith(dir, Options{SyncOnWrite: true})
	tx := d.Begin()
	tx.Put([]byte("a"), []byte("1"))
	tx.Put([]byte("b"), []byte("2"))
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	d.Close()

	d2, _ := Open(dir)
	defer d2.Close()
	for _, c := range []struct{ k, v string }{{"a", "1"}, {"b", "2"}} {
		got, err := d2.Get([]byte(c.k))
		if err != nil || !bytes.Equal(got, []byte(c.v)) {
			t.Errorf("after restart Get(%q): got %q err=%v", c.k, got, err)
		}
	}
}

// TestRecoveryDiscardsUncommittedTxn simulates a crash between writing
// data records and writing the OpCommit marker. We do this by
// poking the WAL directly, then reopening the DB and confirming the
// uncommitted records are NOT visible.
func TestRecoveryDiscardsUncommittedTxn(t *testing.T) {
	dir := t.TempDir()

	d, _ := OpenWith(dir, Options{SyncOnWrite: true})
	d.Put([]byte("committed"), []byte("yes"))

	uncommittedTS := d.NextTimestampForTesting()
	rec := &record.Record{
		Op:        record.OpPut,
		Timestamp: uncommittedTS,
		Key:       []byte("uncommitted"),
		Value:     []byte("ghost"),
	}
	w, _ := wal.OpenWith(dir, wal.Options{SyncOnWrite: true})
	w.Append(rec)
	w.Close()
	d.Close()

	d2, _ := Open(dir)
	defer d2.Close()
	got, err := d2.Get([]byte("committed"))
	if err != nil || !bytes.Equal(got, []byte("yes")) {
		t.Errorf("committed: got %q err=%v", got, err)
	}
	if _, err := d2.Get([]byte("uncommitted")); !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("uncommitted: err = %v, want ErrKeyNotFound", err)
	}
}

func TestRecoveryAppliesMultiKeyTxn(t *testing.T) {
	dir := t.TempDir()
	d, _ := OpenWith(dir, Options{SyncOnWrite: true})
	tx := d.Begin()
	tx.Put([]byte("a"), []byte("1"))
	tx.Put([]byte("b"), []byte("2"))
	tx.Put([]byte("c"), []byte("3"))
	tx.Commit()
	d.Close()

	info, _ := os.Stat(filepath.Join(dir, walFilename))
	if info.Size() == 0 {
		t.Fatal("WAL was unexpectedly empty before reopen")
	}

	d2, _ := Open(dir)
	defer d2.Close()
	for _, c := range []struct{ k, v string }{{"a", "1"}, {"b", "2"}, {"c", "3"}} {
		got, err := d2.Get([]byte(c.k))
		if err != nil || !bytes.Equal(got, []byte(c.v)) {
			t.Errorf("after recovery Get(%q): got %q err=%v", c.k, got, err)
		}
	}
}

// ---------- Snapshot isolation across flush ----------

func TestTxnSurvivesFlushBoundary(t *testing.T) {
	d, _ := OpenWith(t.TempDir(), Options{SyncOnWrite: false})
	defer d.Close()

	d.Put([]byte("k"), []byte("v1"))
	tx := d.Begin()

	d.Put([]byte("k"), []byte("v2"))
	if err := d.FlushForTesting(); err != nil {
		t.Fatal(err)
	}

	got, err := tx.Get([]byte("k"))
	if err != nil || !bytes.Equal(got, []byte("v1")) {
		t.Errorf("after flush, txn read = %q err=%v, want v1", got, err)
	}
	tx.Rollback()
}
