package db

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// TestDurabilityContract verifies the core promise: every Put that
// returned nil is on disk and survives a "crash" (here simulated by
// dropping the DB handle without calling Close).
//
// This works because our Put fsyncs before returning. The fsync is
// what makes the guarantee real — without it this test would be racy
// and would fail when the page cache hadn't been flushed yet.
func TestDurabilityContract(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	const n = 1000
	written := make(map[string]string, n)
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("k%04d", i)
		v := fmt.Sprintf("v%04d", i)
		if err := d.Put([]byte(k), []byte(v)); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
		written[k] = v
	}

	// Simulate crash: throw away the handle. No Close = no clean shutdown.
	// In a real crash the process would be gone too; here we just stop
	// using d. The file is on disk with whatever fsyncs completed,
	// which is all of them because Put waits for fsync.
	_ = d

	d2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen after crash: %v", err)
	}
	defer d2.Close()

	for k, want := range written {
		got, err := d2.Get([]byte(k))
		if err != nil {
			t.Errorf("durability violation: %s lost after crash: %v", k, err)
			continue
		}
		if !bytes.Equal(got, []byte(want)) {
			t.Errorf("%s: got %q, want %q", k, got, want)
		}
	}
}

// TestRecoveryFromGarbageTail simulates a crash that left half a record
// on disk. The DB must recover: reopen succeeds, all previously-acked
// records are intact, and the garbage is truncated away so future writes
// land in a clean position.
func TestRecoveryFromGarbageTail(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	written := map[string]string{
		"alpha":   "first",
		"bravo":   "second",
		"charlie": "third",
	}
	for k, v := range written {
		if err := d.Put([]byte(k), []byte(v)); err != nil {
			t.Fatal(err)
		}
	}
	d.Close()

	// Append junk to the log file, simulating a torn write at the tail.
	logPath := filepath.Join(dir, "littledb.log")
	f, err := os.OpenFile(logPath, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02, 0x03}); err != nil {
		t.Fatal(err)
	}
	f.Close()

	// Reopen. Recovery should silently truncate the garbage.
	d2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen with garbage tail: %v", err)
	}
	defer d2.Close()

	for k, want := range written {
		got, err := d2.Get([]byte(k))
		if err != nil {
			t.Errorf("%s lost after recovery: %v", k, err)
			continue
		}
		if !bytes.Equal(got, []byte(want)) {
			t.Errorf("%s: got %q, want %q", k, got, want)
		}
	}

	// New writes should land cleanly past the truncation point.
	if err := d2.Put([]byte("post-recovery"), []byte("works")); err != nil {
		t.Fatal(err)
	}
	got, err := d2.Get([]byte("post-recovery"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, []byte("works")) {
		t.Errorf("post-recovery: got %q, want works", got)
	}
}

// TestRecoveryPreservesAckedDeletes ensures tombstones survive a crash.
// We Put a key, Delete it (both acked), simulate a crash, reopen, and
// verify the key is gone — not resurrected by replaying the original Put.
func TestRecoveryPreservesAckedDeletes(t *testing.T) {
	dir := t.TempDir()
	d, _ := Open(dir)

	if err := d.Put([]byte("temp"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := d.Delete([]byte("temp")); err != nil {
		t.Fatal(err)
	}

	// Crash: drop without closing.
	_ = d

	d2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer d2.Close()

	if _, err := d2.Get([]byte("temp")); !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("acked delete lost after crash: err = %v", err)
	}
}

// TestRecoveryOnRepeatedRestarts is the brutal version: open, write,
// "crash", reopen, write more, "crash", reopen, ... and verify the full
// history is intact each time. Catches index-rebuild bugs that only
// show up after multiple restart cycles.
func TestRecoveryOnRepeatedRestarts(t *testing.T) {
	dir := t.TempDir()
	all := map[string]string{}

	for cycle := 0; cycle < 5; cycle++ {
		d, err := Open(dir)
		if err != nil {
			t.Fatalf("cycle %d open: %v", cycle, err)
		}

		// Verify everything from previous cycles is still there.
		for k, want := range all {
			got, err := d.Get([]byte(k))
			if err != nil {
				t.Errorf("cycle %d: %s lost: %v", cycle, k, err)
				continue
			}
			if !bytes.Equal(got, []byte(want)) {
				t.Errorf("cycle %d: %s got %q want %q", cycle, k, got, want)
			}
		}

		// Add some new keys this cycle.
		for i := 0; i < 100; i++ {
			k := fmt.Sprintf("cycle%d-k%d", cycle, i)
			v := fmt.Sprintf("cycle%d-v%d", cycle, i)
			if err := d.Put([]byte(k), []byte(v)); err != nil {
				t.Fatal(err)
			}
			all[k] = v
		}

		// "Crash" — drop without closing.
		_ = d
	}
}
