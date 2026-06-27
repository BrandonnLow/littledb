package db

import (
	"os"
	"testing"

	"github.com/BrandonnLow/littledb/internal/record"
)

// collectSnapshot drains a SnapshotScan into a KV slice (copying, as the cluster
// follower path must).
func collectSnapshot(t *testing.T, src *DB) (kvs []KV, lastIncludedIndex, snapshotTS uint64) {
	t.Helper()
	it, lii, ts, err := src.SnapshotScan()
	if err != nil {
		t.Fatalf("SnapshotScan: %v", err)
	}
	defer it.Close()
	for it.Next() {
		kvs = append(kvs, KV{
			Key:   append([]byte(nil), it.Key()...),
			Value: append([]byte(nil), it.Value()...),
		})
	}
	if err := it.Err(); err != nil {
		t.Fatalf("snapshot iterate: %v", err)
	}
	return kvs, lii, ts
}

func TestSnapshotScanBuildRoundTrip(t *testing.T) {
	srcDir := t.TempDir()
	src, err := OpenWith(srcDir, Options{SyncOnWrite: false, DisableBackgroundCompaction: true})
	if err != nil {
		t.Fatalf("open src: %v", err)
	}

	// Apply a handful of entries through the replicated path so appliedIndex /
	// appliedTS advance exactly as on a real follower. Include an overwrite and a
	// delete so the snapshot must reflect only the newest visible version and
	// drop the tombstoned key.
	apply := func(idx uint64, recs ...*recordSpec) {
		t.Helper()
		entry := encodeEntry(recs...)
		if err := src.ApplyEntry(idx, entry); err != nil {
			t.Fatalf("apply %d: %v", idx, err)
		}
	}
	apply(1, put("a", "1"), put("b", "1"), commit(10))
	apply(2, put("a", "2"), put("c", "1"), commit(11))
	apply(3, del("b"), commit(12))

	kvs, lii, ts := collectSnapshot(t, src)
	if lii != 3 {
		t.Fatalf("lastIncludedIndex = %d, want 3", lii)
	}
	if ts != 12 {
		t.Fatalf("snapshotTS = %d, want 12", ts)
	}
	// Expect a=2, c=1; b deleted.
	want := map[string]string{"a": "2", "c": "1"}
	if len(kvs) != len(want) {
		t.Fatalf("snapshot has %d keys, want %d (%v)", len(kvs), len(want), kvs)
	}
	for _, kv := range kvs {
		if want[string(kv.Key)] != string(kv.Value) {
			t.Fatalf("snapshot key %q = %q, want %q", kv.Key, kv.Value, want[string(kv.Key)])
		}
	}
	if err := src.Close(); err != nil {
		t.Fatalf("close src: %v", err)
	}

	// Materialize the snapshot into a fresh staged dir and reopen it.
	stagedDir := t.TempDir()
	if err := BuildSnapshotDB(stagedDir, kvs, lii, ts); err != nil {
		t.Fatalf("BuildSnapshotDB: %v", err)
	}
	dst, err := OpenWith(stagedDir, Options{SyncOnWrite: false, DisableBackgroundCompaction: true})
	if err != nil {
		t.Fatalf("open staged: %v", err)
	}
	defer dst.Close()

	if got := dst.RecoveredAppliedIndex(); got != lii {
		t.Fatalf("staged RecoveredAppliedIndex = %d, want %d", got, lii)
	}
	if got := dst.NextTimestampForTesting(); got != ts+1 {
		t.Fatalf("staged nextTimestamp = %d, want %d", got, ts+1)
	}
	for k, v := range want {
		got, err := dst.Get([]byte(k))
		if err != nil {
			t.Fatalf("staged Get %q: %v", k, err)
		}
		if string(got) != v {
			t.Fatalf("staged Get %q = %q, want %q", k, got, v)
		}
	}
	if _, err := dst.Get([]byte("b")); err != ErrKeyNotFound {
		t.Fatalf("staged Get b: err = %v, want ErrKeyNotFound", err)
	}
}

func TestBuildSnapshotDBEmpty(t *testing.T) {
	stagedDir := t.TempDir()
	if err := BuildSnapshotDB(stagedDir, nil, 42, 99); err != nil {
		t.Fatalf("BuildSnapshotDB empty: %v", err)
	}
	// No SSTable should exist; applied.base alone carries the index.
	if entries, _ := os.ReadDir(stagedDir); len(entries) == 0 {
		t.Fatalf("staged dir empty, expected applied.base")
	}
	dst, err := OpenWith(stagedDir, Options{SyncOnWrite: false, DisableBackgroundCompaction: true})
	if err != nil {
		t.Fatalf("open staged: %v", err)
	}
	defer dst.Close()
	if got := dst.RecoveredAppliedIndex(); got != 42 {
		t.Fatalf("empty snapshot RecoveredAppliedIndex = %d, want 42", got)
	}
}

func TestBuildSnapshotDBRejectsOutOfOrder(t *testing.T) {
	stagedDir := t.TempDir()
	kvs := []KV{{Key: []byte("b"), Value: []byte("1")}, {Key: []byte("a"), Value: []byte("2")}}
	if err := BuildSnapshotDB(stagedDir, kvs, 1, 1); err == nil {
		t.Fatalf("BuildSnapshotDB accepted out-of-order keys, want error")
	}
}

// --- tiny entry-encoding helpers for the apply path ---

type recordSpec struct {
	op  byte
	k   string
	v   string
	ts  uint64
	cmt bool
}

func put(k, v string) *recordSpec  { return &recordSpec{op: 1, k: k, v: v} }
func del(k string) *recordSpec     { return &recordSpec{op: 2, k: k} }
func commit(ts uint64) *recordSpec { return &recordSpec{cmt: true, ts: ts} }

func encodeEntry(recs ...*recordSpec) []byte {
	var ts uint64
	for _, r := range recs {
		if r.cmt {
			ts = r.ts
		}
	}
	var out []byte
	for _, r := range recs {
		if r.cmt {
			out = append(out, record.Encode(&record.Record{Op: record.OpCommit, Timestamp: r.ts})...)
			continue
		}
		out = append(out, record.Encode(&record.Record{
			Op: record.Op(r.op), Timestamp: ts, Key: []byte(r.k), Value: []byte(r.v),
		})...)
	}
	return out
}
