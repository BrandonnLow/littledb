package db

import (
	"fmt"
	"path/filepath"

	"github.com/BrandonnLow/littledb/internal/record"
	"github.com/BrandonnLow/littledb/internal/sstable"
)

// KV is one key/value pair in a Raft snapshot. Keys arrive in ascending
// userKey order (the order SnapshotScan yields), one version per key.
type KV struct {
	Key   []byte
	Value []byte
}

// BuildSnapshotDB materializes a fresh, standalone DB at dir containing exactly
// kvs, every pair stamped at snapshotTS, with the applied Raft index recorded as
// lastIncludedIndex. It is the follower side of InstallSnapshot: the received
// logical state is written into a staging directory which is then swapped into
// place. dir must already exist and be empty.
//
// Reopening dir reconstructs RecoveredAppliedIndex() == lastIncludedIndex and
// (when kvs is non-empty) appliedTS == snapshotTS / nextTimestamp ==
// snapshotTS+1, because the single SSTable's footer carries both appliedIndex
// and maxTimestamp, and applied.base independently carries the index.
//
// kvs must be in strictly ascending userKey order (which SnapshotScan
// guarantees); out-of-order or duplicate keys are reported as an error rather
// than silently producing a corrupt table.
//
// Empty-keyspace edge: with no kvs there is no SSTable to carry maxTimestamp, so
// only applied.base is written. Reopen still recovers the index exactly, but
// appliedTS resets to 0 / nextTimestamp to 1. That is benign here — a follower
// never mints timestamps (only an elected leader does, via PrepareCommit), and
// the first applied entry after the install re-raises nextTimestamp through
// ApplyEntry's max. A snapshot with zero live keys (everything deleted as of
// snapshotTS) is the only way to reach this.
func BuildSnapshotDB(dir string, kvs []KV, lastIncludedIndex, snapshotTS uint64) error {
	if len(kvs) > 0 {
		path := filepath.Join(dir, sstableFilename(1))
		w, err := sstable.NewWriter(path, len(kvs), lastIncludedIndex)
		if err != nil {
			return fmt.Errorf("db: build snapshot: new sstable: %w", err)
		}
		for _, kv := range kvs {
			if err := w.Add(record.OpPut, kv.Key, kv.Value, snapshotTS); err != nil {
				w.Abort()
				return fmt.Errorf("db: build snapshot: add %q: %w", kv.Key, err)
			}
		}
		if err := w.Finish(); err != nil {
			return fmt.Errorf("db: build snapshot: finish sstable: %w", err)
		}
	}

	// applied.base carries the index even when there is no SSTable, and matches
	// the footer otherwise, so recovery's max(footer, base+walCount) resolves to
	// lastIncludedIndex in every case.
	if err := writeAppliedBase(dir, lastIncludedIndex); err != nil {
		return fmt.Errorf("db: build snapshot: applied base: %w", err)
	}
	return nil
}
