package db

import (
	"bytes"
	"container/heap"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"

	"github.com/BrandonnLow/littledb/internal/mvcckey"
	"github.com/BrandonnLow/littledb/internal/record"
	"github.com/BrandonnLow/littledb/internal/sstable"
)

// mergeEntry is one record from one of the input SSTables on the
// merge heap. encKey is the MVCC-encoded key (userKey + ^timestamp),
// which is what defines sort order.
type mergeEntry struct {
	encKey    []byte
	value     []byte
	op        record.Op
	sourceIdx int // lower = newer source
}

// mergeHeap orders by encKey ascending, ties broken by sourceIdx
// ascending (newer source wins).
type mergeHeap []mergeEntry

func (h mergeHeap) Len() int { return len(h) }
func (h mergeHeap) Less(i, j int) bool {
	if c := bytes.Compare(h[i].encKey, h[j].encKey); c != 0 {
		return c < 0
	}
	return h[i].sourceIdx < h[j].sourceIdx
}
func (h mergeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *mergeHeap) Push(x any)   { *h = append(*h, x.(mergeEntry)) }
func (h *mergeHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// compactSSTables // compactSSTables merges inputs (newest-first) into one new SSTable
// at outputPath.
//
// Version GC is now performed.
//
// Watermark = min(oldest active txn's readSnap, nextTimestamp - 1).
// A version V@T for userKey K is observable only by snapshots in
// [T, T_next), where T_next is the timestamp of K's next-newer
// version (or +∞ for K's latest). For V to be reachable by some
// active or future snapshot, T_next must exceed the watermark.
// So during the merge, walking newest-first per userKey:
//   - The first (newest) version of each userKey is always emitted,
//     UNLESS it's a tombstone with T ≤ watermark AND we're at the
//     bottom of the LSM (no older versions exist below us). In that
//     case the entire userKey vanishes — the tombstone is no longer
//     needed because no observable snapshot would distinguish "key
//     deleted" from "key never existed."
//   - Older versions are emitted only if their next-newer version's
//     ts > watermark (= there's still a snapshot in the gap).
//
// bottomOfLSM is true when this compaction's output occupies the
// oldest slot in the LSM — no older SSTables exist below it that
// might contain versions the tombstone is masking. Our size-tiered
// compaction always satisfies this (we merge the oldest N tail).
//
// Identical encoded keys across inputs are soft-deduped (the
// first popped wins).
func compactSSTables(inputs []*sstable.Reader, outputPath string, watermark uint64, bottomOfLSM bool) error {
	sources := make([][]mergeEntry, len(inputs))
	totalRecords := 0
	for i, r := range inputs {
		var src []mergeEntry
		err := r.Iterate(func(op record.Op, encKey, value []byte) bool {
			src = append(src, mergeEntry{
				encKey:    append([]byte(nil), encKey...),
				value:     append([]byte(nil), value...),
				op:        op,
				sourceIdx: i,
			})
			return true
		})
		if err != nil {
			return fmt.Errorf("compaction: read source %d: %w", i, err)
		}
		sources[i] = src
		totalRecords += len(src)
	}

	positions := make([]int, len(sources))
	h := &mergeHeap{}
	for i, src := range sources {
		if len(src) > 0 {
			heap.Push(h, src[0])
			positions[i] = 1
		}
	}

	w, err := sstable.NewWriter(outputPath, totalRecords)
	if err != nil {
		return err
	}

	var lastEncKey []byte
	var lastValid bool

	// Per-userKey state for version GC.
	var currentUserKey []byte
	var prevTS uint64

	for h.Len() > 0 {
		e := heap.Pop(h).(mergeEntry)

		if positions[e.sourceIdx] < len(sources[e.sourceIdx]) {
			heap.Push(h, sources[e.sourceIdx][positions[e.sourceIdx]])
			positions[e.sourceIdx]++
		}

		// Dedup: same encoded key (= same userKey + same timestamp)
		// means a duplicate version. Keep the first popped (newer
		// source) and skip subsequent matches.
		if lastValid && bytes.Equal(e.encKey, lastEncKey) {
			continue
		}
		lastEncKey = e.encKey
		lastValid = true

		userKey, ts, ok := mvcckey.Decode(e.encKey)
		if !ok {
			w.Abort()
			return fmt.Errorf("compaction: malformed encoded key (len=%d)", len(e.encKey))
		}

		// Detect transition to a new userKey group.
		firstOfUserKey := currentUserKey == nil || !bytes.Equal(userKey, currentUserKey)
		if firstOfUserKey {
			currentUserKey = append(currentUserKey[:0], userKey...)
		}

		if firstOfUserKey {
			// Newest version of this userKey. Tombstone GC: at the
			// bottom of the LSM, a tombstone with ts ≤ watermark
			// can't be observed by any current or future snapshot,
			// and there are no older versions below to expose.
			// Drop it and let the cascade below drop older versions.
			if bottomOfLSM && e.op == record.OpDelete && ts <= watermark {
				prevTS = ts // ≤ watermark, so older versions drop too
				continue
			}
		} else {
			// Older version. Observable only if prevTS > watermark.
			// Only safe at the bottom of the LSM: dropping an older
			// version mid-layer would expose even-older versions in
			// lower SSTables that this one was shadowing for reads
			// in [ts, prevTS).
			if bottomOfLSM && prevTS <= watermark {
				continue
			}
		}

		prevTS = ts

		if err := w.Add(e.op, userKey, e.value, ts); err != nil {
			w.Abort()
			return fmt.Errorf("compaction: write: %w", err)
		}
	}

	return w.Finish()
}

func (db *DB) compactLoop() {
	defer close(db.compactDoneCh)
	for range db.compactCh {
		for {
			ran, err := db.tryCompactOnce()
			if err != nil {
				break
			}
			if !ran {
				break
			}
		}
	}
}

func (db *DB) CompactForTesting() error {
	for {
		ran, err := db.tryCompactOnce()
		if err != nil {
			return err
		}
		if !ran {
			return nil
		}
	}
}

func (db *DB) tryCompactOnce() (bool, error) {
	db.compactMu.Lock()
	defer db.compactMu.Unlock()

	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return false, nil
	}
	n := db.opts.CompactionTrigger
	if len(db.sstables) < n {
		db.mu.Unlock()
		return false, nil
	}

	start := len(db.sstables) - n
	inputs := make([]*sstable.Reader, n)
	inputIDs := make([]int, n)
	copy(inputs, db.sstables[start:])
	copy(inputIDs, db.sstableIDs[start:])

	// Determine if this compaction's output reaches the bottom of the
	// LSM — i.e., our inputs include the SSTable with the smallest ID
	// (the oldest one), so no older SSTable will sit below our output.
	// Derived from the actual input set rather than from a structural
	// assumption about how `inputs` was selected; correct regardless
	// of compaction strategy.
	bottomOfLSM := slices.Min(inputIDs) == slices.Min(db.sstableIDs)

	outputID := inputIDs[0]
	db.mu.Unlock()

	// Compute watermark outside the write lock (takes RLock + the
	// activeTxnsMu briefly).
	watermark := db.computeWatermark()

	outputPath := filepath.Join(db.dir, sstableFilename(outputID))

	if err := compactSSTables(inputs, outputPath, watermark, bottomOfLSM); err != nil {
		os.Remove(outputPath)
		os.Remove(outputPath + ".tmp")
		return false, err
	}

	r, err := sstable.OpenReader(outputPath)
	if err != nil {
		os.Remove(outputPath)
		return false, err
	}

	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		r.Close()
		os.Remove(outputPath)
		return false, nil
	}

	tailStart := len(db.sstables) - len(inputIDs)
	if tailStart < 0 {
		db.mu.Unlock()
		r.Close()
		os.Remove(outputPath)
		return false, errors.New("compaction: sstables shrank unexpectedly")
	}
	for i := 0; i < len(inputIDs); i++ {
		if db.sstableIDs[tailStart+i] != inputIDs[i] {
			db.mu.Unlock()
			r.Close()
			os.Remove(outputPath)
			return false, errors.New("compaction: tail IDs changed unexpectedly")
		}
	}

	newSSTables := make([]*sstable.Reader, 0, tailStart+1)
	newSSTables = append(newSSTables, db.sstables[:tailStart]...)
	newSSTables = append(newSSTables, r)
	newIDs := make([]int, 0, tailStart+1)
	newIDs = append(newIDs, db.sstableIDs[:tailStart]...)
	newIDs = append(newIDs, outputID)

	db.sstables = newSSTables
	db.sstableIDs = newIDs
	db.mu.Unlock()

	for _, id := range inputIDs {
		if id == outputID {
			continue
		}
		os.Remove(filepath.Join(db.dir, sstableFilename(id)))
	}

	return true, nil
}
