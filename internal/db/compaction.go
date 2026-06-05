package db

import (
	"bytes"
	"container/heap"
	"errors"
	"fmt"
	"os"
	"path/filepath"

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

// compactSSTables merges inputs (newest-first) into one new SSTable
// at outputPath.
//
// Records carry MVCC timestamps in the ncoded key, and we DO NOT drop
// tombstones here. Tombstones at any timestamp may still be needed to
// mask older versions visible to active transactions. TODO: introduce a
// "low watermark" — the  oldest read timestamp held by any active transaction
// — and drop tombstones/versions older than that.
//
// Identical encoded keys (same userKey AND same timestamp) shouldn't
// occur in normal operation, but if they do across SSTables being
// merged we keep the first popped (newer source) and silently drop
// the rest. Letting sstable.Writer's ErrDuplicate bubble up here
// would crash compaction; a soft handling lets the system keep
// running and surface the bigger upstream bug later.
func compactSSTables(inputs []*sstable.Reader, outputPath string) error {
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

		// Decode the encoded key back into userKey + ts so the Writer
		// can re-encode internally. The double encode/decode is a
		// small cost; we accept it for API cleanliness (the Writer's
		// public interface takes userKey + ts, not encoded keys).
		userKey, ts, ok := mvcckey.Decode(e.encKey)
		if !ok {
			w.Abort()
			return fmt.Errorf("compaction: malformed encoded key (len=%d)", len(e.encKey))
		}
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

	// Output reuses max(inputIDs) so on-disk ID order matches recency
	// order. See the compaction ID/recency bug fix in DESIGN.md.
	outputID := inputIDs[0]
	db.mu.Unlock()

	outputPath := filepath.Join(db.dir, sstableFilename(outputID))

	if err := compactSSTables(inputs, outputPath); err != nil {
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
