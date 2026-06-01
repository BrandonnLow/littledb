package db

import (
	"bytes"
	"container/heap"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/BrandonnLow/littledb/internal/record"
	"github.com/BrandonnLow/littledb/internal/sstable"
)

// mergeEntry is one record from one of the input SSTables, queued on
// the merge heap.
type mergeEntry struct {
	key       []byte
	value     []byte
	op        record.Op
	sourceIdx int // index in the inputs slice; lower = newer
}

// mergeHeap is a min-heap ordering by (key ascending, sourceIdx
// ascending). Since inputs are passed newest-first, the smallest
// sourceIdx wins on duplicate keys.
type mergeHeap []mergeEntry

func (h mergeHeap) Len() int { return len(h) }
func (h mergeHeap) Less(i, j int) bool {
	if c := bytes.Compare(h[i].key, h[j].key); c != 0 {
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

// compactSSTables merges inputs (newest-first) into one new SSTable at
// outputPath. If dropTombstones is true, OpDelete records produce no
// output entry; otherwise they're kept so older overlapping SSTables
// can still be masked.
//
// Memory: this implementation buffers every record from every input
// before merging. Fine for the SSTable sizes. A streaming version
// using pull-based iterators would be the production approach.
func compactSSTables(inputs []*sstable.Reader, outputPath string, dropTombstones bool) error {
	sources := make([][]mergeEntry, len(inputs))
	totalRecords := 0
	for i, r := range inputs {
		var src []mergeEntry
		err := r.Iterate(func(op record.Op, k, v []byte) bool {
			src = append(src, mergeEntry{
				key:       append([]byte(nil), k...),
				value:     append([]byte(nil), v...),
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

	var lastKey []byte
	var lastKeyValid bool

	for h.Len() > 0 {
		e := heap.Pop(h).(mergeEntry)

		if positions[e.sourceIdx] < len(sources[e.sourceIdx]) {
			heap.Push(h, sources[e.sourceIdx][positions[e.sourceIdx]])
			positions[e.sourceIdx]++
		}

		if lastKeyValid && bytes.Equal(e.key, lastKey) {
			continue
		}
		lastKey = e.key
		lastKeyValid = true

		if dropTombstones && e.op == record.OpDelete {
			continue
		}

		if err := w.Add(e.op, e.key, e.value); err != nil {
			w.Abort()
			return fmt.Errorf("compaction: write: %w", err)
		}
	}

	return w.Finish()
}

// compactLoop runs in a goroutine. Reads signals from db.compactCh and
// runs compactions until no more work is pending. Exits when the
// channel is closed.
func (db *DB) compactLoop() {
	defer close(db.compactDoneCh)
	for range db.compactCh {
		for {
			ran, err := db.tryCompactOnce()
			if err != nil {
				// Background path swallows errors silently. Tests use
				// CompactForTesting which returns errors directly.
				break
			}
			if !ran {
				break
			}
		}
	}
}

// CompactForTesting runs compactions until no more work is pending,
// returning the first error encountered.
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

// tryCompactOnce attempts one compaction cycle. Returns (true, nil) if
// it ran a compaction, (false, nil) if there was no work to do, and
// (false, err) on any failure.
//
// compactMu serializes calls so the background goroutine and
// CompactForTesting don't step on each other.
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

	// inputs is newest-first within the oldest-N tail: inputs[0] is the
	// newest of the four, inputs[n-1] is the absolute oldest. This
	// matches mergeHeap's "smallest sourceIdx wins on duplicates".
	//
	// The merged file inherits the MAX input ID, not a fresh one from
	// nextID. Why: filename order must match recency order so that
	// reopening (which sorts filenames by ID ascending and treats the
	// largest as newest) agrees with the in-memory ordering (where the
	// merged file sits at the OLDER end of the slice). If we used a
	// fresh higher ID, the on-disk recency would flip on reopen and
	// stale data would shadow newer SSTables. The reused ID belongs to
	// one of the inputs; sstable.Writer renames its .tmp file over that
	// input, which on Linux atomically detaches the old inode (any
	// still-open reader keeps a valid FD until GC). The delete loop
	// below skips that reused ID.
	outputID := inputIDs[0] // max because inputs are newest-first
	db.mu.Unlock()

	outputPath := filepath.Join(db.dir, sstableFilename(outputID))

	// Slow merge happens without the DB lock so reads continue normally.
	// Since we're compacting the oldest tail, no older SSTable exists
	// for tombstones to mask — drop them.
	if err := compactSSTables(inputs, outputPath, true); err != nil {
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

	// Unlink the input files. We do NOT explicitly close the input
	// Readers — in-flight Gets that captured the old slice may still
	// be reading from them. The unlink removes the directory entry
	// immediately; the underlying file descriptors will be closed by
	// GC finalizers on *os.File once no references remain.
	//
	// This is a simplification. Production systems use
	// refcounting per reader.
	for _, id := range inputIDs {
		if id == outputID {
			continue
		}
		os.Remove(filepath.Join(db.dir, sstableFilename(id)))
	}

	return true, nil
}
