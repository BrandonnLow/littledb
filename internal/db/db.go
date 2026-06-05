// Package db is the top-level littledb storage engine.
//
// MVCC reads. Each Put/Delete is assigned a logical timestamp from a monotonic counter;
// multiple versions of a userKey coexist; reads ask for "the version as of snapshot T."
//
// The single-version Get(k) internally reads as-of the current write-counter value,
// so it sees the latest committed version of k. Transactions will
// introduce explicit Begin() returning a Txn that captures its own
// snapshot timestamp.
package db

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"sync"

	"github.com/BrandonnLow/littledb/internal/memtable"
	"github.com/BrandonnLow/littledb/internal/record"
	"github.com/BrandonnLow/littledb/internal/sstable"
	"github.com/BrandonnLow/littledb/internal/wal"
)

const (
	walFilename              = "littledb.log"
	defaultMemtableSizeMax   = 4 * 1024 * 1024
	defaultCompactionTrigger = 4
)

var (
	ErrKeyNotFound = errors.New("db: key not found")
	errClosed      = errors.New("db: closed")
	sstableNameRE  = regexp.MustCompile(`^(\d{6})\.sst$`)
)

type Options struct {
	SyncOnWrite                 bool
	MemtableSizeMax             int64
	CompactionTrigger           int
	DisableBackgroundCompaction bool
}

func DefaultOptions() Options {
	return Options{
		SyncOnWrite:       true,
		MemtableSizeMax:   defaultMemtableSizeMax,
		CompactionTrigger: defaultCompactionTrigger,
	}
}

// DB is an MVCC LSM-tree key-value store.
type DB struct {
	mu         sync.RWMutex
	dir        string
	opts       Options
	wal        *wal.WAL
	memtable   *memtable.Memtable
	frozen     *memtable.Memtable
	sstables   []*sstable.Reader
	sstableIDs []int
	nextID     int

	// nextTimestamp is the next logical timestamp to assign on a write.
	// Reads use its current value as their snapshot. Bumped on every
	// Put/Delete while holding the write lock; readers capture it
	// under the read lock alongside memtable/sstable pointers, which
	// ensures a Get can never see a counter value > any
	// not-yet-applied write.
	nextTimestamp uint64

	closed bool

	compactMu     sync.Mutex
	compactCh     chan struct{}
	compactDoneCh chan struct{}
}

func Open(dir string) (*DB, error) { return OpenWith(dir, DefaultOptions()) }

func OpenWith(dir string, opts Options) (*DB, error) {
	if opts.MemtableSizeMax <= 0 {
		opts.MemtableSizeMax = defaultMemtableSizeMax
	}
	if opts.CompactionTrigger < 2 {
		opts.CompactionTrigger = defaultCompactionTrigger
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("db: mkdir: %w", err)
	}

	sstIDs, err := discoverSSTables(dir)
	if err != nil {
		return nil, fmt.Errorf("db: discover sstables: %w", err)
	}

	var ssts []*sstable.Reader
	var sstIDsRev []int
	var maxTS uint64
	for i := len(sstIDs) - 1; i >= 0; i-- {
		id := sstIDs[i]
		path := filepath.Join(dir, sstableFilename(id))
		r, err := sstable.OpenReader(path)
		if err != nil {
			for _, opened := range ssts {
				opened.Close()
			}
			return nil, fmt.Errorf("db: open sstable %s: %w", path, err)
		}
		ssts = append(ssts, r)
		sstIDsRev = append(sstIDsRev, id)
		if r.MaxTimestamp() > maxTS {
			maxTS = r.MaxTimestamp()
		}
	}

	w, err := wal.OpenWith(dir, wal.Options{SyncOnWrite: opts.SyncOnWrite})
	if err != nil {
		for _, r := range ssts {
			r.Close()
		}
		return nil, err
	}

	mt := memtable.New()
	err = w.Scan(func(offset int64, rec *record.Record) error {
		if rec.Timestamp > maxTS {
			maxTS = rec.Timestamp
		}
		switch rec.Op {
		case record.OpPut:
			return mt.Put(rec.Key, rec.Value, rec.Timestamp)
		case record.OpDelete:
			return mt.Delete(rec.Key, rec.Timestamp)
		default:
			return fmt.Errorf("db: unknown op %d in wal", rec.Op)
		}
	})
	if err != nil {
		w.Close()
		for _, r := range ssts {
			r.Close()
		}
		return nil, fmt.Errorf("db: replay wal: %w", err)
	}

	nextID := 1
	if len(sstIDs) > 0 {
		nextID = sstIDs[len(sstIDs)-1] + 1
	}

	db := &DB{
		dir:           dir,
		opts:          opts,
		wal:           w,
		memtable:      mt,
		sstables:      ssts,
		sstableIDs:    sstIDsRev,
		nextID:        nextID,
		nextTimestamp: maxTS + 1, // first write gets at least 1
		compactCh:     make(chan struct{}, 1),
		compactDoneCh: make(chan struct{}),
	}

	if !opts.DisableBackgroundCompaction {
		go db.compactLoop()
		db.signalCompact()
	} else {
		close(db.compactDoneCh)
	}

	return db, nil
}

// Put writes (key, value) at a freshly-allocated timestamp.
func (db *DB) Put(key, value []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return errClosed
	}

	// Allocate the timestamp BEFORE the WAL append, and inside the
	// write lock. This is the ordering invariant: any reader that sees
	// counter value T has also seen every Put/Delete with ts < T
	// applied to the memtable, because both happened under the same
	// lock acquisition.
	ts := db.nextTimestamp
	db.nextTimestamp++

	rec := &record.Record{Op: record.OpPut, Timestamp: ts, Key: key, Value: value}
	if _, err := db.wal.Append(rec); err != nil {
		return fmt.Errorf("db: put wal: %w", err)
	}
	if err := db.memtable.Put(key, value, ts); err != nil {
		return fmt.Errorf("db: put memtable: %w", err)
	}

	if db.memtable.ApproximateSize() >= db.opts.MemtableSizeMax {
		if err := db.flushLocked(); err != nil {
			return fmt.Errorf("db: flush after put: %w", err)
		}
		db.signalCompact()
	}
	return nil
}

// Delete writes a tombstone at a freshly-allocated timestamp.
func (db *DB) Delete(key []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return errClosed
	}

	ts := db.nextTimestamp
	db.nextTimestamp++

	rec := &record.Record{Op: record.OpDelete, Timestamp: ts, Key: key}
	if _, err := db.wal.Append(rec); err != nil {
		return fmt.Errorf("db: delete wal: %w", err)
	}
	if err := db.memtable.Delete(key, ts); err != nil {
		return fmt.Errorf("db: delete memtable: %w", err)
	}

	if db.memtable.ApproximateSize() >= db.opts.MemtableSizeMax {
		if err := db.flushLocked(); err != nil {
			return fmt.Errorf("db: flush after delete: %w", err)
		}
		db.signalCompact()
	}
	return nil
}

// Get returns the latest committed version of key. Equivalent to
// GetAsOf(key, current write counter).
func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return nil, errClosed
	}
	// Capture counter + state under the lock atomically.
	snapshot := db.nextTimestamp
	activeMT := db.memtable
	frozenMT := db.frozen
	ssts := db.sstables
	db.mu.RUnlock()

	return db.getAsOfSnapshot(key, snapshot, activeMT, frozenMT, ssts)
}

// GetAsOf returns the version of key visible at snapshot. Exposed for
// future transaction support and for testing MVCC semantics directly.
func (db *DB) GetAsOf(key []byte, snapshot uint64) ([]byte, error) {
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return nil, errClosed
	}
	activeMT := db.memtable
	frozenMT := db.frozen
	ssts := db.sstables
	db.mu.RUnlock()

	return db.getAsOfSnapshot(key, snapshot, activeMT, frozenMT, ssts)
}

func (db *DB) getAsOfSnapshot(
	key []byte,
	snapshot uint64,
	activeMT, frozenMT *memtable.Memtable,
	ssts []*sstable.Reader,
) ([]byte, error) {
	if v, op, found := activeMT.GetAsOf(key, snapshot); found {
		if op == memtable.OpDelete {
			return nil, ErrKeyNotFound
		}
		return v, nil
	}
	if frozenMT != nil {
		if v, op, found := frozenMT.GetAsOf(key, snapshot); found {
			if op == memtable.OpDelete {
				return nil, ErrKeyNotFound
			}
			return v, nil
		}
	}
	for _, r := range ssts {
		v, op, found, err := r.GetAsOf(key, snapshot)
		if err != nil {
			return nil, fmt.Errorf("db: get sstable: %w", err)
		}
		if found {
			if op == record.OpDelete {
				return nil, ErrKeyNotFound
			}
			return v, nil
		}
	}
	return nil, ErrKeyNotFound
}

func (db *DB) flushLocked() error {
	if db.memtable.Len() == 0 {
		return nil
	}

	db.memtable.Freeze()
	db.frozen = db.memtable
	db.memtable = memtable.New()

	id := db.nextID
	path := filepath.Join(db.dir, sstableFilename(id))

	w, err := sstable.NewWriter(path, db.frozen.Len())
	if err != nil {
		return err
	}

	var iterErr error
	db.frozen.Iterate(func(userKey, value []byte, op memtable.Op, ts uint64) bool {
		// memtable.Op and record.Op share byte values; the cast is safe.
		if err := w.Add(record.Op(op), userKey, value, ts); err != nil {
			iterErr = err
			return false
		}
		return true
	})
	if iterErr != nil {
		w.Abort()
		return iterErr
	}
	if err := w.Finish(); err != nil {
		return err
	}

	// At this point the SSTable (including its MaxTimestamp footer) is
	// fully durable on disk via Finish's fsync + rename + syncDir.
	// Only NOW is it safe to touch the WAL — if a crash had happened
	// before Finish returned, the WAL would still hold the records
	// and a subsequent Open would re-derive them.

	r, err := sstable.OpenReader(path)
	if err != nil {
		return err
	}

	if err := db.wal.Close(); err != nil {
		r.Close()
		return err
	}
	walPath := filepath.Join(db.dir, walFilename)
	if err := os.Remove(walPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		r.Close()
		return err
	}
	newWAL, err := wal.OpenWith(db.dir, wal.Options{SyncOnWrite: db.opts.SyncOnWrite})
	if err != nil {
		r.Close()
		return err
	}

	db.wal = newWAL
	db.sstables = append([]*sstable.Reader{r}, db.sstables...)
	db.sstableIDs = append([]int{id}, db.sstableIDs...)
	db.frozen = nil
	db.nextID++
	return nil
}

func (db *DB) signalCompact() {
	if db.closed || db.opts.DisableBackgroundCompaction {
		return
	}
	select {
	case db.compactCh <- struct{}{}:
	default:
	}
}

func (db *DB) Close() error {
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return nil
	}
	db.closed = true
	db.mu.Unlock()

	close(db.compactCh)
	<-db.compactDoneCh

	db.mu.Lock()
	defer db.mu.Unlock()
	var firstErr error
	if err := db.wal.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	for _, r := range db.sstables {
		if err := r.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (db *DB) NumSSTablesForTesting() int {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return len(db.sstables)
}

func (db *DB) FlushForTesting() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return errClosed
	}
	if err := db.flushLocked(); err != nil {
		return err
	}
	db.signalCompact()
	return nil
}

// NextTimestampForTesting returns the current value of the timestamp
// counter (next-to-assign). Used by tests to capture a snapshot point
// between writes.
func (db *DB) NextTimestampForTesting() uint64 {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.nextTimestamp
}

func sstableFilename(id int) string { return fmt.Sprintf("%06d.sst", id) }

func discoverSSTables(dir string) ([]int, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var ids []int
	for _, e := range entries {
		m := sstableNameRE.FindStringSubmatch(e.Name())
		if m == nil {
			continue
		}
		id, _ := strconv.Atoi(m[1])
		ids = append(ids, id)
	}
	sort.Ints(ids)
	return ids, nil
}
