// Package db is the top-level littledb storage engine.
//
// Architecture: an LSM tree. Writes go through a write-ahead log
// for durability, then into an in-memory memtable. When the memtable
// crosses a size threshold it's frozen and flushed to an immutable
// SSTable file; reads consult the memtable first, then SSTables newest
// to oldest. The first hit wins.
//
// Public API: Open / Put / Get / Delete / Close
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
	walFilename            = "littledb.log"
	defaultMemtableSizeMax = 4 * 1024 * 1024 // 4 MB
)

var (
	// ErrKeyNotFound is returned by Get when the key is absent or has
	// been deleted.
	ErrKeyNotFound = errors.New("db: key not found")

	errClosed = errors.New("db: closed")

	sstableNameRE = regexp.MustCompile(`^(\d{6})\.sst$`)
)

// Options configures a DB.
type Options struct {
	// SyncOnWrite controls whether each Put/Delete fsyncs the WAL.
	// Default true.
	SyncOnWrite bool

	// MemtableSizeMax is the approximate byte threshold at which the
	// memtable is frozen and flushed. Default 4 MB.
	MemtableSizeMax int64
}

// DefaultOptions returns the safe defaults.
func DefaultOptions() Options {
	return Options{SyncOnWrite: true, MemtableSizeMax: defaultMemtableSizeMax}
}

// DB is an LSM-tree key-value store.
type DB struct {
	mu       sync.RWMutex
	dir      string
	opts     Options
	wal      *wal.WAL
	memtable *memtable.Memtable
	frozen   *memtable.Memtable // non-nil only during a flush
	sstables []*sstable.Reader  // newest first
	nextID   int
	closed   bool
}

// Open creates or opens a DB rooted at dir with default options.
func Open(dir string) (*DB, error) {
	return OpenWith(dir, DefaultOptions())
}

// OpenWith creates or opens a DB rooted at dir with the given options.
func OpenWith(dir string, opts Options) (*DB, error) {
	if opts.MemtableSizeMax <= 0 {
		opts.MemtableSizeMax = defaultMemtableSizeMax
	}

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("db: mkdir: %w", err)
	}

	sstIDs, err := discoverSSTables(dir)
	if err != nil {
		return nil, fmt.Errorf("db: discover sstables: %w", err)
	}

	var ssts []*sstable.Reader
	for i := len(sstIDs) - 1; i >= 0; i-- {
		path := filepath.Join(dir, sstableFilename(sstIDs[i]))
		r, err := sstable.OpenReader(path)
		if err != nil {
			for _, opened := range ssts {
				opened.Close()
			}
			return nil, fmt.Errorf("db: open sstable %s: %w", path, err)
		}
		ssts = append(ssts, r)
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
		switch rec.Op {
		case record.OpPut:
			return mt.Put(rec.Key, rec.Value)
		case record.OpDelete:
			return mt.Delete(rec.Key)
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

	return &DB{
		dir:      dir,
		opts:     opts,
		wal:      w,
		memtable: mt,
		sstables: ssts,
		nextID:   nextID,
	}, nil
}

// Put writes a key/value pair durably.
func (db *DB) Put(key, value []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return errClosed
	}

	rec := &record.Record{Op: record.OpPut, Key: key, Value: value}
	if _, err := db.wal.Append(rec); err != nil {
		return fmt.Errorf("db: put wal: %w", err)
	}
	if err := db.memtable.Put(key, value); err != nil {
		return fmt.Errorf("db: put memtable: %w", err)
	}

	if db.memtable.ApproximateSize() >= db.opts.MemtableSizeMax {
		if err := db.flushLocked(); err != nil {
			return fmt.Errorf("db: flush after put: %w", err)
		}
	}
	return nil
}

// Delete writes a tombstone for key. Idempotent.
func (db *DB) Delete(key []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return errClosed
	}

	rec := &record.Record{Op: record.OpDelete, Key: key}
	if _, err := db.wal.Append(rec); err != nil {
		return fmt.Errorf("db: delete wal: %w", err)
	}
	if err := db.memtable.Delete(key); err != nil {
		return fmt.Errorf("db: delete memtable: %w", err)
	}

	if db.memtable.ApproximateSize() >= db.opts.MemtableSizeMax {
		if err := db.flushLocked(); err != nil {
			return fmt.Errorf("db: flush after delete: %w", err)
		}
	}
	return nil
}

// Get returns the value for key. Searches the active memtable, the
// frozen memtable (if any), then SSTables newest to oldest. The first
// hit wins; a tombstone hit returns ErrKeyNotFound.
func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return nil, errClosed
	}
	activeMT := db.memtable
	frozenMT := db.frozen
	ssts := db.sstables
	db.mu.RUnlock()

	if v, op, found := activeMT.Get(key); found {
		if op == memtable.OpDelete {
			return nil, ErrKeyNotFound
		}
		return v, nil
	}

	if frozenMT != nil {
		if v, op, found := frozenMT.Get(key); found {
			if op == memtable.OpDelete {
				return nil, ErrKeyNotFound
			}
			return v, nil
		}
	}

	for _, r := range ssts {
		v, op, found, err := r.Get(key)
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

// flushLocked freezes the active memtable, writes it as an SSTable,
// truncates the WAL, and commits. Must be called with db.mu held.
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
	db.frozen.Iterate(func(k, v []byte, op memtable.Op) bool {
		// memtable.Op and record.Op share byte values (1=Put, 2=Delete).
		if err := w.Add(record.Op(op), k, v); err != nil {
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

	r, err := sstable.OpenReader(path)
	if err != nil {
		return err
	}

	// Truncate the WAL: close, remove, reopen. We hold the DB lock so
	// no Append can race with this.
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

	// Commit. Allocate a new slice so any concurrent Get holding the
	// old slice header sees a consistent snapshot.
	db.wal = newWAL
	db.sstables = append([]*sstable.Reader{r}, db.sstables...)
	db.frozen = nil
	db.nextID++
	return nil
}

// Close flushes and closes the DB. Safe to call more than once.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return nil
	}
	db.closed = true

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

// NumSSTablesForTesting returns the number of open SSTable readers.
func (db *DB) NumSSTablesForTesting() int {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return len(db.sstables)
}

// FlushForTesting forces a flush of the current memtable, regardless
// of size. Used by tests to exercise the flush path deterministically.
func (db *DB) FlushForTesting() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return errClosed
	}
	return db.flushLocked()
}

func sstableFilename(id int) string {
	return fmt.Sprintf("%06d.sst", id)
}

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
