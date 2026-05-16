// Package db is the top-level littledb storage engine.
//
// A DB is an append-only key-value store with an in-memory index and a
// write-ahead log. On Open, the WAL is recovered (any corrupt or truncated
// tail is truncated away) and the index is rebuilt by scanning the log.
//
// Concurrency: safe for many concurrent readers and one writer. Get takes
// a read lock only long enough to look up the disk offset; the disk read
// itself runs without holding the DB lock.
package db

import (
	"errors"
	"fmt"
	"sync"

	"github.com/BrandonnLow/littledb/internal/record"
	"github.com/BrandonnLow/littledb/internal/wal"
)

// ErrKeyNotFound is returned by Get and is the sentinel callers should
// test with errors.Is when distinguishing "missing" from other errors.
var ErrKeyNotFound = errors.New("db: key not found")

// Options configures a DB. The zero value is fine for most purposes:
// see DefaultOptions for the safe defaults.
type Options struct {
	SyncOnWrite bool
}

// DefaultOptions returns the safe defaults for a DB.
func DefaultOptions() Options {
	return Options{SyncOnWrite: true}
}

// DB is a key-value store backed by a write-ahead log.
type DB struct {
	mu     sync.RWMutex
	wal    *wal.WAL
	index  map[string]int64 // key (as string) -> offset of latest record in wal
	closed bool
}

// Open creates or opens a DB rooted at dir with default options.
// Equivalent to OpenWith(dir, DefaultOptions()).
func Open(dir string) (*DB, error) {
	return OpenWith(dir, DefaultOptions())
}

// OpenWith creates or opens a DB rooted at dir with the given options.
// It recovers the WAL and rebuilds the in-memory index by replaying the log.
func OpenWith(dir string, opts Options) (*DB, error) {
	w, err := wal.OpenWith(dir, wal.Options{SyncOnWrite: opts.SyncOnWrite})
	if err != nil {
		return nil, fmt.Errorf("db: open wal: %w", err)
	}

	db := &DB{
		wal:   w,
		index: make(map[string]int64),
	}

	// Replay the log to rebuild the index. Records are in write order, so
	// later writes naturally overwrite earlier ones in the map.
	err = w.Scan(func(offset int64, rec *record.Record) error {
		switch rec.Op {
		case record.OpPut:
			db.index[string(rec.Key)] = offset
		case record.OpDelete:
			delete(db.index, string(rec.Key))
		default:
			return fmt.Errorf("db: unknown op %d at offset %d", rec.Op, offset)
		}
		return nil
	})
	if err != nil {
		w.Close()
		return nil, fmt.Errorf("db: replay: %w", err)
	}
	return db, nil
}

// Put stores value under key. If key already exists, its value is replaced.
// On return, the write is durable: a process crash or power loss will not
// lose it.
func (db *DB) Put(key, value []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return errors.New("db: put on closed db")
	}

	rec := &record.Record{Op: record.OpPut, Key: key, Value: value}
	offset, err := db.wal.Append(rec)
	if err != nil {
		return fmt.Errorf("db: put: %w", err)
	}
	db.index[string(key)] = offset
	return nil
}

// Get returns the value stored under key, or ErrKeyNotFound if the key
// does not exist.
func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return nil, errors.New("db: get on closed db")
	}

	offset, ok := db.index[string(key)]
	db.mu.RUnlock()

	if !ok {
		return nil, ErrKeyNotFound
	}

	// Read the record from disk without holding the DB lock. The WAL has
	// its own lock for the underlying file, and the log is append-only,
	// so the offset stays valid for the lifetime of the DB.
	rec, err := db.wal.ReadAt(offset)
	if err != nil {
		return nil, fmt.Errorf("db: get: %w", err)
	}

	// Defensive: the record at this offset must be a Put. If it's not,
	// either the index is wrong or the WAL is corrupt. Either is a bug
	// that should surface, not be silently ignored.
	if rec.Op != record.OpPut {
		return nil, fmt.Errorf("db: get: expected put at offset %d, got op %d", offset, rec.Op)
	}
	return rec.Value, nil
}

// Delete removes key from the DB. Delete on a missing key is a no-op
// and returns nil.
func (db *DB) Delete(key []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return errors.New("db: delete on closed db")
	}

	if _, ok := db.index[string(key)]; !ok {
		// Key isn't here. Don't waste a tombstone — there's nothing to
		// undo on replay either.
		return nil
	}

	rec := &record.Record{Op: record.OpDelete, Key: key}
	if _, err := db.wal.Append(rec); err != nil {
		return fmt.Errorf("db: delete: %w", err)
	}
	delete(db.index, string(key))
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
	return db.wal.Close()

}
