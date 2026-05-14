// Package wal implements a write-ahead log: an append-only file of records
// with crash recovery.
//
// Durability contract: when Append returns nil, the record is on disk.
// Every Append is followed by an fsync.
//
// Crash recovery: Open scans the log on startup. If a corrupt or truncated
// record is found anywhere in the file, the file is truncated to the offset
// of that bad record. After Open returns, the file contains only valid
// records and is positioned for append.
package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/BrandonnLow/littledb/internal/record"
)

const logFileName = "littledb.log"

// WAL is a write-ahead log. Safe for concurrent use.
type WAL struct {
	mu     sync.Mutex
	f      *os.File
	path   string
	size   int64 // current logical size of the log in bytes
	closed bool
}

// Open creates or opens a WAL in dir. If the log file exists and has a
// corrupt or truncated tail, the file is truncated to the last good record
// before Open returns. The returned WAL is ready for append.
func Open(dir string) (*WAL, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("wal: mkdir %q: %w", dir, err)
	}

	path := filepath.Join(dir, logFileName)

	// Stat before open so we can tell if we created the file.
	created := false
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		created = true
	} else if err != nil {
		return nil, fmt.Errorf("wal: stat %q: %w", path, err)
	}

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("wal: open %q: %w", path, err)
	}

	// If we just created the file, fsync the directory to make the new
	// directory entry durable. Without this, a crash right after creation
	// can leave the file's contents on disk but no entry pointing to it.
	if created {
		if err := syncDir(dir); err != nil {
			f.Close()
			return nil, fmt.Errorf("wal: sync dir after create: %w", err)
		}
	}

	w := &WAL{f: f, path: path}

	if err := w.recover(); err != nil {
		f.Close()
		return nil, err
	}

	// Position the file offset at the end so subsequent Writes append.
	if _, err := w.f.Seek(w.size, io.SeekStart); err != nil {
		f.Close()
		return nil, fmt.Errorf("wal: seek to end: %w", err)
	}

	return w, nil
}

// syncDir fsyncs the directory at dir. On Linux this guarantees the
// directory's entries (file creations, renames, deletions inside it)
// are durable.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// recover walks the file from offset 0. If it finds a truncated or
// corrupt record, it truncates the file to that offset and fsyncs.
// After recover returns nil, w.size is the size of the validated prefix.
func (w *WAL) recover() error {
	info, err := w.f.Stat()
	if err != nil {
		return fmt.Errorf("wal: stat during recovery: %w", err)
	}
	fileSize := info.Size()
	if fileSize == 0 {
		w.size = 0
		return nil
	}
	// Read the whole file into memory. Fine for now: logs are small
	// because we have no compaction yet. TODO: chunked reads
	// once logs can grow large.
	buf := make([]byte, fileSize)
	if _, err := w.f.ReadAt(buf, 0); err != nil {
		return fmt.Errorf("wal: read during recovery: %w", err)
	}

	offset := int64(0)
	for offset < fileSize {
		_, n, derr := record.Decode(buf[offset:])
		if derr == nil {
			offset += int64(n)
			continue
		}
		if errors.Is(derr, io.ErrUnexpectedEOF) || errors.Is(derr, record.ErrCorrupt) {
			// Bad tail. Truncate to the last good offset and fsync.
			if err := w.f.Truncate(offset); err != nil {
				return fmt.Errorf("wal: truncate at %d: %w", offset, err)
			}
			if err := w.f.Sync(); err != nil {
				return fmt.Errorf("wal: sync after truncate: %w", err)
			}
			w.size = offset
			return nil
		}

		// Any other error (ErrInvalidOp, etc.): refuse to start. The CRC
		// matched, so this isn't a torn write — it's either a bug or
		// real on-disk corruption that needs human attention.
		return fmt.Errorf("wal: corruption at offset %d: %w", offset, derr)

	}

	w.size = fileSize
	return nil
}

// Append writes one record to the log, fsyncs, and returns the offset
// at which the record was written. That offset is what the DB stores
// in its in-memory index.
func (w *WAL) Append(rec *record.Record) (int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return 0, errors.New("wal: append on closed wal")
	}

	encoded := record.Encode(rec)
	offset := w.size

	if _, err := w.f.Write(encoded); err != nil {
		return 0, fmt.Errorf("wal: write: %w", err)
	}
	if err := w.f.Sync(); err != nil {
		return 0, fmt.Errorf("wal: sync: %w", err)
	}

	w.size += int64(len(encoded))
	return offset, nil
}

// ReadAt reads and decodes the record at the given offset.
// Used by the DB layer to fetch values during Get.
func (w *WAL) ReadAt(offset int64) (*record.Record, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil, errors.New("wal: read on closed wal")
	}
	if offset < 0 || offset >= w.size {
		return nil, fmt.Errorf("wal: invalid offset %d (size %d)", offset, w.size)
	}

	// Read the header to learn the total record size, then read the body.
	hdr := make([]byte, record.HeaderSize)
	if _, err := w.f.ReadAt(hdr, offset); err != nil {
		return nil, fmt.Errorf("wal: read header at %d: %w", offset, err)
	}

	keyLen := binary.LittleEndian.Uint32(hdr[5:9])
	valueLen := binary.LittleEndian.Uint32(hdr[9:13])
	total := record.HeaderSize + int(keyLen) + int(valueLen)

	buf := make([]byte, total)
	copy(buf, hdr)
	if total > record.HeaderSize {
		if _, err := w.f.ReadAt(buf[record.HeaderSize:], offset+record.HeaderSize); err != nil {
			return nil, fmt.Errorf("wal: read body at %d: %w", offset, err)
		}
	}

	rec, _, err := record.Decode(buf)
	if err != nil {
		return nil, fmt.Errorf("wal: decode at %d: %w", offset, err)
	}
	return rec, nil
}

// Scan iterates over all records in the log from start to end, calling
// fn for each. Stops if fn returns a non-nil error and returns that error.
// Used by the DB at startup to rebuild the in-memory index.
//
// Scan does not perform recovery: Open has already truncated any bad tail.
// If Scan finds a decode error here, the log is corrupt in an unexpected
// way and the error is returned.
func (w *WAL) Scan(fn func(offset int64, rec *record.Record) error) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return errors.New("wal: scan on closed wal")
	}

	if w.size == 0 {
		return nil
	}

	buf := make([]byte, w.size)
	if _, err := w.f.ReadAt(buf, 0); err != nil {
		return fmt.Errorf("wal: read for scan: %w", err)
	}

	offset := int64(0)
	for offset < w.size {
		rec, n, err := record.Decode(buf[offset:])

		if err != nil {
			return fmt.Errorf("wal: decode during scan at %d: %w", offset, err)
		}
		if err := fn(offset, rec); err != nil {
			return err
		}
		offset += int64(n)
	}
	return nil
}

// Size returns the current size of the log file in bytes.
func (w *WAL) Size() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.size
}

// Close fsyncs and closes the underlying file. Safe to call more than once.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}

	w.closed = true
	err := w.f.Sync()
	if cerr := w.f.Close(); err == nil {
		err = cerr
	}
	return err
}
