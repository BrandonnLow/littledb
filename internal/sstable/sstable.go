// Package sstable implements immutable sorted-string tables on disk.
//
// An SSTable is the durable form of a flushed memtable: records in
// strictly ascending key order, written once, never modified. Each
// record uses the same binary format as the WAL (see the record
// package): a CRC32 header followed by op, key length, value length,
// key bytes, and value bytes.
//
// Reads do a linear scan of the file.
// TODO: add a sparse index and footer so reads can binary-search
// to the right block instead of scanning the whole file.
//
// SSTables are created atomically. The Writer writes to "<path>.tmp",
// fsyncs, then renames to <path>. Readers never observe a partial file.
package sstable

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/BrandonnLow/littledb/internal/record"
)

var (
	// ErrOutOfOrder is returned by Writer.Add if the caller provides
	// keys that are not strictly ascending.
	ErrOutOfOrder = errors.New("sstable: keys out of order")
	// ErrDuplicate is returned by Writer.Add when the same key appears twice.
	ErrDuplicate = errors.New("sstable: duplicate key")
)

// Writer builds an SSTable file. Keys must be Added in strictly
// ascending order. Finish makes the file visible at its final path;
// Abort removes the temp file without publishing it.
type Writer struct {
	path    string // final path, e.g. "data/000001.sst"
	tmpPath string // working path, "<path>.tmp"
	dir     string // directory of path, for fsync after rename
	f       *os.File
	bw      *bufio.Writer

	lastKey []byte // most recent Added key, for order check
	count   int    // records written

	closed bool
}

// NewWriter creates a writer that will eventually produce an SSTable
// at path. The directory containing path must already exist.
func NewWriter(path string) (*Writer, error) {
	dir := filepath.Dir(path)
	tmpPath := path + ".tmp"

	f, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, fmt.Errorf("sstable: open %s: %w", tmpPath, err)
	}
	return &Writer{
		path:    path,
		tmpPath: tmpPath,
		dir:     dir,
		f:       f,
		bw:      bufio.NewWriter(f),
	}, nil
}

// Add appends one record. Keys must be strictly ascending; duplicates
// and out-of-order keys return an error and leave the Writer unusable
// (callers should Abort).
func (w *Writer) Add(op record.Op, key, value []byte) error {
	if w.closed {
		return errors.New("sstable: write on closed writer")
	}
	if w.lastKey != nil {
		switch cmp := bytesCompare(key, w.lastKey); {
		case cmp == 0:
			return ErrDuplicate
		case cmp < 0:
			return ErrOutOfOrder
		}
	}

	rec := &record.Record{Op: op, Key: key, Value: value}
	encoded := record.Encode(rec)
	if _, err := w.bw.Write(encoded); err != nil {
		return fmt.Errorf("sstable: write record: %w", err)
	}

	w.lastKey = append(w.lastKey[:0], key...)
	w.count++
	return nil
}

// Finish flushes buffered writes, fsyncs the temp file, renames it to
// the final path, and fsyncs the parent directory. After Finish the
// Writer is unusable.
func (w *Writer) Finish() error {
	if w.closed {
		return errors.New("sstable: finish on closed writer")
	}
	w.closed = true

	if err := w.bw.Flush(); err != nil {
		return fmt.Errorf("sstable: flush: %w", err)
	}
	if err := w.f.Sync(); err != nil {
		return fmt.Errorf("sstable: sync: %w", err)
	}
	if err := w.f.Close(); err != nil {
		return fmt.Errorf("sstable: close: %w", err)
	}
	if err := os.Rename(w.tmpPath, w.path); err != nil {
		return fmt.Errorf("sstable: rename %s -> %s: %w", w.tmpPath, w.path, err)
	}
	if err := syncDir(w.dir); err != nil {
		return fmt.Errorf("sstable: sync dir %s: %w", w.dir, err)
	}
	return nil
}

// Abort closes and removes the temp file. Safe to call after a partial
// Add sequence on the error path. Calling Abort after Finish is a no-op.
func (w *Writer) Abort() error {
	if w.closed {
		return nil
	}
	w.closed = true
	w.f.Close()
	if err := os.Remove(w.tmpPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("sstable: abort remove %s: %w", w.tmpPath, err)
	}
	return nil
}

// Count returns the number of records added so far.
func (w *Writer) Count() int { return w.count }

// Reader reads an immutable SSTable. Safe for concurrent reads once opened.
type Reader struct {
	path   string
	f      *os.File
	size   int64
	closed bool
}

// OpenReader opens an SSTable at path for reading.
func OpenReader(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("sstable: open %s: %w", path, err)
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("sstable: stat %s: %w", path, err)
	}
	return &Reader{path: path, f: f, size: info.Size()}, nil
}

// Get returns the value for key.
//
// found=false        → key is not in this SSTable
// op=OpDelete, found → tombstone (key is deleted at this SSTable)
// op=OpPut,   found  → value is returned
//
// Since records are sorted, Get can stop scanning as soon as it sees
// a key larger than the target.
func (r *Reader) Get(key []byte) (value []byte, op record.Op, found bool, err error) {
	if r.closed {
		return nil, 0, false, errors.New("sstable: read on closed reader")
	}
	if r.size == 0 {
		return nil, 0, false, nil
	}

	buf := make([]byte, r.size)
	if _, err := r.f.ReadAt(buf, 0); err != nil {
		return nil, 0, false, fmt.Errorf("sstable: read: %w", err)
	}

	offset := int64(0)
	for offset < r.size {
		rec, n, derr := record.Decode(buf[offset:])
		if derr != nil {
			return nil, 0, false, fmt.Errorf("sstable %s: decode at offset %d: %w", r.path, offset, derr)
		}
		cmp := bytesCompare(rec.Key, key)
		if cmp == 0 {
			if rec.Op == record.OpDelete {
				return nil, record.OpDelete, true, nil
			}
			return append([]byte(nil), rec.Value...), record.OpPut, true, nil
		}
		if cmp > 0 {
			return nil, 0, false, nil
		}
		offset += int64(n)
	}
	return nil, 0, false, nil
}

// Iterate yields each record in sorted order. Return false from fn to
// stop. Returns a non-nil error if the file is corrupt.
func (r *Reader) Iterate(fn func(op record.Op, key, value []byte) bool) error {
	if r.closed {
		return errors.New("sstable: iterate on closed reader")
	}
	if r.size == 0 {
		return nil
	}

	buf := make([]byte, r.size)
	if _, err := r.f.ReadAt(buf, 0); err != nil {
		return fmt.Errorf("sstable: read: %w", err)
	}

	offset := int64(0)
	for offset < r.size {
		rec, n, derr := record.Decode(buf[offset:])
		if derr != nil {
			return fmt.Errorf("sstable %s: decode at offset %d: %w", r.path, offset, derr)
		}
		if !fn(rec.Op, rec.Key, rec.Value) {
			return nil
		}
		offset += int64(n)
	}
	return nil
}

// Close releases the underlying file. Safe to call more than once.
func (r *Reader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	return r.f.Close()
}

// syncDir fsyncs a directory so that file creation, rename, and removal
// inside it are durable on Linux.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			return err
		}
	}
	return nil
}

// bytesCompare is a thin lexicographic comparator used by the Add
// ordering check and the Get hot loop.
func bytesCompare(a, b []byte) int {
	if len(a) == len(b) {
		for i := range a {
			if a[i] != b[i] {
				if a[i] < b[i] {
					return -1
				}
				return 1
			}
		}
		return 0
	}
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] != b[i] {
			if a[i] < b[i] {
				return -1
			}
			return 1
		}
	}
	if len(a) < len(b) {
		return -1
	}
	return 1
}
