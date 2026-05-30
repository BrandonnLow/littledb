// Package sstable implements immutable sorted-string tables on disk.
//
// File layout:
//
//	┌─────────────────────────────────────────────────────────┐
//	│  DATA SECTION                                           │
//	│    Block 1: [record][record]...[record]                 │
//	│    Block 2: ...                                         │
//	│    ...                                                  │
//	├─────────────────────────────────────────────────────────┤
//	│  INDEX SECTION                                          │
//	│    One record per block: key = first key in block,      │
//	│    value = [blockOffset:8][blockSize:8]                 │
//	├─────────────────────────────────────────────────────────┤
//	│  BLOOM SECTION                                          │
//	│    Serialized bloom filter bytes (see bloom package).   │
//	│    Used by Reader.Get to skip files that definitely     │
//	│    do not contain the target key.                       │
//	├─────────────────────────────────────────────────────────┤
//	│  FOOTER (40 bytes, fixed)                               │
//	│    indexOffset, indexSize,                              │
//	│    bloomOffset, bloomSize,                              │
//	│    magic                                                │
//	└─────────────────────────────────────────────────────────┘
//
// Reads work as: read footer → load index and bloom into memory → on
// Get, check bloom first; if maybe-present, binary-search index for
// the right block; read just that block; linear scan within it.
//
// SSTables are created atomically via "<path>.tmp" + rename + dir fsync.
package sstable

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync/atomic"

	"github.com/BrandonnLow/littledb/internal/bloom"
	"github.com/BrandonnLow/littledb/internal/record"
)

const (
	// blockSize targets one OS page. Records do not split across blocks;
	// a single oversized record produces a single oversized block.
	blockSize = 4096
	// footerSize is the fixed footer length at the end of every SSTable.
	// Layout: indexOffset (8) + indexSize (8) + magic (8).
	footerSize = 40
	// magic identifies a valid SSTable file. "LILLEDB!" interpreted as a
	// little-endian uint64.
	magic           = uint64(0x21424445_4C4C494C) // "LILLEDB!"
	bloomBitsPerKey = 10                          // ~1% FPR, matches LevelDB
)

var (
	// ErrOutOfOrder is returned by Writer.Add if the caller provides
	// keys that are not strictly ascending.
	ErrOutOfOrder = errors.New("sstable: keys out of order")
	// ErrDuplicate is returned by Writer.Add when the same key appears twice.
	ErrDuplicate = errors.New("sstable: duplicate key")
	// ErrBadMagic is returned by OpenReader when the footer's magic
	// number does not match.
	ErrBadMagic = errors.New("sstable: bad magic; not an sstable file")
)

// indexEntry describes one data block in the SSTable. Held in memory
// after the index is loaded.
type indexEntry struct {
	firstKey    []byte
	blockOffset int64
	blockSize   int64
}

// Writer builds an SSTable. Keys must be Added in strictly ascending
// order. Finish makes the file visible at its final path; Abort removes
// the temp file without publishing it.
type Writer struct {
	path    string
	tmpPath string
	dir     string
	f       *os.File
	bw      *bufio.Writer

	blockBuf      []byte
	blockFirstKey []byte
	blockOffset   int64

	index  []indexEntry
	filter *bloom.Filter

	lastKey []byte
	written int64
	count   int

	closed bool
}

// NewWriter creates a writer for path with a bloom filter sized for
// expectedKeys. Estimating the count is fine — over-estimating wastes
// a little bloom space, under-estimating raises the filter's false-
// positive rate above target.
func NewWriter(path string, expectedKeys int) (*Writer, error) {
	dir := filepath.Dir(path)
	tmpPath := path + ".tmp"

	f, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, fmt.Errorf("sstable: open %s: %w", tmpPath, err)
	}
	return &Writer{
		path:     path,
		tmpPath:  tmpPath,
		dir:      dir,
		f:        f,
		bw:       bufio.NewWriter(f),
		blockBuf: make([]byte, 0, blockSize),
		filter:   bloom.New(expectedKeys, bloomBitsPerKey),
	}, nil
}

// Add appends one record. Keys must be strictly ascending; duplicates
// and out-of-order keys return an error.
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

	encoded := record.Encode(&record.Record{Op: op, Key: key, Value: value})

	// If adding this record would overflow the current block AND the
	// block already has at least one record, close the current block
	// first. We never split a record across blocks, so an oversized
	// record (larger than blockSize on its own) ends up in a block by
	// itself.
	if len(w.blockBuf) > 0 && len(w.blockBuf)+len(encoded) > blockSize {
		if err := w.flushBlock(); err != nil {
			return err
		}
	}
	if len(w.blockBuf) == 0 {
		w.blockFirstKey = append([]byte(nil), key...)
	}
	w.blockBuf = append(w.blockBuf, encoded...)

	w.filter.Add(key)
	w.lastKey = append(w.lastKey[:0], key...)
	w.count++
	return nil
}

// flushBlock writes the current block to the file, records an index
// entry, and resets the block state.
func (w *Writer) flushBlock() error {
	if len(w.blockBuf) == 0 {
		return nil
	}
	if _, err := w.bw.Write(w.blockBuf); err != nil {
		return fmt.Errorf("sstable: write block: %w", err)
	}
	w.index = append(w.index, indexEntry{
		firstKey:    w.blockFirstKey,
		blockOffset: w.written,
		blockSize:   int64(len(w.blockBuf)),
	})
	w.written += int64(len(w.blockBuf))
	w.blockBuf = w.blockBuf[:0]
	w.blockFirstKey = nil
	w.blockOffset = w.written
	return nil
}

// Finish writes the index, the bloom filter, and the footer; fsyncs;
// renames the temp file to the final path; fsyncs the directory.
func (w *Writer) Finish() error {
	if w.closed {
		return errors.New("sstable: finish on closed writer")
	}
	w.closed = true

	if err := w.flushBlock(); err != nil {
		return err
	}

	// Index section.
	indexOffset := w.written
	var indexBytes []byte
	for _, e := range w.index {
		v := make([]byte, 16)
		binary.LittleEndian.PutUint64(v[0:8], uint64(e.blockOffset))
		binary.LittleEndian.PutUint64(v[8:16], uint64(e.blockSize))
		enc := record.Encode(&record.Record{Op: record.OpPut, Key: e.firstKey, Value: v})
		indexBytes = append(indexBytes, enc...)
	}
	if _, err := w.bw.Write(indexBytes); err != nil {
		return fmt.Errorf("sstable: write index: %w", err)
	}
	indexSize := int64(len(indexBytes))
	w.written += indexSize

	// Bloom section.
	bloomOffset := w.written
	bloomBytes := w.filter.Bytes()
	if _, err := w.bw.Write(bloomBytes); err != nil {
		return fmt.Errorf("sstable: write bloom: %w", err)
	}
	bloomSize := int64(len(bloomBytes))
	w.written += bloomSize

	// Footer.
	footer := make([]byte, footerSize)
	binary.LittleEndian.PutUint64(footer[0:8], uint64(indexOffset))
	binary.LittleEndian.PutUint64(footer[8:16], uint64(indexSize))
	binary.LittleEndian.PutUint64(footer[16:24], uint64(bloomOffset))
	binary.LittleEndian.PutUint64(footer[24:32], uint64(bloomSize))
	binary.LittleEndian.PutUint64(footer[32:40], magic)
	if _, err := w.bw.Write(footer); err != nil {
		return fmt.Errorf("sstable: write footer: %w", err)
	}

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

// Count returns the number of records Added so far.
func (w *Writer) Count() int { return w.count }

// Reader reads an immutable SSTable. Safe for concurrent reads once opened.
type Reader struct {
	path   string
	f      *os.File
	size   int64
	index  []indexEntry
	filter *bloom.Filter

	// blockReadCount counts block reads served by Get. Used by tests
	// to verify the bloom filter is skipping reads. Atomic so concurrent
	// Gets stay race-clean.
	blockReadCount atomic.Int64

	closed bool
}

// OpenReader opens an SSTable at path and loads its index into memory.
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
	size := info.Size()
	if size < footerSize {
		f.Close()
		return nil, fmt.Errorf("sstable %s: too small (%d bytes)", path, size)
	}

	footer := make([]byte, footerSize)
	if _, err := f.ReadAt(footer, size-footerSize); err != nil {
		f.Close()
		return nil, fmt.Errorf("sstable: read footer: %w", err)
	}
	gotMagic := binary.LittleEndian.Uint64(footer[32:40])
	if gotMagic != magic {
		f.Close()
		return nil, fmt.Errorf("%w: got %#x", ErrBadMagic, gotMagic)
	}
	indexOffset := int64(binary.LittleEndian.Uint64(footer[0:8]))
	indexSize := int64(binary.LittleEndian.Uint64(footer[8:16]))
	bloomOffset := int64(binary.LittleEndian.Uint64(footer[16:24]))
	bloomSize := int64(binary.LittleEndian.Uint64(footer[24:32]))

	limit := size - footerSize
	if indexOffset < 0 || indexSize < 0 || indexOffset+indexSize > limit ||
		bloomOffset < 0 || bloomSize < 0 || bloomOffset+bloomSize > limit {
		f.Close()
		return nil, fmt.Errorf("sstable %s: footer offsets out of range", path)
	}

	// Index.
	indexBuf := make([]byte, indexSize)
	if indexSize > 0 {
		if _, err := f.ReadAt(indexBuf, indexOffset); err != nil {
			f.Close()
			return nil, fmt.Errorf("sstable: read index: %w", err)
		}
	}
	var idx []indexEntry
	offset := 0
	for offset < len(indexBuf) {
		rec, n, derr := record.Decode(indexBuf[offset:])
		if derr != nil {
			f.Close()
			return nil, fmt.Errorf("sstable %s: decode index at offset %d: %w", path, offset, derr)
		}
		if len(rec.Value) != 16 {
			f.Close()
			return nil, fmt.Errorf("sstable %s: index entry value is %d bytes, want 16", path, len(rec.Value))
		}
		idx = append(idx, indexEntry{
			firstKey:    append([]byte(nil), rec.Key...),
			blockOffset: int64(binary.LittleEndian.Uint64(rec.Value[0:8])),
			blockSize:   int64(binary.LittleEndian.Uint64(rec.Value[8:16])),
		})
		offset += n
	}

	// Bloom.
	var filter *bloom.Filter
	if bloomSize > 0 {
		bloomBuf := make([]byte, bloomSize)
		if _, err := f.ReadAt(bloomBuf, bloomOffset); err != nil {
			f.Close()
			return nil, fmt.Errorf("sstable: read bloom: %w", err)
		}
		filter, err = bloom.Load(bloomBuf)
		if err != nil {
			f.Close()
			return nil, fmt.Errorf("sstable: load bloom: %w", err)
		}
	}

	return &Reader{path: path, f: f, size: size, index: idx, filter: filter}, nil
}

// Get returns the value for key.
//
// found=false        → key is not in this SSTable
// op=OpDelete, found → tombstone
// op=OpPut,   found  → live value
func (r *Reader) Get(key []byte) (value []byte, op record.Op, found bool, err error) {
	if r.closed {
		return nil, 0, false, errors.New("sstable: read on closed reader")
	}
	if len(r.index) == 0 {
		return nil, 0, false, nil
	}

	// Bloom check: if the filter says no, skip the block read entirely.
	if r.filter != nil && !r.filter.MayContain(key) {
		return nil, 0, false, nil
	}

	hi := sort.Search(len(r.index), func(i int) bool {
		return bytesCompare(r.index[i].firstKey, key) > 0
	})
	if hi == 0 {
		return nil, 0, false, nil
	}
	blk := r.index[hi-1]

	buf := make([]byte, blk.blockSize)
	if _, err := r.f.ReadAt(buf, blk.blockOffset); err != nil {
		return nil, 0, false, fmt.Errorf("sstable: read block: %w", err)
	}
	r.blockReadCount.Add(1)

	offset := int64(0)
	for offset < blk.blockSize {
		rec, n, derr := record.Decode(buf[offset:])
		if derr != nil {
			return nil, 0, false, fmt.Errorf("sstable %s: decode in block at %d: %w", r.path, blk.blockOffset+offset, derr)
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

// Iterate yields each record in sorted order across all blocks. Return
// false from fn to stop. Returns a non-nil error if the file is corrupt.
func (r *Reader) Iterate(fn func(op record.Op, key, value []byte) bool) error {
	if r.closed {
		return errors.New("sstable: iterate on closed reader")
	}
	if len(r.index) == 0 {
		return nil
	}

	for _, blk := range r.index {
		buf := make([]byte, blk.blockSize)
		if _, err := r.f.ReadAt(buf, blk.blockOffset); err != nil {
			return fmt.Errorf("sstable: read block at %d: %w", blk.blockOffset, err)
		}
		offset := int64(0)
		for offset < blk.blockSize {
			rec, n, derr := record.Decode(buf[offset:])
			if derr != nil {
				return fmt.Errorf("sstable %s: decode in block at %d: %w", r.path, blk.blockOffset+offset, derr)
			}
			if !fn(rec.Op, rec.Key, rec.Value) {
				return nil
			}
			offset += int64(n)
		}
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

// NumBlocks returns the number of data blocks in this SSTable. Useful
// for tests and debugging.
func (r *Reader) NumBlocks() int { return len(r.index) }

// BlockReadsForTesting returns the number of block reads served by Get
// so far. Used by tests to verify the bloom filter is doing its job.
func (r *Reader) BlockReadsForTesting() int64 { return r.blockReadCount.Load() }

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
