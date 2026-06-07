// Package sstable implements immutable sorted-string tables on disk.
// MVCC: each record's key is MVCC-encoded (userKey + descending timestamp)
// so multiple versions of a userKey coexist.
//
// File layout:
//
//	┌─────────────────────────────────────────────────────────┐
//	│  DATA SECTION                                           │
//	│    ~4 KB blocks of records with MVCC-encoded keys       │
//	├─────────────────────────────────────────────────────────┤
//	│  INDEX SECTION                                          │
//	│    One record per block: firstKey + offset + size       │
//	├─────────────────────────────────────────────────────────┤
//	│  BLOOM SECTION                                          │
//	│    Filter over userKeys (not encoded keys); a positive  │
//	│    means "some version of userKey may be in this file." │
//	├─────────────────────────────────────────────────────────┤
//	│  FOOTER (48 bytes, fixed)                               │
//	│    indexOffset, indexSize, bloomOffset, bloomSize,      │
//	│    maxTimestamp, magic                                  │
//	└─────────────────────────────────────────────────────────┘
//
// Reads work as: bloom check on userKey → binary-search index for the
// block whose firstKey is the largest ≤ Encode(userKey, snapshot) →
// scan that block for the first key ≥ target. The first match's
// userKey tells us whether the file holds a visible version.
package sstable

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync/atomic"

	"github.com/BrandonnLow/littledb/internal/bloom"
	"github.com/BrandonnLow/littledb/internal/mvcckey"
	"github.com/BrandonnLow/littledb/internal/record"
)

const (
	blockSize       = 4096
	footerSize      = 48
	magic           = uint64(0x21424445_4C4C494C) // "LILLEDB!"
	bloomBitsPerKey = 10
)

var (
	ErrOutOfOrder = errors.New("sstable: keys out of order")
	ErrDuplicate  = errors.New("sstable: duplicate key")
	ErrBadMagic   = errors.New("sstable: bad magic; not an sstable file")
)

type indexEntry struct {
	firstKey    []byte
	blockOffset int64
	blockSize   int64
}

// Writer builds an SSTable. Records must be added in strictly
// ascending MVCC-encoded-key order — equivalently, ascending userKey
// and within each userKey descending timestamp.
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

	lastEncKey   []byte
	maxTimestamp uint64
	written      int64
	count        int

	closed bool
}

// NewWriter creates a writer for path. expectedKeys sizes the bloom
// filter; an over-estimate wastes a little memory, an under-estimate
// raises the false-positive rate above target.
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

// Add appends one record. (userKey, ts) must form a strictly
// ascending MVCC-encoded-key sequence across calls. The bloom filter
// is updated with userKey (not the encoded key), so any future
// timestamp query for userKey will look up correctly.
func (w *Writer) Add(op record.Op, userKey, value []byte, ts uint64) error {
	if w.closed {
		return errors.New("sstable: write on closed writer")
	}
	encKey := mvcckey.Encode(userKey, ts)
	if w.lastEncKey != nil {
		switch cmp := bytes.Compare(encKey, w.lastEncKey); {
		case cmp == 0:
			return ErrDuplicate
		case cmp < 0:
			return ErrOutOfOrder
		}
	}

	encoded := record.Encode(&record.Record{
		Op:        op,
		Timestamp: ts,
		Key:       encKey,
		Value:     value,
	})

	if len(w.blockBuf) > 0 && len(w.blockBuf)+len(encoded) > blockSize {
		if err := w.flushBlock(); err != nil {
			return err
		}
	}
	if len(w.blockBuf) == 0 {
		w.blockFirstKey = append([]byte(nil), encKey...)
	}
	w.blockBuf = append(w.blockBuf, encoded...)

	// Bloom filter sees userKey, not encKey. This is the critical
	// invariant: a Get(userKey, anyTimestamp) needs the filter to
	// return "maybe" for any version of userKey we've added.
	w.filter.Add(userKey)

	w.lastEncKey = append(w.lastEncKey[:0], encKey...)
	if ts > w.maxTimestamp {
		w.maxTimestamp = ts
	}
	w.count++
	return nil
}

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

// Finish writes the index, bloom filter, and footer; fsyncs; renames
// the temp file to the final path; fsyncs the directory.
//
// Ordering invariant: by the time this returns, the SSTable file
// (including its footer with MaxTimestamp) is fully durable on disk.
// Callers that subsequently truncate or rotate the WAL depend on this.
func (w *Writer) Finish() error {
	if w.closed {
		return errors.New("sstable: finish on closed writer")
	}
	w.closed = true

	if err := w.flushBlock(); err != nil {
		return err
	}

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

	bloomOffset := w.written
	bloomBytes := w.filter.Bytes()
	if _, err := w.bw.Write(bloomBytes); err != nil {
		return fmt.Errorf("sstable: write bloom: %w", err)
	}
	bloomSize := int64(len(bloomBytes))
	w.written += bloomSize

	footer := make([]byte, footerSize)
	binary.LittleEndian.PutUint64(footer[0:8], uint64(indexOffset))
	binary.LittleEndian.PutUint64(footer[8:16], uint64(indexSize))
	binary.LittleEndian.PutUint64(footer[16:24], uint64(bloomOffset))
	binary.LittleEndian.PutUint64(footer[24:32], uint64(bloomSize))
	binary.LittleEndian.PutUint64(footer[32:40], w.maxTimestamp)
	binary.LittleEndian.PutUint64(footer[40:48], magic)
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

func (w *Writer) Count() int { return w.count }

// Reader reads an immutable SSTable. Safe for concurrent reads.
type Reader struct {
	path         string
	f            *os.File
	size         int64
	index        []indexEntry
	filter       *bloom.Filter
	maxTimestamp uint64

	blockReadCount atomic.Int64
	closed         bool
}

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
	gotMagic := binary.LittleEndian.Uint64(footer[40:48])
	if gotMagic != magic {
		f.Close()
		return nil, fmt.Errorf("%w: got %#x", ErrBadMagic, gotMagic)
	}
	indexOffset := int64(binary.LittleEndian.Uint64(footer[0:8]))
	indexSize := int64(binary.LittleEndian.Uint64(footer[8:16]))
	bloomOffset := int64(binary.LittleEndian.Uint64(footer[16:24]))
	bloomSize := int64(binary.LittleEndian.Uint64(footer[24:32]))
	maxTS := binary.LittleEndian.Uint64(footer[32:40])

	limit := size - footerSize
	if indexOffset < 0 || indexSize < 0 || indexOffset+indexSize > limit ||
		bloomOffset < 0 || bloomSize < 0 || bloomOffset+bloomSize > limit {
		f.Close()
		return nil, fmt.Errorf("sstable %s: footer offsets out of range", path)
	}

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

	return &Reader{
		path:         path,
		f:            f,
		size:         size,
		index:        idx,
		filter:       filter,
		maxTimestamp: maxTS,
	}, nil
}

// MaxTimestamp returns the largest timestamp recorded in this file's
// footer. Used by the DB layer to compute the initial value of its
// timestamp counter on Open.
func (r *Reader) MaxTimestamp() uint64 { return r.maxTimestamp }

// GetAsOf returns the version of userKey visible at snapshot.
//
//   - (value, OpPut, true,  nil) — a live value
//   - (nil,   OpDelete, true,  nil) — masked by a tombstone
//   - (nil,   0,        false, nil) — no visible version
func (r *Reader) GetAsOf(userKey []byte, snapshot uint64) (value []byte, op record.Op, found bool, err error) {
	if r.closed {
		return nil, 0, false, errors.New("sstable: read on closed reader")
	}
	if len(r.index) == 0 {
		return nil, 0, false, nil
	}
	// Bloom check is on userKey, NOT the encoded key. If we hashed the
	// encoded key we'd have a separate filter entry per version, and
	// a lookup at any snapshot other than an exact previous write
	// timestamp would falsely report "absent."
	if r.filter != nil && !r.filter.MayContain(userKey) {
		return nil, 0, false, nil
	}

	target := mvcckey.Encode(userKey, snapshot)
	n := len(r.index)

	// Largest block N with firstKey <= target. The answer (if any) is
	// either inside block N or at the start of block N+1.
	hi := sort.Search(n, func(i int) bool {
		return bytes.Compare(r.index[i].firstKey, target) > 0
	})
	startBlock := 0
	if hi > 0 {
		startBlock = hi - 1
	}

	for blkIdx := startBlock; blkIdx < n && blkIdx <= startBlock+1; blkIdx++ {
		blk := r.index[blkIdx]
		buf := make([]byte, blk.blockSize)
		if _, err := r.f.ReadAt(buf, blk.blockOffset); err != nil {
			return nil, 0, false, fmt.Errorf("sstable: read block: %w", err)
		}
		r.blockReadCount.Add(1)

		offset := int64(0)
		for offset < blk.blockSize {
			rec, decN, derr := record.Decode(buf[offset:])
			if derr != nil {
				return nil, 0, false, fmt.Errorf("sstable %s: decode in block at %d: %w", r.path, blk.blockOffset+offset, derr)
			}
			offset += int64(decN)

			// Skip records whose encoded key sorts before target
			// (i.e., versions newer than our snapshot).
			if bytes.Compare(rec.Key, target) < 0 {
				continue
			}

			// First record at-or-after target: check userKey match.
			recUserKey, _, ok := mvcckey.Decode(rec.Key)
			if !ok {
				return nil, 0, false, fmt.Errorf("sstable %s: malformed encoded key", r.path)
			}
			if !bytes.Equal(recUserKey, userKey) {
				// Moved past userKey without a visible version.
				return nil, 0, false, nil
			}
			if rec.Op == record.OpDelete {
				return nil, record.OpDelete, true, nil
			}
			return append([]byte(nil), rec.Value...), record.OpPut, true, nil
		}
		// Block exhausted without finding a key >= target — the answer
		// (if any) is at the start of the next block; loop continues.
	}

	return nil, 0, false, nil
}

// NewestVersionTS returns the timestamp of the newest stored version
// of userKey in this SSTable, or (0, false) if userKey is not present.
// Used by the DB's commit-time conflict check.
//
// Mirrors GetAsOf's structure: bloom check, binary-search the index
// for the block whose firstKey is the largest ≤ target, scan that
// block (and possibly the next) for the first key ≥ target. Target is
// Encode(userKey, ^uint64(0)), which sorts at the position of the
// newest possible version of userKey.
func (r *Reader) NewestVersionTS(userKey []byte) (ts uint64, found bool, err error) {
	if r.closed {
		return 0, false, errors.New("sstable: read on closed reader")
	}
	if len(r.index) == 0 {
		return 0, false, nil
	}
	if r.filter != nil && !r.filter.MayContain(userKey) {
		return 0, false, nil
	}

	target := mvcckey.Encode(userKey, ^uint64(0))
	n := len(r.index)

	hi := sort.Search(n, func(i int) bool {
		return bytes.Compare(r.index[i].firstKey, target) > 0
	})
	startBlock := 0
	if hi > 0 {
		startBlock = hi - 1
	}

	for blkIdx := startBlock; blkIdx < n && blkIdx <= startBlock+1; blkIdx++ {
		blk := r.index[blkIdx]
		buf := make([]byte, blk.blockSize)
		if _, err := r.f.ReadAt(buf, blk.blockOffset); err != nil {
			return 0, false, fmt.Errorf("sstable: read block: %w", err)
		}
		r.blockReadCount.Add(1)

		offset := int64(0)
		for offset < blk.blockSize {
			rec, decN, derr := record.Decode(buf[offset:])
			if derr != nil {
				return 0, false, fmt.Errorf("sstable %s: decode in block at %d: %w",
					r.path, blk.blockOffset+offset, derr)
			}
			offset += int64(decN)

			if bytes.Compare(rec.Key, target) < 0 {
				continue
			}
			recUserKey, recTS, ok := mvcckey.Decode(rec.Key)
			if !ok {
				return 0, false, fmt.Errorf("sstable %s: malformed encoded key", r.path)
			}
			if !bytes.Equal(recUserKey, userKey) {
				return 0, false, nil
			}
			return recTS, true, nil
		}
	}
	return 0, false, nil
}

// VersionCountForTesting returns the number of stored versions of
// userKey in this SSTable. Used to verify GC behaviour. Scans the
// whole file; slow, fine for tests.
func (r *Reader) VersionCountForTesting(userKey []byte) int {
	if r.closed {
		return 0
	}
	if r.filter != nil && !r.filter.MayContain(userKey) {
		return 0
	}
	count := 0
	_ = r.Iterate(func(op record.Op, encKey, value []byte) bool {
		decodedUserKey, _, ok := mvcckey.Decode(encKey)
		if !ok {
			return true
		}
		if bytes.Equal(decodedUserKey, userKey) {
			count++
		}
		return true
	})
	return count
}

// Iterate yields each record in sorted order (encoded-key ascending).
// Used by compaction; the encoded key contains both userKey and
// timestamp.
func (r *Reader) Iterate(fn func(op record.Op, encKey, value []byte) bool) error {
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

func (r *Reader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	return r.f.Close()
}

func (r *Reader) NumBlocks() int              { return len(r.index) }
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
