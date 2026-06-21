// Package sstable implements immutable sorted-string tables on disk.
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
	footerSize      = 56
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
	appliedIndex uint64
	written      int64
	count        int

	closed bool
}

func NewWriter(path string, expectedKeys int, appliedIndex uint64) (*Writer, error) {
	dir := filepath.Dir(path)
	tmpPath := path + ".tmp"
	f, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, fmt.Errorf("sstable: open %s: %w", tmpPath, err)
	}
	return &Writer{
		path:         path,
		tmpPath:      tmpPath,
		dir:          dir,
		f:            f,
		bw:           bufio.NewWriter(f),
		blockBuf:     make([]byte, 0, blockSize),
		filter:       bloom.New(expectedKeys, bloomBitsPerKey),
		appliedIndex: appliedIndex,
	}, nil
}

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
	binary.LittleEndian.PutUint64(footer[40:48], w.appliedIndex)
	binary.LittleEndian.PutUint64(footer[48:56], magic)
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

type Reader struct {
	path         string
	f            *os.File
	size         int64
	index        []indexEntry
	filter       *bloom.Filter
	maxTimestamp uint64
	appliedIndex uint64

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
	gotMagic := binary.LittleEndian.Uint64(footer[48:56])
	if gotMagic != magic {
		f.Close()
		return nil, fmt.Errorf("%w: got %#x", ErrBadMagic, gotMagic)
	}
	indexOffset := int64(binary.LittleEndian.Uint64(footer[0:8]))
	indexSize := int64(binary.LittleEndian.Uint64(footer[8:16]))
	bloomOffset := int64(binary.LittleEndian.Uint64(footer[16:24]))
	bloomSize := int64(binary.LittleEndian.Uint64(footer[24:32]))
	maxTS := binary.LittleEndian.Uint64(footer[32:40])
	appliedIndex := binary.LittleEndian.Uint64(footer[40:48])

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
		appliedIndex: appliedIndex,
	}, nil
}

func (r *Reader) MaxTimestamp() uint64 { return r.maxTimestamp }

// AppliedIndex returns the Raft log index represented by this SSTable: the
// highest index whose applied state is folded into it at flush time. Zero for
// SSTables written outside a replication cluster.
func (r *Reader) AppliedIndex() uint64 { return r.appliedIndex }

func (r *Reader) GetAsOf(userKey []byte, snapshot uint64) (value []byte, op record.Op, found bool, err error) {
	if r.closed {
		return nil, 0, false, errors.New("sstable: read on closed reader")
	}
	if len(r.index) == 0 {
		return nil, 0, false, nil
	}
	if r.filter != nil && !r.filter.MayContain(userKey) {
		return nil, 0, false, nil
	}

	target := mvcckey.Encode(userKey, snapshot)
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

			if bytes.Compare(rec.Key, target) < 0 {
				continue
			}

			recUserKey, _, ok := mvcckey.Decode(rec.Key)
			if !ok {
				return nil, 0, false, fmt.Errorf("sstable %s: malformed encoded key", r.path)
			}
			if !bytes.Equal(recUserKey, userKey) {
				return nil, 0, false, nil
			}
			if rec.Op == record.OpDelete {
				return nil, record.OpDelete, true, nil
			}
			return append([]byte(nil), rec.Value...), record.OpPut, true, nil
		}
	}

	return nil, 0, false, nil
}

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
