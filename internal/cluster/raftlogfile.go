package cluster

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
)

const raftLogFileName = "raft.log"

// raftLogFile is the per-node persistent Raft log: an append-only-with-
// truncation file of entries {term, len, bytes}. It is the durable home of
// appended (possibly uncommitted) entries — distinct from the data WAL, which
// holds only applied (committed) state — and it reconstructs the in-memory
// RaftLog on open. A torn tail (partial entry from a crash mid-append) is
// truncated on load.
type raftLogFile struct {
	mu      sync.Mutex
	f       *os.File
	sync    bool
	offsets []int64 // offsets[i] = byte offset of entry index i+1
	size    int64
}

// persistedEntry is one entry recovered from the log file, in index order.
type persistedEntry struct {
	term uint64
	data []byte
}

const raftEntryHeaderSize = 12 // 8 (term) + 4 (len)

// openRaftLogFile opens or creates the log file and returns it together with
// the entries it holds (index order) for RaftLog reconstruction.
func openRaftLogFile(path string, sync bool) (*raftLogFile, []persistedEntry, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, nil, fmt.Errorf("cluster: open raft log %q: %w", path, err)
	}
	lf := &raftLogFile{f: f, sync: sync}
	entries, err := lf.load()
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	return lf, entries, nil
}

func (lf *raftLogFile) load() ([]persistedEntry, error) {
	info, err := lf.f.Stat()
	if err != nil {
		return nil, fmt.Errorf("cluster: stat raft log: %w", err)
	}
	size := info.Size()
	if size == 0 {
		return nil, nil
	}
	buf := make([]byte, size)
	if _, err := lf.f.ReadAt(buf, 0); err != nil {
		return nil, fmt.Errorf("cluster: read raft log: %w", err)
	}

	var entries []persistedEntry
	var off int64
	for off < size {
		if off+raftEntryHeaderSize > size {
			break // torn header
		}
		term := binary.LittleEndian.Uint64(buf[off:])
		n := binary.LittleEndian.Uint32(buf[off+8:])
		end := off + raftEntryHeaderSize + int64(n)
		if end > size {
			break // torn body
		}
		entries = append(entries, persistedEntry{
			term: term,
			data: append([]byte(nil), buf[off+raftEntryHeaderSize:end]...),
		})
		lf.offsets = append(lf.offsets, off)
		off = end
	}

	if off < size {
		// Drop the torn tail so future appends start clean.
		if err := lf.f.Truncate(off); err != nil {
			return nil, fmt.Errorf("cluster: truncate torn raft log: %w", err)
		}
	}
	lf.size = off
	if _, err := lf.f.Seek(off, io.SeekStart); err != nil {
		return nil, fmt.Errorf("cluster: seek raft log: %w", err)
	}
	return entries, nil
}

// append writes one entry, fsyncing if configured. Called UNLOCKED w.r.t.
// raftMu: it is the hot replication path and must not serialize Raft progress
// behind disk latency.
func (lf *raftLogFile) append(term uint64, data []byte) error {
	lf.mu.Lock()
	defer lf.mu.Unlock()
	var hdr [raftEntryHeaderSize]byte
	binary.LittleEndian.PutUint64(hdr[0:], term)
	binary.LittleEndian.PutUint32(hdr[8:], uint32(len(data)))
	off := lf.size
	if _, err := lf.f.Write(hdr[:]); err != nil {
		return fmt.Errorf("cluster: raft log append header: %w", err)
	}
	if _, err := lf.f.Write(data); err != nil {
		return fmt.Errorf("cluster: raft log append data: %w", err)
	}
	if lf.sync {
		if err := lf.f.Sync(); err != nil {
			return fmt.Errorf("cluster: raft log sync: %w", err)
		}
	}
	lf.offsets = append(lf.offsets, off)
	lf.size += raftEntryHeaderSize + int64(len(data))
	return nil
}

// truncateFrom discards entry `index` and everything after it. Rare (divergence
// repair only), so it may run under raftMu.
func (lf *raftLogFile) truncateFrom(index uint64) error {
	lf.mu.Lock()
	defer lf.mu.Unlock()
	if index < 1 || index > uint64(len(lf.offsets)) {
		return nil
	}
	off := lf.offsets[index-1]
	if err := lf.f.Truncate(off); err != nil {
		return fmt.Errorf("cluster: raft log truncate: %w", err)
	}
	if lf.sync {
		if err := lf.f.Sync(); err != nil {
			return fmt.Errorf("cluster: raft log truncate sync: %w", err)
		}
	}
	lf.offsets = lf.offsets[:index-1]
	lf.size = off
	if _, err := lf.f.Seek(off, io.SeekStart); err != nil {
		return fmt.Errorf("cluster: raft log seek after truncate: %w", err)
	}
	return nil
}

func (lf *raftLogFile) close() error {
	lf.mu.Lock()
	defer lf.mu.Unlock()
	if lf.f == nil {
		return nil
	}
	err := lf.f.Close()
	lf.f = nil
	return err
}
