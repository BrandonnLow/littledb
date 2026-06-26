package cluster

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const raftLogFileName = "raft.log"

// raftLogMagic prefixes raft.log. Its presence distinguishes the snapshot-aware
// format (with a base header) from pre-snapshot files, which are rejected loudly
// rather than misparsed.
const raftLogMagic uint64 = 0x52414654_4C4F4721

// raftLogHeaderSize is the fixed file header: magic(8) | baseIndex(8) | baseTerm(8).
const raftLogHeaderSize = 24

// raftEntryHeaderSize is the per-frame header: term(8) | len(4).
const raftEntryHeaderSize = 12

// ErrBadRaftLog is returned by load when the file lacks the magic — a
// pre-snapshot raft.log or a foreign file. Surfaced loudly rather than guessed.
var ErrBadRaftLog = fmt.Errorf("cluster: raft log missing magic (pre-snapshot or foreign file)")

// raftLogFile is the per-node persistent Raft log: a 24-byte base header
// (magic, baseIndex, baseTerm) followed by append-only-with-truncation frames
// {term, len, bytes}. Entries at-or-below baseIndex are compacted away (held
// durably in the data WAL); the surviving suffix reconstructs the in-memory
// RaftLog above its base on open. A torn trailing frame is truncated on load;
// compaction rewrites the file atomically (tmp -> fsync -> rename -> fsync-dir).
type raftLogFile struct {
	mu        sync.Mutex
	f         *os.File
	path      string
	dir       string
	sync      bool
	baseIndex uint64
	baseTerm  uint64
	offsets   []int64 // offsets[j] = byte offset of the j-th surviving entry (index baseIndex+1+j)
	size      int64   // absolute file size, including the 24-byte header
}

// persistedEntry is one entry recovered from (or to be written to) the log file.
type persistedEntry struct {
	term uint64
	data []byte
}

// openRaftLogFile opens or creates the log file and returns the surviving
// entries (index order) for RaftLog reconstruction. The recovered base is left
// in lf.baseIndex / lf.baseTerm for the caller to read.
func openRaftLogFile(path string, sync bool) (*raftLogFile, []persistedEntry, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, nil, fmt.Errorf("cluster: open raft log %q: %w", path, err)
	}
	lf := &raftLogFile{f: f, path: path, dir: filepath.Dir(path), sync: sync}
	entries, err := lf.load()
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	return lf, entries, nil
}

// writeFreshHeader resets the file to a header-only state at the given base.
// Caller guarantees no surviving frames need preserving (fresh or torn-header
// file).
func (lf *raftLogFile) writeFreshHeader(baseIndex, baseTerm uint64) error {
	var hdr [raftLogHeaderSize]byte
	binary.LittleEndian.PutUint64(hdr[0:], raftLogMagic)
	binary.LittleEndian.PutUint64(hdr[8:], baseIndex)
	binary.LittleEndian.PutUint64(hdr[16:], baseTerm)
	if err := lf.f.Truncate(0); err != nil {
		return fmt.Errorf("cluster: reset raft log: %w", err)
	}
	if _, err := lf.f.WriteAt(hdr[:], 0); err != nil {
		return fmt.Errorf("cluster: write raft log header: %w", err)
	}
	if lf.sync {
		if err := lf.f.Sync(); err != nil {
			return fmt.Errorf("cluster: sync raft log header: %w", err)
		}
	}
	lf.baseIndex, lf.baseTerm = baseIndex, baseTerm
	lf.offsets = nil
	lf.size = raftLogHeaderSize
	if _, err := lf.f.Seek(raftLogHeaderSize, io.SeekStart); err != nil {
		return fmt.Errorf("cluster: seek raft log: %w", err)
	}
	return nil
}

func (lf *raftLogFile) load() ([]persistedEntry, error) {
	info, err := lf.f.Stat()
	if err != nil {
		return nil, fmt.Errorf("cluster: stat raft log: %w", err)
	}
	size := info.Size()

	if size == 0 {
		return nil, lf.writeFreshHeader(0, 0) // fresh file: base-0 header
	}

	buf := make([]byte, size)
	if _, err := lf.f.ReadAt(buf, 0); err != nil {
		return nil, fmt.Errorf("cluster: read raft log: %w", err)
	}

	if size < 8 {
		// Too small to hold even the magic — a torn fresh header (no complete
		// frame can fit below 8 bytes). Reinitialize at base 0.
		return nil, lf.writeFreshHeader(0, 0)
	}
	if binary.LittleEndian.Uint64(buf[0:8]) != raftLogMagic {
		return nil, ErrBadRaftLog // pre-snapshot or foreign: refuse to guess
	}
	if size < raftLogHeaderSize {
		// Magic present but header torn (crash mid initial-header write). No
		// frames can exist yet (they start at offset 24); reinitialize at base 0.
		return nil, lf.writeFreshHeader(0, 0)
	}

	lf.baseIndex = binary.LittleEndian.Uint64(buf[8:16])
	lf.baseTerm = binary.LittleEndian.Uint64(buf[16:24])

	var entries []persistedEntry
	off := int64(raftLogHeaderSize)
	for off < size {
		if off+raftEntryHeaderSize > size {
			break // torn frame header
		}
		term := binary.LittleEndian.Uint64(buf[off:])
		n := binary.LittleEndian.Uint32(buf[off+8:])
		end := off + raftEntryHeaderSize + int64(n)
		if end > size {
			break // torn frame body
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
		if lf.sync {
			if err := lf.f.Sync(); err != nil {
				return nil, fmt.Errorf("cluster: sync after truncate: %w", err)
			}
		}
	}
	lf.size = off
	if _, err := lf.f.Seek(off, io.SeekStart); err != nil {
		return nil, fmt.Errorf("cluster: seek raft log: %w", err)
	}
	return entries, nil
}

// append writes one entry, fsyncing if configured. Called under raftMu, paired
// with the in-memory RaftLog append so the file never diverges from memory. The
// fsync happens here (eager) so an entry is durable before the commit index can
// advance over it; this serializes Raft progress behind the fsync, the cost of
// keeping the two logs trivially consistent. Lifting the fsync off raftMu needs
// a separately-tracked durable index (matchIndex[self]) so the leader never
// counts an unsynced entry toward commit — deferred.
func (lf *raftLogFile) append(term uint64, data []byte) error {
	lf.mu.Lock()
	defer lf.mu.Unlock()
	if lf.f == nil {
		return fmt.Errorf("cluster: raft log append on closed file")
	}
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

// truncateFrom discards entry `index` and everything after it. Called under
// raftMu, paired with the in-memory RaftLog truncation, so the file and memory
// drop the same suffix atomically. index <= baseIndex is a no-op (compacted /
// committed entries are never truncated); index past the end is a no-op. The
// truncation target is always >= raftLogHeaderSize, so the header is preserved.
func (lf *raftLogFile) truncateFrom(index uint64) error {
	lf.mu.Lock()
	defer lf.mu.Unlock()
	if lf.f == nil {
		return fmt.Errorf("cluster: raft log truncate on closed file")
	}
	if index <= lf.baseIndex {
		return nil
	}
	pos := index - lf.baseIndex - 1 // 0-based into offsets
	if pos >= uint64(len(lf.offsets)) {
		return nil // past the end
	}
	off := lf.offsets[pos]
	if err := lf.f.Truncate(off); err != nil {
		return fmt.Errorf("cluster: raft log truncate: %w", err)
	}
	if lf.sync {
		if err := lf.f.Sync(); err != nil {
			return fmt.Errorf("cluster: raft log truncate sync: %w", err)
		}
	}
	lf.offsets = lf.offsets[:pos]
	lf.size = off
	if _, err := lf.f.Seek(off, io.SeekStart); err != nil {
		return fmt.Errorf("cluster: raft log seek after truncate: %w", err)
	}
	return nil
}

// compact rewrites the file as header(newBaseIndex, newBaseTerm) + survivors,
// atomically (tmp -> fsync -> rename -> fsync-dir). A crash before the rename
// leaves the old (longer) file intact, so recovery simply hasn't compacted yet —
// safe, since committed state is durable in the data WAL independently. Called
// under raftMu, paired with the in-memory RaftLog.compactTo: on error the file
// is unchanged and the caller leaves memory uncompacted, so the two stay
// mirrored.
func (lf *raftLogFile) compact(newBaseIndex, newBaseTerm uint64, survivors []persistedEntry) error {
	lf.mu.Lock()
	defer lf.mu.Unlock()
	if lf.f == nil {
		return fmt.Errorf("cluster: raft log compact on closed file")
	}

	tmp := lf.path + ".tmp"
	tf, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("cluster: open raft log tmp: %w", err)
	}
	fail := func(format string, err error) error {
		tf.Close()
		os.Remove(tmp)
		return fmt.Errorf(format, err)
	}

	var hdr [raftLogHeaderSize]byte
	binary.LittleEndian.PutUint64(hdr[0:], raftLogMagic)
	binary.LittleEndian.PutUint64(hdr[8:], newBaseIndex)
	binary.LittleEndian.PutUint64(hdr[16:], newBaseTerm)
	if _, err := tf.Write(hdr[:]); err != nil {
		return fail("cluster: write compacted header: %w", err)
	}

	newOffsets := make([]int64, 0, len(survivors))
	off := int64(raftLogHeaderSize)
	var fhdr [raftEntryHeaderSize]byte
	for _, e := range survivors {
		binary.LittleEndian.PutUint64(fhdr[0:], e.term)
		binary.LittleEndian.PutUint32(fhdr[8:], uint32(len(e.data)))
		if _, err := tf.Write(fhdr[:]); err != nil {
			return fail("cluster: write compacted frame: %w", err)
		}
		if _, err := tf.Write(e.data); err != nil {
			return fail("cluster: write compacted frame: %w", err)
		}
		newOffsets = append(newOffsets, off)
		off += raftEntryHeaderSize + int64(len(e.data))
	}
	if lf.sync {
		if err := tf.Sync(); err != nil {
			return fail("cluster: sync compacted raft log: %w", err)
		}
	}
	if err := tf.Close(); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("cluster: close compacted raft log: %w", err)
	}
	// Commit point: rename atomically replaces the live file.
	if err := os.Rename(tmp, lf.path); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("cluster: rename compacted raft log: %w", err)
	}
	if lf.sync {
		if err := syncDir(lf.dir); err != nil {
			return fmt.Errorf("cluster: sync dir after compact: %w", err)
		}
	}
	// Open the new file before closing the old, so a failure here leaves a
	// usable fd on the old inode rather than a nil one.
	nf, err := os.OpenFile(lf.path, os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("cluster: reopen compacted raft log: %w", err)
	}
	if _, err := nf.Seek(off, io.SeekStart); err != nil {
		nf.Close()
		return fmt.Errorf("cluster: seek compacted raft log: %w", err)
	}
	_ = lf.f.Close()
	lf.f = nf
	lf.baseIndex, lf.baseTerm = newBaseIndex, newBaseTerm
	lf.offsets = newOffsets
	lf.size = off
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
