package cluster

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

const raftStateFileName = "state"

// raftStateFileSize is the on-disk size of the durable hard state:
// currentTerm(8) + votedFor(8, signed int64 so noVote == -1 round-trips).
const raftStateFileSize = 16

// hardState is the Raft state that must survive a restart for the
// election-safety proof to hold: the node's currentTerm and the candidate it
// voted for in that term. The log is the third piece of persistent state and
// lives in raftLogFile; here we only carry term and vote.
type hardState struct {
	currentTerm uint64
	votedFor    NodeID
	// present is false when no state file existed yet (a fresh node, or a
	// legacy dir). Callers treat absence as (term 0, noVote) for the
	// max-load reconciliation at restart.
	present bool
}

// hardStatePersister is the seam through which a Node persists its hard state.
// Production uses *raftStateFile; tests inject a fake to exercise persist
// failures (the rollback/decline and degrade-safe paths). A nil persister is a
// no-op via persistHardStateLocked's guard — never assign a typed-nil
// (*raftStateFile)(nil), which would make the interface non-nil and defeat that
// guard.
type hardStatePersister interface {
	save(currentTerm uint64, votedFor NodeID) error
	close() error
}

// raftStateFile is the per-node durable home of (currentTerm, votedFor). Every
// change to either field is written through save before the node externalizes
// anything that depends on it — a granted-vote reply or a RequestVote broadcast
// — exactly as a log entry is made durable before it can be committed.
//
// Writes are crash-atomic via temp -> fsync -> rename -> fsync-dir (the same
// pattern db.writeAppliedBase uses): the visible file is always a COMPLETE
// prior or new value, never a torn mix, so no CRC or version field is needed.
// The file is created lazily by the first save's rename; openRaftStateFile only
// reads, so it is not the O_CREATE path (raft.log is).
type raftStateFile struct {
	mu   sync.Mutex
	path string
	dir  string
	sync bool
}

// openRaftStateFile prepares the state-file handle and reads any existing hard
// state. A missing or short/torn file yields hardState{present: false} (read as
// term 0, noVote); a complete file yields the persisted term and vote.
func openRaftStateFile(path string, sync bool) (*raftStateFile, hardState, error) {
	sf := &raftStateFile{path: path, dir: filepath.Dir(path), sync: sync}
	buf, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return sf, hardState{votedFor: noVote}, nil
	}
	if err != nil {
		return nil, hardState{}, fmt.Errorf("cluster: read raft state %q: %w", path, err)
	}
	if len(buf) < raftStateFileSize {
		// Only reachable for a dir not written by the atomic path; the rename
		// guarantees a complete file otherwise. Treat as absent: the restart
		// max-load falls back to the log term.
		return sf, hardState{votedFor: noVote}, nil
	}
	return sf, hardState{
		currentTerm: binary.LittleEndian.Uint64(buf[0:8]),
		votedFor:    NodeID(int64(binary.LittleEndian.Uint64(buf[8:16]))),
		present:     true,
	}, nil
}

// save durably records (currentTerm, votedFor). It is called under the node's
// raftMu, paired with the in-memory mutation, so disk never lags the field the
// node is about to act on. Lock order: raftMu -> raftStateFile.mu, never the
// reverse. The fsync is gated on sync (matching raftLogFile.append) so non-sync
// test/bench runs stay fast; the durability guarantee holds whenever sync is on.
func (sf *raftStateFile) save(currentTerm uint64, votedFor NodeID) error {
	sf.mu.Lock()
	defer sf.mu.Unlock()

	var buf [raftStateFileSize]byte
	binary.LittleEndian.PutUint64(buf[0:8], currentTerm)
	binary.LittleEndian.PutUint64(buf[8:16], uint64(int64(votedFor)))

	tmp := sf.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("cluster: write raft state: %w", err)
	}
	if _, err := f.Write(buf[:]); err != nil {
		f.Close()
		return fmt.Errorf("cluster: write raft state: %w", err)
	}
	if sf.sync {
		if err := f.Sync(); err != nil {
			f.Close()
			return fmt.Errorf("cluster: sync raft state: %w", err)
		}
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("cluster: close raft state: %w", err)
	}
	if err := os.Rename(tmp, sf.path); err != nil {
		return fmt.Errorf("cluster: rename raft state: %w", err)
	}
	if sf.sync {
		// The rename creates a new directory entry; fsync the dir so it survives.
		if err := syncDir(sf.dir); err != nil {
			return fmt.Errorf("cluster: sync dir after raft state: %w", err)
		}
	}
	return nil
}

// close releases the state file. The atomic save keeps no long-lived handle, so
// this is a no-op today; it exists for symmetry with raftLogFile.close and to
// keep Cluster.Close uniform.
func (sf *raftStateFile) close() error { return nil }

// syncDir fsyncs a directory so a just-created or renamed entry within it is
// durable. Mirrors the helpers in the db, wal, and sstable packages.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
