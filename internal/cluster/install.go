package cluster

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/BrandonnLow/littledb/internal/db"
)

// InstallSnapshot transfers a leader's state to a follower whose nextIndex has
// fallen into the leader's compacted prefix. The transfer is A-logical: the
// leader streams the live key/value set (one SnapshotScan, pinned and
// snapshot-consistent), and the follower WIPES its DB and rebuilds from that set
// — deletes are absent from the stream, so an in-place merge would leave stale
// live versions of keys deleted before the snapshot.
//
// The install is crash-atomic via a durable marker. ApplyEntry is not idempotent
// on re-apply (it double-appends to the data WAL, which inflates the recovered
// applied index and silently skips a real entry on the next restart), so the
// node must never come up in a state where applied entries get re-driven. The
// marker gates trust in the live data dir: while it is present, the dir may be
// mid-swap and completion (re-)runs idempotently from the staged dir. Completion
// MUST run on Open before the recovered-vs-base guard, since a legitimate
// mid-install crash leaves recovered < base until the swap lands.

const (
	installMarkerName  = "install.marker"
	stagedDBDirName    = "staged-db"
	installMarkerMagic = uint64(0x4C44425F494E5354) // "LDB_INST"
	installMarkerSize  = 32                         // magic(8)|lii(8)|lit(8)|ts(8)
)

// installReopen* bound the retry on reopening the swapped-in store before the
// node gives up and parks (see completeInstall).
const (
	installReopenAttempts = 5
	installReopenBackoff  = 20 * time.Millisecond
)

// installResponseTimeout bounds the leader's wait for an InstallSnapshot ack. It
// is far larger than appendResponseTimeout because the follower builds, swaps,
// and reopens a whole DB synchronously before acking — work that scales with the
// snapshot size, not a constant round trip.
//
// NOTE (deferred to the streaming patch): this is a stopgap, not a fix. A large
// snapshot still blocks the follower's run() goroutine for the entire install
// (deaf to other messages), so once real payloads ship the install wants to be
// ASYNC (staged off the hot path, acking progress) rather than merely given a
// bigger timeout.
const installResponseTimeout = 5 * time.Second

type installMarker struct {
	lastIncludedIndex uint64
	lastIncludedTerm  uint64
	snapshotTS        uint64
}

func installMarkerPath(raftDir string) string { return filepath.Join(raftDir, installMarkerName) }
func stagedDBPath(raftDir string) string      { return filepath.Join(raftDir, stagedDBDirName) }

// writeInstallMarker durably records an install intent (tmp -> fsync -> rename ->
// fsync-dir). Its appearance is the commit point: a marker on disk means the
// staged DB is fully built and the swap may be (re-)applied.
func writeInstallMarker(raftDir string, m installMarker, sync bool) error {
	var buf [installMarkerSize]byte
	binary.LittleEndian.PutUint64(buf[0:], installMarkerMagic)
	binary.LittleEndian.PutUint64(buf[8:], m.lastIncludedIndex)
	binary.LittleEndian.PutUint64(buf[16:], m.lastIncludedTerm)
	binary.LittleEndian.PutUint64(buf[24:], m.snapshotTS)

	tmp := installMarkerPath(raftDir) + ".tmp"
	f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("cluster: write install marker: %w", err)
	}
	if _, err := f.Write(buf[:]); err != nil {
		f.Close()
		return fmt.Errorf("cluster: write install marker: %w", err)
	}
	if sync {
		if err := f.Sync(); err != nil {
			f.Close()
			return fmt.Errorf("cluster: sync install marker: %w", err)
		}
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("cluster: close install marker: %w", err)
	}
	if err := os.Rename(tmp, installMarkerPath(raftDir)); err != nil {
		return fmt.Errorf("cluster: rename install marker: %w", err)
	}
	if sync {
		if err := syncDir(raftDir); err != nil {
			return fmt.Errorf("cluster: sync dir after install marker: %w", err)
		}
	}
	return nil
}

// readInstallMarker returns the marker if present. ok is false when no install
// is pending.
func readInstallMarker(raftDir string) (m installMarker, ok bool, err error) {
	buf, rerr := os.ReadFile(installMarkerPath(raftDir))
	if rerr != nil {
		if os.IsNotExist(rerr) {
			return installMarker{}, false, nil
		}
		return installMarker{}, false, fmt.Errorf("cluster: read install marker: %w", rerr)
	}
	if len(buf) < installMarkerSize || binary.LittleEndian.Uint64(buf[0:]) != installMarkerMagic {
		// Torn or foreign marker: treat as no pending install rather than guess.
		// A torn marker means the rename never completed, so nothing was swapped.
		return installMarker{}, false, nil
	}
	m.lastIncludedIndex = binary.LittleEndian.Uint64(buf[8:])
	m.lastIncludedTerm = binary.LittleEndian.Uint64(buf[16:])
	m.snapshotTS = binary.LittleEndian.Uint64(buf[24:])
	return m, true, nil
}

func clearInstallMarker(raftDir string, sync bool) error {
	if err := os.Remove(installMarkerPath(raftDir)); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("cluster: clear install marker: %w", err)
	}
	if sync {
		if err := syncDir(raftDir); err != nil {
			return fmt.Errorf("cluster: sync dir after clearing marker: %w", err)
		}
	}
	return nil
}

// copyFileSync copies src to dst (truncating dst) and fsyncs dst.
func copyFileSync(src, dst string, sync bool) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	if sync {
		if err := out.Sync(); err != nil {
			out.Close()
			return err
		}
	}
	return out.Close()
}

// swapInStagedDB replaces the DB data files in dir with the files staged under
// stagedDir, preserving dir/raft (which holds the Raft log, state, marker, and
// the staged dir itself). It is idempotent: it always clears the old DB files
// first, so a re-run after a partial copy converges. The staged DB is flat
// (SSTables + applied.base), so only top-level files are copied.
func swapInStagedDB(dir, stagedDir string, sync bool) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("cluster: read data dir for swap: %w", err)
	}
	for _, e := range entries {
		if e.Name() == "raft" {
			continue // Raft files live here, including the staged dir and marker
		}
		if err := os.RemoveAll(filepath.Join(dir, e.Name())); err != nil {
			return fmt.Errorf("cluster: clear old db file %q: %w", e.Name(), err)
		}
	}
	staged, err := os.ReadDir(stagedDir)
	if err != nil {
		return fmt.Errorf("cluster: read staged dir: %w", err)
	}
	for _, e := range staged {
		if e.IsDir() {
			continue
		}
		if err := copyFileSync(filepath.Join(stagedDir, e.Name()), filepath.Join(dir, e.Name()), sync); err != nil {
			return fmt.Errorf("cluster: copy staged %q: %w", e.Name(), err)
		}
	}
	if sync {
		if err := syncDir(dir); err != nil {
			return fmt.Errorf("cluster: sync data dir after swap: %w", err)
		}
	}
	return nil
}

// completeInstallIfPending finishes an interrupted InstallSnapshot found on Open,
// BEFORE the store and raft log are opened and before the recovered-vs-base
// guard. A marker present means the live dir may be mid-swap, so completion
// (re-)runs idempotently from the staged dir until the marker clears. No marker:
// nothing to do.
func completeInstallIfPending(dir, raftDir string, sync bool) error {
	m, ok, err := readInstallMarker(raftDir)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	stagedDir := stagedDBPath(raftDir)
	if _, statErr := os.Stat(stagedDir); statErr != nil {
		// Marker present but no staged data — should not happen (the marker is
		// written only after the staged build is durable), but if it does the
		// safe action is to discard the intent: nothing was swapped, so the node
		// simply stays behind and re-receives a snapshot.
		return clearInstallMarker(raftDir, sync)
	}
	if err := swapInStagedDB(dir, stagedDir, sync); err != nil {
		return err
	}
	if err := resetRaftLogFileTo(filepath.Join(raftDir, raftLogFileName), m.lastIncludedIndex, m.lastIncludedTerm, sync); err != nil {
		return err
	}
	if err := clearInstallMarker(raftDir, sync); err != nil {
		return err
	}
	return os.RemoveAll(stagedDir)
}

// handleInstallSnapshot is the follower side of InstallSnapshot. It adopts a
// higher term, defers to the leader (resetting its election timer like an
// AppendEntries), and — if the snapshot is genuinely ahead of what it has —
// builds, marks, and installs it, then acks with MatchIndex = lastIncludedIndex.
// Runs on the run() goroutine, so no other message handling interleaves.
func (n *Node) handleInstallSnapshot(m Message) {
	n.raftMu.Lock()
	if m.Term < n.currentTerm {
		term := n.currentTerm
		n.raftMu.Unlock()
		_ = n.transport.Send(m.From, Message{Type: MsgInstallSnapshotResponse, From: n.id, Term: term, Success: false})
		return
	}
	if m.Term > n.currentTerm {
		n.stepDownLocked(m.Term)
	}
	n.role = Follower
	n.resetElectionTimer()
	term := n.currentTerm

	if n.installing {
		// A prior install left us parked (e.g. a reopen failure after the swap).
		// It completes on the next restart from the durable marker; don't re-enter
		// the swap in-process.
		n.raftMu.Unlock()
		_ = n.transport.Send(m.From, Message{Type: MsgInstallSnapshotResponse, From: n.id, Term: term, Success: false})
		return
	}

	if m.LastIncludedIndex <= n.lastApplied {
		// We already hold this much (or more) applied. Don't reinstall — just ack
		// so the leader advances its view; a later AppendEntries reconciles any
		// extra suffix we have.
		n.raftMu.Unlock()
		_ = n.transport.Send(m.From, Message{
			Type: MsgInstallSnapshotResponse, From: n.id, Term: term,
			Success: true, MatchIndex: m.LastIncludedIndex,
		})
		return
	}
	n.installing = true
	n.raftMu.Unlock()

	// Copy the batch off the shared (in-process) transport slices.
	kvs := make([]db.KV, len(m.Snapshot))
	for i, kp := range m.Snapshot {
		kvs[i] = db.KV{
			Key:   append([]byte(nil), kp.Key...),
			Value: append([]byte(nil), kp.Value...),
		}
	}

	if recoverable, err := n.installSnapshot(m.LastIncludedIndex, m.LastIncludedTerm, m.SnapshotTS, kvs); err != nil {
		if recoverable {
			// The old store is intact (failure before the swap): clear installing
			// so the node resumes serving its prior state and a later attempt can
			// retry.
			n.raftMu.Lock()
			n.installing = false
			n.raftMu.Unlock()
		}
		// Otherwise the swap got past the point of no return (store closed, data
		// replaced) but couldn't reopen: leave installing true so the node stays
		// PARKED — the apply loop won't touch the closed store — until a restart
		// finishes completion from the durable marker.
		_ = n.transport.Send(m.From, Message{Type: MsgInstallSnapshotResponse, From: n.id, Term: term, Success: false})
		return
	}
	_ = n.transport.Send(m.From, Message{
		Type: MsgInstallSnapshotResponse, From: n.id, Term: term,
		Success: true, MatchIndex: m.LastIncludedIndex,
	})
}

// installSnapshot stages the received state, writes the durable marker (the
// commit point), then completes the swap idempotently. It reports whether a
// failure is recoverable in-process: staging and marker errors happen BEFORE the
// old store is touched (recoverable — the caller may clear installing and the
// node keeps serving its old state), whereas a completeInstall error happens
// AFTER the store is closed and the data swapped (NOT recoverable — the node
// must stay parked until a restart re-runs completion from the marker).
func (n *Node) installSnapshot(lastIncludedIndex, lastIncludedTerm, snapshotTS uint64, kvs []db.KV) (recoverable bool, err error) {
	stagedDir := stagedDBPath(n.raftDir)
	if err := os.RemoveAll(stagedDir); err != nil {
		return true, err
	}
	if err := os.MkdirAll(stagedDir, 0o755); err != nil {
		return true, err
	}
	if err := db.BuildSnapshotDB(stagedDir, kvs, lastIncludedIndex, snapshotTS); err != nil {
		return true, err
	}
	if err := writeInstallMarker(n.raftDir, installMarker{
		lastIncludedIndex: lastIncludedIndex,
		lastIncludedTerm:  lastIncludedTerm,
		snapshotTS:        snapshotTS,
	}, n.opts.SyncOnWrite); err != nil {
		return true, err
	}
	if err := n.completeInstall(lastIncludedIndex, lastIncludedTerm); err != nil {
		return false, err
	}
	return true, nil
}

// completeInstall performs the live swap: close the store, replace the data
// files from the staged dir, reopen, reset the Raft log to the snapshot base,
// and set the applied/commit watermarks. storeMu (exclusive) is held across the
// swap so no apply or read sees a half-swapped store; raftMu is taken INSIDE it
// (storeMu is strictly outer), consistent with applyCommitted's RLock->raftMu
// ordering, so the two never deadlock.
//
// On a successful return installing is cleared and the apply loop resumes. If
// the reopen fails after the point of no return (old store closed, data already
// swapped), the node is left PARKED — installing stays true, watermarks are not
// advanced, and the marker is left in place — so the apply loop never runs
// against the closed store; a restart finishes completion from the marker.
func (n *Node) completeInstall(lastIncludedIndex, lastIncludedTerm uint64) error {
	n.storeMu.Lock()
	defer n.storeMu.Unlock()

	if err := n.store.Close(); err != nil {
		return fmt.Errorf("cluster: close store for install: %w", err)
	}
	if err := swapInStagedDB(n.dir, stagedDBPath(n.raftDir), n.opts.SyncOnWrite); err != nil {
		return err
	}
	// Reopen the swapped-in data dir. This is past the point of no return: the
	// old store is closed and the files are already replaced, so a failure here
	// cannot roll back. Retry a few times to ride out a transient I/O hiccup;
	// if it still fails, return with the node parked (installing untouched, so
	// still true) rather than letting the apply loop panic on the closed store.
	var newStore *db.DB
	var oerr error
	for attempt := 0; attempt < installReopenAttempts; attempt++ {
		if attempt > 0 {
			time.Sleep(installReopenBackoff)
		}
		newStore, oerr = db.OpenWith(n.dir, n.opts)
		if oerr == nil {
			break
		}
	}
	if oerr != nil {
		return fmt.Errorf("cluster: reopen store after install (node parked; restart completes from marker): %w", oerr)
	}
	newStore.SetCommitOverride(n.commit)
	n.store = newStore

	n.raftMu.Lock()
	if err := n.logFile.resetToBase(lastIncludedIndex, lastIncludedTerm); err != nil {
		n.raftMu.Unlock()
		return fmt.Errorf("cluster: reset raft log for install: %w", err)
	}
	n.log.resetToBase(lastIncludedIndex, lastIncludedTerm)
	n.commitIndex = lastIncludedIndex
	n.lastApplied = lastIncludedIndex
	n.installing = false
	n.appliedCond.Broadcast()
	// Give the just-installed follower a fresh election window: a long install
	// may have outlasted an election timeout, and standing for election before
	// the leader's next AppendEntries arrives would be needless churn.
	n.resetElectionTimer()
	n.raftMu.Unlock()

	if err := clearInstallMarker(n.raftDir, n.opts.SyncOnWrite); err != nil {
		return err
	}
	n.installsApplied.Add(1)
	return os.RemoveAll(stagedDBPath(n.raftDir))
}

// sendSnapshot is the leader's side of InstallSnapshot for peer p, invoked from
// sendLoop when p's nextIndex has fallen into the compacted prefix. It captures
// a pinned, consistent snapshot off the store (no raftMu), reads the snapshot's
// term from the committed log, ships it, and on a successful ack fast-forwards
// p's match/next. Returns false only on quit.
func (n *Node) sendSnapshot(p NodeID, term uint64) bool {
	n.storeMu.RLock()
	it, lastIncludedIndex, snapshotTS, err := n.store.SnapshotScan()
	if err != nil {
		n.storeMu.RUnlock()
		return true
	}
	var kvs []KVPair
	for it.Next() {
		kvs = append(kvs, KVPair{
			Key:   append([]byte(nil), it.Key()...),
			Value: append([]byte(nil), it.Value()...),
		})
	}
	iterErr := it.Err()
	it.Close()
	n.storeMu.RUnlock()
	if iterErr != nil {
		return true
	}

	n.raftMu.Lock()
	if n.role != Leader || n.currentTerm != term {
		n.raftMu.Unlock()
		return true
	}
	if lastIncludedIndex < n.log.baseIndex {
		// Our base advanced past the snapshot index between the scan and now;
		// term(lastIncludedIndex) would index the compacted prefix. Retry next
		// round with a fresher snapshot.
		n.raftMu.Unlock()
		return true
	}
	lastIncludedTerm := n.log.term(lastIncludedIndex)
	leaderCommit := n.commitIndex
	n.raftMu.Unlock()

	msg := Message{
		Type: MsgInstallSnapshot, From: n.id, Term: term,
		LastIncludedIndex: lastIncludedIndex,
		LastIncludedTerm:  lastIncludedTerm,
		SnapshotTS:        snapshotTS,
		Snapshot:          kvs,
		LeaderCommit:      leaderCommit,
	}
	if err := n.transport.Send(p, msg); err != nil {
		return true
	}

	var resp Message
	select {
	case resp = <-n.respCh[p]:
	case <-time.After(installResponseTimeout):
		return true
	case <-n.quit:
		return false
	}

	n.raftMu.Lock()
	if resp.Term > n.currentTerm {
		n.stepDownLocked(resp.Term)
		n.raftMu.Unlock()
		return true
	}
	if n.role != Leader || n.currentTerm != term {
		n.raftMu.Unlock()
		return true
	}
	if resp.Success {
		if lastIncludedIndex > n.matchIndex[p] {
			n.matchIndex[p] = lastIncludedIndex
		}
		n.nextIndex[p] = lastIncludedIndex + 1
	}
	n.raftMu.Unlock()
	return true
}
