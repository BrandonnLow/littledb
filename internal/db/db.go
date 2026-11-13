// Package db is the top-level littledb storage engine.
package db

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"sync"

	"github.com/BrandonnLow/littledb/internal/memtable"
	"github.com/BrandonnLow/littledb/internal/record"
	"github.com/BrandonnLow/littledb/internal/sstable"
	"github.com/BrandonnLow/littledb/internal/wal"
)

const (
	walFilename              = "littledb.log"
	appliedBaseFilename      = "applied.base"
	sessionsFilename         = "sessions"
	defaultMemtableSizeMax   = 4 * 1024 * 1024
	defaultCompactionTrigger = 4
)

var (
	ErrKeyNotFound = errors.New("db: key not found")
	ErrConflict    = errors.New("db: transaction conflict")

	// ErrUnexpectedWAL is returned by OpenWith in ExternalLog mode when the
	// directory holds a non-empty data WAL. Such a dir was written by a WAL-mode
	// store (standalone, or a replica from before the Raft log became the only
	// log); its unflushed writes live in that WAL and in no replicated log, so
	// opening it as a replica would silently drop them. Refuse instead.
	ErrUnexpectedWAL = errors.New("db: data WAL present in an ExternalLog store")

	errClosed         = errors.New("db: closed")
	errNotExternalLog = errors.New("db: replicated apply requires Options.ExternalLog")
	errNoCommitPath   = errors.New("db: ExternalLog store has no commit override; commits must go through the replication layer")
	sstableNameRE     = regexp.MustCompile(`^(\d{6})\.sst$`)
)

type Options struct {
	SyncOnWrite                 bool
	MemtableSizeMax             int64
	CompactionTrigger           int
	DisableBackgroundCompaction bool

	// ExternalLog makes the caller's replicated log (the Raft log) this store's
	// write-ahead log. The store keeps no data WAL: ApplyEntry writes only the
	// memtable, and applied state becomes durable in the store's own files only
	// at a flush (SSTable, then sessions, then applied.base — the commit point).
	// In exchange the caller must (1) retain every log entry above
	// DurableAppliedIndex(), and (2) after Open, replay its committed entries
	// from RecoveredAppliedIndex()+1. The single-node commit path is disabled;
	// every commit goes through the installed commit override. Set by the
	// cluster for every replica; a standalone store leaves it false and keeps
	// its WAL.
	ExternalLog bool
}

func DefaultOptions() Options {
	return Options{
		SyncOnWrite:       true,
		MemtableSizeMax:   defaultMemtableSizeMax,
		CompactionTrigger: defaultCompactionTrigger,
	}
}

type DB struct {
	mu         sync.RWMutex
	dir        string
	opts       Options
	wal        *wal.WAL
	memtable   *memtable.Memtable
	frozen     *memtable.Memtable
	sstables   []*sstable.Reader
	sstableIDs []int
	nextID     int

	nextTimestamp uint64

	// appliedIndex is the highest Raft log index whose entry has been applied
	// to this DB (memtable + SSTables). The cluster passes it to ApplyEntry,
	// which accepts only appliedIndex+1, so the two never drift. Zero outside a
	// replication cluster (ExternalLog unset).
	appliedIndex uint64

	// durableIndex is the highest applied index whose effects are durable in
	// this DB's own files: everything at or below it is in SSTables, with the
	// session table and applied.base recorded to match. It advances only at a
	// flush. Entries in (durableIndex, appliedIndex] exist only in the memtable
	// and in the caller's log, which is why the caller may compact its log only
	// up to here (DurableAppliedIndex).
	durableIndex uint64

	// recoveredApplied is the durable applied index found at Open (the index
	// recorded in applied.base by the last flush). The cluster seeds
	// commitIndex/lastApplied from it and re-applies its log from the next
	// index (RecoveredAppliedIndex).
	recoveredApplied uint64

	// appliedTS is the highest commit timestamp actually applied to the
	// memtable. On a single-node DB and on followers it equals the highest
	// allocated commit (allocate and apply happen together), but on a
	// replication leader it lags nextTimestamp during an in-flight commit:
	// PrepareCommit allocates before the entry is applied. Begin derives a
	// txn's read snapshot from this, not nextTimestamp, so a snapshot never
	// claims to include a commit whose data is not yet visible.
	appliedTS uint64

	// sessions maps a client session id to the highest per-session sequence
	// number applied, for exactly-once dedup. Updated in ApplyEntry together with
	// the memtable (same db.mu critical section), so PrepareCommit's dedup check
	// and its conflict check see a consistent view. Loaded at Open from the
	// sessions file written by the last flush; the caller's replay of its log
	// rebuilds everything after that. Not expired yet (v1).
	sessions map[string]uint64

	closed bool

	// commitOverride, if non-nil, replaces the single-node commit path:
	// Txn.Commit delegates the entire commit to it (leader role under a
	// replication cluster). Set via SetCommitOverride.
	commitOverride func(*Txn) error

	activeTxnsMu sync.Mutex
	activeTxns   map[*Txn]struct{}

	compactMu     sync.Mutex
	compactCh     chan struct{}
	compactDoneCh chan struct{}
}

func Open(dir string) (*DB, error) { return OpenWith(dir, DefaultOptions()) }

func OpenWith(dir string, opts Options) (*DB, error) {
	if opts.MemtableSizeMax <= 0 {
		opts.MemtableSizeMax = defaultMemtableSizeMax
	}
	if opts.CompactionTrigger < 2 {
		opts.CompactionTrigger = defaultCompactionTrigger
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("db: mkdir: %w", err)
	}

	sstIDs, err := discoverSSTables(dir)
	if err != nil {
		return nil, fmt.Errorf("db: discover sstables: %w", err)
	}

	var ssts []*sstable.Reader
	var sstIDsRev []int
	var maxTS uint64
	for i := len(sstIDs) - 1; i >= 0; i-- {
		id := sstIDs[i]
		path := filepath.Join(dir, sstableFilename(id))
		r, err := sstable.OpenReader(path)
		if err != nil {
			for _, opened := range ssts {
				opened.Close()
			}
			return nil, fmt.Errorf("db: open sstable %s: %w", path, err)
		}
		ssts = append(ssts, r)
		sstIDsRev = append(sstIDsRev, id)
		if r.MaxTimestamp() > maxTS {
			maxTS = r.MaxTimestamp()
		}
	}
	closeSSTables := func() {
		for _, r := range ssts {
			r.Close()
		}
	}

	var (
		w        *wal.WAL
		mt       = memtable.New()
		sessions = map[string]uint64{}
		applied  uint64
	)
	if opts.ExternalLog {
		// The caller's log is the WAL, so there is nothing to replay here: the
		// memtable starts empty and the caller re-applies its committed entries
		// above the durable index. What the last flush made durable is the
		// SSTables plus the two files it wrote alongside them.
		if err := checkNoDataWAL(dir); err != nil {
			closeSSTables()
			return nil, err
		}
		base, err := readAppliedBase(dir)
		if err != nil {
			closeSSTables()
			return nil, err
		}
		// applied.base, not the SSTable footers, decides the recovered index: it
		// is written last at a flush, so it is the flush's commit point (see
		// flushExternalLocked for why a footer must not be trusted on its own).
		applied = base.index
		// Its timestamp covers entries that leave no data behind (read-index
		// no-ops), which no SSTable footer records, so the next commit timestamp
		// after a restart can never fall below one already handed out.
		if base.ts > maxTS {
			maxTS = base.ts
		}
		if sessions, err = readSessionsFile(dir); err != nil {
			closeSSTables()
			return nil, err
		}
	} else {
		if w, err = wal.OpenWith(dir, wal.Options{SyncOnWrite: opts.SyncOnWrite}); err != nil {
			closeSSTables()
			return nil, err
		}
		if err := replayWAL(w, mt, &maxTS); err != nil {
			w.Close()
			closeSSTables()
			return nil, fmt.Errorf("db: replay wal: %w", err)
		}
	}

	nextID := 1
	if len(sstIDs) > 0 {
		nextID = sstIDs[len(sstIDs)-1] + 1
	}

	db := &DB{
		dir:           dir,
		opts:          opts,
		wal:           w,
		memtable:      mt,
		sstables:      ssts,
		sstableIDs:    sstIDsRev,
		nextID:        nextID,
		nextTimestamp: maxTS + 1,
		// appliedTS is maxTS: the SSTable footers plus, in WAL mode, the replayed
		// WAL (after apply-all recovery everything committed is applied) or, in
		// ExternalLog mode, the timestamp applied.base recorded. An uncommitted
		// WAL tail (discarded on a torn commit) can push maxTS past the true
		// watermark, but harmlessly — nothing is committed in that gap and the
		// next commit allocates a still-higher ts, so no conflict is missed or
		// spuriously raised.
		appliedTS:        maxTS,
		appliedIndex:     applied,
		durableIndex:     applied,
		recoveredApplied: applied,
		sessions:         sessions,
		activeTxns:       make(map[*Txn]struct{}),
		compactCh:        make(chan struct{}, 1),
		compactDoneCh:    make(chan struct{}),
	}

	if !opts.DisableBackgroundCompaction {
		go db.compactLoop()
		db.signalCompact()
	} else {
		close(db.compactDoneCh)
	}

	return db, nil
}

// replayWAL rebuilds a WAL-mode store's memtable from its data WAL. Records are
// buffered by transaction and applied only when the transaction's OpCommit
// marker arrives, so a torn or never-committed tail is discarded (the
// atomicity contract). maxTS is raised to the highest timestamp seen.
func replayWAL(w *wal.WAL, mt *memtable.Memtable, maxTS *uint64) error {
	var buffer []*record.Record
	return w.Scan(func(offset int64, rec *record.Record) error {
		if rec.Timestamp > *maxTS {
			*maxTS = rec.Timestamp
		}
		switch rec.Op {
		case record.OpPut, record.OpDelete:
			buffer = append(buffer, rec)
		case record.OpCommit:
			for _, br := range buffer {
				if br.Timestamp != rec.Timestamp {
					return fmt.Errorf("db: replay: ts mismatch (data %d vs commit %d)",
						br.Timestamp, rec.Timestamp)
				}
				switch br.Op {
				case record.OpPut:
					if err := mt.Put(br.Key, br.Value, br.Timestamp); err != nil {
						return err
					}
				case record.OpDelete:
					if err := mt.Delete(br.Key, br.Timestamp); err != nil {
						return err
					}
				}
			}
			buffer = buffer[:0]
		default:
			return fmt.Errorf("db: replay: unknown op %d", rec.Op)
		}
		return nil
	})
}

// checkNoDataWAL refuses to open an ExternalLog store over a non-empty data
// WAL (see ErrUnexpectedWAL). An empty one holds nothing and is ignored.
func checkNoDataWAL(dir string) error {
	info, err := os.Stat(filepath.Join(dir, walFilename))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("db: stat data wal: %w", err)
	}
	if info.Size() > 0 {
		return fmt.Errorf("%w: %s (%d bytes)", ErrUnexpectedWAL, filepath.Join(dir, walFilename), info.Size())
	}
	return nil
}

func (db *DB) Put(key, value []byte) error {
	t := db.Begin()
	if err := t.Put(key, value); err != nil {
		return err
	}
	return t.Commit()
}

func (db *DB) Delete(key []byte) error {
	t := db.Begin()
	if err := t.Delete(key); err != nil {
		return err
	}
	return t.Commit()
}

func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return nil, errClosed
	}
	snapshot := db.nextTimestamp
	activeMT := db.memtable
	frozenMT := db.frozen
	ssts := db.sstables
	db.mu.RUnlock()

	return db.getAsOfSnapshot(key, snapshot, activeMT, frozenMT, ssts)
}

func (db *DB) GetAsOf(key []byte, snapshot uint64) ([]byte, error) {
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return nil, errClosed
	}
	activeMT := db.memtable
	frozenMT := db.frozen
	ssts := db.sstables
	db.mu.RUnlock()

	return db.getAsOfSnapshot(key, snapshot, activeMT, frozenMT, ssts)
}

func (db *DB) getAsOfSnapshot(
	key []byte,
	snapshot uint64,
	activeMT, frozenMT *memtable.Memtable,
	ssts []*sstable.Reader,
) ([]byte, error) {
	if v, op, found := activeMT.GetAsOf(key, snapshot); found {
		if op == memtable.OpDelete {
			return nil, ErrKeyNotFound
		}
		return v, nil
	}
	if frozenMT != nil {
		if v, op, found := frozenMT.GetAsOf(key, snapshot); found {
			if op == memtable.OpDelete {
				return nil, ErrKeyNotFound
			}
			return v, nil
		}
	}
	for _, r := range ssts {
		v, op, found, err := r.GetAsOf(key, snapshot)
		if err != nil {
			return nil, fmt.Errorf("db: get sstable: %w", err)
		}
		if found {
			if op == record.OpDelete {
				return nil, ErrKeyNotFound
			}
			return v, nil
		}
	}
	return nil, ErrKeyNotFound
}

func (db *DB) flushLocked() error {
	if db.opts.ExternalLog {
		return db.flushExternalLocked()
	}
	if db.memtable.Len() == 0 {
		return nil
	}

	r, id, err := db.writeMemtableLocked()
	if err != nil {
		return err
	}

	// WAL mode: the SSTable now holds everything the WAL did, so start a fresh
	// WAL. Holding db.mu for the whole flush means no commit can append between
	// the close and the reopen.
	if err := db.wal.Close(); err != nil {
		r.Close()
		return err
	}
	walPath := filepath.Join(db.dir, walFilename)
	if err := os.Remove(walPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		r.Close()
		return err
	}
	newWAL, err := wal.OpenWith(db.dir, wal.Options{SyncOnWrite: db.opts.SyncOnWrite})
	if err != nil {
		r.Close()
		return err
	}
	db.wal = newWAL
	db.installSSTableLocked(r, id)
	return nil
}

// flushExternalLocked is the ExternalLog flush — the only point at which applied
// state becomes durable in this store's own files. Until it completes, the
// caller's log is the sole durable copy of every entry applied since the
// previous flush. It writes, in order:
//
//  1. the memtable as an SSTable (skipped if no data was applied — e.g. only
//     read-index no-ops or membership changes),
//  2. the session dedup table,
//  3. applied.base — (appliedIndex, appliedTS) — the commit point.
//
// Recovery trusts applied.base alone, never an SSTable footer, and the caller
// replays its log from the index recorded there. A crash before step 3 therefore
// leaves the previous flush's index in force and the caller re-applies entries
// whose data may already sit in the new SSTable or whose sessions may already
// sit in the new sessions file. Both are harmless. Re-applying data writes the
// same keys at the same timestamps; reads, scans and compaction all collapse
// duplicate versions. And a sessions file that is already ahead dedups exactly
// the sessioned entries of this flush, whose data step 1 has made durable.
// Writing the sessions file before the SSTable would NOT be safe (dedup would
// skip data that is nowhere durable), and trusting a footer written in step 1
// would skip replaying entries whose sessions step 2 never recorded — hence this
// order and this commit point.
func (db *DB) flushExternalLocked() error {
	if db.memtable.Len() == 0 && db.durableIndex == db.appliedIndex {
		return nil // nothing applied since the last flush
	}
	if db.memtable.Len() > 0 {
		r, id, err := db.writeMemtableLocked()
		if err != nil {
			return err
		}
		db.installSSTableLocked(r, id)
	}
	if err := writeSessionsFile(db.dir, db.sessions); err != nil {
		return err
	}
	if err := writeAppliedBase(db.dir, db.appliedIndex, db.appliedTS); err != nil {
		return err
	}
	db.durableIndex = db.appliedIndex
	return nil
}

// writeMemtableLocked freezes the active memtable and writes it out as the next
// SSTable (stamped with the applied index), returning the opened reader and its
// id. db.frozen keeps the data visible to readers until installSSTableLocked
// swaps the reader in. Must hold db.mu (write).
func (db *DB) writeMemtableLocked() (*sstable.Reader, int, error) {
	db.memtable.Freeze()
	db.frozen = db.memtable
	db.memtable = memtable.New()

	id := db.nextID
	path := filepath.Join(db.dir, sstableFilename(id))

	w, err := sstable.NewWriter(path, db.frozen.Len(), db.appliedIndex)
	if err != nil {
		return nil, 0, err
	}

	var iterErr error
	db.frozen.Iterate(func(userKey, value []byte, op memtable.Op, ts uint64) bool {
		if err := w.Add(record.Op(op), userKey, value, ts); err != nil {
			iterErr = err
			return false
		}
		return true
	})
	if iterErr != nil {
		w.Abort()
		return nil, 0, iterErr
	}
	if err := w.Finish(); err != nil {
		return nil, 0, err
	}

	r, err := sstable.OpenReader(path)
	if err != nil {
		return nil, 0, err
	}
	return r, id, nil
}

// installSSTableLocked makes a just-written SSTable the newest layer and drops
// the frozen memtable it replaces. Must hold db.mu (write).
func (db *DB) installSSTableLocked(r *sstable.Reader, id int) {
	db.sstables = append([]*sstable.Reader{r}, db.sstables...)
	db.sstableIDs = append([]int{id}, db.sstableIDs...)
	db.frozen = nil
	db.nextID++
}

func (db *DB) signalCompact() {
	if db.closed || db.opts.DisableBackgroundCompaction {
		return
	}
	select {
	case db.compactCh <- struct{}{}:
	default:
	}
}

func (db *DB) Close() error {
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return nil
	}
	db.closed = true
	db.mu.Unlock()

	close(db.compactCh)
	<-db.compactDoneCh

	db.mu.Lock()
	defer db.mu.Unlock()
	var firstErr error
	// No flush on close in ExternalLog mode: a clean shutdown and a crash
	// recover the same way (replay the caller's log above the durable index),
	// so tests that close a node exercise the crash path.
	if db.wal != nil {
		if err := db.wal.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	for _, r := range db.sstables {
		if err := r.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (db *DB) NumSSTablesForTesting() int {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return len(db.sstables)
}

func (db *DB) FlushForTesting() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return errClosed
	}
	if err := db.flushLocked(); err != nil {
		return err
	}
	db.signalCompact()
	return nil
}

func (db *DB) NextTimestampForTesting() uint64 {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.nextTimestamp
}

// SetCommitOverride installs fn as the replacement for the single-node commit
// path: when set, Txn.Commit delegates the entire commit to fn (leader role
// under a replication cluster), which is responsible for conflict detection
// (via PrepareCommit), logging, replication, and apply. Intended to be called
// once during setup, before any commits. Passing nil restores the single-node
// path.
func (db *DB) SetCommitOverride(fn func(*Txn) error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.commitOverride = fn
}

// LastAppliedTS returns the timestamp of the most recently applied commit
// (nextTimestamp - 1). Note that under deferred apply the leader's
// nextTimestamp advances at PrepareCommit, ahead of the actual memtable
// apply; cluster catch-up is tracked by Raft log index, not this value.
func (db *DB) LastAppliedTS() uint64 {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.nextTimestamp == 0 {
		return 0
	}
	return db.nextTimestamp - 1
}

// RecoveredAppliedIndex returns the durable applied Raft index found at Open —
// the index the last flush recorded in applied.base. In ExternalLog mode the
// store holds exactly the state as of that index, and the caller seeds
// commitIndex/lastApplied with it and re-applies its committed log entries from
// the next index.
func (db *DB) RecoveredAppliedIndex() uint64 { return db.recoveredApplied }

// DurableAppliedIndex returns the highest applied Raft index whose effects are
// durable in this store's own files. In ExternalLog mode it advances only at a
// flush; entries applied after it live only in the memtable and in the caller's
// log, so the caller must not discard its log above this index.
func (db *DB) DurableAppliedIndex() uint64 {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.durableIndex
}

// FlushApplied makes everything applied so far durable in the store's own
// files — flushing the memtable to an SSTable if it holds data, then recording
// the applied index (see flushExternalLocked) — so DurableAppliedIndex catches
// up to the applied index and the caller can compact its log that far. It is a
// no-op if nothing was applied since the last flush. ExternalLog mode only.
func (db *DB) FlushApplied() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return errClosed
	}
	if !db.opts.ExternalLog {
		return errNotExternalLog
	}
	if err := db.flushLocked(); err != nil {
		return err
	}
	db.signalCompact()
	return nil
}

// SessionLastSeq returns the highest sequence number applied for a session (0 if
// none), for inspection and tests.
func (db *DB) SessionLastSeq(session []byte) uint64 {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.sessions[string(session)]
}

// SessionTable returns a copy of the full session dedup table (session -> last
// applied seq). It is the source of the exactly-once state shipped in an
// InstallSnapshot, so a follower rebuilt from the snapshot keeps deduplicating
// retries of commands applied before the snapshot.
func (db *DB) SessionTable() map[string]uint64 {
	db.mu.RLock()
	defer db.mu.RUnlock()
	out := make(map[string]uint64, len(db.sessions))
	for k, v := range db.sessions {
		out[k] = v
	}
	return out
}

// encodeSeq / decodeSeq (de)serialize a session sequence number carried in the
// OpCommit marker's Value field: 8 bytes, little-endian. A short/absent slice
// decodes to 0 (no sequence).
func encodeSeq(seq uint64) []byte {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], seq)
	return b[:]
}

func decodeSeq(b []byte) uint64 {
	if len(b) < 8 {
		return 0
	}
	return binary.LittleEndian.Uint64(b)
}

// appliedBase is the content of applied.base: the applied Raft index made
// durable by the last flush, and the highest commit timestamp applied as of it.
type appliedBase struct {
	index uint64
	ts    uint64
}

// readAppliedBase reads applied.base. An absent file means no flush yet (zero
// base). The format is index(8) | ts(8), little-endian; an 8-byte file (written
// before the timestamp was recorded) yields ts 0, and a file shorter than 8
// bytes is treated as absent. The atomic rename in writeAppliedBase means a torn
// file should never be observed.
func readAppliedBase(dir string) (appliedBase, error) {
	buf, err := os.ReadFile(filepath.Join(dir, appliedBaseFilename))
	if errors.Is(err, os.ErrNotExist) {
		return appliedBase{}, nil
	}
	if err != nil {
		return appliedBase{}, fmt.Errorf("db: read applied base: %w", err)
	}
	if len(buf) < 8 {
		return appliedBase{}, nil
	}
	b := appliedBase{index: binary.LittleEndian.Uint64(buf)}
	if len(buf) >= 16 {
		b.ts = binary.LittleEndian.Uint64(buf[8:])
	}
	return b, nil
}

// writeAppliedBase durably records (index, ts) as the last flush's commit
// point: write a temp file, fsync it, rename it over the real name, then fsync
// the directory so the rename survives a crash. The visible file is therefore
// always a complete old or new value. It must be the LAST file a flush writes
// (see flushExternalLocked): recovery resumes replay from the index recorded
// here, so it may only advance once the SSTable and sessions file it vouches for
// are durable.
func writeAppliedBase(dir string, index, ts uint64) error {
	var buf [16]byte
	binary.LittleEndian.PutUint64(buf[0:], index)
	binary.LittleEndian.PutUint64(buf[8:], ts)
	tmp := filepath.Join(dir, appliedBaseFilename+".tmp")
	f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("db: write applied base: %w", err)
	}
	if _, err := f.Write(buf[:]); err != nil {
		f.Close()
		return fmt.Errorf("db: write applied base: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("db: sync applied base: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("db: close applied base: %w", err)
	}
	if err := os.Rename(tmp, filepath.Join(dir, appliedBaseFilename)); err != nil {
		return fmt.Errorf("db: rename applied base: %w", err)
	}
	// The rename creates a new directory entry; fsync the dir to make it durable.
	if err := syncDir(dir); err != nil {
		return fmt.Errorf("db: sync dir after applied base: %w", err)
	}
	return nil
}

// syncDir fsyncs the directory so a just-created or renamed entry within it is
// durable. Mirrors the helpers in the wal and sstable packages.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// readSessionsFile loads the persisted session dedup table (map session->lastSeq)
// written at the last flush. A missing file yields an empty table (no flush yet, or
// no sessions). A torn/short file is treated as absent: the atomic rename
// guarantees a complete file otherwise, and the caller's log replay rebuilds
// post-flush state on top regardless. Format: count(8) then count records of
// keyLen(4)|key|seq(8).
func readSessionsFile(dir string) (map[string]uint64, error) {
	out := map[string]uint64{}
	buf, err := os.ReadFile(filepath.Join(dir, sessionsFilename))
	if errors.Is(err, os.ErrNotExist) {
		return out, nil
	}
	if err != nil {
		return nil, fmt.Errorf("db: read sessions: %w", err)
	}
	if len(buf) < 8 {
		return out, nil // torn; treat as absent
	}
	n := binary.LittleEndian.Uint64(buf)
	off := 8
	for i := uint64(0); i < n; i++ {
		if off+4 > len(buf) {
			return map[string]uint64{}, nil // torn: discard; log replay recovers post-flush
		}
		klen := int(binary.LittleEndian.Uint32(buf[off:]))
		off += 4
		if off+klen+8 > len(buf) {
			return map[string]uint64{}, nil
		}
		key := string(buf[off : off+klen])
		off += klen
		out[key] = binary.LittleEndian.Uint64(buf[off:])
		off += 8
	}
	return out, nil
}

// writeSessionsFile durably records the session dedup table at a flush, with the
// same crash-atomic temp -> fsync -> rename -> fsync-dir dance writeAppliedBase
// uses. Durability is required, not best-effort: once applied.base advances, the
// caller may discard the log entries that carried those sessions, so if this file
// were lost a restart would forget them and a post-restart retry could
// double-apply.
func writeSessionsFile(dir string, sessions map[string]uint64) error {
	buf := make([]byte, 8, 8+len(sessions)*24)
	binary.LittleEndian.PutUint64(buf, uint64(len(sessions)))
	for k, seq := range sessions {
		var hdr [4]byte
		binary.LittleEndian.PutUint32(hdr[:], uint32(len(k)))
		buf = append(buf, hdr[:]...)
		buf = append(buf, k...)
		var s [8]byte
		binary.LittleEndian.PutUint64(s[:], seq)
		buf = append(buf, s[:]...)
	}

	tmp := filepath.Join(dir, sessionsFilename+".tmp")
	f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("db: write sessions: %w", err)
	}
	if _, err := f.Write(buf); err != nil {
		f.Close()
		return fmt.Errorf("db: write sessions: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("db: sync sessions: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("db: close sessions: %w", err)
	}
	if err := os.Rename(tmp, filepath.Join(dir, sessionsFilename)); err != nil {
		return fmt.Errorf("db: rename sessions: %w", err)
	}
	if err := syncDir(dir); err != nil {
		return fmt.Errorf("db: sync dir after sessions: %w", err)
	}
	return nil
}

// ApplyEntry applies the committed Raft entry at index idx to the memtable and
// the session table, marking this DB applied through idx. It writes nothing to
// disk: the Raft log already holds the entry durably and remains its only
// durable copy until a flush folds it into an SSTable (DurableAppliedIndex), so
// after a crash the caller simply applies it again. Entries must arrive in log
// order — idx must be exactly the applied index + 1 — so a gap or a re-apply is
// an error rather than silent divergence. ExternalLog mode only.
//
// The entry must be a valid txn: data records (OpPut/OpDelete) terminated by an
// OpCommit marker, all sharing one timestamp. A bare OpCommit is a no-op (the
// read-index barrier's entry): it advances the applied index and changes no key.
func (db *DB) ApplyEntry(idx uint64, entry []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return errClosed
	}
	if err := db.checkApplyIndexLocked(idx); err != nil {
		return err
	}

	var recs []*record.Record
	offset := 0
	for offset < len(entry) {
		rec, n, err := record.Decode(entry[offset:])
		if err != nil {
			return fmt.Errorf("db: apply entry: decode at %d: %w", offset, err)
		}
		recs = append(recs, rec)
		offset += n
	}
	if len(recs) == 0 {
		return errors.New("db: apply entry: empty entry")
	}
	commitRec := recs[len(recs)-1]
	if commitRec.Op != record.OpCommit {
		return errors.New("db: apply entry: entry not terminated by OpCommit")
	}
	commitTS := commitRec.Timestamp
	for _, rec := range recs[:len(recs)-1] {
		if rec.Timestamp != commitTS {
			return fmt.Errorf("db: apply entry: ts mismatch (data %d vs marker %d)",
				rec.Timestamp, commitTS)
		}
		if rec.Op != record.OpPut && rec.Op != record.OpDelete {
			return fmt.Errorf("db: apply entry: unexpected op %d before marker", rec.Op)
		}
	}

	// Exactly-once dedup. A sessioned commit carries (session, seq) on its marker.
	// If this seq is already applied for the session, the entry is a duplicate
	// retry (e.g. the leader proposed it before applying the original): apply it as
	// a no-op that still advances the applied index, touching neither the memtable
	// nor the session table. This runs under db.mu, the same lock PrepareCommit
	// checks the table under, so the dedup and conflict views never disagree.
	session := string(commitRec.Key)
	var seq uint64
	if session != "" {
		seq = decodeSeq(commitRec.Value)
	}
	if session == "" || seq > db.sessions[session] {
		for _, rec := range recs[:len(recs)-1] {
			switch rec.Op {
			case record.OpPut:
				if err := db.memtable.Put(rec.Key, rec.Value, rec.Timestamp); err != nil {
					panic(fmt.Sprintf("db: memtable.Put on apply entry (ts=%d): %v", rec.Timestamp, err))
				}
			case record.OpDelete:
				if err := db.memtable.Delete(rec.Key, rec.Timestamp); err != nil {
					panic(fmt.Sprintf("db: memtable.Delete on apply entry (ts=%d): %v", rec.Timestamp, err))
				}
			}
		}
		if session != "" {
			db.sessions[session] = seq
		}
	}

	if commitTS >= db.nextTimestamp {
		db.nextTimestamp = commitTS + 1
	}
	if commitTS > db.appliedTS {
		db.appliedTS = commitTS
	}
	db.appliedIndex = idx

	if db.memtable.ApproximateSize() >= db.opts.MemtableSizeMax {
		if err := db.flushLocked(); err != nil {
			return fmt.Errorf("db: apply entry: flush: %w", err)
		}
		db.signalCompact()
	}
	return nil
}

// ApplyControlEntry applies a non-data Raft entry (e.g. a membership change) as a
// no-op at the state machine: it advances the applied index and changes no key.
// The control payload itself (the new configuration) is interpreted by the Raft
// layer, not here. Same ordering rule and mode requirement as ApplyEntry.
func (db *DB) ApplyControlEntry(idx uint64) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return errClosed
	}
	if err := db.checkApplyIndexLocked(idx); err != nil {
		return err
	}
	db.appliedIndex = idx
	return nil
}

// checkApplyIndexLocked enforces the replicated-apply contract: the store is in
// ExternalLog mode and idx is the very next index. Under it, the store's applied
// index and the caller's lastApplied move in lockstep, so recovery can resume
// replay exactly where the durable state ends. Must hold db.mu.
func (db *DB) checkApplyIndexLocked(idx uint64) error {
	if !db.opts.ExternalLog {
		return errNotExternalLog
	}
	if idx != db.appliedIndex+1 {
		return fmt.Errorf("db: apply index %d out of order (applied through %d)", idx, db.appliedIndex)
	}
	return nil
}

func (db *DB) registerTxn(t *Txn) {
	db.activeTxnsMu.Lock()
	db.activeTxns[t] = struct{}{}
	db.activeTxnsMu.Unlock()
}

func (db *DB) unregisterTxn(t *Txn) {
	db.activeTxnsMu.Lock()
	delete(db.activeTxns, t)
	db.activeTxnsMu.Unlock()
}

func (db *DB) computeWatermark() uint64 {
	db.mu.RLock()
	watermark := db.nextTimestamp
	db.mu.RUnlock()
	if watermark > 0 {
		watermark--
	}

	db.activeTxnsMu.Lock()
	for t := range db.activeTxns {
		if t.readSnap < watermark {
			watermark = t.readSnap
		}
	}
	db.activeTxnsMu.Unlock()

	return watermark
}

func (db *DB) hasCommitNewerThanLocked(userKey []byte, readSnap uint64) (bool, error) {
	if ts, found := db.memtable.NewestVersionTS(userKey); found && ts > readSnap {
		return true, nil
	}
	if db.frozen != nil {
		if ts, found := db.frozen.NewestVersionTS(userKey); found && ts > readSnap {
			return true, nil
		}
	}
	for _, r := range db.sstables {
		ts, found, err := r.NewestVersionTS(userKey)
		if err != nil {
			return false, err
		}
		if found && ts > readSnap {
			return true, nil
		}
	}
	return false, nil
}

func (db *DB) VersionCountForTesting(userKey []byte) int {
	db.mu.RLock()
	defer db.mu.RUnlock()
	count := db.memtable.VersionCountForTesting(userKey)
	if db.frozen != nil {
		count += db.frozen.VersionCountForTesting(userKey)
	}
	for _, r := range db.sstables {
		count += r.VersionCountForTesting(userKey)
	}
	return count
}

func sstableFilename(id int) string { return fmt.Sprintf("%06d.sst", id) }

func discoverSSTables(dir string) ([]int, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var ids []int
	for _, e := range entries {
		m := sstableNameRE.FindStringSubmatch(e.Name())
		if m == nil {
			continue
		}
		id, _ := strconv.Atoi(m[1])
		ids = append(ids, id)
	}
	sort.Ints(ids)
	return ids, nil
}
