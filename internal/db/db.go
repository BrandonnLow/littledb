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
	defaultMemtableSizeMax   = 4 * 1024 * 1024
	defaultCompactionTrigger = 4
)

var (
	ErrKeyNotFound = errors.New("db: key not found")
	ErrConflict    = errors.New("db: transaction conflict")
	errClosed      = errors.New("db: closed")
	sstableNameRE  = regexp.MustCompile(`^(\d{6})\.sst$`)
)

type Options struct {
	SyncOnWrite                 bool
	MemtableSizeMax             int64
	CompactionTrigger           int
	DisableBackgroundCompaction bool
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
	// to this DB (memtable + SSTables). The cluster passes it to ApplyEntry; the
	// DB stamps it into each SSTable footer at flush and into the post-flush WAL
	// base, so a restart can reconstruct lastApplied across a flush (the LSM
	// otherwise forgets log indices). Zero outside a replication cluster.
	appliedIndex uint64

	// recoveredApplied is the applied Raft index reconstructed at Open:
	// max(highest SSTable footer appliedIndex, walBase + replayed OpCommit
	// count). The cluster reads it via RecoveredAppliedIndex to seed
	// commitIndex/lastApplied on restart.
	recoveredApplied uint64

	// appliedTS is the highest commit timestamp actually applied to the
	// memtable. On a single-node DB and on followers it equals the highest
	// allocated commit (allocate and apply happen together), but on a
	// replication leader it lags nextTimestamp during an in-flight commit:
	// PrepareCommit allocates before the entry is applied. Begin derives a
	// txn's read snapshot from this, not nextTimestamp, so a snapshot never
	// claims to include a commit whose data is not yet visible.
	appliedTS uint64

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
	var maxFooterApplied uint64
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
		if r.AppliedIndex() > maxFooterApplied {
			maxFooterApplied = r.AppliedIndex()
		}
	}

	w, err := wal.OpenWith(dir, wal.Options{SyncOnWrite: opts.SyncOnWrite})
	if err != nil {
		for _, r := range ssts {
			r.Close()
		}
		return nil, err
	}

	mt := memtable.New()
	var buffer []*record.Record
	var walOpCommits uint64
	err = w.Scan(func(offset int64, rec *record.Record) error {
		if rec.Timestamp > maxTS {
			maxTS = rec.Timestamp
		}
		switch rec.Op {
		case record.OpPut, record.OpDelete:
			buffer = append(buffer, rec)
		case record.OpCommit:
			walOpCommits++
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

	if err != nil {
		w.Close()
		for _, r := range ssts {
			r.Close()
		}
		return nil, fmt.Errorf("db: replay wal: %w", err)
	}

	nextID := 1
	if len(sstIDs) > 0 {
		nextID = sstIDs[len(sstIDs)-1] + 1
	}

	// Reconstruct the applied Raft index across the flush boundary. walBase is
	// the appliedIndex folded into SSTables at the last flush; the current WAL
	// holds walOpCommits applied entries on top of it. The max() with the
	// footer watermark survives the flush<->WAL-removal crash window, where the
	// just-flushed entries sit in both the SSTable and the not-yet-removed WAL
	// and both expressions resolve to the same index.
	walBase, err := readAppliedBase(dir)
	if err != nil {
		w.Close()
		for _, r := range ssts {
			r.Close()
		}
		return nil, err
	}
	recoveredApplied := walBase + walOpCommits
	if maxFooterApplied > recoveredApplied {
		recoveredApplied = maxFooterApplied
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
		// appliedTS is maxTS, not just the highest WAL commit: maxTS folds in
		// the SSTable footers too, and after apply-all recovery everything
		// committed is applied. An uncommitted WAL tail (discarded on a torn
		// commit) can push maxTS past the true watermark, but harmlessly —
		// nothing is committed in that gap and the next commit allocates a
		// still-higher ts, so no conflict is missed or spuriously raised.
		appliedTS:        maxTS,
		appliedIndex:     recoveredApplied,
		recoveredApplied: recoveredApplied,
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
	if db.memtable.Len() == 0 {
		return nil
	}

	db.memtable.Freeze()
	db.frozen = db.memtable
	db.memtable = memtable.New()

	id := db.nextID
	path := filepath.Join(db.dir, sstableFilename(id))

	w, err := sstable.NewWriter(path, db.frozen.Len(), db.appliedIndex)
	if err != nil {
		return err
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
		return iterErr
	}
	if err := w.Finish(); err != nil {
		return err
	}

	r, err := sstable.OpenReader(path)
	if err != nil {
		return err
	}

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
	db.sstables = append([]*sstable.Reader{r}, db.sstables...)
	db.sstableIDs = append([]int{id}, db.sstableIDs...)
	db.frozen = nil
	db.nextID++

	// Record the new WAL's base index AFTER the new WAL exists. Ordering
	// (SSTable -> remove old WAL -> new WAL -> base) keeps recovery's
	// max(footer, base+walCount) correct in every crash window. writeAppliedBase
	// fsyncs the file and the directory: the base must be durable before any
	// post-flush commit can append to the new WAL, or recovery under-counts and
	// re-applies (see writeAppliedBase). This runs under db.mu before flushLocked
	// returns, so no apply can interleave before the base is on disk.
	if err := writeAppliedBase(db.dir, db.appliedIndex); err != nil {
		return err
	}
	return nil
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
	if err := db.wal.Close(); err != nil && firstErr == nil {
		firstErr = err
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

// RecoveredAppliedIndex returns the applied Raft index reconstructed at Open.
// The cluster seeds commitIndex/lastApplied with it on restart.
func (db *DB) RecoveredAppliedIndex() uint64 { return db.recoveredApplied }

// readAppliedBase reads the post-flush WAL base index (the appliedIndex folded
// into SSTables at the last flush). Absent file means no flush yet: base 0.
func readAppliedBase(dir string) (uint64, error) {
	buf, err := os.ReadFile(filepath.Join(dir, appliedBaseFilename))
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("db: read applied base: %w", err)
	}
	if len(buf) < 8 {
		return 0, nil // torn write; treat as absent (max() falls back to footer)
	}
	return binary.LittleEndian.Uint64(buf), nil
}

// writeAppliedBase records the new WAL's base index after a flush, durably:
// write a temp file, fsync it, rename it over the real name, then fsync the
// directory so the rename survives a crash. Durability is required, not
// best-effort: once a post-flush commit lands in the new WAL, recovery computes
// walBase + walOpCommits, so a lost base resolves BELOW the footer, max() picks
// the footer, and recovery UNDER-counts lastApplied — re-applying entries the
// data WAL already holds (the duplicate-apply bug). flushLocked calls this
// before returning, so the base is durable before any later apply can append to
// the new WAL.
func writeAppliedBase(dir string, base uint64) error {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], base)
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

// ApplyEntry durably records a committed entry's records to the WAL (identical
// framing to a single-node commit, so node WALs stay byte-identical) and
// applies them to the memtable, marking this DB applied through Raft index idx.
// The entry must be a valid txn: data records (OpPut/OpDelete) terminated by an
// OpCommit marker, all sharing one timestamp. Driven by the commit index, this
// is the only place a replicated write reaches the data WAL — so the WAL holds
// only committed state and recovery is commit-bounded by construction.
func (db *DB) ApplyEntry(idx uint64, entry []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return errClosed
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

	// Durably append every record (data + marker) to the WAL before touching
	// the memtable, exactly as a single-node commit does, so a follower's WAL is
	// byte-identical to the leader's.
	for _, rec := range recs {
		if _, err := db.wal.Append(rec); err != nil {
			return fmt.Errorf("db: apply entry: wal append: %w", err)
		}
	}

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

	if commitTS >= db.nextTimestamp {
		db.nextTimestamp = commitTS + 1
	}
	if commitTS > db.appliedTS {
		db.appliedTS = commitTS
	}
	if idx > db.appliedIndex {
		db.appliedIndex = idx
	}

	if db.memtable.ApproximateSize() >= db.opts.MemtableSizeMax {
		if err := db.flushLocked(); err != nil {
			return fmt.Errorf("db: apply entry: flush: %w", err)
		}
		db.signalCompact()
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
