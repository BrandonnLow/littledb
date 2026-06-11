// Package db is the top-level littledb storage engine.
//
// MVCC reads. Each Put/Delete is assigned a logical timestamp from a monotonic counter;
// multiple versions of a userKey coexist; reads ask for "the version as of snapshot T."
//
// The single-version Get(k) internally reads as-of the current write-counter value,
// so it sees the latest committed version of k.
package db

import (
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
	defaultMemtableSizeMax   = 4 * 1024 * 1024
	defaultCompactionTrigger = 4
)

var (
	ErrKeyNotFound = errors.New("db: key not found")
	// ErrConflict is returned by Txn.Commit when a concurrent
	// transaction committed a write to one of this txn's keys between
	// Begin and Commit. Snapshot isolation with first-committer-wins:
	// the loser must Begin a fresh txn and retry. See Txn.Commit.
	ErrConflict   = errors.New("db: transaction conflict")
	errClosed     = errors.New("db: closed")
	sstableNameRE = regexp.MustCompile(`^(\d{6})\.sst$`)
)

// Replicator is an optional hook invoked during Txn.Commit, after the txn's
// records are durable in the WAl and before they are applied to the memtable.
// On a replication leader it ships the entry (the txn's encoded records —
// data records plus the OpCommit marker, all at one commitTS) to followers
// and blocks until a quorum has it; returning an error aborts the commit.
// A standalone DB leaves this nil and the commit path is unchanged.
type Replicator interface {
	Replicate(entry []byte, commitTS uint64) error
}

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

// DB is an MVCC LSM-tree key-value store.
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

	// nextTimestamp is the next logical timestamp to assign on a write.
	// Reads use its current value as their snapshot. Bumped on every
	// Put/Delete while holding the write lock; readers capture it
	// under the read lock alongside memtable/sstable pointers, which
	// ensures a Get can never see a counter value > any
	// not-yet-applied write.
	nextTimestamp uint64

	closed bool

	// replicator, if non-nil, is invoked on each commit between WAL
	// durability and memtable apply (leader role). Set via SetReplicator.
	replicator Replicator

	// activeTxnMu protects activeTxns. It's a leaf mutex — never
	// acquired while holding db.mu — so the watermark computation has
	// to sample db.nextTimestamp under db.mu.RLock first and then
	// iterate activeTxns under activeTxnsMu. The two-phase sampling
	// is benign for correctness; see computeWaterMark.
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

	w, err := wal.OpenWith(dir, wal.Options{SyncOnWrite: opts.SyncOnWrite})
	if err != nil {
		for _, r := range ssts {
			r.Close()
		}
		return nil, err
	}

	mt := memtable.New()
	// Buffer pending records until we see their Opcommit. Records
	// without a matching commit marker (an uncommitted partial txn at
	// the WAL tail) are silently discarded — that's the atomicity
	// guarantee. All records in `buffer` share a single timestamp,
	// because txns serialize at commit time under the write lock.
	var buffer []*record.Record
	err = w.Scan(func(offset int64, rec *record.Record) error {
		if rec.Timestamp > maxTS {
			maxTS = rec.Timestamp
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

	db := &DB{
		dir:           dir,
		opts:          opts,
		wal:           w,
		memtable:      mt,
		sstables:      ssts,
		sstableIDs:    sstIDsRev,
		nextID:        nextID,
		nextTimestamp: maxTS + 1, // first write gets at least 1
		activeTxns:    make(map[*Txn]struct{}),
		compactCh:     make(chan struct{}, 1),
		compactDoneCh: make(chan struct{}),
	}

	if !opts.DisableBackgroundCompaction {
		go db.compactLoop()
		db.signalCompact()
	} else {
		close(db.compactDoneCh)
	}

	return db, nil
}

// Put writes (key, value) at a freshly-allocated timestamp.
func (db *DB) Put(key, value []byte) error {
	t := db.Begin()
	if err := t.Put(key, value); err != nil {
		return err
	}
	return t.Commit()
}

// Delete writes a tombstone at a freshly-allocated timestamp.
func (db *DB) Delete(key []byte) error {
	t := db.Begin()
	if err := t.Delete(key); err != nil {
		return err
	}
	return t.Commit()
}

// Get returns the latest committed version of key. Equivalent to
// GetAsOf(key, current write counter).
func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return nil, errClosed
	}
	// Capture counter + state under the lock atomically.
	snapshot := db.nextTimestamp
	activeMT := db.memtable
	frozenMT := db.frozen
	ssts := db.sstables
	db.mu.RUnlock()

	return db.getAsOfSnapshot(key, snapshot, activeMT, frozenMT, ssts)
}

// GetAsOf returns the version of key visible at snapshot. Exposed for
// future transaction support and for testing MVCC semantics directly.
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

	w, err := sstable.NewWriter(path, db.frozen.Len())
	if err != nil {
		return err
	}

	var iterErr error
	db.frozen.Iterate(func(userKey, value []byte, op memtable.Op, ts uint64) bool {
		// memtable.Op and record.Op share byte values; the cast is safe.
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

	// At this point the SSTable (including its MaxTimestamp footer) is
	// fully durable on disk via Finish's fsync + rename + syncDir.
	// Only NOW is it safe to touch the WAL — if a crash had happened
	// before Finish returned, the WAL would still hold the records
	// and a subsequent Open would re-derive them.

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

// NextTimestampForTesting returns the current value of the timestamp
// counter (next-to-assign). Used by tests to capture a snapshot point
// between writes.
func (db *DB) NextTimestampForTesting() uint64 {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.nextTimestamp
}

// SetReplicator attaches r as the commit replication hook. Intended to be
// called once during setup, before any commits. Passing nil detaches it.
func (db *DB) SetReplicator(r Replicator) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.replicator = r
}

// LastAppliedTS returns the timestamp of the most recently applied commit
// (nextTimestamp - 1). On a leader this is the last allocated commit; on a
// follower it is the last commit received from the leader. Used to detect
// when followers have caught up.
func (db *DB) LastAppliedTS() uint64 {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.nextTimestamp == 0 {
		return 0
	}
	return db.nextTimestamp - 1
}

// ApplyReplicated applies a replicated commit entry — the leader's exact
// encoded bytes for one txn (data records followed by an OpCommit marker, all
// sharing one commitTS) — to this (follower) DB. It appends the records to
// the local WAL and applies them to the memtable at their embedded
// timestamps, advancing nextTimestamp to track the leader. No conflict check
// and no timestamp allocation: the leader already resolved the commit.
//
// The entry is validated before anything is written: a malformed entry (no
// trailing OpCommit, or records whose timestamps disagree with the marker)
// is rejected without touching the WAL.
func (db *DB) ApplyReplicated(entry []byte) error {
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
			return fmt.Errorf("db: apply replicated: decode at %d: %w", offset, err)
		}
		recs = append(recs, rec)
		offset += n
	}
	if len(recs) == 0 {
		return errors.New("db: apply replicated: empty entry")
	}
	commitRec := recs[len(recs)-1]
	if commitRec.Op != record.OpCommit {
		return errors.New("db: apply replicated: entry not terminated by OpCommit")
	}
	commitTS := commitRec.Timestamp
	for _, rec := range recs[:len(recs)-1] {
		if rec.Op != record.OpPut && rec.Op != record.OpDelete {
			return fmt.Errorf("db: apply replicated: unexpected op %d before marker", rec.Op)
		}
		if rec.Timestamp != commitTS {
			return fmt.Errorf("db: apply replicated: ts mismatch (data %d vs marker %d)",
				rec.Timestamp, commitTS)
		}
	}

	for _, rec := range recs {
		if _, err := db.wal.Append(rec); err != nil {
			return fmt.Errorf("db: apply replicated: wal append: %w", err)
		}
	}
	for _, rec := range recs[:len(recs)-1] {
		switch rec.Op {
		case record.OpPut:
			if err := db.memtable.Put(rec.Key, rec.Value, rec.Timestamp); err != nil {
				panic(fmt.Sprintf("db: memtable.Put on replicated apply (ts=%d): %v", rec.Timestamp, err))
			}
		case record.OpDelete:
			if err := db.memtable.Delete(rec.Key, rec.Timestamp); err != nil {
				panic(fmt.Sprintf("db: memtable.Delete on replicated apply (ts=%d): %v", rec.Timestamp, err))
			}
		}
	}

	if commitTS >= db.nextTimestamp {
		db.nextTimestamp = commitTS + 1
	}

	if db.memtable.ApproximateSize() >= db.opts.MemtableSizeMax {
		if err := db.flushLocked(); err != nil {
			return fmt.Errorf("db: apply replicated: flush: %w", err)
		}
		db.signalCompact()
	}
	return nil
}

// registerTxn adds t to the active set. Called from Begin.
func (db *DB) registerTxn(t *Txn) {
	db.activeTxnsMu.Lock()
	db.activeTxns[t] = struct{}{}
	db.activeTxnsMu.Unlock()
}

// unregisterTxn removes t from the active set. Called from Commit and
// Rollback. Idempotent — removing an absent key is a no-op.
func (db *DB) unregisterTxn(t *Txn) {
	db.activeTxnsMu.Lock()
	delete(db.activeTxns, t)
	db.activeTxnsMu.Unlock()
}

// computeWatermark returns the minimum read snapshot across all
// currently-active transactions, clamped above by the most recent
// committed timestamp (nextTimestamp - 1). Versions of any key with
// no observable snapshot at or above this watermark are GC candidates
// at compaction time.
//
// Two-phase sampling avoids holding both locks at once. Safety rests
// on two observations:
//   - A txn that finished between the phases is gone from our
//     iteration, but its reads have already returned to the caller
//     before unregisterTxn ran — they can't be invalidated by any
//     subsequent GC, so excluding it from the watermark is safe.
//   - A txn that began between the phases has readSnap ≥ the
//     nextTimestamp, so it can't pull our watermark below a value
//     that still protects it.
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

// hasCommitNewerThanLocked reports whether any committed write to
// userKey has timestamp > readSnap. Caller must hold db.mu in write
// mode. Used by Txn.Commit to detect write-write conflicts.
//
// The implementation walks the active memtable, the frozen memtable
// (if any), and every SSTable, asking each for the timestamp of
// userKey's newest version. Short-circuits on the first match.
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

// VersionCountForTesting returns the total number of stored versions
// of userKey across the active memtable, frozen memtable (if any),
// and all SSTables. Used to verify GC.
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
