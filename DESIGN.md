# Design Notes

A living document. Each entry records a decision, the alternatives considered,
and the reasoning.

## Goals

- **Correctness over speed.** No data loss on crash. Reads return what was written.

## Non-goals

- SQL support
- Networked clients (single-process only, until Phase 4)
- Production-grade performance
- Cross-platform file system quirks (Linux only)

## Decision log

## Phase 1 — Append-only KV store, write-ahead log, crash recovery

### Language: Why Go

Considerations include: memory safety without GC stalls dominating writes, a usable concurrency model for reader/writer/compaction workflow, predictable file-and-fsync semantics, and a standard library covering the syscalls a database needs.

The hot path is **synchronous disk I/O with strict ordering**: each write appends a record to the WAL buffer, fsyncs, mutates the in-memory memtable, and returns. A background goroutine flushes a full memtable to a new SSTable (fsync + atomic rename + directory fsync); another merges old SSTables; readers traverse the memtable skiplist and binary-search SSTable indexes. None of this is CPU-bound — all of it is latency-sensitive and concurrent.

#### Three language properties follow directly:

1. **Memory safety.** The engine parses untrusted-shape inputs (malformed recovered WAL records, corrupted SSTable footers), so out-of-bounds reads, type confusion, and use-after-free must be caught without manual byte tracking. A use-after-free in the page cache or an uninitialised-buffer read in the WAL parser is a data corruption bug that silently return wrong bytes and spreads.

2. **First-class concurrency.** There are at least four independent mutexes with a documented acquisition order: a write lock serialising commits, a read lock for snapshot captures, the active-txn registry's mutex, and the memtable's RWMutex — plus a compaction goroutine coordinating with foreground writes over channels. A language whose concurrency is library APIs makes this verbose; one where it's syntax (goroutines, channels, `defer`) makes it expressible.

3. **Direct syscall access.** Correctness rests on `fsync(2)` and `rename(2)`, plus fsync of the parent directory after a rename. The language must expose these without an abstraction hiding whether data is actually durable.

#### Why Go fits:

**GC'd but predictable.** Go's concurrent mark-sweep GC targets sub-millisecond pauses — typically under 500µs. Against the fsync budget (100–300µs to NVMe, 5–10ms to spinning disk) that's invisible noise, so memory safety comes without latency spikes on the write path.

**Goroutines and channels match the architecture.** The compaction loop is one function: `for range db.compactCh { tryCompactOnce() }`. Signalling is a non-blocking send to a buffered channel that coalesces — `select { case db.compactCh <- struct{}{}: default: }`. The C (`pthread_cond_t` + flag) or C++ (`std::condition_variable`) equivalents are several times the code and surface area, and Java's `ExecutorService` doesn't compose with the `defer`-based cleanup the rest of the code uses.

**`defer` for cleanup.** Several paths must release a resource on every exit — closing a WAL file, unregistering a transaction, removing a temp SSTable on abort. `defer` makes these concise and panic-safe, versus `goto cleanup` chains in C, RAII guards in C++, or verbose `try-finally` in Java.

**Errors are values, not control flow.** Recovery code benefits most: WAL replay distinguishes `io.EOF` (clean end of log), `io.ErrUnexpectedEOF` (a record truncated by a crash mid-write), and `ErrCorrupt` (a CRC mismatch) as three separate conditions, handled with plain comparisons rather than exception unwinding that hides which path the code took. A truncated tail is expected and recoverable; a CRC mismatch is not — keeping them as distinct values keeps that decision explicit.

**Standard library covers it, zero dependencies.** `os.File` gives `Sync()` (fsync), `Write`/`WriteAt`, and `os.Rename`; `hash/crc32` with Castagnoli for CRC-32C; `encoding/binary` for fixed-endianness on-disk formats; `container/heap` for the compaction merge heap; `sort.Search` for the SSTable index. The whole `go.mod` has zero external imports — fewer surprises in GC, fsync, or scheduling behaviour, and less to reason about.

**Single static binary.** `go build` produces one file with no runtime dependency — no interpreter, no shared libraries, no install step. The CLI demo and REPL ship as a standalone executable, without a package manager or build system on the target machine.

**Race detector in the toolchain.** `go test -race` runs on the full suite every commit and catches ordering bugs. TSan and JVM equivalents exist but none is as low-friction as one flag.

**Right-granularity primitives.** `sync/atomic` and `sync.RWMutex` cover both cases: `nextTimestamp` lives behind the write lock, while lock-free reads use `atomic.Int64` (e.g. `blockReadCount` in the SSTable reader). In C you'd choose between pthreads and platform atomics and write the wrappers yourself.

#### Why not the alternatives

**Rust** — the closest competitor: no GC, memory-safe, strong concurrency guarantees, similar reach. The cost is authoring velocity. The MVCC encoding shares byte slices across module boundaries (the memtable stores `userKey || ^ts`, the SSTable rewrites it, compaction decodes and re-encodes), and in Rust each signature forces a choice between `&'a [u8]` (borrows that propagate up call sites), `Vec<u8>` (owning copies that materialise every encoding step), or `Arc<[u8]>` (atomic-refcount overhead in hot paths). Go lets the slices flow as `[]byte` and reclaims them via GC, keeping LSM semantics in the foreground. Production LSMs ship in Rust (TiKV, sled) — it's a productivity trade, not a correctness one.

**C++** — what RocksDB and LevelDB are built in. Against it here: build complexity (CMake, dependency management, header/impl duplication), opt-in memory safety (raw `new`/`delete` and pointer arithmetic are still legal), and concurrency as libraries rather than syntax. Expect increased line count plus explicit move-semantics, copy-constructor, and exception-safety decisions at every API boundary.

**Java (and C#)** — Cassandra proves the JVM can host a serious LSM. But GC pauses are larger and harder to bound (G1's stop-the-world phases run tens of milliseconds, dominating single-write latency on fast storage; ZGC helps but doesn't match Go's profile out of the box, and the .NET CLR shares the same pause disadvantage), `FileChannel`/`RandomAccessFile` are clunkier than `os.File`, there's no `defer` (`try-with-resources` doesn't cover multi-resource cleanup-on-error), and the Gradle/Maven loop is slower than `go test ./...`.

**Python/Ruby** — non-starters: the GIL makes reader, writer, and compaction goroutines contend for one interpreter slot, and per-operation overhead is higher.


### Architecture: Bitcask-style

The Phase 1 storage engine is Bitcask-style: one append-only log file,
an in-memory `map[string]int64` mapping each key to the offset of its
latest record, and reads that do a hashmap lookup followed by a single
disk seek.

**Alternatives considered.** B-tree (Bolt/SQLite/Postgres style) would
have been a single monolithic structure with no clean layering and no
incremental milestones. LSM-from-scratch (memtable + SSTables + compaction
on day 1) was the right destination but too much surface area at once.

**Why Bitcask first.** It's the smallest persistent KV design that
handles crash recovery correctly. The three components — record format,
WAL, top-level DB — split cleanly into testable packages and survive
into Phase 2: when we add SSTables, the WAL stays where it is and
becomes the durability layer in front of a memtable.

**Trade-offs accepted.** All keys must fit in RAM. The log grows forever
(no compaction until Phase 2). Reads pay one disk seek where a fully
RAM-resident KV would be all-memory.

### Record format and crash recovery contract

Each record on disk: `CRC32(4) | Op(1) | KeyLen(4) | ValueLen(4) | Key | Value`.
13-byte header, little-endian, CRC-32C (Castagnoli polynomial). The CRC
covers everything after itself.

**Why CRC-32C.** Hardware-accelerated on modern x86 via the SSE 4.2
CRC32 instruction. Same choice as RocksDB, ext4, btrfs, iSCSI. Not
cryptographic — it detects accidental corruption (bit flips, torn
writes), not adversarial tampering. Right tool for crash recovery.

**Why length-prefixed.** Variable-length keys and values without a
delimiter ambiguity. Reader knows exactly how many bytes to consume.

**The recovery contract is the three Decode error cases.** `io.EOF`
means clean end. `io.ErrUnexpectedEOF` means a record's body was cut
off mid-write (the file ended before the declared lengths could be
read). `ErrCorrupt` means a full record's bytes are present but the
CRC does not match. The WAL treats the latter two identically:
truncate the file to the last good offset and fsync. After Open
returns, the log contains only validated records.

A fourth error case, `ErrInvalidOp`, is reserved for situations where
the CRC matched but the op byte is unknown. This is not a torn-write
signal — it's either a bug in the encoder or real on-disk corruption,
so recovery refuses to start rather than silently dropping data.

### fsync on every write (default)

`Put` and `Delete` issue `fsync(2)` before returning. This is the
durability contract: when the call returns nil, the record is on disk.

**Measured cost on dev hardware (Ryzen 9 8940HX, WSL2 ext4 on .vhdx):**

- With fsync: ~200 writes/sec (~5ms per fsync)
- Without fsync: ~222,000 writes/sec (~4µs CPU + page cache)
- Ratio: ~1,100x

The gap is real and is the entire reason group commit, batched writes,
and async durability exist as concepts. We expose this via
`Options.SyncOnWrite` so the cost can be measured rather than guessed.

Default is true. The unsafe mode is for benchmarks only in Phase 1; in
Phase 3 we'll revisit with group commit.

### Directory fsync on file creation

On Linux, fsyncing a newly-created file does not make the directory
entry pointing to it durable. A crash right after `creat()` can leave
the file's contents on disk but no dirent, effectively losing the file.
`wal.Open` calls `syncDir(dir)` exactly once, when it just created the
log file, to close this window.

### Concurrency: single `sync.RWMutex` at the DB level

`Put` and `Delete` take the write lock. `Get` takes the read lock only
long enough to look up the offset in the index, then releases it before
reading from disk. The WAL has its own internal `sync.Mutex` for the
underlying file. Reads do not block writes after the index lookup.

Verified race-free under `go test -race` with concurrent readers and
writer (`TestConcurrentReadersAndWriter`).

This is the simplest correct model. Sharding the lock or using
sync.Map would be a Phase 2+ optimization once we've measured contention.

### Delete is idempotent and skips writing for missing keys

Deleting a missing key returns nil and writes no tombstone. The
alternative (always write a tombstone) wastes a record and an fsync on
a no-op. Replay still works correctly: there's nothing to undo.

This matches LevelDB/RocksDB semantics. The cost is that callers can't
distinguish "key didn't exist" from "key existed and was deleted" at
the API level. We accept that trade-off — it's the same one every real
KV store accepts.

## Phase 2 — SSTables, compaction, bloom filters

### Architecture transition: Bitcask → LSM tree

The Phase 1 `map[string]int64` index is gone. Writes now go through the
WAL (durability) and into an in-memory **memtable** (sorted, supports
ordered iteration). When the memtable crosses a size threshold it's
**frozen** and written out as an immutable, sorted **SSTable** file.
Reads walk a layered stack: active memtable → frozen memtable (if a
flush is in progress) → SSTables newest-first. The first hit wins,
which is how "latest write wins" semantics fall out of the design.

**Why the change.** Bitcask requires all keys to fit in RAM and never
reclaims overwritten/deleted values. The LSM tree fixes both: only the
memtable (a bounded slice of recent writes) lives in RAM, and
compaction periodically merges SSTables to drop superseded keys.

**The public API didn't change.** `Open / Put / Get / Delete / Close`
have the same shape. The internal redesign was invisible to callers,
which is partly why Phase 1's clean package boundaries paid off.

### Record format reused at every layer

The same `record` package from Phase 1 encodes records in the WAL, in
SSTable data blocks, **and** in SSTable index entries. CRC checksumming
comes free everywhere, the codec is small and unit-tested in isolation,
and any future format change is a single concentrated decision.

The one cost is conceptual: index entries store `key = blockFirstKey`,
`value = [blockOffset:8][blockSize:8]` — a 16-byte structured value
inside the generic record. This convention is private to the sstable
package; no other code sees it.

### Memtable: skiplist with tombstone encoding

The memtable is a skiplist (randomized levels, no rebalancing, simpler
than a balanced tree) wrapped with an `RWMutex` and a size accountant.
Tombstones are encoded as a 1-byte `Op` prefix on every stored value —
`0x01` = live, `0x02` = tombstone. The skiplist itself is a dumb sorted
map; the memtable layer owns the deleted-vs-live concept.

**Get returns three states** — `(value, OpPut, true)`, `(nil, OpDelete,
true)`, or `(nil, _, false)` — so callers (the DB) can distinguish
"deleted here, don't fall through" from "not in this memtable, check
older layers."

**Freeze flips an immutable flag.** Subsequent writes return `ErrFrozen`;
reads keep working. This is what makes the active-vs-frozen handoff
during flush atomic from the API's perspective.

### SSTable layout: data blocks + sparse index + bloom + footer

```
DATA   (sorted records, grouped into ~4 KB blocks)
INDEX  (one record per block: firstKey, blockOffset, blockSize)
BLOOM  (serialized bloom filter bytes)
FOOTER (40 bytes: indexOffset, indexSize, bloomOffset, bloomSize, magic)
```

The reader reads the 40-byte footer first, verifies magic
(`0x21424445_4C4C494C` = `"LILLEDB!"` little-endian), loads the index
and bloom into memory, and serves Gets as: bloom check → binary-search
index → read one block → linear scan. Constant-ish per Get regardless
of file size.

**Why 4 KB blocks.** Matches the OS page size, so each block read
populates one page-cache page cleanly. Bigger blocks waste reads;
smaller blocks bloat the index.

**Records never split across blocks.** A record larger than `blockSize`
produces its own oversized single-record block. Trade-off accepted:
slightly variable block sizes, but no record-spanning logic in the
reader.

**Atomic creation via `.tmp` + rename + dir fsync.** Readers never
observe a partial SSTable. Unlike the WAL — where a torn tail is
*expected* and silently truncated on recovery — corrupt or truncated
SSTables surface errors loudly. By construction, they shouldn't exist.

### Bloom filters: FNV-1a + Kirsch-Mitzenmacher

10 bits per key, target ~1% false-positive rate. Hash with FNV-1a
(deterministic, no seed, hardware-friendly), split the 64-bit output
into two 32-bit halves, and derive `k = 7` hash positions as
`h1 + i × h2 (mod m)`. Measured FPR on dev hardware: 0.78% — well
inside theoretical.

**Why determinism matters.** The filter is serialized into the SSTable
footer and reloaded on Open. Same keys must produce identical bytes
across runs, which means no random seeding.

**The integration into Get is one line.** Before any block read, ask
the bloom whether the key could be present. On a miss-heavy workload
this is roughly a 100× speedup; on a hit-heavy one it's effectively
free.

### Memtable flush: synchronous, WAL truncate-by-delete

When the memtable crosses `MemtableSizeMax` (default 4 MB), the writer
that triggered the threshold does the flush before returning:
freeze the memtable, write it as an SSTable, then **close the WAL,
delete the file, reopen empty**.

**Why delete-and-reopen instead of rotating to a numbered WAL.** We
hold the DB write lock for the entire flush, so no Append can race
with the close/delete/reopen sequence. The new memtable is empty;
there are no records dependent on a WAL we just removed.

**Cost accepted.** Put latency spikes during a flush. Phase 6 (or
Phase 3 if it becomes urgent for transactions) will move the flush to
a background goroutine and use proper WAL rotation with numbered
files.

### Compaction: size-tiered, background, max-input-ID output

A background goroutine watches a `compactCh`. When the SSTable count
reaches `CompactionTrigger` (default 4), it picks the oldest N
SSTables, merges them with a k-way heap (newest source wins on
duplicate keys), and writes a single output SSTable. Because we always
compact the oldest tail, no older SSTable exists for tombstones to
mask — they're dropped during the merge.

**Lock discipline.** A separate `compactMu` serializes the work; the
slow merge runs without the DB lock so reads continue normally. Install
is short and atomic: re-acquire `db.mu`, verify the tail IDs match what
we captured (defensive), swap the slice, release.

**Compacted readers are not explicitly closed.** Their files are
unlinked, but the FDs stay alive while any in-flight Get holds them.
GC finalizers on `*os.File` close them once no references remain.
Production systems would refcount; this is a Phase 2 simplification.

### Compaction ID/recency bug and its fix

**Originally:** the merged output got a fresh ID via `db.nextID++`,
which is always higher than any input. In memory, the merged file
correctly went to the oldest slice position. On disk, its filename had
the highest ID. On reopen, `discoverSSTables` sorts filenames by ID
ascending and treats the largest as newest — so the merged file
**floated to the newest position**, shadowing the genuinely-newer
SSTable that was flushed after compaction.

**Repro** (trigger=4): flush `k=v1`, three filler flushes, flush
`k=v2` (five SSTables). Compact the oldest four. In-process
`Get(k) = v2` (correct: in-memory slice still has v2 first). Close and
reopen. `Get(k) = v1` (wrong: merged file with v1 now has the highest
ID, so it's "newest" on reopen). A second compaction folds v2 into the
merge and destroys it permanently.

**Fix.** The merged file inherits `max(inputIDs)`, not a fresh ID, and
`nextID` is not bumped. `sstable.Writer`'s atomic rename replaces the
file that owns that ID; on Linux, any reader still holding that old
inode keeps a valid FD (the inode is detached, not deleted). The
delete loop skips the reused ID to avoid wiping out the merged output.

**Lesson.** Two ordering systems — in-memory slice position, on-disk
filename — that *imply* each other but aren't *enforced* to agree. The
fix makes them agree by construction. The regression test in
`TestCompactionPreservesNewerSSTableAfterReopen` is the permanent
guard. Caught by external review, not by my own testing — a reminder
that property tests on a single live process can miss bugs that only
manifest across a restart.

### DisableBackgroundCompaction (test-only option)

The compaction tests need to reproduce specific SSTable layouts
deterministically. The background goroutine racing manual
`CompactForTesting` calls makes that unreliable. `Options.
DisableBackgroundCompaction = true` skips the goroutine; production
code should never set it.

## Phase 3A — MVCC, transactions, snapshot isolation

Phase 3 transformed the engine from single-value-per-key to multi-version
concurrency control. Reads slice the database at a logical timestamp; writes
append new versions rather than overwriting; transactions buffer locally and
commit atomically with first-committer-wins conflict detection.

### Record format change

Header grew from 13 to 21 bytes to hold a `Timestamp uint64`:

```
CRC32(4) | Op(1) | Timestamp(8) | KeyLen(4) | ValueLen(4) | Key | Value
```

Field-offset constants exposed in `record` package (`OffsetCRC`, `OffsetOp`,
`OffsetTimestamp`, `OffsetKeyLen`, `OffsetValueLen`) so the WAL doesn't peek
at byte ranges directly; a `HeaderLengths(hdr)` helper returns key/value
lengths. CRC covers everything after itself — a flipped bit anywhere in
op/timestamp/lengths/payload surfaces as `ErrCorrupt`.

A third op was added: `OpCommit = 3` (atomicity marker for multi-write txns;
no key/value, just a timestamp).

Format is **not backward-compatible** with Phase 2's 13-byte header. Clean
cut accepted; a production system would version the on-disk format.

### MVCC key encoding (`internal/mvcckey`)

User keys are paired with descending-timestamp suffixes:

```
Encode(userKey, ts) = userKey || bigEndian(^ts)
```

Sort order: userKey ascending, then timestamp **descending** within userKey.
The bitwise NOT and big-endian together give descending byte-wise sort for
descending numeric timestamps.

What this buys: all versions of any userKey K cluster together with the
newest first. A read "as of snapshot T" seeks `Encode(K, T)`; SeekGE lands
on the first encoded key ≥ that target — which by descending-ts order is
the newest version with `ts ≤ T`.

### Snapshot semantics

**A snapshot S sees the newest version of each userKey with `ts ≤ S`.**

- `nextTimestamp` is the next-to-assign value; one greater than the last assigned.
- "Right now" as a snapshot = `nextTimestamp - 1` (the last-committed value).
- A brand-new DB has `nextTimestamp = 1`, so snapshot `0` represents "before
  any writes." `GetAsOf(k, 0)` always returns not-found.
- Defensive guard at the use site: if `nextTimestamp == 0`, `readSnap` stays 0.
  Protects against any future bug that might wrap the counter to 0.

### Memtable (rewritten)

Skiplist now stores **encoded keys**, not raw user keys. API:

```go
Put(userKey, value, ts) error
Delete(userKey, ts) error
GetAsOf(userKey, snapshot) → (value, op, found)
NewestVersionTS(userKey) → (ts, found)   // for conflict detection
Iterate(fn(userKey, value, op, ts))      // for flush
```

Multiple versions of one userKey coexist as separate skiplist nodes.
`GetAsOf` is one `SeekGE(Encode(userKey, snapshot))` plus a userKey-match
check on the landed node. `NewestVersionTS` seeks to
`Encode(userKey, ^uint64(0))` — the smallest possible encoded key for that
userKey — which lands on its newest version.

The skiplist itself gained `SeekGE(target) *Node` and `Node.Next()`.

### SSTable (rewritten)

`Writer.Add(op, userKey, value, ts)`; internally encodes for the data block;
tracks `maxTimestamp` seen. Reader gains `GetAsOf` and `NewestVersionTS`
mirroring the memtable shape.

**Critical invariant: the bloom filter hashes `userKey`, not the encoded
key.** Otherwise a lookup at any timestamp other than an exact previous
write would falsely report "absent" because every version would have its
own bloom entry. `TestBloomMatchesAnyTimestamp` is the regression guard.

**Block search uses a two-block scan.** Binary search finds the largest
block whose `firstKey ≤ target`. The answer is either in that block, or —
if target sorts past every key in it — at the start of the next block. The
Reader scans up to two consecutive blocks per lookup.

**Footer grew 40 → 48 bytes.** New `MaxTimestamp uint64` slot between
bloom-size and magic. Used by the DB to derive its initial counter value
on Open.

### DB timestamp counter

`nextTimestamp uint64` on the DB struct, protected by `db.mu`. Bumped on
every Put/Delete inside the write lock, **before** the WAL append.

Why this ordering matters: any reader that captures counter value `T` under
the read lock has, by virtue of the same lock acquisition, also seen every
write with `ts < T` applied to the memtable. The counter and storage state
are in lockstep from a reader's perspective.

On `Open`, the counter is restored from
`max(WAL max-ts, SSTable footer MaxTimestamp) + 1`. Monotonic across restarts.

### Transactions (`internal/db/txn.go`)

```go
type Txn struct { /* ... */ }
db.Begin() *Txn
tx.Get(key) ([]byte, error)
tx.Put(key, value) error
tx.Delete(key) error
tx.Commit() error    // may return ErrConflict
tx.Rollback() error
```

**Concurrency contract**: one goroutine per Txn (undefined behavior
otherwise). Different goroutines holding different Txns are safe.

- **Begin** captures `readSnap = nextTimestamp - 1` under the read lock,
  allocates a local write buffer, registers in the active-txn set. No WAL
  or memtable activity.
- **Get** checks the local buffer first (read-your-own-writes), then falls
  through to `db.GetAsOf(key, readSnap)`.
- **Put/Delete** stash into the local map with a defensive byte-copy of
  the user's value.
- **Commit** runs the conflict check, allocates one `commitTS`, writes all
  data records + one `OpCommit` marker to the WAL, applies to the memtable.
- **Rollback** clears the buffer, marks finished, unregisters.

Single-statement `db.Put` / `db.Delete` route through `Begin → Put/Delete →
Commit`. Same atomicity guarantee, two fsyncs per write (data + commit
marker) instead of one. Acceptable trade for the unified code path;
optimizable later with batched fsync.

### Atomicity via OpCommit

The atomicity problem: a multi-write transaction can't be atomic via a
single record, and a sequence of fsynced records leaves a crash window
where some records are durable and others aren't.

Solution: a sentinel `OpCommit` record at the same timestamp as the txn's
data records. The WAL recovery loop buffers data records by timestamp; an
`OpCommit` drains the buffer to the memtable; if the scan reaches EOF with
un-drained buffered records, they're an uncommitted partial txn —
discarded.

Three crash windows, all recoverable:

1. **Crash mid-data-records**: a torn record stops `WAL.Scan`, no `OpCommit`
   ever seen for that ts, buffer discarded.
2. **Crash between last data record and commit marker**: scan reaches EOF
   cleanly with un-drained records, discard.
3. **Crash after commit marker**: full sequence on disk, replay normally.

The recovery loop enforces that all records inside one txn share one
timestamp; a mismatch is treated as corruption.

### Memtable failure at commit time

After the `OpCommit` is durable, the txn is committed *from disk's
perspective*. If `memtable.Put` or `memtable.Delete` fails in the apply
loop, in-memory state and durable state diverge — the next reader sees an
inconsistent view.

The only documented memtable failure is `ErrFrozen`, which is structurally
impossible while we hold `db.mu` (freezing happens inside `flushLocked`,
same lock). If it ever does fire, we **panic**: a process restart re-reads
the durable log and reconstructs the correct state. Continuing in-memory
would be strictly worse.

### Active-txn registry and watermark

`activeTxns map[*Txn]struct{}` on the DB, protected by its own `sync.Mutex`
named `activeTxnsMu`. `Begin` inserts; `Commit` (via deferred call) and
`Rollback` remove.

**Lock ordering invariant**: `activeTxnsMu` is a leaf mutex. No code path
holds it while waiting for `db.mu`. The watermark computation respects
this by sampling in two phases:

1. `db.mu.RLock` → sample `nextTimestamp` → release.
2. `activeTxnsMu.Lock` → iterate the registry → release.

Combined into `min(oldestActiveReadSnap, nextTimestamp - 1)`.

The two-phase sampling is benign:

- A txn that **finished** between the phases is gone from our iteration,
  but its reads have already returned to the caller before `unregisterTxn`
  ran — they can't be invalidated by any subsequent GC, so excluding its
  `readSnap` is safe.
- A txn that **began** between the phases has `readSnap ≥ nextTimestamp`
  we sampled in phase 1, so it can't pull our watermark below a still-safe
  value.

### First-committer-wins conflict detection

`ErrConflict` is returned by `Commit` when a concurrent committer wrote
one of our keys between our Begin and our Commit.

The check, for each key in the write set: "has anyone committed a write
to K with `commitTS > our readSnap`?" Implementation walks active memtable
→ frozen memtable → SSTables, asking each `NewestVersionTS(userKey)`.
Short-circuits on the first match.

**Placement matters**: the check must be inside the write lock and
**before** allocating `commitTS`. Outside this critical section, a
competing committer could slip in between the check and the allocation.
Inside, allocation immediately follows the check; the WAL writes follow
that; all atomic relative to other committers.

A conflicted txn writes no records to the WAL — recovery never sees
abandoned writes.

### Version GC during compaction

The watermark drives version garbage collection. A version `V@T` for
userKey K is observable only by snapshots in `[T, T_next)`, where `T_next`
is the timestamp of K's next-newer version (or +∞ for K's latest). For V
to be reachable by some active or future snapshot, `T_next` must exceed
the watermark.

Per userKey, walking newest-first (encoded-key-ascending):

- **Newest version**: always emitted, *unless* it's a tombstone with
  `ts ≤ watermark` AND we're at the bottom of the LSM. In that case the
  entire userKey vanishes (no observable snapshot would distinguish
  "deleted" from "never existed").
- **Older version**: emitted only if `prevTS > watermark` AND bottom of
  LSM. Otherwise dropped.

The cascade for the dropped-head-tombstone case is automatic: setting
`prevTS = ts ≤ watermark` makes the older-version branch drop every
remaining version in the same userKey group.

### The bottomOfLSM invariant

Both GC paths are conditional on `bottomOfLSM`. Dropping a record
mid-layer could expose stale data: a tombstone might be masking even-older
versions in lower SSTables; an older version might be shadowing
even-older-still versions.

**Derived from data, not structure**, at the call site:

```go
bottomOfLSM := slices.Min(inputIDs) == slices.Min(db.sstableIDs)
```

"Our inputs include the oldest SSTable in the LSM." Today's size-tiered
strategy always merges the tail, so this evaluates to `true`; the explicit
derivation keeps the answer correct for any future strategy that compacts
a middle range.

**Latent bug caught late in Phase 3**: the original code guarded only the
head-tombstone-drop with `bottomOfLSM`, not the older-version-drop. The
bug never manifested because the call site hardcoded `true`, but a
mid-layer strategy would have silently dropped versions and exposed stale
data. Now both paths are guarded, the flag is derived from inputs, and
two direct tests against `compactSSTables` exercise the mid-layer path.

### What is deliberately not in Phase 3

- **Phantom prevention** — SI does not prevent phantoms. A txn reading
  "all keys with prefix foo" can be undermined by a concurrent insert.
- **Write skew prevention** — SI permits write skew. Would require
  Serializable Snapshot Isolation (SSI) with read-tracking.
- **Batched WAL fsync** — each WAL append fsyncs individually. A 3-write
  txn = 4 fsyncs (3 data + 1 commit marker).
- **Range scans / iterators** — point lookups only. Range scans + MVCC
  snapshot filtering are Phase 4 work.
- **Active-txn registry growth bound** — abandoned txns (Begin without
  Commit/Rollback) leak slots and depress the watermark indefinitely.
  Documented contract; not enforced.

### Package layout (as of end of Phase 3)

```
internal/
  bloom/      Bloom filter (unchanged from Phase 2)
  mvcckey/    Encode/Decode for userKey+timestamp (new in Phase 3)
  skiplist/   Skiplist with SeekGE and Node.Next (extended)
  memtable/   MVCC-aware memtable (rewritten)
  sstable/    MVCC-aware SSTable with MaxTimestamp footer (rewritten)
  record/     Record format with Timestamp + OpCommit (extended)
  wal/        WAL using record.HeaderLengths (minor)
  db/         DB engine, Txn, conflict detection, compaction+GC (extended)
  repl/       Interactive REPL (unchanged)
```

## Phase 3B — Range scans / iterators

Point lookups (`Get`/`GetAsOf`) became ordered range scans. Motivated two
ways: Phase 4's snapshot install streams a key range to replicas, and range
iteration was a genuine gap in the user-facing API.

### API

```go
db.Scan(start, end []byte, snapshot uint64) (*Iterator, error)
db.ScanNow(start, end []byte) (*Iterator, error)   // snapshot = nextTimestamp-1
it.Next() bool        // advance; false at end-of-range or on error
it.Key() / it.Value() // current live pair; valid until the next Next()
it.Err() / it.Close()
```

Pull-based (not callback) so Phase 4 can pull a batch, ship it, pull more.
Forward-only; bounds are half-open `[start, end)` on user keys; nil start =
from the first key, nil end = through the last. `Txn.Scan` and reverse
iteration are deliberately deferred.

### Architecture: per-layer cursors → merge → per-userKey collapse

Each layer exposes a cursor yielding records in encoded-key order (userKey
ascending, timestamp descending). A k-way min-heap merges them by encoded
key, breaking ties by layer recency and dropping any record whose encoded
key duplicates the one just emitted (newer layer wins — same rule as
compaction, kept as cheap insurance against future cross-layer overlap).

On top of the merged stream, a per-userKey collapse walks each userKey's
versions newest-first, takes the first with `ts ≤ snapshot` as the visible
version, emits it if a put, and skips the whole userKey if it's a tombstone
or has no visible version. This is `compactSSTables`'s structure minus GC,
plus a snapshot filter.

The memtable layers are materialized: `Memtable.RangeSnapshot` copies the
requested key range into a slice under a brief `RLock`, so a long scan never
holds the memtable lock and never races the skiplist (which is not
safe for concurrent read-during-write). The memtable is size-bounded, so the
copy is bounded. SSTable layers stay lazy: `sstable.Reader.NewIterator`
binary-searches to the start block and reads blocks on demand, stopping at
the end bound.

### Snapshot consistency

`Scan` captures the active memtable, frozen memtable, and SSTable set under
`db.mu.RLock`, then iterates lock-free. The snapshot bounds visibility within
those layers — a write committed after `Scan` returns lands at a higher
timestamp and is filtered out — so the iterator sees a stable, point-in-time
view. A flush or compaction between capture and iteration is harmless: the
memtable snapshot is already copied, and swapped-out readers stay live
through the iterator's reference.

### Reader lifetime

`Iterator.Close()` releases the per-layer cursor buffers but does NOT close
the underlying SSTable readers — they are shared with the live DB and with
other scans. A reader that compaction swaps out of `db.sstables` mid-scan
stays readable through the iterator's reference (Linux keeps the inode behind
the open FD; nothing explicitly closes a swapped-out reader). The accepted
cost: a long scan during heavy compaction pins old SSTable FDs open until GC
finalizers reap them — the same finalizer simplification noted in Phase 2,
stretched to scan timescales. Proper refcounting is deferred to Phase 4,
where snapshot install provides a concrete consumer to design it against.

## Phase 4 — Replication and simulated Raft

### In-process multi-node, fixed leader, quorum commit

A single-node DB replicated across N nodes: one fixed leader streams every
committed txn to followers and returns to the client once a quorum has it.
No election, no failover yet. "RPC" is a channel send; the Raft
invariants are transport-independent, so the in-process transport is a real
implementation, not a mock.

#### The commit seam

The single-node commit pipeline had no place to interpose replication —
Txn.Commit did conflict-check, commitTS allocation, WAL append, and memtable
apply as one closed sequence under db.mu. Raft needs replication to happen
*between* WAL durability and memtable visibility: an entry must be on a
majority before the leader makes it readable. We cut that seam with an
optional `Replicator` hook, invoked in Commit after the WAL appends and before
the memtable apply, receiving the txn's encoded records (data + OpCommit) and
the commitTS. A standalone DB leaves the hook nil; the commit path is byte-for
-byte unchanged. The hook runs while db.mu is held, so commits replicate one
at a time — not maximally concurrent, but unambiguously correct.

#### Leader and follower

The leader allocates commitTS and does conflict detection as before — both
remain leader-only. Its Replicator ships the entry to all followers and
returns once a majority (itself plus floor(N/2) follower acks) has applied it.
Followers apply via `ApplyReplicated`, which validates the entry, appends the
leader's exact bytes to the local WAL, applies them to the memtable at the
leader's timestamps, and advances nextTimestamp to track — no conflict check,
no timestamp allocation. Because followers append the leader's exact bytes,
their WALs are byte-for-byte identical to the leader's (until a flush truncates
them independently).

#### Ordering and acks

Commits serialize under the leader's db.mu, so at most one replication is in
flight; the leader matches acks by commitTS and discards late acks from a
prior commit. The channel transport gives per-node FIFO delivery, so followers
apply entries in commit order. The leader counts its own copy toward the
majority and needs floor(N/2) follower acks; a follower applies before it
acks, so on the client's return the leader plus a majority hold the write.

### Deferred apply via commit index

Earlier implementation conflated three Raft steps: appending to the log, knowing
an entry is committed, and applying it to the state machine. Pull them apart.

- **db primitives.** `PrepareCommit` (conflict-check + allocate commitTS + build
  entry, no side effects), `AppendToLog` (durable WAL append, no memtable),
  `ApplyEntry` (memtable apply at embedded timestamps, no WAL). `Txn.Commit`
  delegates the whole commit to an installed override on a replicated leader,
  else runs the single-node path. The `Replicator` hook is retired.
- **Per-node Raft state.** In-memory `log` (1-based index), `commitIndex`,
  `lastApplied`, and an apply loop applying `lastApplied+1..commitIndex`.
- **Follower path.** `MsgReplicate` → `AppendToLog` + log-append + ack (no
  apply). `MsgCommit` → advance `commitIndex` (bounded by `lastIndex`) → apply
  loop applies. Followers never apply uncommitted entries.
- **Leader commit.** Under `commitMu` (serialized end-to-end so conflict
  detection always sees the prior commit applied): `PrepareCommit` →
  `AppendToLog` + log-append → broadcast `MsgReplicate` → wait quorum of
  append-acks → advance `commitIndex` → apply locally → broadcast `MsgCommit` →
  wait own `lastApplied` ≥ entry index → return.
- **Locking.** `raftMu` (log/indices) and the store's `db.mu` are never held
  simultaneously; applies run outside `raftMu`. `commitMu` is the outermost
  leader lock.
- **Guarantee on return.** A quorum has the entry logged and the leader has
  applied it; minority followers apply slightly later (on their `MsgCommit`).
- **Deferred.** Crash recovery still replays the whole WAL (apply-all); a
  persisted commit index is left. Term is constant 1, in memory. Per-follower
  nextIndex/matchIndex, prevLog checks, and slow-follower catch-up are next.
