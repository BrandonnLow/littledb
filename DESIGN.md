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

### Phase 1 — Language: Why Go

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


### Phase 1 — Architecture: Bitcask-style

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

### Phase 1 — Record format and crash recovery contract

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

### Phase 1 — fsync on every write (default)

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

### Phase 1 — Directory fsync on file creation

On Linux, fsyncing a newly-created file does not make the directory
entry pointing to it durable. A crash right after `creat()` can leave
the file's contents on disk but no dirent, effectively losing the file.
`wal.Open` calls `syncDir(dir)` exactly once, when it just created the
log file, to close this window.

### Phase 1 — Concurrency: single `sync.RWMutex` at the DB level

`Put` and `Delete` take the write lock. `Get` takes the read lock only
long enough to look up the offset in the index, then releases it before
reading from disk. The WAL has its own internal `sync.Mutex` for the
underlying file. Reads do not block writes after the index lookup.

Verified race-free under `go test -race` with concurrent readers and
writer (`TestConcurrentReadersAndWriter`).

This is the simplest correct model. Sharding the lock or using
sync.Map would be a Phase 2+ optimization once we've measured contention.

### Phase 1 — Delete is idempotent and skips writing for missing keys

Deleting a missing key returns nil and writes no tombstone. The
alternative (always write a tombstone) wastes a record and an fsync on
a no-op. Replay still works correctly: there's nothing to undo.

This matches LevelDB/RocksDB semantics. The cost is that callers can't
distinguish "key didn't exist" from "key existed and was deleted" at
the API level. We accept that trade-off — it's the same one every real
KV store accepts.

### Phase 2 — Architecture transition: Bitcask → LSM tree

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

### Phase 2 — Record format reused at every layer

The same `record` package from Phase 1 encodes records in the WAL, in
SSTable data blocks, **and** in SSTable index entries. CRC checksumming
comes free everywhere, the codec is small and unit-tested in isolation,
and any future format change is a single concentrated decision.

The one cost is conceptual: index entries store `key = blockFirstKey`,
`value = [blockOffset:8][blockSize:8]` — a 16-byte structured value
inside the generic record. This convention is private to the sstable
package; no other code sees it.

### Phase 2 — Memtable: skiplist with tombstone encoding

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

### Phase 2 — SSTable layout: data blocks + sparse index + bloom + footer

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

### Phase 2 — Bloom filters: FNV-1a + Kirsch-Mitzenmacher

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

### Phase 2 — Memtable flush: synchronous, WAL truncate-by-delete

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

### Phase 2 — Compaction: size-tiered, background, max-input-ID output

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

### Phase 2 — Compaction ID/recency bug and its fix

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

### Phase 2 — DisableBackgroundCompaction (test-only option)

The compaction tests need to reproduce specific SSTable layouts
deterministically. The background goroutine racing manual
`CompactForTesting` calls makes that unreliable. `Options.
DisableBackgroundCompaction = true` skips the goroutine; production
code should never set it.

## Package layout

```
cmd/littledb/          CLI entry: flags, signals, plumbing only
internal/record/       Pure logic: encode/decode + CRC, no I/O
internal/wal/          Append-only log file: writer, scanner, recovery
internal/skiplist/     Sorted in-memory map (probabilistic balancing)
internal/memtable/     Tombstone-aware write buffer; RWMutex + freeze
internal/bloom/        FNV-1a + Kirsch-Mitzenmacher bloom filter
internal/sstable/      Immutable sorted file: data blocks + index + bloom
internal/db/           LSM orchestration: memtable + SSTables + WAL + compaction
internal/repl/         Command parser and dispatcher
```

Each `internal/` package can be tested in isolation. Dependencies flow
upward: `record` is the foundation; `wal`, `skiplist`, and `bloom`
depend only on it (or nothing); `memtable` wraps `skiplist`; `sstable`
combines `record` and `bloom`; `db` orchestrates everything; `repl`
depends on `db` via an interface so tests substitute a fake store.

## Open questions for Phase 3

- **Timestamp source.** Logical counter (simple, deterministic) vs.
  wall clock (interoperable but exposes us to clock skew). Probably
  logical.
- **MVCC encoding.** Append timestamp to the key (`key|ts`), or carry
  it as a separate field in the record format? Key suffix is simpler
  for the existing sorted SSTable code; a separate field is cleaner
  semantically.
- **Snapshot isolation vs. serializable.** Snapshot isolation is the
  classic LSM choice (LevelDB, RocksDB) and is what we'll implement.
  Write-skew is the known anomaly we accept.
- **Garbage collection of old versions.** When can compaction drop a
  version? Only when no active transaction has a read timestamp older
  than the next-newer version of the same key. Requires tracking
  oldest active read timestamp.
- **Group commit.** Phase 1 measured ~1100× fsync overhead. With
  transactions, batching multiple commits into one fsync is the
  obvious win. Probably introduce this alongside the transaction API.
