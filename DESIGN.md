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

## Package layout
cmd/littledb/      CLI entry: flags, signals, plumbing only
internal/record/   Pure logic: encode/decode + CRC, no I/O
internal/wal/      Append-only log file: writer, scanner, recovery
internal/db/       Top-level KV: WAL + in-memory index + RWMutex
internal/repl/     Command parser and dispatcher

Each `internal/` package can be tested in isolation. `record` is the
foundation everything builds on; `wal` depends only on `record`; `db`
depends on both; `repl` depends on `db` via an interface so tests can
substitute a fake store.

## Open questions for 2nd phase

- When to flush the memtable to an SSTable: by size? by record count? by time?
- Leveled compaction vs size-tiered: leveled has better read amplification
  but more code; size-tiered is simpler. Likely size-tiered first.
- Bloom filter false positive rate: 1%? Lower means bigger filters.
- SSTable block size: 4 KiB to match page cache, or larger for sequential reads?
