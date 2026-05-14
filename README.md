# littledb

A small LSM-tree key-value database, built from scratch in Go for learning.

## Status

Under active construction. This is a learning project — not for production use.

## Roadmap

- **1** — Append-only KV store, write-ahead log, crash recovery
- **2** — SSTables, compaction, bloom filters
- **3** — MVCC, transactions, snapshot isolation
- **4** — Replication, leader election simulation (Raft)
- **5** — Visual debugger UI, event timeline, failure injection

## Design

See [DESIGN.md](./DESIGN.md) for architecture decisions and notes.

## Building

```sh
go build ./...
```

## License

MIT — see [LICENSE](./LICENSE).
