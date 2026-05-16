# littledb

A small LSM-tree key-value database, built from scratch in Go for learning.

## Status

Under active construction. This is a learning project — not for production use.

## Roadmap

- **Phase 1** — Append-only KV store, write-ahead log, crash recovery
- **Phase 2** — SSTables, compaction, bloom filters
- **Phase 3** — MVCC, transactions, snapshot isolation
- **Phase 4** — Replication, leader election simulation (Raft)
- **Phase 5** — Visual debugger UI, event timeline, failure injection

## Design

See [DESIGN.md](./DESIGN.md) for architecture decisions and notes.

## Building

Verify the build and tests:

```sh
go build ./...
go vet ./...
go test ./... -race
```

## Usage

```sh
go build -o littledb ./cmd/littledb
./littledb -dir ./data
```

You'll see the prompt. Try:

```
> HELP
> PUT name John
> PUT lang go
> GET name
> GET lang
> DELETE name
> GET name
> EXIT
```

## License

MIT — see [LICENSE](./LICENSE).
