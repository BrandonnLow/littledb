# littledb

A small LSM-tree key-value database, built from scratch in Go for learning.

## Status

Under active construction. This is a learning project — not for production use.

## Roadmap

- **Phase 1** — Append-only KV store, write-ahead log, crash recovery
- **Phase 2** — SSTables, compaction, bloom filters
- **Phase 3** — MVCC, transactions, snapshot isolation
- **Phase 4** — Replication and consensus (Raft: elections, durable log, snapshots)
- **Phase 5** — Correctness verification (linearizability checker, fault injection)
- **Phase 6** — Linearizable reads (read-index); client sessions next

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
