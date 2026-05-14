# Design Notes

A living document. Each entry records a decision, the alternatives considered,
and the reasoning. Future-me should be able to read this and understand why
past-me made a choice without re-deriving it.

## Goals

- **Correctness over speed.** No data loss on crash. Reads return what was written.
- **Learn by building.** Prefer the educational path over the optimal one.
- **Incremental.** Every layer must work end-to-end before the next is added.

## Non-goals

- SQL support
- Networked clients (single-process only)
- Production-grade performance
- Cross-platform file system quirks (Linux only)

## Decision log

(Decisions will be appended here as we make them.)
