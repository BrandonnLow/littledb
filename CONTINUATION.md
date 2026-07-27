# littledb — Session Handoff / Continuation Guide

> **The membership-changes track is COMPLETE (Stages 1–6, all committed on `main`).**
> The authoritative write-up is **`DESIGN.md` Phase 8**. This file is now just a
> progress record + working-conventions handoff; it is safe to delete. The next
> roadmap item is the **network transport** (replace the in-process `ChannelTransport`)
> — see §6.

---

## 0. TL;DR

`littledb` is a from-scratch Go LSM-tree + Raft key/value store, **correctness-first,
zero external dependencies**. Phases 1–4 (storage engine, MVCC/txns, real Raft with
elections/durable-log/snapshots) pre-existed. This work added, in order, all
committed on `main`:

1. **Phase 5** — a from-scratch linearizability checker + fault-injection harness.
2. **Phase 6** — linearizable reads (read-index barrier).
3. A polished **history visualizer** (`lincheck.RenderHistoryFragment`).
4. **Phase 7** — client sessions / exactly-once dedup.
5. **Membership groundwork** — config-driven quorum, then configuration-in-the-log.

**The membership-changes track is COMPLETE.** All six stages are done and committed
(remove / add-with-learner / leadership-transfer + the durability items), each
validated under the checker, with the capstone soak running all three operations
concurrent with partition faults and config-carrying snapshots. **Phase 8 in
`DESIGN.md` is the full decision-log write-up.**

The checker is the oracle: build a stage, then run it under the checker/fault
injection to confirm it stays linearizable before moving on.

---

## 1. Environment & how to run — READ THIS FIRST

- **Repo:** `\\wsl.localhost\Ubuntu\home\user\projects\littledb` (WSL2 Ubuntu). Work
  from inside WSL for git/go.
- **Go toolchain quirk:** `go1.26.2` is installed at `/usr/local/go/bin/go`, but it's
  only on `PATH` in the user's *interactive* shell (via `.bashrc`). A non-interactive
  shell (`bash -lc`, or tool invocations) does NOT get it. From tooling, always
  prefix a clean PATH:
  ```bash
  export PATH="/usr/local/go/bin:/usr/bin:/bin"
  ```
- **Build / verify (must all pass before committing):**
  ```bash
  gofmt -l .            # must print nothing
  go build ./...
  go vet ./...
  go test ./... -race -count=1
  ```
- **Test flakiness:** the fault-injection soaks (`internal/cluster`, tests named
  `TestClusterConverges*`, `TestExactlyOnceCounterUnderCrashes`, the install tests)
  are timing-sensitive under the CPU contention of the *full* `-race` suite. Generous
  settle timeouts (15–30s) are already in place. An occasional settle/quiesce timeout
  or a "node stuck far behind" dump under `-count=2 -race` (all packages at once) is
  **load-flake, not a regression** — re-run, and check it passes standalone
  (`go test ./internal/cluster/ -race -count=3`). Do not chase it unless it fails
  *consistently* in isolation.
- **Git / commit conventions:**
  - Commit style: lowercase area-prefix subject (e.g. `membership: ...`, `phase7: ...`),
    a prose body explaining the *why*, and this footer on every commit:
    `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`
  - The user works **directly on `main`** (linear history — no feature branches).
  - **Commit via WSL**, not Windows git, to avoid CRLF/permission issues.
  - **Multi-line commit messages: write them to `.git/CCMSG` and `git commit -F .git/CCMSG`.**
    Do NOT pipe a heredoc through `wsl bash -c '...'` — parentheses and apostrophes in
    the body break the outer single-quoted shell (this bit us twice; one commit
    landed with a truncated message and had to be `--amend`ed).
  - Generated `*.html` (timeline dumps) are gitignored.

---

## 2. What's committed (most recent first)

| Commit | What |
|---|---|
| `67a52d8` | membership: **durable config+sessions across compaction & snapshots** — Stage 6 |
| `4e5e65d` | membership: **leadership transfer / TimeoutNow** — Stage 5 |
| `f3968e3` | membership: **add a server + learner catch-up** — Stage 4 |
| `0130417` | membership: **remove a server + disruption prevention** — Stage 3 |
| `f9f3e85` | membership: **configuration in the Raft log** (config entries) — Stage 2 |
| `156c798` | membership: **derive quorum from a per-node config** (groundwork) — Stage 1 |
| `24177b7` | **phase7: client sessions / exactly-once** dedup |
| `c784913` | lincheck: embeddable timeline **fragment** + design polish |
| `a415a7e` | **phase6: linearizable reads via read-index**, + the exactly-once finding |
| `36c41eb` | **phase5: from-scratch linearizability checker** + fault-injection harness |

`DESIGN.md` documents Phases 1–7 in the project's decision-log style
(decision → why → alternatives). Match that style for Phase 8.

---

## 3. Architecture map — the seams you'll touch

**`internal/db`** (the state machine). Key entry points the cluster drives:
- `PrepareCommit(txn) -> (entry, commitTS, err)` — conflict-check + allocate ts + build
  the encoded entry (data records + `OpCommit`). Also does **session dedup** (returns a
  nil entry for a duplicate) — under `db.mu`, consistent with the memtable.
- `ApplyEntry(idx, entry)` — the ONLY path a committed write reaches the data WAL;
  dedups a duplicate to a no-op. Keeps applied-index accounting in lockstep.
- `ApplyControlEntry(idx)` — applies a **non-data** entry (config change) as a no-op:
  writes one bare `OpCommit` to keep the applied-index count in lockstep, changes no key.
- `PrepareNoop()` — a bare-`OpCommit` entry for the read-index barrier.
- Session table (`map[session]uint64`) updated in `ApplyEntry`; persisted to a `sessions`
  file at flush; reconstructed on Open. `SetCommitOverride`, `RecoveredAppliedIndex`.

**`internal/cluster`** (Raft):
- `Node` — one replica. Holds `config` (current voting set), `baseConfig`, `configPath`,
  `peers` (the transport-level universe of node ids this process can reach — fixed),
  plus per-peer `nextIndex/matchIndex/respCh/replSignal` maps and one `replicateTo`
  goroutine per peer.
- `buildNode(...)` — constructs (not starts) a node; used by `New*` AND crash-rebuild.
  Reconstructs Raft state from disk (log, hard state, applied watermark, **config**).
- `proposeAndAwait(term, kind, entry)` — shared core of client commit / read barrier /
  (future) config change: append at `term`, replicate, wait for the leader's own apply.
  Adopts config on append when `kind == EntryConfig`.
- `commit(txn)` (the commit override), `readBarrier()`, `commitSessioned(...)`.
- Apply loop (`applyCommitted`) — routes `EntryData -> ApplyEntry`, `EntryConfig ->
  ApplyControlEntry`.
- Quorum from config: `majority()`, `votingPeers()`, `maybeAdvanceCommitLocked()` all
  derive from `config` (nil `config` falls back to `peers` for bare-Node tests).
- Config machinery in **`config.go`**: `encodeConfig/decodeConfig`, `readBaseConfigFile/
  writeBaseConfigFile`, `adoptConfigLocked`, `deriveConfigLocked`.
- Log: `raftlog.go` (`RaftLog`, entries carry `kind`), `raftlogfile.go` (durable frame
  now `term(8)|kind(1)|len(4)|bytes`; `raftLogMagic` bumped). `transport.go` defines
  `EntryKind {EntryData, EntryConfig}` and the wire `Entry{Term, Kind, Data}`.

**`internal/lincheck`** (the checker, zero-dep): `Check(history) -> Result{Linearizable,
Key, Witness}`, the Wing–Gong search, and `RenderHistoryFragment/RenderHistoryHTML`
(timeline visualizer). The **cluster harness** that drives real clusters under faults and
feeds histories to the checker lives in `internal/cluster/lincheck_harness_test.go`
(realistic *believed-leader* client — NOT the omniscient `currentLeader()` — plus a
`faultInjector` doing partitions + crash/restart).

---

## 4. Membership track — what's DONE (Stages 1–6, COMPLETE)

> The authoritative, decision-log write-up of all of this now lives in **`DESIGN.md`
> Phase 8** (per the user's steer: DESIGN.md is the durable record; this file is just
> progress/handoff). The summaries below are pointers.


**Stage 1 (`156c798`): quorum from a config field.** A per-node `config` (voting members,
incl. self), bootstrapped to all nodes. `majority()`, the vote-solicitation set
(`votingPeers()`), leader init, and commit-index all derive from it. Behavior-preserving.

**Stage 2 (`f9f3e85`): configuration in the Raft log.**
- `EntryKind` (`EntryData` / `EntryConfig`) travels on the wire, in memory, and on disk.
- **Adopt-on-append** (dissertation §4.1): a node adopts a config the instant it *appends*
  a config entry (leader in `proposeAndAwait`, follower in `handleAppendEntries`), before
  commit. A truncation reverts via `deriveConfigLocked` (latest config entry left in the
  log, else `baseConfig`).
- Applying a config entry = control no-op (`ApplyControlEntry`).
- Durable: `baseConfig` persisted in `raft/config`; current config = base + log config
  entries, rebuilt on restart.
- No config-change *API* yet, so `config` stays "all nodes" and the suite is unchanged.
- White-box test: `TestConfigEntryMechanism`, `TestConfigDrivesQuorum`
  (`internal/cluster/membership_test.go`).

**Everything a config change needs at the log/quorum level is already in place.** Stage 3
just adds the API that *creates* the first `EntryConfig`.

**Stage 3 (`0130417`): remove a server + disruption prevention.**
- **`Cluster.RemoveServer(id)`** (leader-routed, like `Put`) → node-side `removeServer`
  builds `newCfg = config \ {id}` and drives an `EntryConfig` through
  `proposeAndAwait`. Adopt-on-append means quorum shrinks immediately and the change
  commits under the *new* majority. Idempotent (re-removing an absent id is a no-op);
  refuses the last member (`ErrCannotRemove`).
- **One-change-at-a-time gate:** `latestConfigEntryCommittedLocked` refuses a new change
  while the newest config entry in the log is still uncommitted (`ErrConfigChangeInProgress`).
- **Self-removal (§4.2.2):** a leader removing itself commits `C_new` then
  `relinquishLeadershipLocked` (drops to follower at the *same* term). This required
  fixing **`maybeAdvanceCommitLocked` to count self only when self ∈ config** (via
  `inConfigLocked`) — else a leader outside its own config would commit `C_new` one ack
  early. That was a latent correctness gap the removal path exposed.
- **Disruption prevention (§4.2.3):** `Node.lastLeaderContact` is stamped on every
  AppendEntries/InstallSnapshot accepted from a current leader; `handleRequestVote`
  **ignores** a vote request within `ElectionMin` of it (no term adoption, no grant) —
  `withinLeaderStickinessLocked`. Companion candidate-side guard: `maybeStartElection`
  bails when `!inConfigLocked()` (a removed node that knows it's gone won't campaign).
  `Message.LeaderTransfer` (always false until Stage 5) bypasses the stickiness rule for
  `TimeoutNow`.
- **Decisions:** removed nodes keep running and still get heartbeats (harmless, and
  forward-compatible with Stage 4 learners — so replication is NOT restricted to config
  members). Config changes aren't deduped across a leadership change, so an
  `ErrMaybeCommitted` change may need an idempotent retry.
- **Tests (`internal/cluster/remove_test.go`):** `TestRemoveServerShrinksQuorum`
  (5→{0,1,2}, commits through {0,1} alone), `TestRemoveLeaderStepsDown`,
  `TestClusterLinearizableUnderMembershipChange` (lincheck stays linearizable while two
  servers are removed under load), the disruption-rule unit tests, and the
  guard/cannot-remove tests.

**Stage 4 (`f3968e3`): add a server + learner catch-up.**
- **`Cluster.AddServer(id, dir)`** → `spawnNode` (register transport, `buildNode` over a
  fresh dir with an **empty** bootstrap config so the joiner is a non-voting follower
  that won't campaign, append under `nodesMu`, start) then node-side `addServer`:
  - **Phase 1** (raftMu): `ensurePeerLocked(id)` as a learner; record `commitIndex` as the
    catch-up target.
  - **Phase 2** (NO commitMu): poll `matchIndex[id] >= target` or `ErrCatchupTimeout`.
    Client commits proceed — the learner isn't in the config, so it's never counted.
  - **Phase 3** (commitMu + one-change gate): propose `EntryConfig(config ∪ {id})`. It's
    caught up, so the new majority is immediately satisfiable (no availability gap).
- **Dynamic peer growth made race-safe:** `ensurePeerLocked` grows the per-peer maps at
  runtime. `nextIndex`/`matchIndex` stay raftMu-guarded; **`respCh`/`replSignal` get a new
  `replMu`** (leaf lock; raftMu→replMu) because `run()`/`signalReplicators` read them
  outside raftMu. Add-only maps; each replicator captures its signal channel once.
  `becomeLeaderLocked` calls `reconcilePeersLocked` so a follower that wins can reach a
  member added since its construction.
- **`buildNode` signature changed** to `(id, peers, bootstrapConfig, dir, opts, tr, cfg)`;
  `Cluster` retains `opts`/`cfg`; new ids must be the **next dense slot** so `nodes[i]`==node
  i; `AddServer` is retry-safe after a catch-up timeout (skips spawn, retries promotion).
- **Tests (`internal/cluster/add_test.go`):** `TestAddServerJoinsAndVotes` (3→{0,1,2,3};
  after partitioning the leader the survivors' 3-of-4 election *requires* the new node's
  vote), `TestAddServerToSingleNode` (1→{0,1}, learner phase matters most),
  `TestAddServerRejectsNonSequentialID`, `TestClusterLinearizableUnderServerAdd`.

**Stage 5 (`4e5e65d`): leadership transfer / `TimeoutNow`.**
- **`Cluster.TransferLeadership(target)`** → node-side `transferLeadership`: verify leader
  + target is a voting member, set **`transferring`** (gates `proposeAndAwait` so no new
  entries append), ensure `matchIndex[target] == lastIndex` (replicate the tail), send
  **`MsgTimeoutNow`**, wait for step-down. Aborts with `ErrTransferTimeout` / `ErrTransferTarget`.
- **Target side:** `handleTimeoutNow` calls `maybeStartElection(true)` — campaign NOW
  (bypass the randomized timer) with `Message.LeaderTransfer` set so the other voters
  bypass the min-election-timeout stickiness rule. The up-to-date §5.4.1 check is **not**
  bypassed, so a stale target just fails its catch-up wait rather than winning.
- `maybeStartElection` gained a `leaderTransfer bool` param; `stepDownLocked` /
  `becomeLeaderLocked` clear `transferring`.
- **Tests (`internal/cluster/transfer_test.go`):** `TestTransferLeadership` (0→2 in ~5ms,
  old leader steps down, writes flow through the new leader), `TestTransferLeadershipToNonVoter`
  (`ErrTransferTarget`), `TestTransferLeadershipToSelf` (no-op).

**Stage 6 (`67a52d8`): durable config+sessions across compaction & snapshots + validation.**
- **6a — compaction folds config into the base file.** `maybeCompactLocked`, before
  discarding the applied prefix, sets `baseConfig = configAsOfLocked(safe)` (latest config
  entry ≤ safe, else unchanged) and `writeBaseConfigFile`s it to `raft/config` **before**
  compacting the log (crash-safe either order). First time `raft/config` is written after
  bootstrap.
- **6b — InstallSnapshot carries config + sessions.** `Message` gains `Config []byte` +
  `Sessions []SessionKV`. Sessions ride in the DB dir (`BuildSnapshotDB` writes the sessions
  file into the staged DB → swaps in). Config is staged as `raft/install.config` (written
  before the marker); **both** `completeInstall` and `completeInstallIfPending` fold it into
  `raft/config` while the marker still guards the install (crash mid-completion re-runs).
  `db.SessionTable()` added; `configAsOfLocked` computes what the leader ships.
- **Race fix (Stage 4 fallout):** the replication path read `respCh[p]` each round, which a
  concurrent `ensurePeerLocked` could rehash under. Now captured once at replicator start
  (like the signal channel).
- **Tests:** `TestCompactionFoldsConfig` / `…NoConfigLeavesBase`; db-layer
  `TestSnapshotCarriesSessions`; end-to-end `TestSnapshotCarriesConfigAndSessions`
  (partitioned+removed follower learns config {0,1,2} + session table purely from the
  snapshot); capstone `TestMembershipUnderFaults` (all 3 ops + partitions + snapshots +
  workload, converges + linearizable).

---

## 5. Remaining plan — NONE (membership track complete)

All six stages are done (§4) and fully documented in **DESIGN.md Phase 8**. There is no
remaining membership work. The approach throughout was **single-server changes**
(dissertation §4.1), one add/remove at a time — never joint consensus — because any two
majorities of configs differing by one server overlap, which is what makes each
transition safe.

---

## 6. Other known limitations / future tracks (not blocking membership)
- **Session table:** no expiry (unbounded), client-chosen ids — fine for now.
- **Read-index:** the simple one-no-op-per-read variant; the heartbeat-only optimization
  is deferred.
- **After membership:** the remaining roadmap is (5) **network transport** (replace the
  in-process `ChannelTransport`; the `Transport` interface is the seam; `Message` embeds Go
  slices → needs wire serialization + addresses) + client/server split; (6) **optional
  depth** (pipelined replication, batching, block cache, observability — there's currently
  no logging/metrics).

---

## 7. Conventions & philosophy to preserve
- **Correctness over speed.** Every stage is validated by the checker before the next.
- **Zero external dependencies** (`go.mod` has none — keep it that way; the checker was
  written from scratch specifically to honor this).
- **DESIGN.md decision-log style:** decision → why → alternatives considered → lessons.
- The harness client routes to a **believed leader** (redirect on `ErrNotLeader`), NOT the
  omniscient `currentLeader()` — that's what lets the checker observe stale reads. Keep
  that distinction when writing membership tests.
- Match the surrounding code's dense doc-comment style (explain the *why* and the
  invariants, not just the *what*).

---

## 8. First action in the new session
The membership track is **complete** — nothing left to build here. Read `DESIGN.md`
(Phases 4–8) + `git log --oneline -10` for context. The next roadmap item (§6) is the
**network transport**: replace the in-process `ChannelTransport` (the `Transport`
interface is the seam) with wire serialization + addresses, then a client/server split.
This handoff file can be deleted once that track starts. Confirm the baseline is green
first:
```bash
export PATH="/usr/local/go/bin:/usr/bin:/bin"; cd ~/projects/littledb && go test ./... -race -count=1
```
