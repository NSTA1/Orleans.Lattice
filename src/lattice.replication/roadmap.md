
# Orleans.Lattice.Replication Roadmap

Feature plan for the `Orleans.Lattice.Replication` package — a cross-cluster replication library layered on top of `Orleans.Lattice`. This roadmap follows the upgrade order recommended in [`docs/replication-design.md` §9](../../docs/lattice.replication/replication-design.md), with the `MultiSiteManufacturing` sample's pull-over-HTTP / gRPC-push pipeline treated as the reference "what to promote, what to fix" artifact.

> **Feature IDs.** Items are numbered `R-XXX` to avoid collision with the core library's `F-XXX` space (tracked in [`../lattice/roadmap.md`](../lattice/roadmap.md)).
>
> **Package boundary.** Everything here ships in a new `Orleans.Lattice.Replication` assembly. Public API lives under `Orleans.Lattice.Replication`; internal grains/types under `Orleans.Lattice.Replication.{Area}`. The package has a single upstream dependency: `Orleans.Lattice`.
>
> **Non-goals for the initial release.** Cross-cluster Orleans cluster membership, multi-region storage provisioning, conflict UIs, user-facing admin tooling. This package is the on-the-wire replication engine only.

---

## Guiding principles

Each phase below has an explicit "what the sample gets wrong" entry from the design doc it is fixing. Don't carry forward the sample's shortcuts:

- **No thread-local cycle-break.** `RequestContext["lattice.replay"]` is fragile across async boundaries — origin is durable metadata, not ambient state.
- **No ship-time value read.** Capture the mutation at commit time; readers of a replog entry never re-read the primary.
- **No post-merge LWW-by-bytes.** The wire carries CRDT deltas for recognised primitives; opaque bytes are the fallback, not the default.
- **No host-level outgoing-call filter.** Replication is produced by the grain at commit time, so value capture is atomic with the write.
- **No reminder-cadence pull for hot paths.** Push transport with backoff is the baseline; HTTP pull is retained only for bootstrap / low-frequency paths.

Preserve what the sample got right (design doc §8): per-peer HLC cursor, advance-strictly-on-ack, don't-replicate-the-replog, per-tree opt-in + per-key filter, janitor as a separate grain.

---

## 🔲 Phase 0 — Scaffolding

Minimum viable package + hosting surface so every subsequent phase has a place to land and is testable end-to-end.

- [ ] **R-000 — Package scaffolding and DI surface**
  New `src/lattice.replication/Orleans.Lattice.Replication.csproj` targeting `net10.0` with a project reference to `Orleans.Lattice`. Public DI entry point `ISiloBuilder.AddLatticeReplication(Action<LatticeReplicationOptions>)` registers core grains, the change-feed subscription, and a no-op `IReplicationTransport`. Public options type `LatticeReplicationOptions` mirrors the layout of `LatticeOptions` (per-tree where it makes sense). Test project `test/lattice.replication/Orleans.Lattice.Replication.Tests.csproj` with a two-site cluster fixture (two two-silo "sites" joined by an in-memory `LoopbackTransport`) so every subsequent phase has an integration harness.

- [ ] **R-001 — Baseline per-peer metrics**
  Static `Meter "orleans.lattice.replication"` with day-one instruments: `entries_behind`, `bytes_behind`, `consecutive_errors`, `last_contact_seconds`, `ship_duration`, `apply_duration`. Every subsequent phase adds tags/instruments to the same meter. Wired into `R-000`'s test fixture so convergence tests can assert on counters rather than side effects.

---

## 🔲 Phase 1 — Value-at-write-time change feed *(design §1c, §5, §7)*

Fixes the three highest-cost sample shortcuts: ship-time reads, post-write best-effort append, and host-level outgoing-call filter.

- [ ] **R-010 — Commit-time change capture**
  Grain-side capture inside `ShardRootGrain` / `BPlusLeafGrain` write paths (via a `Orleans.Lattice`-side hook the core library exposes — tracked as a dependency on the core roadmap). Each mutation emits a fully-formed `ReplogEntry` containing the op (`Set` / `Delete` / `DeleteRange`), the value *or* delta, the HLC, the target key, the tree id, and the origin cluster id (R-020). The entry is persisted before the write returns. Replaces the sample's `Outgoing*CallFilter` host-level append.

- [ ] **R-011 — Single-writer per-shard WAL journal**
  Per-shard write-ahead log (`IReplogShardGrain` keyed by `{treeId}/{shardIndex}`) is the single source of truth for replication. Mutations append-then-apply (not apply-then-best-effort-append); the WAL-append is the commit point. `ReplogEntry` carries op + full value *or* typed delta. Removes the sample's read-amplification (`primary.GetAsync(origKey)` in `ShipOneBatchAsync`) and the "writes coalesced between append and ship collapse silently" / "false-delete on intervening delete" data-loss bugs.

- [ ] **R-012 — Per-tree opt-in and per-key filter**
  `LatticeReplicationOptions.ReplicatedTrees` (names) + `LatticeReplicationOptions.KeyFilter` (`Func<string, bool>` or declarative prefix set) — parity with the sample's `mfg-part-crdt` label-only split. The filter runs on the *producer* side so non-replicated mutations never touch the WAL.

- [ ] **R-013 — `IChangeFeed` public surface**
  Subscriber API for in-process consumers (tests, bridges, custom transports): `IChangeFeed.Subscribe(treeName, cursorHlc)` returning `IAsyncEnumerable<ReplogEntry>`. The outbound ship loop in later phases is one consumer among many.

- [ ] **R-014 — Strict-vs-best-effort append modes**
  `LatticeReplicationOptions.AppendMode = Strict | BestEffort` (default `Strict`). In `Strict` mode, WAL failure surfaces to the caller of `ILattice.SetAsync` / etc.; in `BestEffort` mode, failures are logged and the write succeeds (current sample behaviour — retained but no longer default). Removes the sample's "writer swallows storage failures" silent-drop hazard.

---

## 🔲 Phase 2 — Origin-stamped HLC + idempotent apply *(design §1b, §2, §5)*

Makes cycle-break durable, enables exactly-once apply, unlocks transitive topologies (A → B → C).

- [ ] **R-020 — Origin cluster id in mutation metadata**
  New `[Id]` slot on `LwwValue` / `LwwEntry` for `OriginClusterId` (string, default `LatticeReplicationOptions.ClusterId`). Wire-compatible: missing field on legacy state decodes to `null` and is treated as "local". The field is authored once at commit time (R-010) and propagates through every merge / drain / snapshot path.

- [ ] **R-021 — Durable origin-based cycle-break**
  Outbound ship filters WAL entries where `OriginClusterId == self`. Replaces the sample's `RequestContext["lattice.replay"]` thread-local. Robust across async boundaries, streams, saga compensations, and any apply path that doesn't originate from the inbound call chain.

- [ ] **R-022 — Preserve source HLC on apply**
  Receiver stores `SourceHlc` (not a locally-stamped fresh HLC) in the entry's metadata. Enables transitive replication — B → C can propagate an A-origin write with A's HLC intact — and deterministic resolution for any vector-clock-based conflict scheme.

- [ ] **R-023 — Per-origin high-water-mark table**
  Receiver-side `{(tree, originClusterId) → lastAppliedHlc}` persistent map. Inbound apply checks HWM before merging; re-delivery of `(origin, hlc)` is a no-op. Turns at-least-once delivery into at-most-once apply — required for correctness under typed CRDT counters/sets (phase 3).

- [ ] **R-024 — HWM-driven snapshot integration point**
  The HWM table is the handoff contract for the bootstrap protocol (phase 5): a newly-bootstrapped peer starts incremental replication from `hwm[(tree, origin)]` and the HWM guarantees the handoff is exactly-once across the snapshot/incremental boundary.

---

## 🔲 Phase 3 — Typed CRDT deltas *(design §1a)*

The real CRDT payoff — active-active convergence for the primitives the library ships, rather than cross-cluster LWW-on-bytes that silently loses concurrent set adds / counter increments.

- [ ] **R-030 — Delta contract for core primitives**
  Typed delta records for each replicable primitive the core library ships: `LwwRegisterDelta` (value + HLC + origin), `OrSetDelta` (adds + removes with dot context), `PnCounterDelta` (per-replica +/- increments), `VersionVectorDelta` (vector merge). Each is `readonly record struct`, `[GenerateSerializer][Immutable]` with a stable `[Alias]` constant in a new `ReplicationTypeAliases` class.

- [ ] **R-031 — Delta emission at commit**
  Grain-side commit path (R-010) emits a typed delta when the value type is a recognised CRDT primitive; falls through to opaque-byte `LwwRegisterDelta` for schemaless `byte[]`. Reversal path on the receiver applies via the primitive's `Merge(delta)` operation — **never** via `SetAsync`.

- [ ] **R-032 — Opaque-bytes fallback**
  Unrecognised value types continue to replicate as opaque-byte LWW with origin-stamped HLC (phase 2). Explicitly documented as "won't converge under concurrent updates — if you need convergence use one of the shipped CRDTs". Tooling surface: `ILattice.GetReplicationMode(key)` reports the mode actually used per key.

- [ ] **R-033 — Active-active convergence test matrix**
  Chaos-category integration tests (`[Category("Chaos")]`, excluded from inner-loop runs per repo convention): concurrent adds/removes on an OR-Set across 3+ clusters with random network partitions converge to the same set; concurrent increments on a PN-Counter across N clusters sum correctly; LWW register under concurrent writes picks the highest `(hlc, origin)` lexicographic pair on every cluster.

---

## 🔲 Phase 4 — Push transport + binary framing + compression *(design §3, §10)*

Latency drops from reminder-cadence (~60 s) to sub-second; bandwidth improves ~2× from dropping JSON base64; deduplication makes no-op re-sets free.

- [ ] **R-040 — `IReplicationTransport` abstraction**
  Pluggable seam. Implementations: `LoopbackTransport` (test fixture, R-000), `HttpTransport` (sample's pull path, lifted), `GrpcPushTransport` (sample's push path, lifted). The library ships all three; hosts pick via options. The outbound ship loop is transport-agnostic.

- [ ] **R-041 — Orleans-serializer binary framing**
  Drop JSON-over-HTTP as the *canonical* format (it remains supported on `HttpTransport` for debuggability behind a flag). Default wire format is the Orleans serializer applied to a versioned envelope. ~33% inline bandwidth win on `byte[]` payloads.

- [ ] **R-042 — gRPC streaming push transport**
  Lift the sample's `GrpcPushTransport`: long-lived `PushBatches(stream Batch)` RPC per `(peer, tree)`, reconnect-with-bounded-exponential-backoff-and-jitter, advance cursor strictly on ack. Sender flushes when the local WAL (R-011) advances rather than on reminder ticks.

- [ ] **R-043 — Batch-boundary compression**
  Optional `gzip` / `zstd` (configurable via options, default `None`) at the batch envelope boundary. Measured in R-033's chaos suite to verify CPU cost vs. bandwidth gain on realistic payloads.

- [ ] **R-044 — Content-hash dedup**
  Sender sends batch manifest of `(key, contentHash, hlc)` triples; receiver pulls only missing content-hashes. Matters when the same value is re-set (idempotent writes from upstream retry logic). Disabled by default — opt in when measurement shows payload re-send rate justifies the round-trip.

- [ ] **R-045 — Coalesced per-peer cursor checkpointing**
  Sample persists the cursor on every batch via `WriteStateAsync`. Coalesce to every K batches or T seconds, with a durability checkpoint on graceful shutdown. Cuts storage writes on the ship path by an order of magnitude.

- [ ] **R-046 — Standard transport security**
  mTLS for `GrpcPushTransport`, bearer-token-with-rotation for `HttpTransport`. Retire the sample's `X-Replication-Token` shared-secret header. Integrates with the standard Orleans transport security story — does not invent a new auth scheme.

---

## 🔲 Phase 5 — Snapshot / bootstrap protocol *(design §4)*

Required before any production deployment. Without it, a peer whose cursor falls behind the oldest replog entry can never catch up.

- [ ] **R-050 — `ISnapshotProvider` abstraction**
  Sender-side: streaming `as-of` HLC range scan over the primary tree (not the replog). Backed by the core library's stateful cursor grain (`F-033` from the core roadmap) so snapshots are resumable on silo failover. Chunked by key range so snapshot streams don't monopolise a single shard.

- [ ] **R-051 — Receiver-side bootstrap state machine**
  States: `RequestingSnapshot` → `ApplyingSnapshot` → `IncrementalHandoff` → `LiveIncremental`. On snapshot completion the receiver pins the snapshot's as-of HLC `h` in its per-origin HWM (R-023) and switches to incremental from `h`. The HWM dedupe in R-023 makes the handoff exactly-once regardless of snapshot/incremental overlap.

- [ ] **R-052 — Auto-bootstrap trigger**
  Fires when the inbound apply path detects the sender's cursor is older than the sender's oldest WAL entry ("fall-off-the-log"). Emits a `PeerFellOffLog` event (tied to observability in phase 6) and transitions the peer to `RequestingSnapshot`. No operator intervention required for the common case.

- [ ] **R-053 — Operator-driven re-seed**
  Explicit admin API: `ILatticeReplication.RequestSnapshotAsync(peerId, treeName, CancellationToken)` for scheduled re-seeds (new peer joining, bandwidth-constrained initial sync, post-disaster re-bootstrap). Rate-limited by the sender.

---

## 🔲 Phase 6 — GC, DLQ, back-pressure, observability *(design §4–§6)*

Ops polish and production-grade reliability. Within this phase, **R-060 (DLQ) is the highest priority** because a single poison entry in the current sample stalls the pipeline forever.

- [ ] **R-060 — Poison-entry DLQ**
  After `K` consecutive apply retries on the same `(origin, hlc)` tuple, the receiver moves the entry to a bounded DLQ (new grain `IReplicationDeadLetterGrain`) and advances past it. Configurable via `LatticeReplicationOptions.MaxApplyRetries` (default `5`). Operators inspect the DLQ via a read-only query API and can replay or discard individual entries. Removes the sample's "first exception stalls the stream forever" hazard.

- [ ] **R-061 — GC by min-acked cursor**
  Replace the sample's wall-clock TTL janitor with a GC predicate of `entry.hlc < min(ackedCursor_peer_i)` across all subscribed peers. Trims aggressively while guaranteeing every subscribed peer can always resume without a snapshot. A lagging peer pins the log — coupled with a "lag alert" metric (R-064) operators notice before it becomes a bootstrap scenario. TTL remains as a hard ceiling (configurable) to bound worst-case disk usage.

- [ ] **R-062 — Receiver-side flow control**
  Ack envelope carries `SuggestedBatchSize` and `PauseForMs` hints; sender respects both. Struggling receiver throttles without timing out; recovered receiver re-accelerates. Removes the sample's "sender always ships `BatchSize`" blind-push behaviour.

- [ ] **R-063 — Partitioned replog**
  Shard the WAL N ways keyed by `hash(tree, key) % N` with parallel scans on the ship path + HLC-ordered merge. Eliminates the single-replog hot range under fan-in. Exposed as `LatticeReplicationOptions.ReplogPartitions` (default `1` — unchanged from phase 1 for backwards compat).

- [ ] **R-064 — Per-peer observability**
  Extends R-001's baseline meter with: per-tree replication-lag histogram (`now - source_hlc` at apply), `replog_growth_rate` vs. `ship_rate` ratio, DLQ counters tagged `reason=schema|hlc_skew|oversized|unknown`. Published on the `orleans.lattice.replication` meter so OpenTelemetry pipelines pick them up automatically.

- [ ] **R-065 — Back-pressure `IHealthCheck`**
  ASP.NET Core `IHealthCheck` implementation surfaces the "replog growing faster than ship rate" condition as `Degraded`, and `lag > threshold for duration` as `Unhealthy`. Makes replication health a first-class Kubernetes probe target.

- [ ] **R-066 — Observable topology**
  `ReplicationTopology` exposes `IObservable<PeerChanged>` so peers can be added or removed at runtime without silo restart. Replaces the sample's one-shot `ReplicationTopology.Load` read from `IConfiguration`.

---

## Dependencies on the core library

Several phase-1 items require the core `Orleans.Lattice` library to expose a grain-side commit hook that today is only reachable via the host-level outgoing-call filter in the sample. Tracking that dependency here so it is not forgotten when promoting:

- **Core F-035 — Grain-side mutation hook** *(required by R-010)*: `ShardRootGrain` / `BPlusLeafGrain` expose a single extensibility point (e.g. `IMutationObserver` registered via DI) that is invoked *inside* the write path, before the state write returns. Must carry the `LwwEntry` including `ExpiresAtTicks` and any future `OriginClusterId` field.
- **Core F-036 — `OriginClusterId` on `LwwValue` / `LwwEntry`** *(required by R-020)*: New `[Id]` slot with wire-compatible default. Owned by the core library because every persistence / merge / snapshot / restore path must preserve it end-to-end (same invariant as F-016's TTL handling).
- **Core F-037 — `ReplogTreePrefix` reservation** *(required by R-011)*: Core library reserves a tree-name prefix (e.g. `_lattice_replog_`) analogous to `_lattice_trees`, so the replication package can create system trees for its WAL without the user accidentally naming a tree into collision.

These surface as tracked items on `../lattice/roadmap.md` under a new "Replication enablers" section when phase 1 begins; they must land in the core library *before* the corresponding `R-###` item can be implemented here.

---

## What we are deliberately carrying forward from the sample

Preserved unchanged (per design doc §8):

- Per-peer cursor as HLC.
- Advance cursor strictly by `ack.HighestAppliedHlc` on partial apply.
- Don't replicate the replog itself (reserved tree-name prefix — Core F-037).
- Per-tree opt-in + per-key filter (R-012).
- Janitor as a separate grain (R-061 changes the GC predicate, not the decomposition).

## What the sample's gRPC push transport gives us for free

The sample's gRPC push transport is the reference implementation of `IReplicationTransport` (R-040) — long-lived server-streaming RPC, reconnect-with-backoff, cursor-advance-on-ack, sub-second flush latency. Lift largely verbatim under R-042 once the wire contract (R-041) is settled.
