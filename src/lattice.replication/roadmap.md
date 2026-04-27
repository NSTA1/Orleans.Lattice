# Orleans.Lattice.Replication Roadmap

Feature plan for the `Orleans.Lattice.Replication` package — a cross-cluster replication library layered on top of `Orleans.Lattice`. This roadmap follows the upgrade order recommended in [`docs/replication-design.md` §9](../../docs/lattice.replication/replication-design.md), with the `MultiSiteManufacturing` sample's pull-over-HTTP / gRPC-push pipeline treated as the reference "what to promote, what to fix" artifact.

> **Feature IDs.** Items are numbered `R-XXX` to avoid collision with the core library's `F-XXX` space (tracked in [`../lattice/roadmap.md`](../lattice/roadmap.md)).
>
> **Package boundary.** Everything here ships in a new `Orleans.Lattice.Replication` assembly. Public API lives under `Orleans.Lattice.Replication`; internal grains/types under `Orleans.Lattice.Replication.{Area}`. The package has a single upstream dependency: `Orleans.Lattice`.
>
> **Non-goals for the initial release.** Cross-cluster Orleans cluster membership, multi-region storage provisioning, conflict UIs, user-facing admin tooling. This package is the on-the-wire replication engine only.

---

## Forward compatibility with the WAL-only future

A separate forward-looking design — [`docs/future.md`](../../docs/future.md) — sketches a v2 in which the WAL becomes the **sole** durability mechanism and the storage provider becomes a materialised projection. That direction is not committed work, but several items on this roadmap are direct building blocks for it. To keep the door open, every item below is implemented under three constraints:

1. **The WAL entry schema is the canonical mutation record**, not a replication-only side-car. `ReplogEntry` already carries the operation, key, value-or-delta, HLC and origin cluster id — the same shape a future local apply pipeline would consume.
2. **`IChangeFeed` (R-013) treats the outbound replication ship loop as one consumer among many.** A future background materialiser, secondary index, or projection rebuilder can subscribe at the same seam without replication being installed.
3. **Per-origin HWM (R-023) is keyed `(tree, originClusterId)`, but the shape generalises to local apply.** A `null`/local origin is already a valid key; a future log-replay-on-activation path uses the same table without schema changes.

Where a phase item makes a choice that affects forward compatibility, it is annotated *Future-compat:* below. Net-new work that this roadmap explicitly does **not** ship — but does not block either — is captured at the end of `docs/future.md`.

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

## 🎯 Outstanding — Implementation Order

The phase structure below groups items thematically. This section is the canonical **execution order** — what to pick up next once the previous item lands. Each heading calls out outstanding dependencies (core `F-###` items still on `../lattice/roadmap.md` and prior `R-###` items in this list). Items at the same indentation level are independent and can run in parallel.

> Notation: `[deps: …]` lists outstanding work that must land first. `[deps: none]` means the item is ready to start today. `✓` marks a satisfied dependency.

### Critical path — unblock typed CRDT replication and the wire envelope

1. **R-031 ✓ shipped — Typed-delta dispatch on declared mode**
   Closed the Phase 3 dispatch loop on top of F-038's typed primitive surface. Validator now accepts every defined `ReplicationMode`; the receiver-side applier dispatches Set ops on `entry.Mode` to a state-merge path through `ILattice` for typed CRDTs.

2. **R-072 ✓ shipped — `IChangeFeed` cursor shape decision**
   Locked in: public `IChangeFeed.Subscribe` keeps the HLC cursor; per-shard `(ShardIndex, Offset)` cursors live on the internal `WalResumeToken` reserved for the gRPC push transport. Documented in `docs/lattice.replication/change-feed.md`.

3. **R-070 ✓ shipped — `IWalStorageProvider` abstraction**
   Public `IWalStorageProvider` + `WalEntry` DTO + `LatticeReplicationOptions.WalStorageProvider` per-tree resolver + DI default `InMemoryWalStorageProvider`. Seam ships dormant; R-071 wires the grain to it.

4. **R-071 ✓ shipped — Turn-safe batching protocol**
   `IReplogShardGrain` rewired onto `IWalStorageProvider` via the protocol from WAL design §4. Single in-flight flush per shard, fail-fast rollback on storage failure, recovery via `GetHighestOffsetAsync` on activation, drain on deactivation. See the detailed entry under Phase 7 for deferrals.

5. **R-033 ✓ shipped — Active-active convergence test matrix**
   Three chaos-category fixtures (`OrSetConvergenceChaosTests`, `PnCounterConvergenceChaosTests`, `LwwRegisterConvergenceChaosTests`) plus a `MultiSiteClusterFixtureSmokeTests` diagnostic harness. Pin the Phase 3 typed-CRDT correctness bar end-to-end through real `AddLatticeReplication` silos, real WAL capture, and real `IReplicationApplier` apply paths before any wire-format work commits the contract.

### Wire format + transport

6. **R-040 ✓ shipped — `IReplicationTransport` abstraction**
   Pluggable seam. Can run in parallel with the critical-path items above.

7. **R-041 ✓ shipped — Orleans-serializer binary framing**
   Hardened the on-the-wire envelope: public `ReplicationBatchEnvelope` (`[Alias("olr.be")]`, wire version 1), public `IReplicationBatchEncoder` seam shaped as `void Encode(envelope, IBufferWriter<byte> writer)` so the gRPC push transport hands its stream writer directly through with zero per-batch heap allocation, and the canonical `OrleansBinaryReplicationBatchEncoder` registered via `TryAddSingleton`. Documented in `docs/lattice.replication/wire-format.md`.

8. **R-042 ✓ shipped — gRPC streaming push transport**
   Canonical sender + receiver pair shipped in the new `Orleans.Lattice.Replication.Grpc` sub-package. Sender-side `GrpcPushTransport` replaces `NoOpReplicationTransport` via `AddLatticeReplicationGrpcPushTransport(...)`; receiver via `AddLatticeReplicationGrpcServer()` + `MapLatticeReplicationGrpcService()` on an ASP.NET Core endpoint route builder. One long-lived `GrpcChannel` per `TargetClusterId` with HTTP/2 multiplexing and a cached `CallInvoker` per peer. Wire format is the `ReplicationBatchEnvelope` from R-041; custom `Marshaller<T>` instances hand the stream's `IBufferWriter<byte>` straight through to `IReplicationBatchEncoder.Encode`. No `.proto` and no `Grpc.Tools` dependency. Documented in `docs/lattice.replication/grpc-push-transport.md`. Covered by 42 tests across 6 fixtures including 2 in-process Kestrel integration tests via `Microsoft.AspNetCore.TestHost`. mTLS / token-rotation defaults deferred to R-046; runtime peer-set updates deferred to R-066; sender-side decode round-trip elimination deferred to R-047.

### Production hardening (must-have before any real deployment)

9. **R-060 — Poison-entry DLQ** `[deps: none]`
   **Highest priority in Phase 6** — a single poison entry today stalls the pipeline forever. Land before production rollout of R-042. Runnable in parallel with the wire-format items.

   *Forward dependency:* the DLQ ships today with an **inline** bounded-FIFO buffer over a reserved system tree (`_lattice_replog_dlq_{treeId}`). Any later replication item that generalises this FIFO behaviour into a shared primitive — or introduces a second FIFO-shaped staging buffer alongside the DLQ — must wait for **core `F-042` (`ILatticeQueue<T>`)** to land in `Orleans.Lattice` first, then refactor the DLQ to consume it instead of re-implementing the buffer. F-042 captures the cluster-internal queue contract, the system-tree backing model, the head/tail hot-leaf hazards, and the explicit "not a CRDT-replicated primitive" scope; see [`../lattice/roadmap.md`](../lattice/roadmap.md#replication-enablers) for the full design notes. Until F-042 lands, the DLQ's inline implementation is the canonical reference and must not be duplicated elsewhere in the package.

10. **R-061 — GC by min-acked cursor** `[deps: none]`
    Required before R-052 (auto-bootstrap detects "fall-off-the-log" against the GC predicate). Runnable in parallel with R-060.

11. **R-064 — Per-peer observability** `[deps: none]`
    Lag histograms, growth/ship ratios, DLQ counters. Land before R-065 (health check consumes the same meter) and before R-033's chaos suite asserts on counters.

### Snapshot / bootstrap (paired with core F-025)

12. **R-050 — `ISnapshotProvider` abstraction** `[deps: none — co-build with Core F-025]`
    F-025's `GetEntriesNewerThanAsync(HLC threshold)` leaf scan is the same primitive R-050 needs. Whichever lands first, the other consumes it verbatim — do not introduce a second scan API. Recommend pairing them in a single feature cycle.

13. **R-051 — Receiver-side bootstrap state machine** `[deps: R-050]`

14. **R-052 — Auto-bootstrap trigger** `[deps: R-051, R-061]`
    The "fall-off-the-log" detector reads against R-061's GC predicate.

15. **R-053 — Operator-driven re-seed** `[deps: R-051]`

### Operational polish (after critical-path is in)

16. **R-062 — Receiver-side flow control** `[deps: R-042]`

17. **R-063 — Partitioned replog** `[deps: none]`
    Performance under fan-in; opt-in via `ReplogPartitions`. Does not gate anything.

18. **R-065 — Back-pressure `IHealthCheck`** `[deps: R-064]`

19. **R-066 — Observable topology** `[deps: none]`

20. **R-043 — Batch-boundary compression** `[deps: R-041]`

21. **R-044 — Content-hash dedup** `[deps: R-042]`

22. **R-045 — Coalesced per-peer cursor checkpointing** `[deps: R-042]`

23. **R-046 — Standard transport security** `[deps: R-042]`
    mTLS / token rotation. Required before any production multi-tenant deployment but not before single-tenant pilot.

24. **R-047 — Typed-envelope `IReplicationTransport` shape** `[deps: R-042 ✓]`
    Eliminates the sender-side decode-then-re-encode round-trip the gRPC push transport currently pays. Today `IReplicationTransport.SendAsync` takes `ReplicationBatch` whose `Payload` is `ReadOnlyMemory<byte>`, so `GrpcPushTransport.BuildEnvelope` calls `IReplicationBatchEncoder.Decode(batch.Payload)` purely to satisfy the gRPC marshaller, which then re-encodes via `Encode(envelope, IBufferWriter<byte>)`. The decode allocates one `ReplogEntry` per WAL row in the batch on every send. Widen the transport seam to carry the typed `ReplicationBatchEnvelope` directly — either by adding a typed overload (`SendAsync(ReplicationBatchEnvelope envelope, string targetClusterId, CancellationToken ct)`) or by reshaping `ReplicationBatch` to carry the envelope alongside (or instead of) the byte[] payload, with a backwards-compat fallback for transports that only support bytes. After this change the gRPC hot path is genuinely zero-allocation beyond the gRPC box wrapper. LoopbackTransport / NoOpTransport are unaffected because they never re-encode.

### Extended CRDT modes (gated on outstanding core primitives)

These ship as paired (core primitive ↔ replication delta) deliverables — building either side in isolation freezes a contract before its consumer validates it. Order between the three is by user demand, not technical dependency.

25. **R-034 — MV-Register delta + dispatch** `[deps: Core F-039 outstanding]`
    Pair with F-039 in a single cycle.

26. **R-035 — OR-Map delta + dispatch** `[deps: Core F-040 outstanding]`
    Pair with F-040. Includes the recursive `InnerMode` wire-envelope extension.

27. **R-036 — RGA sequence delta + dispatch** `[deps: Core F-041 outstanding]`
    Pair with F-041. Highest implementation complexity of the three (sequence convergence, back-pressure for high-frequency editors).

### Suggested concurrency

The first wave can run in three parallel streams:

| Stream | Items |
|---|---|
| Typed dispatch | R-031 → R-033 |
| Wire format | R-072 → R-070 → R-071 → R-040 → R-041 → R-042 |
| Reliability | R-060, R-061, R-064 (any order, all `deps: none`) |

Streams converge before R-050 (snapshot/bootstrap), which depends on R-061 transitively via R-052 and benefits from R-064's observability for the bootstrap state machine.

---

## 🔲 Phase 0 — Scaffolding

Minimum viable package + hosting surface so every subsequent phase has a place to land and is testable end-to-end.

- [x] **R-000 — Package scaffolding and DI surface**
  New `src/lattice.replication/Orleans.Lattice.Replication.csproj` targeting `net10.0` with a project reference to `Orleans.Lattice`. Public DI entry point `ISiloBuilder.AddLatticeReplication(Action<LatticeReplicationOptions>)` registers core grains, the change-feed subscription, and a no-op `IReplicationTransport`. Public options type `LatticeReplicationOptions` mirrors the layout of `LatticeOptions` (per-tree where it makes sense). Test project `test/lattice.replication/Orleans.Lattice.Replication.Tests.csproj` with a two-site cluster fixture (two two-silo "sites" joined by an in-memory `LoopbackTransport`) so every subsequent phase has an integration harness.

- [x] **R-001 — Baseline per-peer metrics**
  Static `Meter "orleans.lattice.replication"` with day-one instruments: `entries_behind`, `bytes_behind`, `consecutive_errors`, `last_contact_seconds`, `ship_duration`, `apply_duration`. Every subsequent phase adds tags/instruments to the same meter. Wired into `R-000`'s test fixture so convergence tests can assert on counters rather than side effects.

---

## 🔲 Phase 1 — Value-at-write-time change feed *(design §1c, §5, §7)*

Fixes the three highest-cost sample shortcuts: ship-time reads, post-write best-effort append, and host-level outgoing-call filter.

- [x] **R-010 — Commit-time change capture**
  Grain-side capture inside `ShardRootGrain` / `BPlusLeafGrain` write paths (via a `Orleans.Lattice`-side hook the core library exposes — tracked as a dependency on the core roadmap). Each mutation emits a fully-formed `ReplogEntry` containing the op (`Set` / `Delete` / `DeleteRange`), the value *or* delta, the HLC, the target key, the tree id, and the origin cluster id (R-020). The entry is persisted before the write returns. Replaces the sample's `Outgoing*CallFilter` host-level append.

- [x] **R-011 — Single-writer per-shard WAL journal**
  Per-shard write-ahead log (`IReplogShardGrain` keyed by `{treeId}/{shardIndex}`) is the single source of truth for replication. Mutations append-then-apply (not apply-then-best-effort-append); the WAL-append is the commit point. `ReplogEntry` carries op + full value *or* typed delta. Removes the sample's read-amplification (`primary.GetAsync(origKey)` in `ShipOneBatchAsync`) and the "writes coalesced between append and ship collapse silently" / "false-delete on intervening delete" data-loss bugs.
  *Future-compat:* the grain shape, dense sequence numbers, and `ReplogShardEntry` envelope are reusable as the v2 commit-point WAL (future C-020/C-030). The leaf grain still persists today; promoting the WAL to *the* commit point is a future change to the core lib's commit path, not a wire format change here.

- [x] **R-012 — Per-tree opt-in and per-key filter**
  `LatticeReplicationOptions.ReplicatedTrees` (names) + `LatticeReplicationOptions.KeyFilter` (`Func<string, bool>` or declarative prefix set) — parity with the sample's `mfg-part-crdt` label-only split. The filter runs on the *producer* side so non-replicated mutations never touch the WAL. Filters are precompiled per tree id and cached on the `IMutationObserver` (`ConcurrentDictionary<string, CompiledFilter>` invalidated via `IOptionsMonitor.OnChange`) so the commit-time hot path is a dictionary lookup, a single bool, and at most one delegate plus a linear prefix scan.
  *Future-compat:* in the v2 WAL-only model the WAL must capture every mutation regardless of replication scope (because storage materialisation reads it too). Implement the filter as a *replication consumer* predicate, not a producer-side gate, so a future local materialiser sees every entry. The current "non-replicated mutations never touch the WAL" wording stays accurate for today; it becomes "non-replicated mutations are not shipped" under v2.

- [x] **R-013 — `IChangeFeed` public surface**
  Subscriber API for in-process consumers (tests, bridges, custom transports): `IChangeFeed.Subscribe(treeName, cursorHlc)` returning `IAsyncEnumerable<ReplogEntry>`. The outbound ship loop in later phases is one consumer among many.
  *Future-compat:* this is the seam future C-050 (background materialiser) subscribes to. Keep the contract pure-pull, cursor-driven, and free of replication-specific assumptions (no peer id, no transport-shaped acks). A `Subscribe` parameter for "include locally-originated entries" must default to `true` — the materialiser needs them.

- [x] **R-014 — Strict-only commit semantics**
  WAL failures propagate. A failure inside `IReplogSink.WriteAsync` flows back out of the commit-time observer and surfaces to the caller of `ILattice.SetAsync` / `DeleteAsync` / `DeleteRangeAsync` as the same exception the underlying storage provider threw — guaranteeing every committed mutation is also captured for replication. There is intentionally no opt-in "best-effort" mode that would let the primary write report success while silently dropping the change-feed record; silent change-feed drops are exactly the hazard commit-time capture exists to remove. A host that needs different semantics for a specific tree should compose its own `IMutationObserver` rather than configure correctness away.
  *Future-compat:* this matches the v2 commit semantics (C-030: WAL append = commit) — no behaviour change required when the WAL becomes the sole durability mechanism.

---

## 🔲 Phase 2 — Origin-stamped HLC + idempotent apply *(design §1b, §2, §5)*

Makes cycle-break durable, enables exactly-once apply, unlocks transitive topologies (A → B → C).

- [x] **R-020 — Origin cluster id in mutation metadata**
  New `[Id]` slot on `LwwValue` / `LwwEntry` for `OriginClusterId` (string, default `LatticeReplicationOptions.ClusterId`). Wire-compatible: missing field on legacy state decodes to `null` and is treated as "local". The field is authored once at commit time (R-010) and propagates through every merge / drain / snapshot path. Delivered through two co-ordinated layers: the core library's F-036 added the `[Id(4)]` slot on `LwwValue<T>`, the `[Id(5)]` slot on `LwwEntry`, the `[Id(8)]` slot on `LatticeMutation`, and the public `LatticeOriginContext` ambient (`Current` getter/setter + scoped `With(string?)`) that grain write paths read at the HLC-tick site to stamp the field via `with { OriginClusterId = ... }` — preserved end-to-end through shard-split shadow-forward, saga prepare/compensate, tree snapshot/restore, bulk-load, merge, and tombstone compaction. The replication package's `ReplicationMutationObserver` substitutes the validated local `LatticeReplicationOptions.ClusterId` whenever the incoming `LatticeMutation.OriginClusterId` is `null` (local-origin) and forwards the existing origin verbatim otherwise (remote replays), so every emitted `ReplogEntry.OriginClusterId` is non-`null` for replicated mutations and remote-origin writes never loop back stamped as local. Regression tests covering R-020 specifically: 3 `ChangeCaptureIntegrationTests` additions exercising `SetAsync` / `DeleteAsync` / `DeleteRangeAsync` under `LatticeOriginContext.With(remoteId)` and asserting the resulting `ReplogEntry.OriginClusterId` is the remote id, not the local cluster id — the cross-package end-to-end cycle-break check that closes the loop on F-036's per-grain stamping and the observer's preserve-or-substitute branch.

- [x] **R-021 — Durable origin-based cycle-break**
  Outbound ship filters WAL entries where `OriginClusterId == self`. Replaces the sample's `RequestContext["lattice.replay"]` thread-local. Robust across async boundaries, streams, saga compensations, and any apply path that doesn't originate from the inbound call chain. **Implemented:** `ChangeFeed.ReadSinceAsync` accepts `includeLocalOrigin: false` (the default), filtering entries whose `OriginClusterId` equals the local cluster id before they are shipped. Combined with R-020's per-grain origin stamping at the source, the outbound filter is the durable, async-boundary-safe successor to thread-local replay flags. Covered by `ChangeCaptureIntegrationTests` (cycle-break end-to-end) and `ChangeFeedTests` (filter unit tests).

- [x] **R-022 — Preserve source HLC on apply**
  Receiver stores `SourceHlc` (not a locally-stamped fresh HLC) in the entry's metadata. Enables transitive replication — B → C can propagate an A-origin write with A's HLC intact — and deterministic resolution for any vector-clock-based conflict scheme. **Implemented:** new internal `IReplicationApplyGrain` (core lib) routes `ApplySetAsync` / `ApplyDeleteAsync` through `IShardRootGrain.MergeManyAsync`, persisting the `LwwValue<byte[]>` with the caller-supplied HLC and origin cluster id verbatim. `ApplyDeleteRangeAsync` wraps the per-shard fan-out in a `LatticeOriginContext.With(originClusterId)` scope so range observers publish the remote origin and the outbound filter excludes the resulting WAL rows. Receiver-side `IReplicationApplier` (replication package) is the public seam the transport layer calls. Covered by `LatticeGrainReplicationApplyTests`, `ReplicationApplierTests`, and `ReplicationApplyIntegrationTests` (HLC round-trip, origin round-trip, cycle-break cross-package).

- [x] **R-023 — Per-origin high-water-mark table**
  Receiver-side `{(tree, originClusterId) → lastAppliedHlc}` persistent map. Inbound apply checks HWM before merging; re-delivery of `(origin, hlc)` is a no-op. Turns at-least-once delivery into at-most-once apply — required for correctness under typed CRDT counters/sets (phase 3).
  *Future-compat:* the `(tree, origin)` key shape generalises to v2 C-040: a local-origin row tracks the materialiser's apply progress and a remote-origin row tracks each peer. Keep the table and its grain interface neutral about who writes which row; do not assume `originClusterId != self`. **Implemented:** new internal `IReplicationHighWaterMarkGrain` keyed `{treeId}/{originClusterId}` with `GetAsync` / `TryAdvanceAsync(candidate)` (monotonic) / `PinSnapshotAsync(value)` (unconditional). `ReplicationApplier` consults `GetAsync` before every Set/Delete apply and short-circuits (`Applied=false`) when `entry.Timestamp <= hwm`; advances via `TryAdvanceAsync` after a successful apply. Range deletes bypass the HWM (idempotent at the leaf layer because they carry `HybridLogicalClock.Zero`). Covered by `ReplicationHighWaterMarkGrainTests`, `ReplicationApplierTests` (dedupe), and `ReplicationApplyIntegrationTests` (per-tree / per-origin isolation).

- [x] **R-024 — HWM-driven snapshot integration point**
  The HWM table is the handoff contract for the bootstrap protocol (phase 5): a newly-bootstrapped peer starts incremental replication from `hwm[(tree, origin)]` and the HWM guarantees the handoff is exactly-once across the snapshot/incremental boundary. **Implemented:** the `PinSnapshotAsync(HybridLogicalClock)` operation on `IReplicationHighWaterMarkGrain` is the explicit handoff seam — phase 5 bootstrap will pin the snapshot frontier (which may be lower than the current HWM if a remote peer rewinds to a snapshot) and then resume incremental replication from that pinned frontier. Unconditional overwrite is intentional: the snapshot defines the apply point, not the receiver's prior progress. Covered by `ReplicationHighWaterMarkGrainTests` (`PinSnapshotAsync_overwrites_unconditionally`, `PinSnapshotAsync_can_lower_high_water_mark`) and `ReplicationApplyIntegrationTests` (`HighWaterMarkGrain_pin_snapshot_overwrites_unconditionally`).

---

## 🔲 Phase 3 — Typed CRDT deltas *(design §1a)*

The real CRDT payoff — active-active convergence for the primitives the library ships, rather than cross-cluster LWW-on-bytes that silently loses concurrent set adds / counter increments.

This phase deliberately reorders the two items below from the design doc's narrative order. The original sequencing assumed the producer could *infer* a delta type from the value; in this codebase every value is an opaque `byte[]` (the core lib ships no typed CRDT value surface), so inference is impossible. The mode is declared, not detected — and that declaration has to land *before* the dispatch wired on top of it. R-032 therefore becomes the gate; R-031 plugs into it.

- [x] **R-030 — Delta contract for core primitives**
  Typed delta records for each replicable primitive the core library ships: `LwwRegisterDelta` (value + HLC + origin), `OrSetDelta` (adds + removes with dot context), `PnCounterDelta` (per-replica +/- increments), `VersionVectorDelta` (vector merge). Each is `readonly record struct`, `[GenerateSerializer][Immutable]` with a stable `[Alias]` constant in a new `ReplicationTypeAliases` class.
  *Future-compat:* these delta types are the v2 commit payload (C-010). The contract — produced by the core lib, consumed by replication — is identical in v1 and v2; the only change in v2 is that the local apply path also consumes them rather than writing through the leaf state directly.

- [x] **R-032 — Mandatory replication-mode declaration *(no implicit opaque-bytes)***
  Replicated trees must declare a `ReplicationMode` at configuration time; there is no implicit fallback that silently picks LWW-on-bytes for an undeclared tree. The declaration converts the previously-implicit "what convergence rule applies here?" question into a config-time decision the validator surfaces, eliminating the silent-data-loss footgun where two clusters could concurrently update the same opaque-byte key and one side's update vanished without an error, log line, or metric. **Implemented:** new public `ReplicationMode` enum (`LwwRegister`/`OrSet`/`PnCounter`/`VersionVector`, full set reserved up-front for wire-format stability); `LatticeReplicationOptions.ReplicatedTrees` reshaped from `IReadOnlyCollection<string>?` to `IReadOnlyDictionary<string, ReplicationMode>?` (`null` and empty both mean "no trees replicate" — the previous "`null` ⇒ everything" default is removed); new public `IReplicationModeResolver` seam with the default options-backed `ReplicationModeResolver` cached per tree id and invalidated on `IOptionsMonitor.OnChange`; new `[Id(9)] ReplogEntry.Mode` slot stamped by the commit-time observer (default `LwwRegister` so legacy persisted state decodes safely); `LatticeReplicationOptionsValidator` rejects null-or-empty keys in the `ReplicatedTrees` map and rejects every declared mode other than `LwwRegister` (with a "typed-primitive surface lands in F-038/R-031" error message); `ReplicationMutationObserver` routes every mutation through the resolver and short-circuits before the sink when the resolver returns `null`. The `Orleans.Lattice.Replication` and `Orleans.Lattice.Replication.Tests` projects build with zero warnings; 302/303 replication tests pass (the one failure is a pre-existing `wal-design.md` hygiene check unrelated to this feature) and all 1432 core tests pass. Covered by `ReplicationModeTests` (wire-stable underlying values), `ReplicationModeResolverTests` (cache, options-change invalidation, null-tree-id guard, dispose), the migrated `ReplicationMutationObserverTests` (mode-resolver gate, `Mode` stamping on every emitted entry, replaces the old "tree allowlist" branch), and `LatticeReplicationOptionsValidatorTests` (null/whitespace key rejection + non-`LwwRegister` mode rejection per enum value).

  **Public surface:**
  - `ReplicationMode` enum with members `LwwRegister`, `OrSet`, `PnCounter`, `VersionVector`. The full set is reserved up-front so adding a typed-primitive mode in a later phase is not a wire-format break.
  - `LatticeReplicationOptions.ReplicatedTrees` is reshaped from `IReadOnlyCollection<string>?` to `IReadOnlyDictionary<string, ReplicationMode>?`. Membership and mode are co-declared — there is no second "modes" dictionary that can drift out of sync with the membership set. `null` and empty both mean "no trees replicate" (the previous "`null` ⇒ everything replicates" default is removed because it is itself a footgun: misconfiguring a host accidentally replicates every tree it owns).
  - `IReplicationModeResolver` (public, silo-side service) — returns the resolved `ReplicationMode` for a given `treeId`, or `null` when the tree is not configured for replication. Cached per tree id; invalidated on `IOptionsMonitor<LatticeReplicationOptions>.OnChange`.
  - `ReplogEntry.Mode` (new `[Id(9)]` slot) — the wire-side dispatch tag that R-031 reads on the receiver. Stamped at commit time from the resolved mode so transitive paths (A → B → C) preserve the originating mode end-to-end. Default value is `LwwRegister` so legacy persisted state without the field decodes safely.

  **Validator gate:**
  - Reject any null-or-empty key in the `ReplicatedTrees` map.
  - Reject every declared mode other than `LwwRegister` in this phase, with a clear "the typed-primitive value surface lands in F-038 / R-031" error message. Reserving the enum members early means R-031 just relaxes this rule — no wire-format or option-shape change.
  - Continue rejecting empty `ClusterId` and non-positive `ReplogPartitions` (existing rules).

  **Producer behaviour:**
  - The commit-time observer resolves the mode via `IReplicationModeResolver` for the mutation's tree; an undeclared tree is skipped (same shape as the previous "tree not in allowlist" branch). A mutation against a declared tree whose mode resolves to `LwwRegister` continues to emit today's `byte[]`-based `ReplogEntry` shape, now stamped with `Mode = LwwRegister`. Other modes are unreachable until the validator relaxation in R-031 + F-038 ships the typed primitive surface that makes the other modes reachable from user code.

  **`LwwRegister` mode framing:**
  - `LwwRegister` is the explicit, opt-in successor to the old implicit opaque-bytes path. Its convergence semantics ("two clusters concurrently updating the same key resolve to the higher `(HLC, originClusterId)` pair; the loser's update is silently dropped") are documented prominently in both the XML doc and the validator error path so any user picking it sees the trade. The mode is correct under single-writer-per-key discipline (the dominant Lattice usage) and is genuinely the right answer for keys with overwrite-with-latest semantics; what it is *not* is a silent default a user can land on without choosing it.

  *Future-compat:* in v2 the local materialiser also dispatches on `Mode`, so adding the field here keeps the WAL entry schema canonical for both today's replication consumer and tomorrow's local-apply consumer. The resolver's `(treeId) → mode?` shape generalises to the local-origin row that v2 needs.

- [x] **R-031 — Typed-delta dispatch on declared mode** *(depends on R-032 ✓ + Core F-038 ✓)*
  Producer- and receiver-side dispatch onto `ReplicationMode` declared by R-032. **Implemented:** `LatticeReplicationOptionsValidator` now accepts every defined `ReplicationMode` value (rejection switched from "non-`LwwRegister`" to `!Enum.IsDefined`); the receiver-side `ReplicationApplier.ApplyPointAsync` dispatches `Set` ops on `entry.Mode`. `LwwRegister` keeps the existing path through `IReplicationApplyGrain.ApplySetAsync` (preserves the source HLC verbatim). `OrSet` / `PnCounter` / `VersionVector` route through a generic `ApplyStateMergeAsync<TState>` helper that deserialises the captured value bytes via `JsonLatticeSerializer<TState>.Default`, reads the local state under optimistic concurrency (`ILattice.GetWithVersionAsync` → `MergeFrom` → `ILattice.SetIfVersionAsync`) inside a `LatticeOriginContext.With(entry.OriginClusterId)` scope so the receiver-side commit-time observer publishes the foreign origin and the producer-side ship loop's origin filter excludes the resulting WAL entry — the durable, async-boundary-safe cycle-break for state-merge CRDTs. CAS retry budget is `StateMergeMaxAttempts = 16`, mirroring the typed accessors' authoring-side budgets. Unknown enum values throw `InvalidOperationException` with the integer cast embedded in the message; future versions will route them to a dead-letter queue. Per-origin HWM dedupe still runs for typed CRDT modes (idempotent merge makes it optional for correctness; it short-circuits redundant grain calls). Range deletes and `Delete` ops are unaffected by this item — only the `Set` op gained mode dispatch. Covered by 8 new unit tests in `ReplicationApplierTests` (one dispatch test per typed mode, HWM advance, HWM dedupe under typed CRDT, null-value guard, CAS retry, unknown-mode throw, `IReplicationApplyGrain` bypass) and 4 new integration tests in `ReplicationApplyIntegrationTests` (`OrSet` end-to-end convergence with a concurrent local add, `PnCounter` cross-replica sum, `VersionVector` pointwise-max preserving the higher local clock, and HWM dedupe of an `OrSet` re-delivery). The validator unit test was refactored from a "rejects non-`LwwRegister`" parametrised test to "succeeds for every defined `ReplicationMode`" + a new "rejects undefined integer cast" case. Full suite is green: 321/321 replication tests + 1520/1520 core tests (non-Chaos).

- [x] **R-033 — Active-active convergence test matrix** *(depends on R-031 ✓)*
  Three chaos-category integration test fixtures (`[Category("Chaos")]`, `[NonParallelizable]`, excluded from inner-loop runs per repo convention) plus a diagnostic smoke fixture pin the Phase 3 typed-CRDT correctness bar end-to-end through real `AddLatticeReplication` silos, real WAL capture, and real `IReplicationApplier` apply paths. **Implemented:** new shared `MultiSiteClusterFixture` spins up three independent `TestCluster` "sites" (one silo each, in-memory grain storage, distinct `ClusterId`s wired through both `ClusterOptions` and `LatticeReplicationOptions` via a `ChaosClusterIdPostConfigure`) and a `ChaosModeResolver` that dispatches each tree to the per-test `ReplicationMode` via a static map keyed off the local silo's `ClusterId`. A new `ChaosDeliveryPump` (`IAsyncDisposable`) drives the N×(N-1) edge fan-out over `IChangeFeed.Subscribe(treeName, cursor, includeLocalOrigin: true, ct)` and routes each entry through `IReplicationApplier`, applying a per-target cycle-break (`entry.OriginClusterId == receiverClusterId` → skip+advance) that mirrors the production ship-loop's origin filter rather than relying on the materialiser-shaped `includeLocalOrigin: false` flag. The pump also keeps a per-receiver `(receiverIndex, key) → last-applied bytes` content-hash cache: state-merge dispatch through `ILattice.SetIfVersionAsync` necessarily generates a fresh local HLC at the receiver (R-022's preserve-source-HLC only applies to the LWW path), so without value-equality dedup a converged CRDT would still re-ship byte-identical entries forever. Skipping byte-identical re-deliveries is safe because state-merge is value-idempotent; the cache lives entirely in the test pump and never touches R-031 dispatch code. Three convergence fixtures: `OrSetConvergenceChaosTests` (3 sites × 25 disjoint adds, mid-run `IsolateSite(2)` / `HealSite(2)` partition, asserts union of `Elements()` across sites); `PnCounterConvergenceChaosTests` (3 sites × 30 increments + 10 decrements per site = expected sum 60, `IncrementWithRetryAsync`/`DecrementWithRetryAsync` helpers wrap the local accessor with 8 outer retries on `InvalidOperationException ~ "CAS budget exhausted"` to mitigate contention between the local CAS loop and the pump's foreign-origin `SetIfVersionAsync` writes, plus a 1ms inter-op yield); `LwwRegisterConvergenceChaosTests` (3 sites × 40 point writes under partition, asserts pointwise `(Value, Version)` equality). The diagnostic `MultiSiteClusterFixtureSmokeTests` covers per-cluster change-feed yield and a single LWW pump delivery for fixture sanity. All five chaos tests pass three consecutive runs (~7s total per run; LWW 2s, OR-Set 2-3s, PN-Counter 1s, smokes ~0.7s each); inner-loop suite remains green at 380/380 replication + 1520/1520 core. Each fixture explicitly configures the relevant tree via `ReplicatedTrees[tree] = ReplicationMode.OrSet` (etc.) so the matrix exercises the full mode declaration → producer dispatch → receiver merge pipeline. The state-merge feedback loop and the PN-Counter CAS-budget contention are documented hazards on the test-side mitigations rather than R-031 changes — production fixes (preserve-source-HLC for state-merge applies; lift the accessor CAS budget under contended foreign-origin writes) are tracked as outstanding follow-on work, not blockers for the convergence bar this item asserts.

- [ ] **R-034 — MV-Register delta + dispatch** *(depends on Core F-039)*
  New `MvRegisterDelta` (`readonly record struct`, `[GenerateSerializer][Immutable]`, alias in `ReplicationTypeAliases`) carrying the dot-context-tagged set of `(replicaId, counter, value)` triples added by the producing write, plus the dot-context that observed-and-superseded prior dots. Producer-side: when the resolver returns `ReplicationMode.MvRegister`, `ReplicationMutationObserver` extracts the delta from the typed call site that F-039's `MvRegisterAccessor` captures (the producer must record the *added* dots, not the post-merge full state, otherwise convergence under concurrent writes is broken). Receiver-side: `IReplicationApplier` dispatches `MvRegisterDelta` through the primitive's `Merge(delta)` operation rather than `SetAsync`. Validator relaxation: `LatticeReplicationOptionsValidator` accepts `ReplicationMode.MvRegister` once F-039 ships. Convergence test added to R-033's chaos matrix: concurrent `Set` from N clusters resolves to the union of live values on every cluster (no value silently dropped).

- [ ] **R-035 — OR-Map delta + dispatch** *(depends on Core F-040)*
  New `OrMapDelta<TKey, TValue>` carrying added entries (`(key, dot, valueDelta)` — the *value* component is itself a typed delta of `TValue`'s replication mode, recursing through F-040's `ICrdt<TValue>` shape) and the dot-context of removed entries. The recursive value-delta is the gnarly part: the wire envelope must carry the inner mode tag too, because a peer receiving an `OrMapDelta<string, OrSet<string>>` entry needs to know the inner payload is an `OrSetDelta`, not opaque bytes. Solution: extend `ReplogEntry.Mode` with a nested `InnerMode` slot when the outer mode is `OrMap`, and reuse the existing dispatch table for inner application. New `ReplicationMode.OrMap` enum member. Producer: extracts the delta from F-040's `OrMapAccessor` call site. Receiver: applies via `OrMap<TKey, TValue>.Merge(delta)`. Convergence test added to R-033: concurrent `Set` of CRDT-typed values under the same key converges per-key to the inner CRDT's expected merged state.

- [ ] **R-036 — RGA sequence delta + dispatch** *(depends on Core F-041)*
  New `RgaDelta<T>` carrying inserted nodes (`(dot, parentDot, value)`) and tombstoned dots. Producer: extracts the delta from F-041's `RgaAccessor<T>` (`InsertAfterAsync` / `RemoveAtAsync` resolve to dot-explicit operations at the call site so the delta captures the structural intent rather than the post-merge sequence). Receiver: applies via `Rga<T>.Merge(delta)`. New `ReplicationMode.Sequence` enum member. Convergence test added to R-033: a chaos suite specific to sequences that asserts identical traversal order on every cluster after concurrent insert / delete bursts under random partitions — the meaningful correctness bar for an RGA, since pointwise set equality of nodes is necessary but not sufficient. Documents the back-pressure hazard separately: a high-frequency editor against a single sequence key generates one WAL entry per keystroke, so users with co-edited documents should consider a debounce policy on the producer side or a coarser-grained `Snapshot` mode for cold sequences.

---

## 🔲 Phase 4 — Push transport + binary framing + compression *(design §3, §10)*

Latency drops from reminder-cadence (~60 s) to sub-second; bandwidth improves ~2× from dropping JSON base64; deduplication makes no-op re-sets free.

- [x] **R-040 — `IReplicationTransport` abstraction**
  Pluggable seam. Implementations: `LoopbackTransport` (test fixture, R-000), `HttpTransport` (sample's pull path, lifted), `GrpcPushTransport` (sample's push path, lifted). The library ships all three; hosts pick via options. The outbound ship loop is transport-agnostic.

- [x] **R-041 — Orleans-serializer binary framing**
  Hardened the on-the-wire envelope: public `ReplicationBatchEnvelope` (`[Alias("olr.be")]`, wire version 1), public `IReplicationBatchEncoder` seam shaped as `void Encode(envelope, IBufferWriter<byte> writer)` so the gRPC push transport hands its stream writer directly through with zero per-batch heap allocation, and the canonical `OrleansBinaryReplicationBatchEncoder` registered via `TryAddSingleton`. Documented in `docs/lattice.replication/wire-format.md`.

- [x] **R-042 — gRPC streaming push transport** *(required R-041 ✓)*
  Canonical sender + receiver pair shipped in the new `Orleans.Lattice.Replication.Grpc` sub-package. Sender-side `GrpcPushTransport` replaces the default `NoOpReplicationTransport` via `AddLatticeReplicationGrpcPushTransport(options => options.PeerEndpoints[...] = ...)`; one long-lived `GrpcChannel` per `TargetClusterId` with HTTP/2 multiplexing, a cached `CallInvoker` per peer (via an internal `PeerChannel` record struct so `SendAsync` does not allocate a fresh invoker per call), and an optional per-peer `ConfigureChannel(name, GrpcChannelOptions)` callback for mTLS / custom `HttpHandler` / retry policy attachment. Receiver-side wired via `AddLatticeReplicationGrpcServer()` + `MapLatticeReplicationGrpcService()` on an ASP.NET Core endpoint route builder. Wire format is the `ReplicationBatchEnvelope` (alias `olr.be`, wire version 1) defined by R-041; the gRPC marshaller hands the stream's `IBufferWriter<byte>` straight through to `IReplicationBatchEncoder.Encode(envelope, writer)` so the envelope's bytes are written directly into the network buffer with no intermediate managed allocation on the encode path. No `.proto` file and no `Grpc.Tools` dependency: custom `Marshaller<T>` instances, internal sealed `ReplicationBatchEnvelopeBox` / `ReplicationAckBox` reference wrappers (gRPC's `Method<TRequest, TResponse>` has a `class` constraint), and a codegen-style `[BindServiceMethod]` topology (abstract `LatticeReplicationGrpcServiceBase` carries the attribute + null-tolerant static `BindService`; sealed `LatticeReplicationGrpcService` is the DI-resolved per-request handler). A static `LatticeReplicationGrpcMethodHolder.Current` bridges the DI-resolved `Method<,>` into the static binding hook because gRPC's static `BindService` callback cannot accept DI dependencies. Each `SendAsync` records `LatticeReplicationMetrics.ShipDuration` tagged `tree` / `peer` / `outcome` (allocation-free via `ValueStopwatch`). Documented in `docs/lattice.replication/grpc-push-transport.md`. Covered by 42 tests across 6 fixtures (options defaults, DI-extension wiring, transport ctor + send-validation + idempotent dispose, service Push validation + HWM accumulation + cancellation + RpcException-on-failure, `[BindServiceMethod]` null-tolerance + holder-not-initialised guard, marshaller Orleans-serializer round-trip on both wrappers, and 2 in-process Kestrel integration tests via `Microsoft.AspNetCore.TestHost` exercising the full wire round-trip end-to-end). mTLS / token-rotation defaults are deferred to R-046; runtime peer-set updates (currently host-restart-required) are deferred to R-066; sender-side decode-then-re-encode round-trip elimination is deferred to R-047.

- [ ] **R-047 — Typed-envelope `IReplicationTransport` shape** `[deps: R-042]`
  Eliminates the sender-side decode-then-re-encode round-trip the gRPC push transport currently pays. Today `IReplicationTransport.SendAsync` takes `ReplicationBatch` whose `Payload` is `ReadOnlyMemory<byte>`, so `GrpcPushTransport.BuildEnvelope` calls `IReplicationBatchEncoder.Decode(batch.Payload)` purely to satisfy the gRPC marshaller, which then re-encodes via `Encode(envelope, IBufferWriter<byte>)`. The decode allocates one `ReplogEntry` per WAL row in the batch on every send. Widen the transport seam to carry the typed `ReplicationBatchEnvelope` directly — either by adding a typed overload (`SendAsync(ReplicationBatchEnvelope envelope, string targetClusterId, CancellationToken ct)`) or by reshaping `ReplicationBatch` to carry the envelope alongside (or instead of) the byte[] payload, with a backwards-compat fallback for transports that only support bytes. After this change the gRPC hot path is genuinely zero-allocation beyond the gRPC box wrapper. LoopbackTransport / NoOpTransport are unaffected because they never re-encode.

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
  *Future-compat:* the same interface satisfies v2 C-060 (snapshot + WAL-tail restore for fast local recovery). Today it scans the primary tree and is consumed by the bootstrap protocol; in v2 it scans the materialised projection and is consumed by both bootstrap and crash recovery. Avoid hard-coding "remote peer" in the API surface — keep it `ISnapshotProvider.ExportAsync(treeName, asOfHlc, ct)` and let the consumer decide what to do with the stream.

- [ ] **R-051 — Receiver-side bootstrap state machine**
  States: `RequestingSnapshot` → `ApplyingSnapshot` → `IncrementalHandoff` → `LiveIncremental`. On snapshot completion the receiver pins the snapshot's as-of HLC `h` in its per-origin HWM (R-023) and switches to incremental from `h`. The HWM dedupe in R-023 makes the handoff exactly-once regardless of snapshot/incremental overlap.

- [ ] **R-052 — Auto-bootstrap trigger**
  Fires when the inbound apply path detects the sender's cursor is older than the sender's oldest WAL entry ("fall-off-the-log"). Emits a `PeerFellOffLog` event (tied to observability in phase 6) and transitions the peer to `RequestingSnapshot`. No operator intervention required for the common case.

- [ ] **R-053 — Operator-driven re-seed**
  Explicit admin API: `ILatticeReplication.RequestSnapshotAsync(peerId, treeName, CancellationToken)` for scheduled re-seeds (new peer joining, bandwidth-constrained initial sync, post-disaster re-bootstrap). Rate-limited by the sender.

---

## 🔲 Phase 6 — GC, DLQ, back-pressure, observability *(design §4–§6)*

Ops polish and production-grade reliability. Within this phase, **R-060 (DLQ) is the highest priority** because a single poison entry in the current sample stalls the pipeline forever.

- [x] **R-060 — Poison-entry DLQ**
  After `K` consecutive apply retries on the same `(origin, hlc)` tuple, the receiver moves the entry to a bounded DLQ (new grain `IReplicationDeadLetterGrain`) and advances past it. Configurable via `LatticeReplicationOptions.MaxApplyRetries` (default `5`). Operators inspect the DLQ via a read-only query API and can replay or discard individual entries. Removes the sample's "first exception stalls the stream forever" hazard. **Implemented:** new public `DeadLetterEntry` (record struct, alias `olr.dl`) and `ILatticeReplicationDeadLetters` seam (`ListAsync` / `CountAsync` / `DiscardAsync` / `ReplayAsync`) in `Orleans.Lattice.Replication`; internal `IReplicationDeadLetterGrain` (alias `olr.gd`) per tree, backed by a reserved system tree `_lattice_replog_dlq_{treeId}` (resolved through `ISystemLattice`) so parked entries inherit the scaling, sharding, and persistence of the core B+ tree rather than living inside one grain's persistent-state row. Each parked row is keyed `e/{19-padded-id}` and holds an Orleans-binary-serialised `DeadLetterEntry`. The grain bulk-loads its rows on activation into an in-memory cache and writes through on every mutation; FIFO eviction keeps the cache at or below `LatticeReplicationOptions.DeadLetterQueueCapacity` (default `1000`, validator `>= 1`). New internal `DeadLetterTrackingReplicationApplier` decorator wraps the canonical `ReplicationApplier`, tracks consecutive failures per `(treeId, originClusterId, timestamp, key, op)` tuple in a `ConcurrentDictionary`, and on the `MaxApplyRetries`-th failure parks the entry, advances the per-origin HWM past it (point ops only — `DeleteRange` skips HWM advance because the canonical applier doesn't consult the HWM for ranges), clears the counter, and returns `Applied=false`. Successful apply (or filtered re-delivery) clears the counter for that tuple. The decorator is wired via DI factory in `AddLatticeReplication` so the canonical `ReplicationApplier` is registered as a concrete singleton (reused by the `ILatticeReplicationDeadLetters` replay path to bypass the decorator and avoid an infinite re-park loop on a deterministically-failing parked entry) while the public `IReplicationApplier` resolves to the decorator. `ReplayAsync` removes the parked entry on any non-throwing return — including HWM-filtered re-delivery — so a `Replay` after parking is terminal-for-cleanup. New DLQ metrics on the `orleans.lattice.replication` meter: `dead_letter.enqueued` and `dead_letter.removed` counters, both tagged `tree` and `reason`; reason values `discarded` / `replayed` / `evicted` / `unknown` are exposed as `LatticeReplicationMetrics.Reason*` constants. New options `MaxApplyRetries` (default `5`) and `DeadLetterQueueCapacity` (default `1000`) on `LatticeReplicationOptions` with `>= 1` validator rules. `EntryKey` row-key formatting uses `string.Create` for a single-allocation key. Test coverage: 14 unit tests on the grain (FIFO eviction, rehydration, per-reason metric tagging, helpers), 7 on the decorator (success/below-threshold-rethrow/threshold-park/range-delete-skip-HWM/counter-clearing/cancellation-bypass/null entry), 8 on the public seam (null-arg validation + grain routing on every method), 3 on `DeadLetterEntry`, 4 on the new metric instruments, options & validator extensions, plus 4 cluster-backed integration tests (`DeadLetterIntegrationTests`) that boot a single-silo `TestCluster` with `AddLattice` + `AddLatticeReplication`, inject failures via a substituted inner `IReplicationApplier`, and verify the park / no-park-on-transient-success / replay-bypasses-decorator / persistence paths against the real `IReplicationDeadLetterGrain` activation, real `ISystemLattice` system tree, real `IReplicationHighWaterMarkGrain`, and real DI-resolved seam. Documented in [`docs/lattice.replication/dead-letter-queue.md`](../../docs/lattice.replication/dead-letter-queue.md).

- [ ] **R-061 — GC by min-acked cursor**
  Replace the sample's wall-clock TTL janitor with a GC predicate of `entry.hlc < min(ackedCursor_peer_i)` across all subscribed peers. Trims aggressively while guaranteeing every subscribed peer can always resume without a snapshot. A lagging peer pins the log — coupled with a "lag alert" metric (R-064) operators notice before it becomes a bootstrap scenario. TTL remains as a hard ceiling (configurable) to bound worst-case disk usage.
  *Future-compat:* the GC predicate must consult **every** consumer's cursor, not just remote peers. In v2 the local materialiser is one such consumer and a lagging materialiser must pin the log exactly the same way a lagging peer does. Express the predicate as `min(cursor across IChangeFeed subscribers)` rather than `min(cursor across remote peers)`.

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

## 🔲 Phase 7 — WAL design alignment *(design [`docs/lattice.replication/wal-design.md`](../../docs/lattice.replication/wal-design.md))*

R-011 landed the per-shard WAL grain shape and dense sequence numbers, but the underlying *durability mechanism* is still Orleans grain persistence — whichever provider the host configured for `IReplogShardGrain`'s state. The WAL design doc specifies a precise turn-safe batching protocol against an append-only `(PartitionKey, RowKey)` storage model (Azure Table Storage as the canonical implementation). Phase 7 closes the gap so the WAL meets that contract without coupling the replication package to a single storage backend, and resolves the one wire-format choice that must be settled before R-041 / R-042 lock the on-the-wire envelope.

These items are gating for **R-041** (binary framing) and **R-042** (gRPC push transport) — the cursor shape decision (R-072) determines whether the wire envelope keys batches by HLC or by per-shard offset, and the storage abstraction (R-070) determines what a peer's "resume from" token actually points at.

- [x] **R-070 — `IWalStorageProvider` abstraction**
  Pluggable durability seam for `IReplogShardGrain`. **Implemented:** new public `IWalStorageProvider` interface in `Orleans.Lattice.Replication` with `AppendBatchAsync`, `ReadAsync`, `GetHighestOffsetAsync`, and `TrimAsync` methods, exchanging a new public `WalEntry` (`readonly record struct`, `(Offset, ReplogEntry)`) DTO at the boundary. The `WalEntry` shape is intentionally distinct from the internal `ReplogShardEntry` grain RPC envelope so the in-cluster grain protocol can evolve without breaking host-supplied storage backends. Configurable per-tree via the new `LatticeReplicationOptions.WalStorageProvider` resolver (`Func<string treeId, IWalStorageProvider>?`); when `null` (the default), the WAL falls back to the DI-registered singleton, which `AddLatticeReplication` registers as `InMemoryWalStorageProvider` via `TryAddSingleton` (a host can pre-register its own implementation to win the registration). Atomicity is surfaced on the interface contract: `AppendBatchAsync` is all-or-nothing per call, and backends that cannot meet that for a particular batch must reject it at validation time rather than fragmenting silently. Offsets are caller-assigned, dense, and validated against the persisted tail by the in-memory provider. **Scope note:** the seam ships dormant — `IReplogShardGrain` itself is not yet rewired to the provider (that is the turn-safe batching protocol work, R-071). Today's persistence still flows through `IPersistentState<ReplogShardState>`; configuring `WalStorageProvider` does not yet change observable behaviour. The canonical Azure Table Storage implementation (matching the `(PartitionKey, RowKey) = ({TreeId}/{ShardIndex}, zero-padded-19-digit-offset)` layout described in `wal-design.md`) is deferred to a separate package so the core replication library does not pull in an Azure dependency. Covered by `WalEntryTests` (4 tests; record equality, default values, init), `InMemoryWalStorageProviderTests` (24 tests covering append density, all-or-nothing failure isolation, read-from-offset-exclusive semantics, max-entries cap, trim idempotency, per-shard isolation, null/cancellation guards on every method), and three new DI tests in `LatticeReplicationServiceCollectionExtensionsTests` (default registration, singleton lifetime, pre-registered-singleton-wins).
  *Future-compat:* identical contract to the future v2 commit-point WAL. Today it backs the replication-only WAL; in v2 the same provider backs the primary commit log. Per-tree configurability is an essential v2 requirement and lands here so v2 inherits a settled API rather than retrofitting it.

- [x] **R-071 — Turn-safe batching protocol** *(required R-070 ✓)*
  Rewired `IReplogShardGrain`'s internal append path onto `IWalStorageProvider` per WAL design §4: in-memory `_pendingBatch` + `_pendingAcks` + `_pendingBatchSizeBytes`, single `_inFlightFlush` task, no Orleans grain-state churn. `AppendAsync` enforces `WalMaxBatchEntries` (default `100`) and `WalMaxBatchBytes` (default `4 MB`) cutovers, parks each caller on a `TaskCompletionSource<long>` (with `RunContinuationsAsynchronously`), and starts a flush iff none is in flight. `OnActivateAsync` recovers the next-offset counter via `GetHighestOffsetAsync`; `OnDeactivateAsync` drains the in-flight + pending batch before returning so a graceful deactivation never leaves a caller observing a hung TCS. Flush failures fail-fast: the offset counter rolls back to the start of the failed batch, and every TCS in the failed batch *and* the currently-accumulating pending batch (whose offsets are now stale) is faulted with the underlying exception so writers retry against a clean offset. `FlushAsync` starts with `await Task.Yield()` to ensure synchronously-completing providers do not race the `_inFlightFlush` assignment with the `finally` clear (a hazard discovered during R-071 test development). Documented in `docs/lattice.replication/wal.md`. Pinned by 28 `ReplogShardGrainTests` (CRUD, recovery, fail-fast rollback with switch-to-healthy, deactivation drain, coalescing under in-flight flush via gated provider, batch-cap overflow, sequential burst across multiple flush cycles, null-arg guards, cancellation on every method) plus 9 new option-defaults tests in `LatticeReplicationOptionsTests` and 4 validator tests in `LatticeReplicationOptionsValidatorTests`. WAL design §10 acceptance rows satisfied: 9.1.1 (offset assignment after enqueue), 9.1.2 (next-offset recovery on activation), 9.2.2 (deactivation flush drain). **Carries explicit deferrals:** `WalMaxPendingBatches` is declared and validated (`>= 1`) but not consumed by the grain — single-inflight is hard-coded in v1; a follow-on item will lift the cap and satisfy 9.1.4. Also deferred: 9.1.3 (exact-bytes accounting vs the current `key.Length * 2 + value.Length + 128` estimate) and 9.3.2 (`ArraySegment<byte>` provider contract). The Azure Table Storage canonical implementation that motivates the seam is tracked separately under R-073.
  *Future-compat:* this is the v2 commit hot path. The protocol is published-once here so the v2 promotion is "swap callers from `ILattice.SetAsync → leaf state.WriteStateAsync` to `ILattice.SetAsync → IReplogShardGrain.AppendAsync`", not a redesign of the batching loop.

- [ ] **R-073 — Azure Table Storage `IWalStorageProvider`** *(low priority; required: separate package)* `[deps: R-070 ✓, R-071 ✓]`
  Ship the canonical durable `IWalStorageProvider` against Azure Table Storage as a **separate** package (e.g. `Orleans.Lattice.Replication.AzureTableStorage`) so the core replication library does not pull in an Azure dependency. Implements the `(PartitionKey, RowKey) = ({TreeId}/{ShardIndex}, zero-padded 19-digit Offset)` layout from `docs/lattice.replication/wal-design.md` §3 / §6 — `RowKey` ordering matches dense offset ordering byte-for-byte under lexicographic compare so `ReadAsync(fromOffsetExclusive)` is a single `RowKey gt …` server-side filter. `AppendBatchAsync` uses Table Storage's transactional batch (entity-group transaction) keyed by `PartitionKey`, satisfying the all-or-nothing contract for batches up to the 100-entity / 4 MB service limit (which is exactly why R-071 picked those defaults for `WalMaxBatchEntries` / `WalMaxBatchBytes`). `GetHighestOffsetAsync` is a single descending-`RowKey` `Top(1)` query. `TrimAsync` deletes by partition + `RowKey le …` range — idempotent against missing entries.
  Acceptance criteria (track against WAL design §10 rows 9.1.3, 9.1.4, 9.3.2 once the matching grain-side work also lands):
  - All `IWalStorageProvider` happy-path + edge-case tests from `InMemoryWalStorageProviderTests` pass against an Azurite-backed harness.
  - Cross-shard isolation (different `(treeId, shardIndex)` writes never bleed across `PartitionKey`).
  - Trim of an offset that does not yet exist reserves the trim point for a future append (matches in-memory provider semantics).
  - Service-throttling / 503 retries respect `LatticeReplicationOptions` retry settings without violating the dense-offset invariant.
  - Connection string / managed identity wiring through a new `AddLatticeReplicationAzureTableWal(...)` extension method on `IServiceCollection` / `ISiloBuilder`.
  - Integration test against a real (or Azurite) Table Storage account behind a CI-only opt-in category.

  Companion fix to land alongside this work — minor grain accounting bug discovered during R-071 review:
  - `IReplogShardGrain.GetEntryCountAsync` currently returns `_nextOffset`, which equals the persisted entry count only because today no caller invokes `IWalStorageProvider.TrimAsync`. Once trimming is exercised in production (e.g. by R-061's GC-by-min-acked-cursor), the counter would diverge from the actual persisted count. The fix is to either (a) rename the member to `GetNextSequenceAsync`-style and drop the misleading "count" name, or (b) compute the count from `(GetHighestOffsetAsync - GetLowestOffsetAsync + 1)` (introducing a `GetLowestOffsetAsync` member on `IWalStorageProvider`). Option (b) is preferred because it surfaces the trimmed prefix to callers; either way the change is contained to the grain + provider interface and lands cleanly with this item.

  *Future-compat:* identical contract to the future v2 commit-point WAL — today the Azure Table Storage provider backs the replication-only WAL; in v2 the same provider backs the primary commit log without any code change to the consumer.

- [x] **R-072 — `IChangeFeed` cursor shape decision** *(decision required before R-041)*
  WAL design §7 describes replication consuming entries "in offset order"; today `IChangeFeed.Subscribe` keys cursors by HLC (R-013). Both shapes are defensible — HLC-cursor preserves transitive-replication HLC fidelity and aligns with HWM dedupe (R-023); offset-cursor is trivially monotonic per shard, matches `RowKey` ordering 1:1, and removes HLC-skew edge cases on resume. **Decision (locked in):** the public `IChangeFeed.Subscribe` contract stays HLC-cursor-shaped — preserves transitive replication HLC fidelity, aligns 1:1 with the per-origin high-water-mark dedup table, and matches the shape a future cross-tree materialiser needs (no notion of per-shard offset). Per-shard `(ShardIndex, Offset)` cursors are exposed only on the internal transport-side seam via a new `internal readonly record struct WalResumeToken` ([Id(0)] ShardIndex, [Id(1)] Offset; `[GenerateSerializer][Immutable]` with stable `[Alias]`) reserved for the gRPC push transport. Receivers store this token alongside their per-origin HWM purely as a diagnostic fast-path; the HWM remains the authoritative dedup key. Documented in `docs/lattice.replication/change-feed.md` (new "Cursor shape" section comparing the two options) and inline on `WalResumeToken` itself. Locks the decision before the wire envelope hardens. Covered by 5 `WalResumeTokenTests` (default values, init, equality including per-component differentiation).

  Downstream impact:
  - **R-013** — no signature change. `Subscribe(treeName, cursorHlc, includeLocalOrigin, ct)` stays as-is.
  - **R-023** — HWM continues to key on `(tree, originClusterId) → HLC` regardless (HLC is the dedup key; cursor shape only governs *resume* tokens).
  - **R-041** — the batch envelope's public "resume-from" field is HLC-shaped; the gRPC stream may carry an opaque `WalResumeToken` alongside as a diagnostic fast-path.
  - **R-050** — bootstrap handoff (`PinSnapshotAsync` arg type) takes `HybridLogicalClock`, not an offset.

---

## Dependencies on the core library

Several phase-1 items require the core `Orleans.Lattice` library to expose a grain-side commit hook that today is only reachable via the host-level outgoing-call filter in the sample. Tracking that dependency here so it is not forgotten when promoting:

- **Core F-035 — Grain-side mutation hook** *(required by R-010)*: `ShardRootGrain` / `BPlusLeafGrain` expose a single extensibility point (e.g. `IMutationObserver` registered via DI) that is invoked *inside* the write path, before the state write returns. Must carry the `LwwEntry` including `ExpiresAtTicks` and any future `OriginClusterId` field.
- **Core F-036 — `OriginClusterId` on `LwwValue` / `LwwEntry`** *(required by R-020)*: New `[Id]` slot with wire-compatible default. Owned by the core library because every persistence / merge / snapshot / restore path must preserve it end-to-end (same invariant as F-016's TTL handling).
- **Core F-037 — `ReplogTreePrefix` reservation** *(required by R-011)*: Core library reserves a tree-name prefix (e.g. `_lattice_replog_`) analogous to `_lattice_trees`, so the replication package can create system trees for its WAL without the user accidentally naming a tree into collision.
- **Core F-038 — Typed primitive value surface** *(required by R-031)*: Core library ships typed value-surface accessors on top of the byte[] storage — at minimum `ILattice.OrSet(key)`, `ILattice.PnCounter(key)`, and `ILattice.VersionVector(key)` — so the user can author a CRDT value through a typed API rather than hand-rolling bytes. R-031's typed-delta emission has no path to extract an `OrSetDelta` / `PnCounterDelta` / `VersionVectorDelta` from a raw `byte[]` mutation; the producer needs the typed call site to capture the operation (add / remove / increment) at the moment the user expresses it. R-032 reserves the matching `ReplicationMode` enum members and rejects them at the validator until F-038 lands; R-031 then lifts the validator restriction.
- **Core F-039 — MV-Register primitive + accessor** *(required by R-034)*: Core library ships `MvRegister<T>` + `MvRegisterAccessor` so the producer can capture concurrent-write dots at the call site. Adds `ReplicationMode.MvRegister`. Strictly distinct from `LwwRegister` because the convergence rule (preserve concurrent values for app-level resolution) cannot be reconstructed from a post-merge `byte[]`.
- **Core F-040 — OR-Map primitive + accessor** *(required by R-035)*: Core library ships `OrMap<TKey, TValue>` + `OrMapAccessor`. Requires an internal `ICrdt<TValue>` shape so the map can recurse value-merge through nested CRDTs. The replication wire envelope's `Mode` slot extends with an `InnerMode` tag so receivers can dispatch the inner-value delta. Adds `ReplicationMode.OrMap`.
- **Core F-041 — RGA sequence primitive + accessor** *(required by R-036)*: Core library ships `Rga<T>` + `RgaAccessor<T>` (`InsertAtAsync` / `RemoveAtAsync` / `ToListAsync` plus dot-explicit `InsertAfterAsync`). The accessor must expose the dot of each insert so the producer can build a dot-shaped `RgaDelta<T>` at commit time. Adds `ReplicationMode.Sequence`.

These surface as tracked items on `../lattice/roadmap.md` under the "Replication enablers" section; they must land in the core library *before* the corresponding `R-###` item can be implemented here. F-035 / F-036 / F-037 / F-038 are complete; F-039 / F-040 / F-041 are net-new and gate their respective Phase 3 follow-ons.

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
