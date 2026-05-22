# Changelog

All notable changes to the Orleans.Lattice package family are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

This changelog covers the **package family**: `Orleans.Lattice`, `Orleans.Lattice.Replication`, `Orleans.Lattice.Replication.Grpc`, `Orleans.Lattice.Storage.AzureTable`, and `Orleans.Lattice.Dashboards`. Packages ship in lockstep on the major and minor digits; patch digits may advance per-package.

## [Unreleased]

### Planned for v6.0.0

A v6 release plan is being assembled. Candidate themes:

- Loosen the LWW-by-default contract on `ILattice.SetAsync` / `GetAsync` via an opt-in per-tree `DefaultMergeMode` (so `MvRegister` and other CRDT shapes can be the default for trees that want concurrency-preserving semantics).
- Retire any deprecated seams that accumulated across the v4 and v5 series.
- Migration guide (core roadmap **F-021**) to accompany any breaking changes.

Track outstanding work in [`src/lattice/roadmap.md`](src/lattice/roadmap.md) and [`src/lattice.replication/roadmap.md`](src/lattice.replication/roadmap.md). See [`docs/RELEASING.md`](docs/RELEASING.md) for the per-package tag-and-publish protocol.

---

## [5.1.0] - 2026-05-22

Core-only minor release. The downstream packages (`Replication`, `Replication.Grpc`, `Storage.AzureTable`, `Dashboards`) remain at `5.0.1`.

### Added

- **Delta-only producer-side CRDT state model (R-120).** The leaf-as-`byte[]` model is replaced with a leaf-as-delta-journal-plus-cache model. Every CRDT accessor authors a typed delta via the producer-side delta-apply seam instead of running `JsonLatticeSerializer<TState>.Serialize(current)` on every `SetAsync`. The leaf state row is collapsed to topology + checkpoint + digest, and a runtime-owned per-activation `LeafEntryCache` is rebuilt from the WAL on every activation.
- **Leaf snapshot safety net.** New `ILeafSnapshotStorageGrain` captures a point-in-time projection of a leaf's entry cache when its persisted `ProjectionCheckpointOffset` approaches the WAL retention boundary, and the reactivation path prefers the snapshot over a from-scratch WAL replay whenever the snapshot offset is strictly newer than the persisted checkpoint.
- **`ILattice.ApplyCrdtDeltaAsync(string, LatticeMergeMode, byte[], CancellationToken)`** for hosts that pre-compute typed deltas off-grain and want to commit them through the same producer-side path.
- **Per-tree OR-Map shape registration** via `ISiloBuilder.AddOrMapShape<TKey, TValue>(string)`; closed-shape modes resolve through the registry's global fallback.
- **`LatticeOptions.LeafSnapshotMargin`** (default `0.30`) and **`LatticeOptions.LeafSnapshotReClassifyEveryNCheckpoints`** (default `64`) to tune the snapshot-on-fall-off trigger.

### Changed

- The persisted `LeafNodeState.Entries` and `LeafNodeState.LiveCount` slots are gone; the runtime-owned cache is the source of truth and is rebuilt from the WAL on every activation. This is wire-additive on the WAL side (the typed-delta payload is the same shape the typed CRDT accessors already authored) but a persisted-state shape change on the leaf grain.

### Fixed

- **Post-restart projection drop.** The runtime-owned `LeafEntryCache` is rebuilt from the WAL on every activation. The persisted `ProjectionCheckpointOffset` survives across activations, so trusting it after a cold start would cause WAL replay to read only `(checkpoint, head]` and silently drop every offset `<= checkpoint` from the rebuilt cache. Activation now computes a local replay-start override when no snapshot rehydrated and the cache is empty, so the replay covers the entire readable window. The persisted slot is never mutated by activation; the override is local to the replay loop only, preserving the public empty-tree digest contract.

---

## [5.0.1] - 2026-05-21

### Fixed

- **`LwwValue.Merge` tie-break instability on equal HLC.** Concurrent writes with identical HLC timestamps no longer hash-order non-deterministically; ties are broken by `(OriginClusterId, payload bytes)` lexicographic order for byte-exact convergence across replicas.
- **Bounded retry on `StaleShardRoutingException`** across the public `ILattice` surface. Reshards and merges no longer surface a transient routing exception to user code; the public surface retries with a small bounded budget and only surfaces if the budget is exhausted.

---

## [5.0.0] - 2026-05-19

Major release. Drops the full-state CRDT wire payload, harmonises the WAL wire format across in-memory and Azure Table providers, and ships several large replication-side correctness and observability features.

### Breaking

- **`WalRecord.Value` is dropped from CRDT-mode WAL and wire payloads (R-119).** CRDT entries now carry only the typed delta; full-state serialisation is no longer emitted on the producer side. LWW entries are unaffected. Receivers from v5+ understand both shapes; v3.x receivers do not understand the new shape. Replicating to a v3.x peer is not supported.
- **Typed-delta wire collapse and OR-Map replication (R-035).** Replication envelopes carry the typed delta directly rather than a fold-then-reserialise round trip.
- **`IReplicationTransport` typed-envelope shape (R-047).** The transport contract now exchanges typed envelopes, not opaque `byte[]`. Hosts implementing a custom transport must migrate.
- **Azure Table WAL wire format harmonised with the in-memory baseline (R-079).** The on-disk row layout under `WalEntry` changed to match the canonical shape. Existing Azure Table data must be migrated or drained before upgrading.
- **`IReplicationTopology` widened to govern doorbell and fall-off probes (R-113).** Topology providers now answer both routing and fall-off-probe authority questions.

### Added

- **OR-Map CRDT primitive and accessor (F-040)** - observed-remove map with recursive CRDT-value merge.
- **RGA sequence primitive and accessor (F-041)** - Replicated Growable Array for collaborative ordered lists and text.
- **MV-Register primitive and accessor (F-039)** + receiver-side replication dispatch (R-034).
- **Framing-tail compression** for replication envelopes with a reusable `ILatticeCompressor` core seam (R-043) that the Azure Table provider can stack onto in a follow-on (core F-075).
- **Receiver-side flow control (R-062)** - back-pressure policy hook on the apply side.
- **Back-pressure `IHealthCheck` for the replication shipper (R-065)**.
- **Observable replication topology with runtime peer changes (R-066)** - topology can be updated without a restart.
- **Snapshot-bootstrap-time atomic visibility for in-flight sagas (R-160)** - cross-cluster receivers now see saga effects all-or-nothing across the bootstrap boundary.
- **Bootstrap-applier seam.** Snapshot drain runs through `IReplicationApplier` instead of bypassing it.
- **Operator force re-bootstrap admin RPC (R-157)** and **operator-driven re-seed (R-053)**.
- **Bootstrap progress observability (R-156)** and **auto-bootstrap on fall-off-the-log (R-052)**.
- **Operator tooling for projection inspection and rebuild (F-052)**.
- **Snapshot-isolated cursors via WAL replay (F-065)** and **point-in-time cursors (F-064)**.
- **Causal+ consistency stack (R-080..R-088, R-090, R-093).** Producer-side stamping, per-origin FIFO invariant, bounded apply buffer, causal-stable snapshot cut-point and WAL GC frontier, causal+ observability instruments, ambient-VC capture on atomic-write saga prepare, intra-cluster restore VC reseed.
- **Production replication drivers and apply-duration instrumentation (R-067, R-068)**.
- **`Orleans.Lattice.Dashboards`** package - opt-in Grafana / OTel dashboard JSON for the `orleans.lattice` and `orleans.lattice.replication` meters (G-016).
- **Compaction policy controls, telemetry, and configurable cadence (F-071, F-072)**.
- **Intra-shard leaf-walk batching for tombstone compaction (F-073)**.
- **Shard-root dirty-leaf tracking for compaction (F-074)**.
- **Per-row WAL payload trims (R-114, R-115, R-116, R-117)** strip `TreeId`, `Mode`, `ShardIndex`, and the per-entry origin envelope from the on-wire bytes; the receiver reconstructs them from the envelope context.
- **Pipelined phase-2 commit on the Azure Table WAL provider (F-070)** and **batch WAL append on the leaf write path (F-069)**.

### Changed

- **Receiver-side per-entry shard RPCs collapse into batched `ApplyMergeManyAsync`.** Apply throughput improves materially on multi-entry batches.
- **Zero-copy WAL provider contract via `IWalMutationEncoder` (R-076)** - providers receive a writer rather than an allocated byte array.
- **Exact byte accounting in WAL pending-batch sizing (R-075)**.
- **Trim-aware WAL live-entry accounting (R-077)**.
- **Multi-batch in-flight WAL flush concurrency (R-074)**.

### Fixed

- **DLQ-park no longer advances per-origin HWM for saga terminals** - a parked saga terminal no longer permanently blocks subsequent saga-terminal apply attempts from the same origin.
- **`DeleteRange` apply seam no longer overwrites foreign-origin writes with higher HLCs** - the range-delete path now respects per-key HLC ordering.
- **`WriteStateAsync` failure recovery hardened** across every persistent grain in the family (`BPlusInternalGrain`, `BPlusLeafGrain` leaf-init, `ShardRootGrain.EnsureRootAsync`, `TreeDeletionGrain`, `TreeMergeGrain`, `TreeReshardGrain`, `TreeSnapshotGrain`, `TreeResizeGrain`, `TombstoneCompactionGrain`, `TxRegistryGrain`, `TreeShardSplitGrain`, `LatticeBootstrapCoordinatorGrain`, `AtomicWriteGrain`, `LatticeCursorGrain`, `HotShardMonitorGrain`, `ReplicationMaintenanceGrain`). The pattern is uniform: in-memory state reverts to its pre-write snapshot when the storage call throws, so a subsequent retry observes the same starting state Orleans observers see.
- **`ShardRootGrain` activation-key shape validation** - malformed activation keys no longer surface as opaque downstream NREs.
- **`[Immutable]` removed from records exposing mutable reference-typed properties** - Orleans no longer caches a mutable graph as immutable.
- **`SetManyAsync<T>` / `BulkLoadAsync<T>` reject null entries** with a precise argument exception rather than a downstream NRE.
- **`LatticeOptions` resolved per call in `WalShardGrain`** rather than captured once at activation, so reconfiguration via `IOptionsMonitor<T>` takes effect on the next call.

---

## [4.0.0] - 2026-05-16

Major release. Promotes the WAL to the sole commit point, ships the Azure Table Storage WAL provider, and lands the bulk of the cross-cluster replication and causal+ design.

### Breaking

- **`LeafShadowWrites` toggle removed; WAL is the sole commit point.** Pre-v4 hosts that depended on the dual-write fallback must reconfigure their storage provider; the new contract is WAL-first with a leaf-projection digest verified on activation.
- **`IWalStorageProvider` promoted to `Orleans.Lattice`.** The interface and `WalEntry` value type moved out of the replication package into core. Hosts that referenced these via the replication package must update their `using` directives.

### Added

- **Azure Table Storage `IWalStorageProvider` (R-073)** - the first production-grade WAL provider beyond the in-memory baseline.
- **Pre-merge delta capture on the observer payload (F-047)** - replication observers see the typed delta alongside the merged value.
- **Leaf-grain projection rebuild seam (F-048)** - leaves can be rebuilt deterministically from the WAL.
- **WAL-as-sole-commit-point promotion (F-049a, F-049b, F-049c, F-049d)** - commit-log adapter seams, dual-durability commit, projection digest + replay coordinator, default-flip.
- **Materialiser-side HWM with coalescing checkpoint persist (F-051)**.
- **Leaf-grain cursor-registry integration (F-050)** and **WAL cursor registry and GC promoted to core (F-056)**.
- **Parent-digest publish hop instrumentation on the leaf commit pipeline (F-063)**.
- **Bootstrap state machine for cross-cluster receivers (R-051)** and **gRPC binding for `IRemoteSnapshotTransport` (R-154)**, **sender-side snapshot service handler (R-151)**, **receiver-side `RemoteSnapshotProvider` adapter (R-152)**.
- **Receiver-side bootstrap respects per-tree `LatticeMergeMode` (R-158)**.
- **Bootstrap drain resumes on transient transport faults (R-159)**.
- **Causal+ WAL entry schema and producer-side stamping (R-080)** - first wave of the causal+ design.
- **Per-origin FIFO invariant + out-of-order detection (R-087)**.
- **Causal-stable snapshot cut-point (R-084)** and **causal-stable WAL GC frontier (R-083)**.
- **Causal dependency check + bounded apply buffer (R-082)**.
- **`ISnapshotProvider` abstraction (R-050)** and **transport metadata pass-through contract test (R-086)**.
- **Standard transport security for replication (R-046)**.
- **`MutationCategory` classification and maintenance-skip filter on the replication observer (R-090)** - maintenance writes no longer trip the replication path.
- **EventPipe per-method profiler for the microbench tier**.
- **Retroactive pending-tx sweep and cascading-reshard reliability fixes (F-059)**.
- **WAL-first merge and tombstone-compaction hardening (F-060)**.
- **Cross-cluster atomic visibility gate and cursor-based cache delivery (F-062)**.
- **Per-partition resume cursor on outbound shipper (FX-015)**.

### Changed

- **State-machine pooling on the read path.** `LatticeGrain.GetManyAsync`, `TraverseForReadAsync`, and `TraverseForReadWithVersionAsync` use `PoolingAsyncValueTaskMethodBuilder` to eliminate per-call async state-machine allocation.
- **Shard-bucket pre-sizing in `SetManyAsyncCore`** eliminates the AddWithResize cascade on large batches.
- **Allocation-free dedup in `ShardMap.GetPhysicalShardIndices`**.
- **`ArrayBufferWriter` reuse across an Azure Table WAL encode batch**.
- **`AtomicWriteGrain.ComputeKeyFingerprint` stackalloc UTF-8 scratch**.

---

## Prior to v4

See the git history (`git log v3.2.0`) and per-release GitHub Releases for v3.x, v2.x, v1.x, and v0.x.

The next entries published under [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) discipline begin at v4.0.0; pre-v4 release notes are best read off the per-tag GitHub Release pages.

[Unreleased]: https://github.com/NSTA1/Orleans.Lattice/compare/v5.1.0...HEAD
[5.1.0]: https://github.com/NSTA1/Orleans.Lattice/compare/v5.0.1...v5.1.0
[5.0.1]: https://github.com/NSTA1/Orleans.Lattice/compare/v5.0.0...v5.0.1
[5.0.0]: https://github.com/NSTA1/Orleans.Lattice/compare/v4.0.0...v5.0.0
[4.0.0]: https://github.com/NSTA1/Orleans.Lattice/compare/v3.2.0...v4.0.0