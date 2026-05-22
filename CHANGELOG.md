# Changelog

All notable changes to the Orleans.Lattice package family are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

This changelog covers the **package family**: `Orleans.Lattice`, `Orleans.Lattice.Replication`, `Orleans.Lattice.Replication.Grpc`, `Orleans.Lattice.Storage.AzureTable`, and `Orleans.Lattice.Dashboards`. Packages ship in lockstep on the major and minor digits; patch digits may advance per-package.

## [Unreleased]

Items merged into `main` after the v6.0.0 cut accumulate here under the `### Added` / `### Changed` / `### Fixed` / `### Breaking` headings until the next ship cut.

### Candidate themes for a future major

- Loosen the LWW-by-default contract on `ILattice.SetAsync` / `GetAsync` via an opt-in per-tree `DefaultMergeMode` (so `MvRegister` and other CRDT shapes can be the default for trees that want concurrency-preserving semantics).
- Retire any deprecated seams that accumulate across the v6.x series.
- Migration guide (core roadmap **F-021**) to accompany any breaking changes.

Outstanding work is tracked in [`src/lattice/roadmap.md`](src/lattice/roadmap.md) and [`src/lattice.replication/roadmap.md`](src/lattice.replication/roadmap.md). See [`docs/RELEASING.md`](docs/RELEASING.md) for the per-package tag-and-publish protocol.

---

## [6.0.0] - 2026-05-22

Major release. **Not backwards-compatible.** The on-wire WAL format, the persisted leaf state shape, the replication transport contract, and the public API surface have all changed since the v5.0.x line. There is no in-place upgrade from any v5.x deployment - tenants must drain and reseed Azure Table WAL storage, and every cross-cluster replication peer must be on v6.0.0 before traffic resumes. The historical v5.0.0 / v5.0.1 / v5.1.0 NuGet tags shipped only the F-052 operator-tooling slice on top of the v4.1.1 baseline; everything else listed below is new since that line.

### Breaking - on-wire and persisted format

- **`WalRecord.Value` dropped from CRDT-mode WAL and wire payloads (R-119).** CRDT entries now carry only the typed delta; the full-state serialisation is no longer emitted on the producer side. LWW entries are unaffected in shape. v5.x receivers do not understand the new shape - cross-cluster peers must be upgraded in lockstep.
- **Typed-delta wire collapse and OR-Map replication (R-035).** Replication envelopes carry the typed delta directly rather than the fold-then-reserialise round trip the v5.x line used.
- **Per-row WAL payload trims (R-114, R-115, R-116, R-117).** `TreeId`, `Mode`, `ShardIndex`, and the per-entry origin envelope are stripped from the on-wire bytes; the receiver reconstructs them from the envelope context. The on-disk WAL rows are not readable by a v5.x reader.
- **One-encode commit-to-wire end-to-end (R-114 stage 5, folding R-115).** The producer-side encode happens once and the same bytes flow from leaf-commit through WAL append to replication ship.
- **Azure Table WAL wire format harmonised with the in-memory baseline (R-079).** The on-disk row layout under `WalEntry` is the canonical shape; existing Azure Table data must be drained or migrated before upgrading.
- **Framing-tail compression for replication envelopes (R-043)** with a reusable `ILatticeCompressor` core seam. Compression negotiation is required between v6 peers.

### Breaking - persisted leaf state shape

- **Delta-only producer-side CRDT state model (R-120).** The leaf-as-`byte[]` model is replaced with a leaf-as-delta-journal-plus-cache model. The persisted `LeafNodeState.Entries` and `LeafNodeState.LiveCount` slots are gone; the runtime-owned `LeafEntryCache` is rebuilt from the WAL on every activation. Existing v5.x persisted leaf state is not loadable - tenants must reseed via WAL replay against a clean leaf state row.
- **Leaf snapshot safety net.** New `ILeafSnapshotStorageGrain` captures a point-in-time projection when a leaf's `ProjectionCheckpointOffset` approaches WAL retention; reactivation prefers the snapshot over from-scratch replay when the snapshot offset is strictly newer than the persisted checkpoint.

### Breaking - public API surface

- **`IReplicationTransport` typed-envelope shape (R-047).** The transport contract exchanges typed envelopes, not opaque `byte[]`. Hosts implementing a custom transport must migrate to the typed seam.
- **`ITypedReplicationTransport` is the canonical transport contract.** Receiver-side per-entry shard RPCs collapse into batched `ApplyMergeManyAsync`.
- **`IReplicationTopology` widened to govern doorbell and fall-off probes (R-113).** Topology providers now answer both routing and fall-off-probe authority questions.
- **Zero-copy WAL provider contract via `IWalMutationEncoder` (R-076).** `IWalStorageProvider` implementations receive a writer rather than an allocated byte array; existing providers must migrate.
- **`ILattice.ApplyCrdtDeltaAsync(string, LatticeMergeMode, byte[], CancellationToken)`** added for hosts that pre-compute typed deltas off-grain and want to commit them through the same producer-side path. The producer-side delta-apply seam is the new authoritative write path.
- **Per-tree OR-Map shape registration** via `ISiloBuilder.AddOrMapShape<TKey, TValue>(string)`; closed-shape modes resolve through the registry's global fallback.

### Added - CRDT primitives

- **MV-Register primitive and accessor (F-039)** with receiver-side replication dispatch (R-034).
- **OR-Map CRDT primitive and accessor (F-040)** - observed-remove map with recursive CRDT-value merge.
- **RGA sequence primitive and accessor (F-041)** - Replicated Growable Array for collaborative ordered lists and text.

### Added - replication and transport

- **Receiver-side flow control (R-062)** - back-pressure policy hook on the apply side.
- **Back-pressure `IHealthCheck` for the replication shipper (R-065)**.
- **Observable replication topology with runtime peer changes (R-066)** - topology can be updated without a restart.

### Added - storage and WAL

- **Pipelined phase-2 commit on the Azure Table WAL provider (F-070)**.
- **Batch WAL append on the leaf write path (F-069)**.
- **Pre-built `TableServiceClient` slot on `AzureTableWalStorageOptions` (R-112)** - hosts can hand a pre-configured client to the provider.

### Added - compaction

- **Compaction policy controls and telemetry (F-071)**.
- **Configurable compaction tick cadence (F-072)**.
- **Intra-shard leaf-walk batching for tombstone compaction (F-073)**.
- **Shard-root dirty-leaf tracking for compaction (F-074)**.

### Added - tuning and release infrastructure

- **`LatticeOptions.LeafSnapshotMargin`** (default `0.30`) and **`LatticeOptions.LeafSnapshotReClassifyEveryNCheckpoints`** (default `64`) to tune the snapshot-on-fall-off trigger.
- **`CHANGELOG.md`** - canonical per-version release notes for the package family, governed by [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) discipline going forward.
- **`docs/RELEASING.md`** - per-package tag-and-publish protocol documenting the release-engineering contract the publish workflow expects (tag shapes, per-package push order, accidental-bulk-push recovery, historical-tag backfill guidance).

### Changed

- **Exact byte accounting in WAL pending-batch sizing (R-075)**.
- **Trim-aware WAL live-entry accounting (R-077)**.
- **Multi-batch in-flight WAL flush concurrency (R-074)**.
- **Dead shipper-side encode eliminated on typed transports (R-078)** - the typed transport never re-encodes a payload the producer already emitted.
- **Package family realigned on a single major version.** `Orleans.Lattice.Replication`, `Orleans.Lattice.Replication.Grpc`, `Orleans.Lattice.Storage.AzureTable`, and `Orleans.Lattice.Dashboards` move from `5.0.1` to `6.0.0` in lockstep with the core package.

### Fixed

- **Post-restart projection drop.** The runtime-owned `LeafEntryCache` is rebuilt from the WAL on every activation. The persisted `ProjectionCheckpointOffset` survives across activations, so trusting it after a cold start would cause WAL replay to read only `(checkpoint, head]` and silently drop every offset `<= checkpoint` from the rebuilt cache. Activation now computes a local replay-start override when no snapshot rehydrated and the cache is empty, so the replay covers the entire readable window. The persisted slot is never mutated by activation; the override is local to the replay loop only, preserving the public empty-tree digest contract.
- **`LwwValue.Merge` tie-break instability on equal HLC.** Concurrent writes with identical HLC timestamps no longer hash-order non-deterministically; ties are broken by `(OriginClusterId, payload bytes)` lexicographic order for byte-exact convergence across replicas.
- **Bounded retry on `StaleShardRoutingException`** across the public `ILattice` surface. Reshards and merges no longer surface a transient routing exception to user code; the public surface retries with a small bounded budget and only surfaces if the budget is exhausted.

### Migration notes

- **There is no in-place upgrade from v5.x.** The persisted leaf state shape, the WAL on-disk format, the WAL on-wire format, and the replication transport envelope shape have all changed.
- **Coordinated upgrade required across replication peers.** A v6.0.0 producer cannot ship to a v5.x receiver and a v5.x producer cannot ship to a v6.0.0 receiver. Schedule a maintenance window, drain replication queues, upgrade all peers in lockstep, then resume.
- **Azure Table WAL tenants must drain or reseed.** Existing rows under the configured `WalEntry` table are not readable by the v6 reader. The recommended path is to quiesce writes, drain replication, drop the WAL table, and let v6 reseed from leaf snapshots.
- The migration guide (core roadmap **F-021**) tracks the full upgrade runbook.

---

## Prior to v6.0.0

> ## **Do not use**
>
> All pre-v6.0.0 releases (v0.x through v5.1.0) are superseded and unsupported. They predate the v6 wire, persisted-state, and public-API contracts and have no upgrade path forward. New consumers must start on v6.0.0 or later; existing v5.x deployments must drain and reseed per the v6.0.0 migration notes above.

The v3.x, v4.x, and v5.x release lines are best read from the per-tag GitHub Release pages

| Package | Last v5.x | Last v4.x |
|---|---|---|
| `Orleans.Lattice` | `5.1.0` | `4.1.1` |
| `Orleans.Lattice.Replication` | `5.0.1` | `4.0.0` |
| `Orleans.Lattice.Replication.Grpc` | `5.0.1` | `4.0.0` |
| `Orleans.Lattice.Storage.AzureTable` | `5.0.1` | `4.0.0` |
| `Orleans.Lattice.Dashboards` | `5.0.1` | `4.0.0` |

The v5.0.0 / v5.0.1 / v5.1.0 line shipped on top of `lattice-v4.1.1` and added only the F-052 operator-tooling slice (projection rebuild and materialiser lag). Every other entry in the v6.0.0 section above is genuinely new since v5.1.0 and was not previously published under any v5.x tag.

From v6.0.0 onward this file is the authoritative changelog, governed by [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) discipline.

---
[Unreleased]: https://github.com/NSTA1/Orleans.Lattice/compare/v6.0.0...HEAD
[6.0.0]: https://github.com/NSTA1/Orleans.Lattice/compare/lattice-v5.1.0...v6.0.0