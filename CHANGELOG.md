# Changelog

All notable changes to the Orleans.Lattice package family are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

This changelog covers the whole **package family** - every published `Orleans.Lattice` and `Orleans.Lattice.*` package, spanning the core library and its replication, storage, membership, auth, data, backup, caching, schema, scaling, dashboards, MCP, API/gRPC binding, and Explorer companions. Packages ship in lockstep on the major and minor digits; patch digits may advance per-package.

This is the **v9.x** changelog. Earlier release lines are archived: v8.x in [`CHANGELOG.old.v8.md`](CHANGELOG.old.v8.md), v7.x in [`CHANGELOG.old.v7.md`](CHANGELOG.old.v7.md), and v6.x and earlier in [`CHANGELOG.old.v6.md`](CHANGELOG.old.v6.md).

## Unreleased

Outstanding work is tracked on [GitHub Issues](https://github.com/NSTA1/Orleans.Lattice/issues), labelled per project. See [`docs/RELEASING.md`](docs/RELEASING.md) for the per-package tag-and-publish protocol.

## Released

Published releases, newest first. Each section is keyed by its publish date; within a date, packages advance on their own patch digits per [`docs/RELEASING.md`](docs/RELEASING.md).

## [2026-08-16]

A per-package patch advances `Orleans.Lattice` to `9.0.3` (see Fixed); all other packages remain at their `9.0.x`/`9.0.0` versions.

### Fixed

- **The WAL GC no longer trims a leaf's durably-checkpointed prefix that no durable snapshot covers, closing a cold-restart data-loss path that survived 9.0.2.** A partition can durably advance its checkpoint to offset `N` while the leaf has persisted no snapshot materialising the rows in `[0, N]`; the durable-materialiser pin then reported `(clock, N)`, authorising the shared-shard WAL GC to trim `[0, N]`. Because the leaf's per-activation projection cache is not persisted (it is rebuilt from the WAL on every activation), the next cold rebuild replayed from offset 0 over the trimmed WAL and silently lost the checkpointed prefix - a bounded but permanent loss per cold cycle, distinct from the unbounded-growth class fixed in 9.0.2. The durable pin is now coverage-gated: it authorises trimming only up to `min(checkpoint, snapshotCoveredOffset)` and reports the `(Zero, -1)` block pin for a partition whose checkpointed prefix is not yet snapshot-covered, so the WAL prefix is retained until a covering snapshot exists. Cadence capture is made unconditional (it proceeds whenever any partition has a checkpoint `>= 0`) and records per-partition snapshot offsets (`LeafSnapshotBlob.SnapshotOffsetsByPartition`, wire-compatible - legacy blobs decode with a null array and fall back to the scalar-only path byte-for-byte) so a busy non-zero partition is covered even when partition 0 is idle. A companion `LeafSnapshotStorageGrain.HasCapturedPrefix` guard recognises such a per-partition-only blob (any slot `>= 0` is loadable) across the load, size, and clear seams, so the partition-0-idle scalar `-1` sentinel no longer discards the sole durable copy on cold restart. Verified end-to-end against the local RepoContext container under repeated abrupt `SIGKILL`/restart cycles. (`Orleans.Lattice` 9.0.3, [#1492](https://github.com/NSTA1/Orleans.Lattice/issues/1492))

## [2026-08-15]

A same-day per-package patch advances `Orleans.Lattice` to `9.0.2` (see Fixed); all other packages remain at their `9.0.x`/`9.0.0` versions.

### Fixed

- **The WAL GC no longer trims committed-but-not-yet-checkpointed data for a WAL partition whose protective block pin was overwritten on another partition's first checkpoint.** The durable-materialiser pin offset `-1` is overloaded: it means both "empty partition, safe to trim" and "partition holds committed WAL data never durably checkpointed, so it depends on the whole WAL from offset 0". On the first checkpoint of any partition the leaf upgraded *every* partition's pin to `(clock, checkpoint)`, replacing a data-bearing un-checkpointed partition's protective `(Zero, -1)` block pin with `(clock, -1)`; because the WAL GC skips `-1` offsets when computing the cross-partition offset floor, the global-min floor from the checkpointed partitions then authorised trimming that partition's live low-offset entries, silently losing them on the leaf's next cold rebuild (no restart needed, since foreground writes never checkpoint). Both pin-reporting branches now route through a leaf-side resolver that keeps the `(Zero, -1)` block pin when a partition is un-checkpointed and still holds live cache data (detected via a single `WalPartitionHash.Compute` pass, tombstones counting as live); genuinely empty partitions still report `(clock, -1)` so normal trim proceeds. Distinct from and complementary to the 9.0.1 offset-floor fix. (`Orleans.Lattice` 9.0.2, [#1489](https://github.com/NSTA1/Orleans.Lattice/issues/1489), [#1490](https://github.com/NSTA1/Orleans.Lattice/pull/1490))

## [2026-08-14]

Coordinated family major release - every package in the family advances in lockstep to `9.0.0`. This major consolidates a whole-tree administration control plane, completes the convergent-type primitive catalogue, and re-gates the highest-blast-radius tree operations behind a dedicated capability. The full, per-entry detail of every change is preserved in the v8.x archive's `## Unreleased` section ([`CHANGELOG.old.v8.md`](CHANGELOG.old.v8.md)); the headlines follow.

Two companion packages debut in this line but are **not yet published to NuGet** (build from source today): `Orleans.Lattice.Api.Mcp.RepoContext` and `Orleans.Lattice.Storage.File`.

A same-day per-package patch advances `Orleans.Lattice` to `9.0.1` (see Fixed); all other packages remain at `9.0.0`.

### Breaking

- **The destructive and structural whole-tree operations now require the new `TreeLifecycle` capability instead of `Admin`.** The public `ILattice` soft-delete, recover, hard-purge, online reshard, online resize, and undo-resize verbs move behind a dedicated `LatticeOperation.TreeLifecycle` bit that `Admin` never confers, so an operator must additionally grant `TreeLifecycle` to any subject that must retain destructive tree-lifecycle authority. ([#1375](https://github.com/NSTA1/Orleans.Lattice/issues/1375), [#1448](https://github.com/NSTA1/Orleans.Lattice/issues/1448))

### Added

- **A whole-tree administration control plane: `Orleans.Lattice.Api.TreeAdmin` and its gRPC binding `Orleans.Lattice.Api.TreeAdmin.Grpc`.** A new transport-agnostic control facade that composes the existing single-responsibility facades and surfaces the full whole-tree administration surface - create, configure, and resolve aliases; inspect and diagnose; soft-delete, recover, and purge; online reshard, resize, and snapshot; restore and revert; streamed resumable bulk-load; WAL placement audit and online partition move; materialised-view and tag-index management; shard compaction; and history retention - over gRPC and as a fail-closed, capability-scoped MCP `treeadmin` tool group. ([#1368](https://github.com/NSTA1/Orleans.Lattice/issues/1368), [#1377](https://github.com/NSTA1/Orleans.Lattice/issues/1377), [#1375](https://github.com/NSTA1/Orleans.Lattice/issues/1375), [#1376](https://github.com/NSTA1/Orleans.Lattice/issues/1376), [#1370](https://github.com/NSTA1/Orleans.Lattice/issues/1370), [#1371](https://github.com/NSTA1/Orleans.Lattice/issues/1371), [#1372](https://github.com/NSTA1/Orleans.Lattice/issues/1372), [#1374](https://github.com/NSTA1/Orleans.Lattice/issues/1374), [#1378](https://github.com/NSTA1/Orleans.Lattice/issues/1378), [#1380](https://github.com/NSTA1/Orleans.Lattice/issues/1380))

- **New convergent (CRDT) primitives completing the catalogue.** A grow-only counter (`GCounter`), a grow-only set (`GSet`), a remove-wins observed-remove set (`RwSet`), and a monotone directional register pair (`MaxRegister<T>` / `MinRegister<T>`), each replicated end to end and surfaced through the data facade, its gRPC binding, and paired typed MCP tools. ([#1418](https://github.com/NSTA1/Orleans.Lattice/issues/1418))

- **New capability bits and data-plane verbs.** A dedicated `BulkLoad` capability with a streamed, resumable, idempotency-keyed tree-creation protocol ([#1367](https://github.com/NSTA1/Orleans.Lattice/issues/1367)); a resilient `DeleteRangeAsync` range-delete that drains a whole key range across a transient enumerator loss ([#1443](https://github.com/NSTA1/Orleans.Lattice/issues/1443)); and authorization-posture surfacing with opt-in cluster-wide all-trees grants and delegable access administration ([#1349](https://github.com/NSTA1/Orleans.Lattice/issues/1349), [#1342](https://github.com/NSTA1/Orleans.Lattice/issues/1342)).

- **Two new companion packages (build from source, not yet on NuGet).** `Orleans.Lattice.Api.Mcp.RepoContext`, an opt-in MCP package giving an AI agent durable, conflict-free code context and memory over the CRDT tree, with semantic search over windowed file passages and a structural per-symbol model ([#1437](https://github.com/NSTA1/Orleans.Lattice/issues/1437), [#1451](https://github.com/NSTA1/Orleans.Lattice/issues/1451), [#1470](https://github.com/NSTA1/Orleans.Lattice/issues/1470)); and `Orleans.Lattice.Storage.File`, a durable local-disk write-ahead-log backend for single-node and containerized deployments ([#1434](https://github.com/NSTA1/Orleans.Lattice/issues/1434)).

### Security

- **Membership JWT hardening.** Audience validation now fails closed on misconfiguration ([#1410](https://github.com/NSTA1/Orleans.Lattice/issues/1410)), and the JWT authenticators can pin the accepted token signature algorithms as defense-in-depth against audience/token confusion and algorithm-confusion ([#1154](https://github.com/NSTA1/Orleans.Lattice/issues/1154)).

### Changed

- **CRDT read-path optimisations.** `OrSet`, `OrMap`, and `Rga` liveness and read paths skip redundant per-element tombstone probing on append-only state, and `Rga` serves `Count` / `IsEmpty` in O(1). ([#1406](https://github.com/NSTA1/Orleans.Lattice/pull/1406), [#1407](https://github.com/NSTA1/Orleans.Lattice/issues/1407), [#1408](https://github.com/NSTA1/Orleans.Lattice/issues/1408))

### Fixed

- **WAL retention and same-silo copier hardening.** The durable leaf-materialiser pin is now an authoritative WAL retention barrier that survives graceful shutdown, so a write-once derived tree can no longer wedge with `LeafProjectionStaleException` ([#1453](https://github.com/NSTA1/Orleans.Lattice/issues/1453), [#1464](https://github.com/NSTA1/Orleans.Lattice/issues/1464)); the repo-context enumeration paths recover from a transient enumerator loss instead of truncating ([#1460](https://github.com/NSTA1/Orleans.Lattice/pull/1460)); and every `[GenerateSerializer]` exception deriving from a BCL exception subclass now survives a same-silo deep-copy ([#1445](https://github.com/NSTA1/Orleans.Lattice/issues/1445), [#1446](https://github.com/NSTA1/Orleans.Lattice/pull/1446)).

- **The WAL GC no longer trims a low-HLC / high-offset tombstone-compaction reap past a lagging materialiser leaf's checkpoint.** A tombstone-compaction reap envelope reuses the reaped entry's older (lower) hybrid-logical clock but is appended at a higher WAL offset, which breaks the HLC-monotonic-in-offset assumption the HLC trim floor relied on: the reap's low HLC was trim-eligible under any positive cursor, so the GC could reclaim it before a lagging leaf applied it, tripping the offset-space fall-off detector (`LeafProjectionStaleException`) and wedging the affected consumer's ingest (observed as a repocontext semantic index that never populated, so search silently degraded to keyword mode). The durable materialiser pin store now records each leaf's applied checkpoint offset alongside its frontier, and the WAL GC floors its trim so it never reclaims an entry above the lowest durably-applied leaf checkpoint offset. The floor only ever retains more WAL, so healthy trees are unaffected, and consumers with no WAL-replay dependency (never-checkpointed block pins and in-memory-seeded split siblings) are excluded so steady-state trimming is unchanged. (`Orleans.Lattice` 9.0.1, [#1482](https://github.com/NSTA1/Orleans.Lattice/pull/1482))

## Older releases

Changelog entries for the v8.x release line have been archived to [`CHANGELOG.old.v8.md`](CHANGELOG.old.v8.md).

Changelog entries for the v7.x release line have been archived to [`CHANGELOG.old.v7.md`](CHANGELOG.old.v7.md).

Changelog entries for v6.x and earlier - down to the historical pre-v6.0.0 notes - have been archived to [`CHANGELOG.old.v6.md`](CHANGELOG.old.v6.md).
