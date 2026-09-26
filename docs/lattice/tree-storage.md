# Tree Storage

This document explains how Lattice grain state is persisted and how the three sizing surfaces that actually grow with data interact with storage-provider per-row limits.

For the *mechanics* of changing structural sizing on an existing tree, see [Tree Sizing](tree-sizing.md). For the registry entry that pins these values per tree, see [Tree Registry](tree-registry.md). For the WAL provider contract and per-mutation row sizing, see [WAL Storage Providers](wal-storage-providers.md).

## WAL-first storage model

Every foreground commit (set / delete / range-delete / saga prepare / saga terminal / cross-migration backstop / merge / tombstone reap) is durably written to the per-shard write-ahead log **before** it touches in-memory state. The WAL append is the durability boundary; the leaf grain's persisted state row is a *small, fixed-shape* row carrying topology and lifecycle metadata only - it does **not** carry per-key entries.

There are therefore three distinct storage surfaces with three distinct sizing models:

| Surface | Stored where | Grows with | Sized against |
|---|---|---|---|
| **Leaf grain state row** | Lattice storage provider (`LatticeOptions.StorageProviderName`) | Nothing structural: topology (sibling pointers, key range, split lifecycle), the leaf's own version-vector entry, the projection digest and checkpoint offsets, plus a ledger of unresolved replay work that is empty in the steady state. **Does not grow with `MaxLeafKeys`.** | Storage provider per-row limit |
| **WAL row** (one encoded `WalRecord` per mutation) | WAL provider (`LatticeOptions.WalStorageProvider`) | The single largest mutation: key bytes + value bytes + optional vector clock + optional dependency summary + framing | WAL provider per-row limit, capped by `LatticeOptions.WalMaxBatchBytes` (default 4 MiB) |
| **Leaf snapshot blob** | Lattice storage provider - the same one as the leaf state row, under the separate `leaf-snapshot` grain state name | One row per key in the source leaf, captured whenever the leaf's durable snapshot coverage lags its checkpoint and when its checkpoint nears the WAL retention horizon; a payload above `LeafSnapshotSegmentBytes` is split across segment rows | Storage provider per-row limit |

Every leaf has one state row and every mutation produces one WAL row. The third surface is written for most data-bearing leaves too, because the WAL GC trims only a prefix that a durable snapshot covers: a leaf captures a snapshot whenever its coverage lags its checkpoint - on graceful deactivation, from a coverage-lag timer while it stays active (`LeafSnapshotMaxCoverageLagSeconds`, default 300 s), and at activation when a checkpointed partition has none - as well as when its checkpoint nears the WAL retention horizon (the fall-off-log safety net; see [Snapshot-on-fall-off safety net](projection-rebuild.md#snapshot-on-fall-off-safety-net)).

Shard-root state is small. Beyond its fixed-size fields it carries only bookkeeping that is bounded or normally empty: the dirty-leaf map tombstone compaction consumes (at most one entry per leaf of the shard), the moved-away slot table an adaptive split records (at most one entry per virtual slot), the record of an in-flight split or shard consolidation (whose moved-slot list is likewise at most one entry per virtual slot), a leaf-access histogram capped at 64 leaves that leaf-cache pre-warming reads, and any child links or leaf clears still owed after a fault. Internal-node state grows with `MaxInternalChildren` - one child entry and one child-digest record per child - but carries no per-key data, so it never approaches a storage-provider ceiling at default fan-out.

## Sizing surface 1 - Leaf grain state row

`LeafNodeState` carries only topology and lifecycle metadata. The historical `Entries` slot (`Id(0)`) was removed when the leaf state was collapsed onto a per-activation in-memory cache rebuilt from the leaf's snapshot and the WAL; the slot is permanently reserved and the per-key projection no longer lives in this row.

| Field | Type | Approximate size |
|---|---|---|
| `Clock` (HLC) | `HybridLogicalClock` | 12 bytes |
| Version vector (a single entry, keyed by the leaf's own grain identity) | `VersionVector` | ~60-120 bytes |
| Last-compaction version (a copy of the version vector) | `VersionVector` | ~60-120 bytes |
| `NextSibling` / `PrevSibling` / `OldNextSibling` | `GrainId?` | ~80 bytes each when present |
| `ParentId` | `GrainId?` | ~80 bytes when present |
| Split metadata (lifecycle state, split key, new sibling's id, and the in-flight marker) | mixed | ~80 bytes total when a split is in flight |
| `TreeId` | `string?` | 4 bytes + UTF-8 bytes |
| `ProjectionCheckpointOffset` | `long` | 8 bytes |
| `ProjectionHash` | `byte[]?` (16-byte XOR fold) | ~20 bytes when present |
| `ShardIndex` | `int?` | ~5 bytes |
| `LowKeyInclusive` / `HighKeyExclusive` | `string?` | 4 bytes + UTF-8 bytes each when present |
| `MovedAwaySlots` | `int[]?` | 0 bytes on a leaf that holds no moved-away seal; 4 bytes per sealed slot otherwise |
| `MovedAwayVirtualShardCount` | `int?` | ~5 bytes when set |
| `ProjectionCheckpointOffsetsByPartition` | `long[]?` | 0 bytes on a single-partition tree (`null`); 8 bytes per WAL partition otherwise |
| Partition-0 checkpoint-assigned flag | `bool?` | ~2 bytes |
| `DigestPublishSequence` | `long` | 8 bytes |
| `UnresolvedReplayWork` | `List<UnresolvedReplayWorkEntry>?` | 0 bytes in the steady state (`null` or empty); per outstanding entry, ~20 bytes plus the recorded mutation itself (roughly the size of its WAL row) - see the caveat below |
| Snapshot load hint (the byte size the leaf's last persisted snapshot occupied) | `long` | 8 bytes |
| Orleans state envelope | - | ~100-200 bytes |

**Steady-state leaf state row size: roughly 0.6 to 1.2 KB**, dominated by the grain-identity fields (sibling, parent, and split pointers). The row does **not** scale with `MaxLeafKeys`, `MaxInternalChildren`, or live-entry count, so the leaf state row is comfortably within every supported storage provider's per-row limit (including DynamoDB at 400 KB) regardless of structural sizing.

Only one thing can grow this row past a provider limit, and it is not structural: a backlog of unresolved saga work in the leaf's replay-work ledger. (The version vectors hold only the leaf's own entry - a replicated write's per-origin causal frontier travels on the entry itself, in the WAL and snapshot rows - so writes from many clusters do not grow this row.) Entries are struck off as each saga resolves, so the list is empty in the steady state, and `LatticeOptions.MaxDurableUnresolvedReplayWork` (default 1 024) bounds the deferred terminals it holds. It does **not** bound resident unresolved prepares: dropping one would pin the leaf's flush ceiling permanently, so past the bound a prepare is still recorded and the row is allowed to grow, with the crossing reported through the `orleans.lattice.leaf.unresolved_prepare_ledger_beyond_cap` counter instead. A saga whose terminal never lands therefore leaves its prepare in this row indefinitely, so a leak of such sagas can grow it without limit. That is a write-amplification cost on a SQLite-backed `local` deployment, and a genuine persist hazard where the lattice grain storage is Azure Table Storage, whose ~960 KB grain-state limit (see [Storage provider per-row limits](#storage-provider-per-row-limits)) this row would eventually breach - alert on that counter there. See [`MaxDurableUnresolvedReplayWork`](configuration.md#maxdurableunresolvedreplaywork).

## Sizing surface 2 - WAL row

Every mutation appends exactly one encoded `WalRecord` to the per-shard WAL. Each row carries:

- The key bytes (string, 4-byte length prefix + UTF-8).
- The value bytes (`byte[]?`, 4-byte length prefix + raw bytes; 0 bytes for tombstones).
- The HLC timestamp (12 bytes).
- A tombstone flag (1 byte).
- An optional TTL expiry (`long`, 8 bytes when set).
- An optional origin cluster id (`string?`).
- An optional dependency summary (`VersionVector?`), stamped from the same frontier as the vector clock below whenever a write carries one.
- The mutation kind and the declared merge mode (omitted from the bytes for the default last-writer-wins mode). The tree id is not stored: it is stripped on encode and recovered from the row's storage context.
- An optional vector-clock frontier (`VersionVector?`), stamped on causal-plus and replicated writes.
- For a range delete, the exclusive end key (and, for a predicate-filtered range delete, the keys the predicate matched); for a CRDT merge-mode key, the typed delta bytes, carried instead of a full value.
- The authoring shard index and merge, backstop, and category markers, plus atomic-write and cross-tree transaction metadata (transaction id, batch position, participants) on saga records.
- Orleans framing (~30-40 bytes).

The per-row total is therefore **roughly 60-100 bytes overhead + key bytes + value bytes**, plus the vector-clock frontier and dependency summary when a write carries one.

The WAL provider batches mutations and flushes them as a single storage operation. The batch is bounded by `LatticeOptions.WalMaxBatchBytes` (default 4 MiB) and `LatticeOptions.WalMaxBatchEntries` (default 100). The batch is split only by those two bounds, never by a provider's per-row limit; for provider-specific row limits, see the [WAL Storage Providers catalogue](wal-storage-providers.md#provider-catalogue). On Azure Table Storage in particular, the WAL row limit is far below the ~960 KB grain-state limit the leaf state row gets: the `Orleans.Lattice.Storage.AzureTable` provider stores each entry's encoded record - after optional compression - in a single binary property, and Azure Table Storage allows 64 KiB per binary property. The provider neither splits a record across properties or rows nor checks its size, so a mutation whose stored record exceeds 64 KiB fails the WAL append with the table service's error - and because a flush's entries commit in one table transaction, so does every other append in that flush and every append in flight or queued behind it on the same WAL partition (see [`AzureTableWalStorageProvider`](wal-storage-providers.md#azuretablewalstorageprovider)). Compression, on by default, lets a compressible value go somewhat past 64 KiB; an incompressible one cannot. To refuse such values at the write surface instead, set `LatticeOptions.MaxValueSizeBytes` below that bound.

The single-mutation worst case is the real sizing constraint for value bytes: pick a WAL provider whose per-row limit comfortably exceeds the largest value any caller will write.

## Sizing surface 3 - Leaf snapshot blob

The snapshot blob is written exactly once per snapshot capture and stored as a separate `leaf-snapshot` grain state - an Orleans state name, not a storage provider name - in the same lattice storage provider as the leaf state row (a distinct grain row), persisted through the Orleans binary serializer. Current captures carry their rows as one compact binary frame (while `LeafSnapshotBinaryEncodingEnabled` is on) rather than the legacy row list, and a frame larger than `LeafSnapshotSegmentBytes` (default 4 MiB, clamped to at least 64 KiB) is split into row-aligned segments persisted in separate `leaf-snapshot-segment` grain rows - the blob row then becomes a manifest recording the segment count, the segments' total frame bytes, and a segment generation, but none of the rows. The table below describes the legacy row-list shape, which is still read. It carries:

| Field | Approximate size |
|---|---|
| `SnapshotOffset` (`long?`) | 8 bytes |
| `CapturedAtTicks` (`long`) | 8 bytes |
| Byte footprint of the rows, recorded at capture (`long`) | 8 bytes |
| Per-partition covered WAL offsets (`long[]?`) | 0 bytes on a single-partition tree; 8 bytes per WAL partition otherwise |
| `Rows` (`IReadOnlyList<LeafSnapshotRow>`) | one `(Key, Value, MergeMode)` row per key the leaf holds, tombstones included |
| Orleans state envelope | ~100-200 bytes |

Each `LeafSnapshotRow` carries the same per-key surface a pre-collapse leaf row would have carried, minus the version-vector slot:

| Component | Approximate size |
|---|---|
| Key | 4 bytes length prefix + UTF-8 bytes |
| Value payload | 4 bytes length prefix + raw bytes (0 for tombstones) |
| HLC | 12 bytes |
| Tombstone flag | 1 byte |
| TTL expiry | 8 bytes when set |
| Origin cluster id | 4 bytes + UTF-8 when set |
| Migrated marker | 1 byte |
| Vector-clock frontier | 0 when absent; per origin cluster, the id's UTF-8 bytes + a 12-byte HLC when set |
| Merge-mode discriminator | ~1 byte, set only for a key written under a CRDT merge mode |
| Orleans framing | ~15-25 bytes per row |

**Per-row total:** roughly 45-100 bytes overhead + key bytes + value bytes.

**Snapshot blob size formula:**

```
SnapshotBlobSize ~= 200 + Entries * (45 + avgKeySize + avgValueSize)
```

`Entries` counts every key the leaf holds - live keys plus tombstones not yet compacted. The snapshot blob is the only surface whose size scales with that entry count, and it is the surface to validate against the storage-provider per-row limit when adaptive splits are disabled or `MaxLeafKeys` is large. A snapshot capture against a leaf with 10,000 keys and 4 KB values produces a roughly 40 MB payload, which is well outside the ~960 KB Azure Table Storage and 2 MB Cosmos DB limits and inside the practical Blob Storage limit - although segmentation (below) keeps any single row it writes within `LeafSnapshotSegmentBytes`.

Two operational levers control the snapshot blob's worst-case size:

- **`MaxLeafKeys`** caps the number of entries per leaf - live keys and not-yet-compacted tombstones alike - via the structural split policy. A leaf cannot exceed `MaxLeafKeys` entries at rest, so the snapshot blob is bounded by `MaxLeafKeys * average row size`.
- **`LatticeOptions.MaxLeafBytes`** (default 64 MiB) splits a leaf whose keys and values together exceed it, so it bounds the payload a snapshot must carry however large individual values are.

The storage provider is not a third lever. Snapshot rows are written through the same grain storage provider as the leaf state rows and every other Lattice grain that persists grain state - the one `AddLattice` registers under `LatticeOptions.StorageProviderName` (`"lattice"`) - so the provider cannot be chosen separately for snapshots. Choose that provider with the snapshot surface in mind (see the per-provider limits below).

Independently of the payload size, a payload larger than `LeafSnapshotSegmentBytes` (default 4 MiB, clamped to at least 64 KiB) is persisted as row-aligned segments of at most that size, so the largest single row a snapshot writes is bounded by that setting - only an indivisible entry larger than it can exceed it. Setting it comfortably below a provider's per-row limit therefore keeps every snapshot row within that limit whatever `MaxLeafKeys` is. Set it on the silo-wide options: a leaf plans its own capture's segments against its tree's resolved value, but the snapshot storage it persists through is addressed by leaf identity alone and re-segments against the silo-wide value, so a per-tree override is not honoured on every path.

Setting `LeafSnapshotMargin` to `0.0` disables only the proactive WAL-tail advisory (the hard fall-off triggers continue to apply). It does not stop snapshot capture: the coverage drivers - graceful deactivation, the `LeafSnapshotMaxCoverageLagSeconds` timer, and the activation-time repair - still capture, because the WAL GC trims only prefixes a snapshot covers. `ProjectionRebuildPolicy` does not switch capture off either: it applies only when the WAL has genuinely been trimmed past a checkpoint that no snapshot covers (`SnapshotThenWal`, `FullRebuildFromWal`, or `Fail`), and under every value that activation currently fails with `LeafProjectionStaleException`. See [Projection rebuild](projection-rebuild.md) for the rebuild contract.

## Storage provider per-row limits

The table below lists the per-grain state row limit for each Orleans storage provider. These limits apply to the **leaf state row**, the **internal state row**, **`ShardRootState`**, and the **leaf snapshot blob** - the rows the lattice grain storage provider persists. They are not WAL row limits, even where the WAL provider uses the same backing store: a WAL provider lays out its own rows, and the Azure Table WAL provider stores each entry in a single binary property capped at 64 KiB (see [Sizing the WAL row for a provider](#sizing-the-wal-row-for-a-provider)).

| Storage Provider | Max state size per grain | Limiting factor |
|---|---|---|
| **Azure Table Storage** | ~960 KB | Single-entity limit is 1 MiB including all property overhead, and a single binary property is limited to 64 KiB. Orleans' Azure Table grain storage splits serialized state across up to 15 binary properties of 64 KiB each, so ~960 KB is available for the serialized state, leaving the rest of the entity for the partition key, row key, and timestamp. Budget for 900 KB usable. |
| **Azure Blob Storage** | ~190.7 TiB (block blob) | Block blob max is approximately 190.7 TiB (50,000 blocks * 4,000 MiB). In practice, serialisation and deserialisation memory pressure on `ReadStateAsync` / `WriteStateAsync` makes states above ~50-100 MB impractical. |
| **Azure Cosmos DB** | 2 MB | Maximum document size. Budget ~1.9 MB usable after Cosmos system properties (`_rid`, `_ts`, `_etag`) and the Orleans envelope. |
| **ADO.NET (SQL Server)** | ~2 GB | `VARBINARY(MAX)`. Practical limit is much lower due to memory and query performance - stay well under 10 MB. |
| **ADO.NET (PostgreSQL)** | ~1 GB | `BYTEA`. Same practical caveat. |
| **ADO.NET (MySQL)** | ~4 GB | `LONGBLOB`. Same practical caveat. |
| **Amazon DynamoDB** | 400 KB | Maximum item size. |
| **Redis** | 512 MB | Max value size. Practical limit is much lower; treat as ~5 MB for responsive single-threaded operations. |
| **Memory (dev/test only)** | Unlimited | Bounded by available RAM. Not durable. |

The leaf state row, internal state row, and shard root state row all fit comfortably within every provider at default sizing (`MaxLeafKeys = 128`, `MaxInternalChildren = 128`, structural-only growth). The constrained surfaces are the **WAL row** (per-mutation worst case) and the **snapshot blob** (per-leaf live-entry total).

## Sizing the WAL row for a provider

The WAL row's worst case is one large mutation: a single `SetAsync` with the largest value any caller will write, plus optional vector clock and dependency summary.

**Formula to check fit (WAL provider per-row limit):**

```
WalRowSize ~= 60 + keySize + valueSize + vectorClockSize + dependencySummarySize
```

| Provider | Max value bytes a single mutation can carry (rough budget) |
|---|---|
| Azure Table Storage (`Orleans.Lattice.Storage.AzureTable`) | ~64 KiB minus key bytes, vector clock, framing: each entry's encoded record, after optional compression, is stored in one binary property, which Azure Table Storage caps at 64 KiB, and the provider neither splits nor size-checks it |
| Azure Blob Storage | bounded by `LatticeOptions.WalMaxBatchBytes` (default 4 MiB) - the batch ceiling, not the row ceiling, is the real cap |
| Cosmos DB | ~1.8 MB minus framing |
| ADO.NET providers | bounded by `LatticeOptions.WalMaxBatchBytes` |
| DynamoDB | ~380 KB minus framing |
| Redis | bounded by `LatticeOptions.WalMaxBatchBytes` |

If the application writes values larger than the WAL provider's per-row budget, either pick a higher-capacity WAL provider (Azure Blob, ADO.NET) or store the large value out-of-band and write only a reference to the tree.

## Sizing the snapshot blob for a provider

The snapshot blob is bounded by `MaxLeafKeys * average row size`. These figures size a snapshot's whole payload; the largest single row it writes is additionally bounded by `LeafSnapshotSegmentBytes` (see above), which on a provider with a per-row limit below 4 MiB is an alternative to shrinking `MaxLeafKeys`. With the default `MaxLeafKeys = 128`:

| Avg key | Avg value | `MaxLeafKeys` | Estimated snapshot blob size |
|---|---|---|---|
| 36 B | 8 B | 128 | ~12 KB |
| 50 B | 500 B | 128 | ~76 KB |
| 50 B | 2 KB | 128 | ~268 KB |
| 50 B | 4 KB | 128 | ~530 KB |
| 50 B | 16 KB | 128 | ~2.1 MB |
| 50 B | 64 KB | 128 | ~8.3 MB |

With a larger `MaxLeafKeys`:

| Avg key | Avg value | `MaxLeafKeys` | Estimated snapshot blob size |
|---|---|---|---|
| 50 B | 500 B | 512 | ~302 KB |
| 50 B | 2 KB | 512 | ~1.1 MB |
| 50 B | 500 B | 1,024 | ~603 KB |
| 50 B | 2 KB | 1,024 | ~2.1 MB |

**Sizing recommendation per provider for the snapshot blob:**

- **Azure Table Storage:** keep `MaxLeafKeys * (45 + avgKey + avgValue) < 900 KB`. With 2 KB values, that caps `MaxLeafKeys` near 440; the default 128 is safe up to ~6 KB values.
- **Azure Blob Storage:** effectively unconstrained for typical values. `MaxLeafKeys = 1,024` is comfortable for values up to ~10 KB.
- **Azure Cosmos DB:** keep the blob below ~1.8 MB. With 2 KB values, `MaxLeafKeys = 512` is safe.
- **DynamoDB:** the most constrained provider. With the default `MaxLeafKeys = 128` and 2 KB values, the blob lands at ~268 KB; reduce `MaxLeafKeys` to 64 or 32 for larger values, or lower `LeafSnapshotSegmentBytes` below the item limit (see below).
- **ADO.NET / Redis:** practical 5-10 MB ceiling is far above default workloads.

If the snapshot blob would exceed the provider's per-row limit, the simplest remedy is to lower `LeafSnapshotSegmentBytes` below that limit so each snapshot row fits, or to reduce `MaxLeafKeys`. The snapshot cannot be moved to a higher-capacity provider on its own: its `leaf-snapshot` state is stored through the same grain storage provider (`LatticeOptions.StorageProviderName`) as the leaf state rows, so a different provider means moving all of the lattice grain state to it. Setting `LeafSnapshotMargin = 0.0` is not a remedy: it disables only the WAL-tail advisory, and the coverage drivers still capture.

## Picking `MaxLeafKeys`

`MaxLeafKeys` is the registry-pinned cap on entries (live keys plus tombstones not yet compacted) per leaf. It controls:

- The **snapshot blob size** (directly proportional, see above).
- The **fan-out** of the tree (higher `MaxLeafKeys` = fewer grains, shallower tree, fewer splits).
- The **per-activation in-memory cache size** (one cache per leaf grain, rebuilt from the leaf's snapshot and the WAL on activation).

It does **not** control the leaf state row size and does **not** control the WAL row size.

**Recommended starting points:**

| Workload | Lattice storage provider | Suggested `MaxLeafKeys` | Rationale |
|---|---|---|---|
| Small values (UUIDs, flags) | any | 128 to 1,024 | Snapshot blob is small at any fan-out |
| Medium values (~500 B JSON / DTOs) | Table Storage, Cosmos DB | 128 to 512 | Snapshot blob stays under 1 MB |
| Medium values | Blob Storage, ADO.NET | 512 to 1,024 | Fan-out wins; snapshot blob easily accommodated |
| Large values (>2 KB) | Table Storage, DynamoDB | 32 to 128 | Snapshot blob cap dominates |
| Large values | Blob Storage | 256 to 1,024 | Snapshot blob stays under 10 MB |

> **Applying a new `MaxLeafKeys` / `MaxInternalChildren`:** call [`ResizeAsync`](api.md#resize-and-reshard) on the live tree (online, LWW-safe, undoable via [`UndoResizeAsync`](api.md#resize-and-reshard)), or pre-seed the pin on a new tree via [`ILatticeTreeAdmin.CreateTreeAsync`](../lattice.api.treeadmin/README.md) (the sizing is honoured only on first creation). See [Tree Sizing - Resizing an Existing Tree](tree-sizing.md#resizing-an-existing-tree). To grow the physical shard count, call [`ReshardAsync`](api.md#resize-and-reshard) (online, grow-only).

## Internal node sizing

`InternalNodeState` carries `MaxInternalChildren` `ChildEntry` records plus per-subtree digest aggregates:

| Field | Approximate size |
|---|---|
| `Children` (`List<ChildEntry>`) | `MaxInternalChildren * (60-80 bytes GrainId + 4 bytes + separator key UTF-8 + ~10-15 bytes framing)` |
| `ChildrenAreLeaves` | 1 byte |
| `Clock` (HLC) | 12 bytes |
| `ParentId` | ~80 bytes when present |
| Split metadata (`SplitState`, `SplitKey`, `SplitSiblingId`, `SplitRightChildren`) | ~80 bytes idle; up to `MaxInternalChildren * 90 bytes` during a split |
| `SubtreeProjectionHash` | ~20 bytes when present |
| `SubtreeEntryCount` / `SubtreeHighestCheckpointOffset` | 16 bytes |
| `ChildDigests` (per-child snapshot table) | per child, ~130-170 bytes plus the child's two key bounds (`GrainId` + 16-byte hash + entry, live and tombstone counts + checkpoint offset + publish sequence + subtree depth and fan-out + the child's low and high key bounds + framing) |
| `DigestPublishSequence` | 8 bytes |
| `TreeId` | 4 bytes + UTF-8 bytes |
| Orleans state envelope | ~100-200 bytes |

**Internal state row formula:**

```
InternalStateSize ~= 200 + MaxInternalChildren * (240 + 3 * avgKeySize)
```

The `3 * avgKeySize` term is the separator plus the two key bounds each child's digest record carries. With the default `MaxInternalChildren = 128` and 50-byte keys, the internal state row is roughly 50 KB, comfortably under DynamoDB's 400 KB limit; at the default fan-out, keys would need to average around 1 KB before the row approached it. In practice, **`MaxInternalChildren` is tuned for tree depth and fan-out, not storage limits.**

> **Note on `MaintainProjectionDigest`.** `MaintainProjectionDigest = false` zeroes the per-child digest cost (the `ChildDigests` table is left untouched and the upward publish is skipped); see [Configuration - `MaintainProjectionDigest`](configuration.md#maintainprojectiondigest).

## Default-configuration assessment

The defaults `MaxLeafKeys = 128`, `MaxInternalChildren = 128` are conservative across every supported storage provider:

| Provider | Leaf state row | Internal state row | Snapshot blob at default `MaxLeafKeys` |
|---|---|---|---|
| Azure Table Storage | safe (~1 KB) | safe (~50 KB) | safe up to ~6 KB average value |
| Azure Blob Storage | safe | safe | effectively unlimited |
| Azure Cosmos DB | safe | safe | safe up to ~14 KB average value |
| Amazon DynamoDB | safe | safe | safe up to ~2.7 KB average value |
| ADO.NET (any) | safe | safe | effectively unlimited |
| Redis | safe | safe | safe up to ~30 KB average value |
| Memory | bounded by RAM | bounded by RAM | bounded by RAM |

The defaults cover most workloads (JSON documents, serialised DTOs, small binary payloads up to a few KB).

## Calculating limits for a custom workload

To pick `MaxLeafKeys` against the snapshot blob ceiling for a given storage provider:

```
MaxLeafKeys = floor((ProviderLimit - 200) / (45 + avgKeySize + avgValueSize))
SafeMaxLeafKeys = floor(MaxLeafKeys * 0.75)
```

Where:

- `ProviderLimit` is the usable per-row limit in bytes (e.g., 921,600 for Azure Table Storage; 389,120 for DynamoDB; 1,900,000 for Cosmos DB; 10,000,000 for Blob Storage).
- `avgKeySize` is the average key length in bytes (UTF-8 encoded).
- `avgValueSize` is the average value length in bytes.
- `200` accounts for the snapshot blob's fixed overhead (offset, capture stamp, envelope).
- `45` accounts for per-row serialisation overhead.

**Example:** Azure Table Storage, 100-byte keys, 1 KB values:

```
MaxLeafKeys = floor((921,600 - 200) / (45 + 100 + 1,024)) = floor(921,400 / 1,169) = 788
SafeMaxLeafKeys = floor(788 * 0.75) = 591
```

> **Applying the result:** to change `MaxLeafKeys` / `MaxInternalChildren` on a live tree, call [`ResizeAsync`](api.md#resize-and-reshard) (online, LWW-safe, undoable via [`UndoResizeAsync`](api.md#resize-and-reshard)). To grow the physical shard count, call [`ReshardAsync`](api.md#resize-and-reshard) (online, grow-only). For a brand-new tree, either call these on the empty tree (fast path - no coordinator) or pre-register the pin via [`ILatticeTreeAdmin.CreateTreeAsync`](../lattice.api.treeadmin/README.md) (the sizing is honoured only on first creation). See [Tree Sizing - Resizing an Existing Tree](tree-sizing.md#resizing-an-existing-tree) and [Online Reshard](online-reshard.md).

## Measuring retained storage at runtime

The three sizing surfaces above are *design-time* models. To read the **exact retained on-wire bytes** a tree is costing right now - not an entry-count estimate - call [`ILattice.GetStorageUsageAsync`](api.md#storage-usage). It fans out across the tree's shards and WAL partitions and returns a `TreeStorageUsageReport` with `WalRetainedBytes`, `SnapshotBytes`, `LeafStateBytes`, and their `TotalBytes` sum. A cluster-wide roll-up across every registered tree is available via [`ILatticeAdmin.GetTotalStorageUsageAsync`](api.md#ilatticeadmin).

Reports are coalesced behind a short TTL cache (`LatticeOptions.StorageUsageCacheTtl`, default 10 s) so repeated dashboard scrapes stay cheap. `Partial = true` marks a report as a **lower bound** rather than an exact figure, for either of two reasons: a WAL provider that does not implement byte accounting (`IWalStorageProvider.GetRetainedByteSizeAsync` returns the `-1` "unsupported" sentinel), or a shard root / WAL partition that failed or timed out during the fan-out. A surface that did not answer contributes **nothing** - not a zero - because summing its zeroes would understate the tree while still presenting the total as complete. Consumers should render a partial figure as "n/a" rather than a misleading number. One failing surface never aborts the report, and the next sample after the surface recovers is exact again.

The fan-out is bounded at both levels so a large cluster's roll-up degrades in *latency* rather than collapsing into response-deadline failures. `MaxConcurrentStorageUsageSurfaces` (default 16) caps the shard-root and WAL-partition reads in flight for one tree; `MaxConcurrentStorageUsageTrees` (default 8) caps how many trees a cluster-wide roll-up samples at once. The two multiply, so the cluster-wide peak is 128 concurrent grain calls regardless of tree count or shard width. Concurrency bounds cap the burst but not the total work, so a cluster-wide roll-up is additionally bounded in time by `StorageUsageRollupBudget` (default 20 s): when it expires the roll-up stops dispatching and returns what it has, marking the report `Partial` rather than failing the whole call on the response deadline. See [Configuration](configuration.md#maxconcurrentstorageusagetrees).

The same figures are published as observable gauges on the `orleans.lattice` meter (`storage.wal_bytes`, `storage.snapshot_bytes`, `storage.leaf_state_bytes`, `storage.total_bytes`), alongside a companion `storage.usage_deep_published` 0/1 gauge that reports whether the three deep surfaces were actually measured for the tree, and surfaced on the bundled **Overview** Grafana dashboard. See [Metrics](metrics.md) for the full instrument list.

### Self-populating gauges in a multi-silo cluster

The storage gauges are driven by a per-silo background poller (`StorageUsagePollInterval`, default 15 s), so they populate automatically as soon as a silo starts - no caller has to invoke `GetStorageUsageAsync` to make a dashboard light up. On each tick the poller calls `ILatticeAdmin.PollWalUsageAsync`, which fans out to every registered tree's WAL-only aggregator. **The poll path is leaf-free**: it touches only WAL partition grains, never a leaf, internal node, snapshot storage grain, or shard-root grain, so an idle "cold" tree is never activated by polling and the activation-on-demand model the library is built around stays intact.

Each WAL-only aggregator is a single cluster-wide activation, so its publish lands on **its own host silo's** metrics sink. That means a tree contributes its WAL-bytes series on exactly one silo, and a cross-silo `sum by (tree)` counts it once regardless of how many silos run the poller. Running the poller on every silo is intentional and needs no leader election: redundant polls from sibling silos re-publish the same WAL sample cheaply, and if the silo that would "own" a poll dies the survivors keep the gauge fresh.

Snapshot, leaf-state, and total-bytes gauges populate on a different cadence. Each shard root maintains a running per-leaf byte-footprint total (incrementally updated by every leaf commit, with zero shard-root persistence cost), so `ILattice.GetStorageUsageAsync` reads those totals in O(1) per shard - no leaf-chain walk, no snapshot-storage activation. The gauges update whenever a caller invokes the public API. To keep them live on a dashboard without an explicit API call, set the optional `StorageUsageDeepPollInterval` (default `TimeSpan.Zero`, disabled) to a positive value - typically a small multiple of `StorageUsagePollInterval`: the poller then also calls the non-force `ILatticeAdmin.GetTotalStorageUsageAsync` on that slower cadence, which reads the same O(1) shard-root totals (activating shard roots but never leaves). For an authoritative re-anchored sample (for example after a manual storage migration), call `ILatticeAdmin.RefreshStorageUsageAsync`, which performs a deep leaf-walk and rewrites every shard's running totals; the poller never invokes that force path.

**Until one of those deep paths has run for a tree, the three deep gauges report no data at all - they are never synthesised to `0`.** This matters because the WAL-only poll path above cannot measure them, and it is the only path enabled by default: a tree whose gauges were seeded to zero by a WAL-only publish looks exactly like a tree measured and found empty. That confusion is not hypothetical. A reader consulting `/metrics` on a running deployment found `storage_leaf_state_bytes` and `storage_snapshot_bytes` flat at zero, took the zero as a measurement, and filed a root-cause diagnosis that leaf state and snapshots were never being written; 137 MB of leaf-snapshot state was on disk the whole time and the diagnosis had to be publicly retracted (issues #2692, #2693). The same reasoning already governed `Partial` reports above - a surface that did not answer contributes nothing rather than a zero - and this extends it to a surface that was never asked. Read the companion `storage.usage_deep_published` gauge to tell the three states apart: **no series at all** means the tree has not been observed on this silo, **`0`** means only the cheap WAL poll has reported so the deep surfaces are not measured, and **`1`** means a deep publish ran and any zero on those surfaces is a real zero.

Migration is handled by a staleness horizon. When a tree's aggregator moves to another silo, the old silo stops refreshing that tree's series; after the horizon (four poll intervals, sized off the slower of the WAL and deep cadences and floored at 60 s) the stale series stops being observed on the old silo, so the tree never appears on two scrape targets at once. Set `StorageUsagePollInterval` to `TimeSpan.Zero` (or a negative value) to disable the WAL poll; with `StorageUsageDeepPollInterval` also left disabled, the gauges then populate only when the public storage-usage API is called.

### Advisory byte-pressure WAL retention

WAL retention is normally bounded by consumer cursors and an optional wall-clock TTL (`LatticeOptions.WalRetention`). For a size-based safety valve, set `LatticeOptions.WalMaxRetainedBytes` - an **advisory** per-tree ceiling on retained WAL bytes. When set, each `ILatticeWalGc.RunOnceAsync` pass samples retained bytes before and after its safe trim:

- If the pre-trim total exceeds the ceiling, the policy schedules a byte-pressure trim and increments `orleans.lattice.storage.policy.trim_triggered` (tagged `reason=byte_pressure`). The bytes actually freed are reported on `orleans.lattice.storage.policy.bytes_reclaimed` and on the report's `RetainedBytesBefore` / `RetainedBytesAfter` fields.
- The trim **never crosses the safe frontier** (the minimum consumer cursor intersected with the causal-stable frontier). If a lagging consumer pins the bytes, the data is preserved, `orleans.lattice.storage.policy.over_threshold` reports `1`, and the write path is unaffected. The breach is advisory; the durability invariant wins.

`WalBytePressureReclaimTarget` (default `0.8`) is the low-water fraction of the ceiling that disarms the policy, providing hysteresis so a tree hovering near the ceiling does not thrash: byte pressure arms when retained crosses the full ceiling and keeps re-triggering until a trim drives retained at or below `WalBytePressureReclaimTarget * WalMaxRetainedBytes`, after which growth inside the band does not re-trigger until the ceiling is crossed again. Leaving `WalMaxRetainedBytes` at its default `null` disables the policy entirely with zero hot-path cost.

> **Production caution - set at least one absolute cap.** With every retention knob at its default (`WalRetention = null`, `WalMaxRetainedBytes = null`), the only active bound on WAL size is the consumer cursor frontier. The log shrinks as consumers catch up, but a **permanently lagging or dead consumer pins it and grows it without limit** - and `WalMaxRetainedBytes` will *not* rescue you, because it is advisory and never trims past a live cursor. Only `WalRetention` (a wall-clock floor on consumer lag) trims past a stuck consumer. Any deployment where unbounded growth is unacceptable should set `WalRetention`, and where a hard size budget matters, `WalMaxRetainedBytes` as well. See [How the retention bounds interact](wal.md#how-the-retention-bounds-interact) in the WAL reference for the full bound-by-bound breakdown.

## Key trade-offs

| Direction | Effect |
|---|---|
| **Increase `MaxLeafKeys`** | Fewer grains, shallower tree, fewer splits, larger per-activation cache, **larger snapshot blob** |
| **Decrease `MaxLeafKeys`** | More grains, deeper tree, more splits, smaller per-activation cache, smaller snapshot blob |
| **Increase `MaxInternalChildren`** | Shallower tree, fewer routing hops, slightly larger internal state row |
| **Decrease `MaxInternalChildren`** | Deeper tree, more routing hops, smaller internal state row |
| **Larger individual values** | WAL row grows directly; snapshot blob grows directly; leaf state row unaffected |
| **More distinct origin clusters writing to the same leaf** | Each entry's per-origin vector-clock frontier grows in the WAL and snapshot rows; the leaf state row is unaffected, because its version vectors hold only the leaf's own entry |

## Summary

1. **The leaf state row no longer scales with `MaxLeafKeys`** - the per-key projection lives in a per-activation in-memory cache rebuilt from the leaf's snapshot and the WAL.
2. **Two surfaces have non-trivial growth: the WAL row (per-mutation) and the snapshot blob (per-live-key).** The leaf state row grows only with an unresolved saga backlog.
3. **Size the WAL row against the largest single mutation any caller will write.** Pick a WAL provider whose per-row limit exceeds that worst case, and remember `LatticeOptions.WalMaxBatchBytes` (default 4 MiB) caps the batch, not the row.
4. **Size the snapshot blob against `MaxLeafKeys * average row size`.** This is the surface most workloads need to verify against the storage-provider per-row limit.
5. **Defaults (`MaxLeafKeys = 128`, `MaxInternalChildren = 128`) are safe on every supported provider** for values up to a few KB.
6. **Internal nodes rarely need tuning** for storage limits - only for tree depth versus fan-out trade-offs.
7. **Apply sizing changes online** via [`ResizeAsync`](api.md#resize-and-reshard) (fan-out) or [`ReshardAsync`](api.md#resize-and-reshard) (shard count). Both run without taking the tree offline and update the registry pin atomically.

To change sizing on an existing tree, see [Tree Sizing - Resizing an Existing Tree](tree-sizing.md#resizing-an-existing-tree).