# Tree Storage

This document explains how Lattice grain state is persisted and how the three sizing surfaces that actually grow with data interact with storage-provider per-row limits.

For the *mechanics* of changing structural sizing on an existing tree, see [Tree Sizing](tree-sizing.md). For the registry entry that pins these values per tree, see [Tree Registry](tree-registry.md). For the WAL provider contract and per-mutation row sizing, see [WAL Storage Providers](wal-storage-providers.md).

## WAL-first storage model

Every foreground commit (set / delete / range-delete / saga prepare / saga terminal / cross-migration backstop / merge / tombstone reap) is durably written to the per-shard write-ahead log **before** it touches in-memory state. The WAL append is the durability boundary; the leaf grain's persisted state row is a *small, fixed-shape* row carrying topology and lifecycle metadata only - it does **not** carry per-key entries.

There are therefore three distinct storage surfaces with three distinct sizing models:

| Surface | Stored where | Grows with | Sized against |
|---|---|---|---|
| **Leaf grain state row** (`LeafNodeState`) | Lattice storage provider (`LatticeOptions.StorageProviderName`) | Topology and per-replica history only (sibling pointers, key range, split lifecycle, version vector, projection digest, checkpoint offset). **Does not grow with `MaxLeafKeys`.** | Storage provider per-row limit |
| **WAL row** (one `LatticeMutation` per commit) | WAL provider (`LatticeOptions.WalStorageProvider`) | The single largest mutation: key bytes + value bytes + optional vector clock + optional dependency summary + framing | WAL provider per-row limit, capped by `LatticeOptions.WalMaxBatchBytes` (default 4 MiB) |
| **Leaf snapshot blob** (`LeafSnapshotBlob`) | Lattice storage provider (separate `leaf-snapshot` storage name) | One `LeafSnapshotRow` per live key in the source leaf, captured when a checkpoint is about to fall off WAL retention | Storage provider per-row limit |

The first two are the common case (every leaf has one state row; every mutation produces one WAL row). The third is the fall-off-log safety net and is only written when a leaf is at risk of replaying past the WAL retention horizon - see [Snapshot-on-fall-off safety net](projection-rebuild.md#snapshot-on-fall-off-safety-net).

`ShardRootGrain` state is small and constant-sized. `BPlusInternalGrain` state grows with `MaxInternalChildren` but is dominated by 16-byte digest aggregates, not per-entry data - it never approaches a storage-provider ceiling at default fan-out.

## Sizing surface 1 - Leaf grain state row

`LeafNodeState` carries only topology and lifecycle metadata. The historical `Entries` slot (`Id(0)`) was removed when the leaf state was collapsed onto a per-activation in-memory cache rehydrated from the WAL; the slot is permanently reserved and the per-key projection no longer lives in this row.

| Field | Type | Approximate size |
|---|---|---|
| `Clock` (HLC) | `HybridLogicalClock` | 12 bytes |
| `Version` (per-replica HLC map) | `VersionVector` | ~50-200 bytes (grows with the number of distinct origin clusters that have ever written this leaf) |
| `LastCompactionVersion` | `VersionVector` | ~50-200 bytes |
| `NextSibling` / `PrevSibling` / `OldNextSibling` | `GrainId?` | ~80 bytes each when present |
| `ParentId` | `GrainId?` | ~80 bytes when present |
| Split metadata (`SplitState`, `SplitKey`, `SplitSiblingId`) | mixed | ~80 bytes total when a split is in flight |
| `TreeId` | `string?` | 4 bytes + UTF-8 bytes |
| `ProjectionCheckpointOffset` | `long` | 8 bytes |
| `ProjectionHash` | `byte[]?` (16-byte XOR fold) | ~20 bytes when present |
| `ShardIndex` | `int?` | ~5 bytes |
| `LowKeyInclusive` / `HighKeyExclusive` | `string?` | 4 bytes + UTF-8 bytes each when present |
| `MovedAwaySlots` | `int[]?` | 0 bytes on a non-resharded leaf; 4 bytes per moved slot otherwise |
| `MovedAwayVirtualShardCount` | `int?` | ~5 bytes when set |
| Orleans state envelope | - | ~100-200 bytes |

**Steady-state leaf state row size: roughly 0.6 to 1.2 KB**, dominated by the two version vectors when the tree has been written to by many clusters. The row does **not** scale with `MaxLeafKeys`, `MaxInternalChildren`, or live-entry count, so the leaf state row is comfortably within every supported storage provider's per-row limit (including DynamoDB at 400 KB) regardless of structural sizing.

The only way to grow this row past a provider limit is to write the same leaf from many thousands of distinct origin clusters (each one mints a fresh `VersionVector` entry that is retained indefinitely unless `LatticeOptions.VersionVectorRetention` is set to a finite value). In a typical single-cluster or small-replicated deployment this is not a concern.

## Sizing surface 2 - WAL row

Every commit appends exactly one `LatticeMutation` to the per-shard WAL. Each row carries:

- The key bytes (string, 4-byte length prefix + UTF-8).
- The value bytes (`byte[]?`, 4-byte length prefix + raw bytes; 0 bytes for tombstones).
- The HLC timestamp (12 bytes).
- A tombstone flag (1 byte).
- An optional TTL expiry (`long`, 8 bytes when set).
- An optional origin cluster id (`string?`).
- An optional dependency summary (`VersionVector?`) - only present in replicated-write paths.
- Orleans framing (~30-40 bytes).

The per-row total is therefore **roughly 60-100 bytes overhead + key bytes + value bytes**, plus the dependency summary on replicated writes.

The WAL provider batches mutations and flushes them as a single storage operation. The batch is bounded by `LatticeOptions.WalMaxBatchBytes` (default 4 MiB) and `LatticeOptions.WalMaxBatchEntries` (default 100). For provider-specific row limits and how the batch is split when a single mutation approaches the per-row ceiling, see [WAL Storage Providers](wal-storage-providers.md). On Azure Table Storage in particular, the WAL row shares the same ~1 MB entity ceiling as a leaf state row, so a single mutation whose value bytes approach 900 KB will fail the WAL append regardless of the leaf state row's headroom.

The single-mutation worst case is the real sizing constraint for value bytes: pick a WAL provider whose per-row limit comfortably exceeds the largest value any caller will write.

## Sizing surface 3 - Leaf snapshot blob

`LeafSnapshotBlob` is written exactly once per snapshot capture and stored under a separate `leaf-snapshot` Orleans storage name (the same lattice storage provider, but a distinct grain row). It carries:

| Field | Approximate size |
|---|---|
| `SnapshotOffset` (`long`) | 8 bytes |
| `CapturedAtTicks` (`long`) | 8 bytes |
| `Rows` (`IReadOnlyList<LeafSnapshotRow>`) | one `(string Key, LwwValue<byte[]> Value)` row per live key |
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
| Orleans framing | ~15-25 bytes per row |

**Per-row total:** roughly 45-100 bytes overhead + key bytes + value bytes.

**Snapshot blob size formula:**

```
SnapshotBlobSize ~= 200 + LiveEntries * (45 + avgKeySize + avgValueSize)
```

The snapshot blob is the only surface whose size scales with live-entry count, and it is the surface to validate against the storage-provider per-row limit when adaptive splits are disabled or `MaxLeafKeys` is large. A snapshot capture against a leaf with 10,000 live keys and 4 KB values produces a roughly 40 MB blob, which is well outside the ~960 KB Azure Table Storage limit and inside the practical Blob Storage and Cosmos DB limits.

Two operational levers control the snapshot blob's worst-case size:

- **`MaxLeafKeys`** caps the maximum number of live keys per leaf via the structural split policy. A leaf cannot exceed `MaxLeafKeys` live entries at rest, so the snapshot blob is bounded by `MaxLeafKeys * average row size`.
- **The storage provider for the `leaf-snapshot` Orleans storage name** can be configured independently of the rest of the tree. Azure Blob Storage is the recommended provider for the snapshot grain when the tree's working set has large values, even when the leaf state row itself sits on Azure Table Storage.

If snapshot captures are not viable for a given workload, the snapshot path can be skipped by configuring `ProjectionRebuildPolicy` to `FullRebuild` - the leaf then rebuilds its projection from the WAL on every fall-off-log event instead of capturing a snapshot first. See [Projection rebuild](projection-rebuild.md) for the rebuild contract.

## Storage provider per-row limits

The table below lists the per-grain state row limit for each Orleans storage provider. These limits apply to the **leaf state row**, the **internal state row**, **`ShardRootState`**, the **leaf snapshot blob**, and every WAL row when the WAL provider uses the same backing store.

| Storage Provider | Max state size per grain | Limiting factor |
|---|---|---|
| **Azure Table Storage** | ~960 KB | Single-entity limit is 1 MB including all property overhead. After partition key, row key, ETag, timestamp, and the Orleans single-binary-property envelope, ~960 KB is available for the serialized state. Budget for 900 KB usable. |
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
| Azure Table Storage | ~900 KB minus key bytes, vector clock, framing |
| Azure Blob Storage | bounded by `LatticeOptions.WalMaxBatchBytes` (default 4 MiB) - the batch ceiling, not the row ceiling, is the real cap |
| Cosmos DB | ~1.8 MB minus framing |
| ADO.NET providers | bounded by `LatticeOptions.WalMaxBatchBytes` |
| DynamoDB | ~380 KB minus framing |
| Redis | bounded by `LatticeOptions.WalMaxBatchBytes` |

If the application writes values larger than the WAL provider's per-row budget, either pick a higher-capacity WAL provider (Azure Blob, ADO.NET) or store the large value out-of-band and write only a reference to the tree.

## Sizing the snapshot blob for a provider

The snapshot blob is bounded by `MaxLeafKeys * average row size`. With the default `MaxLeafKeys = 128`:

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
- **DynamoDB:** the most constrained provider. With the default `MaxLeafKeys = 128` and 2 KB values, the blob lands at ~268 KB; reduce `MaxLeafKeys` to 64 or 32 for larger values, or host the snapshot grain on a different provider (see below).
- **ADO.NET / Redis:** practical 5-10 MB ceiling is far above default workloads.

If the snapshot blob would exceed the provider's per-row limit, the simplest remedy is to host the snapshot grain on a higher-capacity provider while leaving the rest of the tree on the original provider. The snapshot grain uses the same lattice storage provider name (`LatticeOptions.StorageProviderName`) under a distinct `leaf-snapshot` Orleans storage name, so a host can register two storage providers under the same lattice storage name configuration if the underlying Orleans setup allows it - or, more simply, disable snapshot capture via `ProjectionRebuildPolicy = FullRebuild`.

## Picking `MaxLeafKeys`

`MaxLeafKeys` is the registry-pinned cap on live entries per leaf. It controls:

- The **snapshot blob size** (directly proportional, see above).
- The **fan-out** of the tree (higher `MaxLeafKeys` = fewer grains, shallower tree, fewer splits).
- The **per-activation in-memory cache size** (one cache per leaf grain, rehydrated from the WAL on activation).

It does **not** control the leaf state row size and does **not** control the WAL row size.

**Recommended starting points:**

| Workload | Snapshot provider | Suggested `MaxLeafKeys` | Rationale |
|---|---|---|---|
| Small values (UUIDs, flags) | any | 128 to 1,024 | Snapshot blob is small at any fan-out |
| Medium values (~500 B JSON / DTOs) | Table Storage, Cosmos DB | 128 to 512 | Snapshot blob stays under 1 MB |
| Medium values | Blob Storage, ADO.NET | 512 to 1,024 | Fan-out wins; snapshot blob easily accommodated |
| Large values (>2 KB) | Table Storage, DynamoDB | 32 to 128 | Snapshot blob cap dominates |
| Large values | Blob Storage | 256 to 1,024 | Snapshot blob stays under 10 MB |

> **Applying a new `MaxLeafKeys` / `MaxInternalChildren`:** call [`ResizeAsync`](api.md#resize) on the live tree (online, LWW-safe, undoable via [`UndoResizeAsync`](api.md#resize)), or pre-seed the pin on a new tree via `ILatticeRegistry.RegisterAsync`. See [Tree Sizing - Resizing an Existing Tree](tree-sizing.md#resizing-an-existing-tree). To grow the physical shard count, call [`ReshardAsync`](api.md#resize) (online, grow-only).

## Internal node sizing

`InternalNodeState` carries `MaxInternalChildren` `ChildEntry` records plus per-subtree digest aggregates:

| Field | Approximate size |
|---|---|
| `Children` (`List<ChildEntry>`) | `MaxInternalChildren * (60-80 bytes GrainId + 4 bytes + separator key UTF-8 + ~10-15 bytes framing)` |
| `Clock` (HLC) | 12 bytes |
| `ParentId` | ~80 bytes when present |
| Split metadata (`SplitState`, `SplitKey`, `SplitSiblingId`, `SplitRightChildren`) | ~80 bytes idle; up to `MaxInternalChildren * 90 bytes` during a split |
| `SubtreeProjectionHash` | ~20 bytes when present |
| `SubtreeEntryCount` / `SubtreeHighestCheckpointOffset` | 16 bytes |
| `ChildDigests` (per-child snapshot table) | ~80 bytes per child (`GrainId` + 16-byte hash + entry count + offset + framing) |
| `TreeId` | 4 bytes + UTF-8 bytes |
| Orleans state envelope | ~100-200 bytes |

**Internal state row formula:**

```
InternalStateSize ~= 200 + MaxInternalChildren * (170 + avgSeparatorKeySize)
```

With the default `MaxInternalChildren = 128` and 50-byte separator keys, the internal state row is roughly 28 KB, comfortably under DynamoDB's 400 KB limit. You would need separator keys averaging over 2 KB and `MaxInternalChildren > 128` before internal nodes become a sizing concern. In practice, **`MaxInternalChildren` is tuned for tree depth and fan-out, not storage limits.**

> **Note on `MaintainProjectionDigest`.** `MaintainProjectionDigest = false` zeroes the per-child digest cost (the `ChildDigests` table is left untouched and the upward publish is skipped); see [Configuration - `MaintainProjectionDigest`](configuration.md#maintainprojectiondigest).

## Default-configuration assessment

The defaults `MaxLeafKeys = 128`, `MaxInternalChildren = 128` are conservative across every supported storage provider:

| Provider | Leaf state row | Internal state row | Snapshot blob at default `MaxLeafKeys` |
|---|---|---|---|
| Azure Table Storage | safe (~1 KB) | safe (~28 KB) | safe up to ~6 KB average value |
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

> **Applying the result:** to change `MaxLeafKeys` / `MaxInternalChildren` on a live tree, call [`ResizeAsync`](api.md#resize) (online, LWW-safe, undoable via [`UndoResizeAsync`](api.md#resize)). To grow the physical shard count, call [`ReshardAsync`](api.md#resize) (online, grow-only). For a brand-new tree, either call these on the empty tree (fast path - no coordinator) or pre-register the pin via `ILatticeRegistry.RegisterAsync`. See [Tree Sizing - Resizing an Existing Tree](tree-sizing.md#resizing-an-existing-tree) and [Online Reshard](online-reshard.md).

## Measuring retained storage at runtime

The three sizing surfaces above are *design-time* models. To read the **exact retained on-wire bytes** a tree is costing right now - not an entry-count estimate - call [`ILattice.GetStorageUsageAsync`](api.md#storage-usage). It fans out across the tree's shards and WAL partitions and returns a `TreeStorageUsageReport` with `WalRetainedBytes`, `SnapshotBytes`, `LeafStateBytes`, and their `TotalBytes` sum. A cluster-wide roll-up across every registered tree is available via [`ILatticeAdmin.GetTotalStorageUsageAsync`](api.md#latticeadmin).

Reports are coalesced behind a short TTL cache (`LatticeOptions.StorageUsageCacheTtl`, default 10 s) so repeated dashboard scrapes stay cheap. A WAL provider that does not implement byte accounting (`IWalStorageProvider.GetRetainedByteSizeAsync` returns the `-1` "unsupported" sentinel) sets `Partial = true`; consumers should render that as "n/a" rather than a misleading zero.

The same figures are published as observable gauges on the `orleans.lattice` meter (`storage.wal_bytes`, `storage.snapshot_bytes`, `storage.leaf_state_bytes`, `storage.total_bytes`) and surfaced on the bundled **Overview** Grafana dashboard. See [Metrics](metrics.md) for the full instrument list.

### Self-populating gauges in a multi-silo cluster

The storage gauges are driven by a per-silo background poller (`StorageUsagePollInterval`, default 15 s), so they populate automatically as soon as a silo starts - no caller has to invoke `GetStorageUsageAsync` to make a dashboard light up. On each tick the poller calls `ILatticeAdmin.GetTotalStorageUsageAsync`, which fans out to every registered tree's storage-usage aggregator.

Each aggregator is a single cluster-wide activation, so its publish lands on **its own host silo's** metrics sink. That means a tree contributes its byte series on exactly one silo, and a cross-silo `sum by (tree)` counts it once regardless of how many silos run the poller. Running the poller on every silo is intentional and needs no leader election: the aggregator's `StorageUsageCacheTtl` coalesces redundant polls from sibling silos, and if the silo that would "own" a poll dies the survivors keep the gauges fresh.

Migration is handled by a staleness horizon. When a tree's aggregator moves to another silo, the old silo stops refreshing that tree's series; after the horizon (four poll intervals, floored at 60 s) the stale series stops being observed on the old silo, so the tree never appears on two scrape targets at once. Set `StorageUsagePollInterval` to `TimeSpan.Zero` (or a negative value) to disable the poller, in which case the gauges populate only when the public storage-usage API is called.

### Advisory byte-pressure WAL retention

WAL retention is normally bounded by consumer cursors and an optional wall-clock TTL (`LatticeOptions.WalRetention`). For a size-based safety valve, set `LatticeOptions.WalMaxRetainedBytes` - an **advisory** per-tree ceiling on retained WAL bytes. When set, each `ILatticeWalGc.RunOnceAsync` pass samples retained bytes before and after its safe trim:

- If the pre-trim total exceeds the ceiling, the policy schedules a byte-pressure trim and increments `orleans.lattice.storage.policy.trim_triggered` (tagged `reason=byte_pressure`). The bytes actually freed are reported on `orleans.lattice.storage.policy.bytes_reclaimed` and on the report's `RetainedBytesBefore` / `RetainedBytesAfter` fields.
- The trim **never crosses the safe frontier** (the minimum consumer cursor intersected with the causal-stable frontier). If a lagging consumer pins the bytes, the data is preserved, `orleans.lattice.storage.policy.over_threshold` reports `1`, and the write path is unaffected. The breach is advisory; the durability invariant wins.

`WalBytePressureReclaimTarget` (default `0.8`) is the low-water fraction of the ceiling that disarms the policy, providing hysteresis so a tree hovering near the ceiling does not thrash: byte pressure arms when retained crosses the full ceiling and keeps re-triggering until a trim drives retained at or below `WalBytePressureReclaimTarget * WalMaxRetainedBytes`, after which growth inside the band does not re-trigger until the ceiling is crossed again. Leaving `WalMaxRetainedBytes` at its default `null` disables the policy entirely with zero hot-path cost.

## Key trade-offs

| Direction | Effect |
|---|---|
| **Increase `MaxLeafKeys`** | Fewer grains, shallower tree, fewer splits, larger per-activation cache, **larger snapshot blob** on fall-off-log capture |
| **Decrease `MaxLeafKeys`** | More grains, deeper tree, more splits, smaller per-activation cache, smaller snapshot blob |
| **Increase `MaxInternalChildren`** | Shallower tree, fewer routing hops, slightly larger internal state row |
| **Decrease `MaxInternalChildren`** | Deeper tree, more routing hops, smaller internal state row |
| **Larger individual values** | WAL row grows directly; snapshot blob grows directly; leaf state row unaffected |
| **More distinct origin clusters writing to the same leaf** | Per-replica `VersionVector` entries accumulate on the leaf state row (set `LatticeOptions.VersionVectorRetention` to bound this) |

## Summary

1. **The leaf state row no longer scales with `MaxLeafKeys`** - the per-key projection lives in a per-activation in-memory cache rehydrated from the WAL.
2. **Three surfaces have non-trivial growth: the WAL row (per-mutation), the snapshot blob (per-live-key), and the per-replica version vectors on the leaf state row.**
3. **Size the WAL row against the largest single mutation any caller will write.** Pick a WAL provider whose per-row limit exceeds that worst case, and remember `LatticeOptions.WalMaxBatchBytes` (default 4 MiB) caps the batch, not the row.
4. **Size the snapshot blob against `MaxLeafKeys * average row size`.** This is the surface most workloads need to verify against the storage-provider per-row limit.
5. **Defaults (`MaxLeafKeys = 128`, `MaxInternalChildren = 128`) are safe on every supported provider** for values up to a few KB.
6. **Internal nodes rarely need tuning** for storage limits - only for tree depth versus fan-out trade-offs.
7. **Apply sizing changes online** via [`ResizeAsync`](api.md#resize) (fan-out) or [`ReshardAsync`](api.md#resize) (shard count). Both run without taking the tree offline and update the registry pin atomically.

To change sizing on an existing tree, see [Tree Sizing - Resizing an Existing Tree](tree-sizing.md#resizing-an-existing-tree).