# Tree Storage

This document explains how Lattice grain state is persisted, how structural sizing (`MaxLeafKeys`, `MaxInternalChildren`, `ShardCount`) interacts with storage-provider limits, and how to size a tree for each supported provider.

For the *mechanics* of changing sizing on an existing tree, see [Tree Sizing](tree-sizing.md). For the registry entry that pins these values per tree, see [Tree Registry](tree-registry.md).

> **WAL-first storage.** Every foreground commit (set / delete / range-delete / saga prepare / saga terminal / cross-migration backstop / merge / tombstone reap) is durably written to the per-shard write-ahead log *before* it touches in-memory state. The WAL append is the durability boundary; the leaf grain's persisted state row is a *small, fixed-shape* row carrying topology / lifecycle metadata (sibling pointers, split lifecycle, key range, parent reference, last-compaction version), a 16-byte projection-digest XOR fold (`ProjectionHash`), and the `ProjectionCheckpointOffset` pointing into the WAL. Per-key LWW entries are **not** persisted: activation rebuilds the per-activation runtime cache by replaying the WAL strictly after `ProjectionCheckpointOffset`. Sizing the leaf state row therefore caps the *topology + checkpoint + digest* surface, **not** the per-key projection - which means the leaf state row no longer grows linearly in `MaxLeafKeys`. The per-mutation commit-log row lives in the WAL provider's storage and is governed by `LatticeOptions.WalMaxBatchBytes` and the WAL provider's own per-row limit. See [Write-Ahead Log](wal.md) and [WAL Storage Providers](wal-storage-providers.md) for the WAL-side sizing model.
>
**Sizing tables in this document are historical.** The per-`MaxLeafKeys` storage-provider sizing tables below were written when the leaf state row carried the per-key `Entries` dictionary. After the leaf-state collapse, the persisted leaf row

> **Structural sizing is registry-pinned, not option-configured.**

## How Grain State Size Is Determined

Every Lattice grain persists its state as a single serialized blob. The two grains whose state grows with data are:

| Grain | State class | What drives size |
|---|---|---|
| `BPlusLeafGrain` | `LeafNodeState` | `MaxLeafKeys` entries, each a `string` key + `LwwValue<byte[]>` (value bytes, HLC timestamp, tombstone flag) + version vector + sibling pointers |
| `BPlusInternalGrain` | `InternalNodeState` | `MaxInternalChildren` child entries, each a `string?` separator key + `GrainId` |

`ShardRootGrain` state is small and constant-sized - it is not a concern.

### Leaf node: per-entry overhead

Each entry in `LeafNodeState.Entries` is a `KeyValuePair<string, LwwValue<byte[]>>`. The `LwwValue<byte[]>` carries every field required for LWW merge, TTL, replication routing, and saga drain discrimination:

| Component | Type | Approximate serialized size |
|---|---|---|
| Key | `string` | 4 bytes length prefix + UTF-8 bytes |
| Value payload | `byte[]?` | 4 bytes length prefix + raw bytes (0 bytes for tombstones) |
| HLC timestamp | `HybridLogicalClock` | 12 bytes (long + int) |
| Tombstone flag | `bool` | 1 byte |
| TTL expiry | `long` (ticks) | 8 bytes (`0` when no TTL) |
| Origin cluster id | `string?` | 4 bytes + UTF-8 bytes when set; ~1 byte null marker otherwise |
| Vector clock | `VersionVector?` | ~50-200 bytes when set (per-replica entries); ~1 byte null marker otherwise |
| Migrated marker | `bool` | 1 byte |
| Orleans serialization framing | - | ~15-25 bytes per entry (field IDs, type headers) |

**Per-entry total (single-cluster, no TTL, no vector clock):** ≈ 45 bytes overhead + key bytes + value bytes.

**Per-entry total (replicated, with vector clock):** ≈ 100-250 bytes overhead + key bytes + value bytes (the vector clock is the dominant variable cost; it grows with the number of distinct origin clusters that have ever written to that key).

In addition, the leaf carries fixed-cost fields persisted alongside `Entries`:

| Field | Approximate size |
|---|---|
| `Clock` (HLC) | 12 bytes |
| `Version` (VersionVector) | ~50-200 bytes (grows with replica count) |
| `LastCompactionVersion` (VersionVector) | ~50-200 bytes |
| Sibling pointers `NextSibling` / `PrevSibling` / `OldNextSibling` | ~80 bytes each when present |
| `ParentId` (`GrainId?`) | ~80 bytes when present |
| Split metadata (`SplitState`, `SplitKey`, `SplitSiblingId`) | ~80 bytes |
| `TreeId` (`string?`) | 4 bytes + UTF-8 bytes |
| `ProjectionCheckpointOffset` (`long`) | 8 bytes |
| `ProjectionHash` (`byte[]?`, 16-byte XOR fold) | ~20 bytes when present |
| `ShardIndex` (`int?`) | ~5 bytes |
| `LowKeyInclusive` / `HighKeyExclusive` (`string?`) | 4 bytes + UTF-8 bytes each when present |
| `MovedAwaySlots` (`int[]?`) / `MovedAwayVirtualShardCount` (`int?`) | sparse: 0 bytes on a non-resharded leaf; 4 bytes per moved slot otherwise |
| Orleans state envelope | ~100-200 bytes |

**Fixed overhead ≈ 0.6-1.2 KB on a steady-state leaf** (sparse slots null, vector clocks small). Add ~4 bytes per moved-away slot on a leaf that has participated in an adaptive split.

### Internal node: per-child overhead

Each `ChildEntry` is:

| Component | Approximate size |
|---|---|
| Separator key (`string?`) | 4 bytes + UTF-8 bytes (null for leftmost) |
| `GrainId` | ~60–80 bytes (type + key) |
| Framing | ~10–15 bytes |

**Per-child total ≈ 80-100 bytes + separator key bytes.**

`InternalNodeState` also carries projection-digest aggregates published upward by descendant leaves:

| Field | Approximate size |
|---|---|
| `Clock` (HLC) | 12 bytes |
| `ParentId` (`GrainId?`) | ~80 bytes when present |
| Split metadata (`SplitState`, `SplitKey`, `SplitSiblingId`, `SplitRightChildren`) | ~80 bytes (children list contributes ~90 bytes per pending right-side child during a split) |
| `SubtreeProjectionHash` (`byte[]?`, 16-byte XOR fold) | ~20 bytes when present |
| `SubtreeEntryCount` / `SubtreeHighestCheckpointOffset` (`long` each) | 16 bytes |
| `ChildDigests` (`Dictionary<GrainId, ChildDigestSnapshot>`) | ~80 bytes per child (`GrainId` + 16-byte hash + count + offset + framing) |
| `TreeId` (`string?`) | 4 bytes + UTF-8 bytes |
| Orleans state envelope | ~100-200 bytes |

**Fixed overhead ≈ 0.4-0.6 KB plus ~80 bytes per child for the digest table.** `MaintainProjectionDigest = false` zeroes the per-child digest cost (the table is left untouched and the upward publish is skipped); see [Configuration - `MaintainProjectionDigest`](configuration.md#maintainprojectiondigest).

## Estimating Maximum Grain State Size

### Leaf grain (worst case)

```
LeafStateSize ≈ 1 KB + MaxLeafKeys × (45 + avgKeySize + avgValueSize)
```

This assumes a single-cluster tree with no TTLs and no per-entry vector clocks. For a tree with vector clocks captured per write (multi-cluster replication), substitute ~150 bytes for the `45` constant; that figure dominates the per-entry budget on heavily replicated trees.

### Internal grain (worst case)

```
InternalStateSize ≈ 200 + MaxInternalChildren × (90 + avgSeparatorKeySize)
```

> **Note:** Tombstoned entries still occupy space until compaction removes them. Between compaction passes, a leaf may contain up to `MaxLeafKeys` tombstoned entries *in addition to* live entries temporarily (during merges), though splits prevent it from exceeding `MaxLeafKeys` total entries at rest.

## Storage Provider Limits

The table below lists the per-grain state size limit for each Orleans storage provider. These limits are **hard caps** - exceeding them causes write failures (exceptions on `WriteStateAsync`). Two distinct surfaces are subject to these limits:

- **The leaf grain state row** stores the projection snapshot (`Entries` plus topology / lifecycle metadata). Its size scales with `MaxLeafKeys` and is what the rest of this document focuses on.
- **The WAL provider's per-entry row** stores one serialized `LatticeMutation`. Its size scales with the largest single value any caller writes, plus the optional vector clock and dependency-summary slots, plus per-batch framing bounded by `LatticeOptions.WalMaxBatchBytes`. On Azure Table Storage the WAL row shares the same 1 MB entity ceiling as a leaf state row, so a single mutation whose value bytes approach 900 KB will fail the WAL append even on a tree whose `MaxLeafKeys` was sized conservatively. See [WAL Storage Providers](wal-storage-providers.md) for the WAL-side budget.

| Storage Provider | Max state size per grain | Limiting factor |
|---|---|---|
| **Azure Table Storage** | **~960 KB** (see note) | Single entity limit is 1 MB including all property overhead. String properties are limited to 64 KB each, but Orleans stores state as a single binary property. The effective limit after partition key, row key, timestamp, and property overhead is ~960 KB. |
| **Azure Blob Storage** | **~190.7 TiB** (block blob) | Block blob max is approximately 190.7 TiB (50,000 blocks × 4,000 MiB). In practice, serialization/deserialization memory is the real constraint - states above ~50–100 MB become impractical. |
| **Azure Cosmos DB** | **2 MB** | Maximum document size is 2 MB. The Orleans provider stores each grain as a single document. |
| **ADO.NET (SQL Server)** | **~2 GB** | `VARBINARY(MAX)` column. Practical limit is much lower due to memory and query performance - stay well under 10 MB. |
| **ADO.NET (PostgreSQL)** | **~1 GB** | `BYTEA` column. Same practical caveat as SQL Server. |
| **ADO.NET (MySQL)** | **~4 GB** | `LONGBLOB` column. Same practical caveat. |
| **Amazon DynamoDB** | **400 KB** | Maximum item size is 400 KB. |
| **Memory (dev/test only)** | **Unlimited** | Bounded only by available RAM. Not durable. |
| **Redis** | **512 MB** | Max value size. Practical limit is much lower. |

> **Important - Azure Table Storage:** Although the Azure Table entity size limit is often cited as 1 MB, the effective payload capacity depends on how the Orleans provider encodes state. The `Orleans.Persistence.AzureStorage` provider stores grain state in a single binary property. After accounting for partition key, row key, ETag, and timestamp properties, approximately **960 KB** is available for the serialized grain state. For safety, plan for a **900 KB** usable budget.

> **Important - Cosmos DB:** The 2 MB document limit is *total* including Cosmos DB system properties (`_rid`, `_ts`, `_etag`, etc.) and the Orleans metadata envelope. Budget approximately **1.9 MB** for actual grain state.

## Sizing Recommendations by Provider

### Azure Table Storage (limit: ~900 KB usable)

This is the most constrained common provider. Size your nodes conservatively.

**Formula to check fit:**

```
MaxLeafKeys × (45 + avgKeySize + avgValueSize) + 1024 < 900 KB (921,600 bytes)
```

| Scenario | Avg key | Avg value | Recommended `MaxLeafKeys` | Estimated max leaf size |
|---|---|---|---|---|
| Small values (UUIDs + flags) | 36 B | 8 B | 512 | ~39 KB ✅ |
| Medium values (IDs + JSON) | 50 B | 500 B | 512 | ~298 KB ✅ |
| Large values (IDs + documents) | 50 B | 2 KB | 256 | ~533 KB ✅ |
| Very large values (blobs) | 50 B | 4 KB | 128 | ~524 KB ✅ |
| Oversized values | 50 B | 8 KB | 64 | ~517 KB ✅ |
| Huge values | 50 B | 16 KB | 32 | ~515 KB ✅ |

**Internal nodes** are rarely a concern for Table Storage. With `MaxInternalChildren = 128` and average separator keys of 50 bytes, an internal node is only ~18 KB.

**Recommendation:** For Table Storage, keep `MaxLeafKeys` × average entry size well under 900 KB. The default of 128 is safe for values up to ~6 KB. If your values exceed 4 KB, reduce `MaxLeafKeys` proportionally. If individual values approach 8 KB+, consider storing them in Blob Storage and keeping only a reference in the tree.

> **Applying a new leaf / internal fan-out:** call [`ResizeAsync`](api.md#resize) on the live tree (online, LWW-safe, undoable via [`UndoResizeAsync`](api.md#resize)), or pre-seed the pin on a new tree via `ILatticeRegistry.RegisterAsync`. See [Tree Sizing - Resizing an Existing Tree](tree-sizing.md#resizing-an-existing-tree).

### Azure Blob Storage (limit: ~50 MB practical)

Blob Storage has an enormous theoretical limit but serialization cost and memory pressure during `ReadStateAsync` / `WriteStateAsync` make very large states impractical.

| Scenario | Avg key | Avg value | Recommended `MaxLeafKeys` | Estimated max leaf size |
|---|---|---|---|---|
| Small values | 36 B | 8 B | 1,024 | ~77 KB |
| Medium values | 50 B | 500 B | 1,024 | ~596 KB |
| Large values | 50 B | 4 KB | 512 | ~2 MB |
| Very large values | 50 B | 16 KB | 256 | ~4 MB |

**Recommendation:** Blob Storage is the most forgiving provider. The defaults (128) are very conservative here. You can safely increase `MaxLeafKeys` to 512–1,024 for better fan-out and fewer grains, as long as total leaf size stays under ~10 MB for comfortable performance.

> **Applying a new leaf / internal fan-out:** call [`ResizeAsync`](api.md#resize) on the live tree (online, LWW-safe, undoable via [`UndoResizeAsync`](api.md#resize)). See [Tree Sizing - Resizing an Existing Tree](tree-sizing.md#resizing-an-existing-tree).

### Azure Cosmos DB (limit: ~1.9 MB usable)

| Scenario | Avg key | Avg value | Recommended `MaxLeafKeys` | Estimated max leaf size |
|---|---|---|---|---|
| Small values | 36 B | 8 B | 1,024 | ~77 KB ✅ |
| Medium values | 50 B | 500 B | 1,024 | ~596 KB ✅ |
| Large values | 50 B | 2 KB | 512 | ~1.0 MB ✅ |
| Very large values | 50 B | 4 KB | 256 | ~1.0 MB ✅ |

**Recommendation:** The default of 128 is safe for all typical workloads. You can increase to 256–512 for small-to-medium values. Avoid exceeding 1,024 unless values are tiny. Watch for the 2 MB boundary - leave at least 100 KB headroom.

> **Applying a new leaf / internal fan-out:** call [`ResizeAsync`](api.md#resize) on the live tree (online, LWW-safe, undoable via [`UndoResizeAsync`](api.md#resize)). See [Tree Sizing - Resizing an Existing Tree](tree-sizing.md#resizing-an-existing-tree).

### Amazon DynamoDB (limit: 400 KB)

This is the **most restrictive** provider. Plan very carefully.

```
MaxLeafKeys × (45 + avgKeySize + avgValueSize) + 1024 < 380 KB (389,120 bytes)
```

| Scenario | Avg key | Avg value | Recommended `MaxLeafKeys` | Estimated max leaf size |
|---|---|---|---|---|
| Small values | 36 B | 8 B | 512 | ~39 KB ✅ |
| Medium values | 50 B | 500 B | 256 | ~149 KB ✅ |
| Large values | 50 B | 2 KB | 128 | ~267 KB ✅ |
| Very large values | 50 B | 4 KB | 64 | ~262 KB ✅ |

**Recommendation:** For DynamoDB, reduce `MaxLeafKeys` aggressively for larger values. The default of 128 works for values up to ~2 KB. For larger values, drop to 64 or 32. Internal nodes with the default `MaxInternalChildren = 128` are fine (~18 KB).

> **Applying a new leaf / internal fan-out:** call [`ResizeAsync`](api.md#resize) on the live tree (online, LWW-safe, undoable via [`UndoResizeAsync`](api.md#resize)), or pre-seed the pin on a new tree via `ILatticeRegistry.RegisterAsync`. See [Tree Sizing - Resizing an Existing Tree](tree-sizing.md#resizing-an-existing-tree).

### ADO.NET / SQL (limit: practical ~10 MB)

Relational providers have very large theoretical limits but performance degrades with large binary columns.

**Recommendation:** Treat as similar to Blob Storage. The defaults are conservative and safe. You can increase `MaxLeafKeys` to 512–1,024 for small values. Keep total leaf state under 5–10 MB for responsive queries.

> **Applying a new leaf / internal fan-out:** call [`ResizeAsync`](api.md#resize) on the live tree (online, LWW-safe, undoable via [`UndoResizeAsync`](api.md#resize)). See [Tree Sizing - Resizing an Existing Tree](tree-sizing.md#resizing-an-existing-tree).

### Redis (limit: practical ~5 MB)

**Recommendation:** Similar to Blob Storage but with tighter practical limits due to single-threaded processing. Keep leaf state under 1–2 MB for responsive operations. Defaults are safe.

> **Applying a new leaf / internal fan-out:** call [`ResizeAsync`](api.md#resize) on the live tree (online, LWW-safe, undoable via [`UndoResizeAsync`](api.md#resize)). See [Tree Sizing - Resizing an Existing Tree](tree-sizing.md#resizing-an-existing-tree).

## Default Configuration Assessment

The defaults (`MaxLeafKeys = 128`, `MaxInternalChildren = 128`) were chosen conservatively:

| Provider | Default safe? | Maximum average value size at defaults |
|---|---|---|
| Azure Table Storage | ✅ Yes | ~6.7 KB per value |
| Azure Blob Storage | ✅ Yes | Effectively unlimited |
| Azure Cosmos DB | ✅ Yes | ~14 KB per value |
| Amazon DynamoDB | ✅ Yes | ~2.7 KB per value |
| ADO.NET (any) | ✅ Yes | Effectively unlimited |
| Redis | ✅ Yes | Effectively unlimited |
| Memory | ✅ Yes | Bounded by RAM |

The defaults are safe for **all providers** with values up to ~2.5 KB - covering most typical use cases (JSON documents, serialized DTOs, small binary payloads).

## Calculating Your Limits

Use this formula to determine the maximum `MaxLeafKeys` for your workload:

```
MaxLeafKeys = floor((ProviderLimit - 1024) / (45 + avgKeySize + avgValueSize))
```

Where:
- `ProviderLimit` is the usable provider limit in bytes (e.g., 921,600 for Table Storage)
- `avgKeySize` is the average key length in bytes (UTF-8 encoded)
- `avgValueSize` is the average value length in bytes
- `1024` accounts for fixed leaf overhead
- `45` accounts for per-entry serialization overhead in a single-cluster tree with no TTL and no vector clock; substitute ~150 for a multi-cluster replicated tree where every write captures a vector clock

**Example:** Azure Table Storage with 100-byte keys and 1 KB values:

```
MaxLeafKeys = floor((921,600 - 1,024) / (45 + 100 + 1,024)) = floor(920,576 / 1,169) = 787
```

SafeMaxLeafKeys = floor(787 × 0.75) = 590

> **Applying the result:** to change `MaxLeafKeys` / `MaxInternalChildren` on a live tree, call [`ResizeAsync`](api.md#resize) (online, LWW-safe, undoable via [`UndoResizeAsync`](api.md#resize)). To grow the physical shard count, call [`ReshardAsync`](api.md#resize) (online, grow-only). For a brand-new tree, either call these on the empty tree (fast path - no coordinator) or pre-register the pin via `ILatticeRegistry.RegisterAsync`. See [Tree Sizing - Resizing an Existing Tree](tree-sizing.md#resizing-an-existing-tree) and [Online Reshard](online-reshard.md).

## Internal Node Sizing

Internal nodes are much smaller than leaf nodes because they only store separator keys and `GrainId` references - no values. The formula is:

```
InternalStateSize ≈ 200 + MaxInternalChildren × (90 + avgSeparatorKeySize)
```

With the default `MaxInternalChildren = 128` and 50-byte separator keys:

```
InternalStateSize ≈ 200 + 128 × 140 = ~18 KB
```

This fits comfortably within **all** storage providers, including DynamoDB (400 KB). You would need separator keys averaging over 2 KB *and* `MaxInternalChildren` over 128 before internal nodes become a sizing concern. In practice, **you do not need to tune `MaxInternalChildren` for storage limits** - only for tree depth vs. fan-out trade-offs.

## Key Trade-Offs

| Direction | Effect |
|---|---|
| **Increase `MaxLeafKeys`** | Fewer grains, shallower tree, fewer splits, larger state per grain, higher risk of hitting provider limits, more data transferred per `ReadStateAsync`/`WriteStateAsync` |
| **Decrease `MaxLeafKeys`** | More grains, deeper tree, more splits, smaller state per grain, well within provider limits, faster individual reads/writes |
| **Increase `MaxInternalChildren`** | Shallower tree, fewer routing hops, slightly larger internal state (rarely a problem) |
| **Decrease `MaxInternalChildren`** | Deeper tree, more routing hops, smaller internal state |

## Summary

1. **Know your storage provider's hard limit** before tuning options.
2. **Estimate your average entry size** (key + value + ~45 bytes overhead for a single-cluster tree, ~150 bytes for a multi-cluster tree with vector clocks).
3. **Apply the formula** with a 25% safety margin.
4. **DynamoDB and Table Storage** are the most constrained - always verify fit.
5. **The defaults are safe for all providers** when values are under ~2.5 KB.
6. **Internal nodes rarely need tuning** for storage limits - only for depth trade-offs.
7. **Monitor grain state sizes** in production - tombstones, version vector growth, and value size variance can push actual sizes above estimates.
8. **Apply sizing changes online** via [`ResizeAsync`](api.md#resize) (fan-out) or [`ReshardAsync`](api.md#resize) (shard count) - both run without taking the tree offline and update the registry pin atomically.

To change sizing on an existing tree, see [Tree Sizing - Resizing an Existing Tree](tree-sizing.md#resizing-an-existing-tree).
