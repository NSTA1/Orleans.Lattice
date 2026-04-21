# Tree Sizing

This document explains how structural sizing (`MaxLeafKeys`, `MaxInternalChildren`, `ShardCount`) interacts with storage provider limits and provides sizing recommendations for each provider.

> **Structural sizing is registry-pinned, not option-configured.** `MaxLeafKeys`, `MaxInternalChildren`, and `ShardCount` live on the `TreeRegistryEntry`, not on `LatticeOptions`. Canonical defaults come from `LatticeConstants` (128 / 128 / 64) and are seeded into the registry on first tree use. After seeding, the pin is the sole source of structural truth — all grains read it via `LatticeOptionsResolver`. The only supported mutation paths are [`ResizeAsync`](#resizing-an-existing-tree) (leaf / internal capacity) and [`ReshardAsync`](tree-registry.md) (shard count); both run online and update the pin atomically. To start a tree with non-default sizing, call `ResizeAsync` / `ReshardAsync` on the freshly-created empty tree (empty-tree fast-path — no coordinator machinery) or pre-register the pin via `ILatticeRegistry.RegisterAsync`.

## How Grain State Size Is Determined

Every Lattice grain persists its state as a single serialized blob. The two grains whose state grows with data are:

| Grain | State class | What drives size |
|---|---|---|
| `BPlusLeafGrain` | `LeafNodeState` | `MaxLeafKeys` entries, each a `string` key + `LwwValue<byte[]>` (value bytes, HLC timestamp, tombstone flag) + version vector + sibling pointers |
| `BPlusInternalGrain` | `InternalNodeState` | `MaxInternalChildren` child entries, each a `string?` separator key + `GrainId` |

`ShardRootGrain` state is small and constant-sized — it is not a concern.

### Leaf node: per-entry overhead

Each entry in `LeafNodeState.Entries` is serialized as:

| Component | Type | Approximate serialized size |
|---|---|---|
| Key | `string` | 4 bytes length prefix + UTF-8 bytes |
| Value payload | `byte[]` | 4 bytes length prefix + raw bytes |
| HLC timestamp | `HybridLogicalClock` | 12 bytes (long + int) |
| Tombstone flag | `bool` | 1 byte |
| Orleans serialization framing | — | ~10–20 bytes per entry (field IDs, type headers) |

**Per-entry total ≈ 31 bytes overhead + key bytes + value bytes.**

In addition, the leaf carries fixed-cost fields:

| Field | Approximate size |
|---|---|
| `Clock` (HLC) | 12 bytes |
| `Version` (VersionVector) | ~50–200 bytes (grows with replica count) |
| `LastCompactionVersion` | ~50–200 bytes |
| Sibling pointers (×3) | ~100 bytes each when present |
| Split metadata | ~50 bytes |
| Orleans state envelope | ~100–200 bytes |

**Fixed overhead ≈ 0.5–1 KB.**

### Internal node: per-child overhead

Each `ChildEntry` is:

| Component | Approximate size |
|---|---|
| Separator key (`string?`) | 4 bytes + UTF-8 bytes (null for leftmost) |
| `GrainId` | ~60–80 bytes (type + key) |
| Framing | ~10–15 bytes |

**Per-child total ≈ 80–100 bytes + separator key bytes.**

Fixed overhead for `InternalNodeState` is small (~200 bytes).

## Estimating Maximum Grain State Size

### Leaf grain (worst case)

```
LeafStateSize ≈ 1 KB + MaxLeafKeys × (31 + avgKeySize + avgValueSize)
```

### Internal grain (worst case)

```
InternalStateSize ≈ 200 + MaxInternalChildren × (90 + avgSeparatorKeySize)
```

> **Note:** Tombstoned entries still occupy space until compaction removes them. Between compaction passes, a leaf may contain up to `MaxLeafKeys` tombstoned entries *in addition to* live entries temporarily (during merges), though splits prevent it from exceeding `MaxLeafKeys` total entries at rest.

## Storage Provider Limits

The table below lists the per-grain state size limit for each Orleans storage provider. These limits are **hard caps** — exceeding them causes write failures (exceptions on `WriteStateAsync`).

| Storage Provider | Max state size per grain | Limiting factor |
|---|---|---|
| **Azure Table Storage** | **~960 KB** (see note) | Single entity limit is 1 MB including all property overhead. String properties are limited to 64 KB each, but Orleans stores state as a single binary property. The effective limit after partition key, row key, timestamp, and property overhead is ~960 KB. |
| **Azure Blob Storage** | **~190.7 TiB** (block blob) | Block blob max is approximately 190.7 TiB (50,000 blocks × 4,000 MiB). In practice, serialization/deserialization memory is the real constraint — states above ~50–100 MB become impractical. |
| **Azure Cosmos DB** | **2 MB** | Maximum document size is 2 MB. The Orleans provider stores each grain as a single document. |
| **ADO.NET (SQL Server)** | **~2 GB** | `VARBINARY(MAX)` column. Practical limit is much lower due to memory and query performance — stay well under 10 MB. |
| **ADO.NET (PostgreSQL)** | **~1 GB** | `BYTEA` column. Same practical caveat as SQL Server. |
| **ADO.NET (MySQL)** | **~4 GB** | `LONGBLOB` column. Same practical caveat. |
| **Amazon DynamoDB** | **400 KB** | Maximum item size is 400 KB. |
| **Memory (dev/test only)** | **Unlimited** | Bounded only by available RAM. Not durable. |
| **Redis** | **512 MB** | Max value size. Practical limit is much lower. |

> **Important — Azure Table Storage:** Although the Azure Table entity size limit is often cited as 1 MB, the effective payload capacity depends on how the Orleans provider encodes state. The `Orleans.Persistence.AzureStorage` provider stores grain state in a single binary property. After accounting for partition key, row key, ETag, and timestamp properties, approximately **960 KB** is available for the serialized grain state. For safety, plan for a **900 KB** usable budget.

> **Important — Cosmos DB:** The 2 MB document limit is *total* including Cosmos DB system properties (`_rid`, `_ts`, `_etag`, etc.) and the Orleans metadata envelope. Budget approximately **1.9 MB** for actual grain state.

## Sizing Recommendations by Provider

### Azure Table Storage (limit: ~900 KB usable)

This is the most constrained common provider. Size your nodes conservatively.

**Formula to check fit:**

```
MaxLeafKeys × (31 + avgKeySize + avgValueSize) + 1024 < 900 KB (921,600 bytes)
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

### Azure Blob Storage (limit: ~50 MB practical)

Blob Storage has an enormous theoretical limit but serialization cost and memory pressure during `ReadStateAsync` / `WriteStateAsync` make very large states impractical.

| Scenario | Avg key | Avg value | Recommended `MaxLeafKeys` | Estimated max leaf size |
|---|---|---|---|---|
| Small values | 36 B | 8 B | 1,024 | ~77 KB |
| Medium values | 50 B | 500 B | 1,024 | ~596 KB |
| Large values | 50 B | 4 KB | 512 | ~2 MB |
| Very large values | 50 B | 16 KB | 256 | ~4 MB |

**Recommendation:** Blob Storage is the most forgiving provider. The defaults (128) are very conservative here. You can safely increase `MaxLeafKeys` to 512–1,024 for better fan-out and fewer grains, as long as total leaf size stays under ~10 MB for comfortable performance.

### Azure Cosmos DB (limit: ~1.9 MB usable)

| Scenario | Avg key | Avg value | Recommended `MaxLeafKeys` | Estimated max leaf size |
|---|---|---|---|---|
| Small values | 36 B | 8 B | 1,024 | ~77 KB ✅ |
| Medium values | 50 B | 500 B | 1,024 | ~596 KB ✅ |
| Large values | 50 B | 2 KB | 512 | ~1.0 MB ✅ |
| Very large values | 50 B | 4 KB | 256 | ~1.0 MB ✅ |

**Recommendation:** The default of 128 is safe for all typical workloads. You can increase to 256–512 for small-to-medium values. Avoid exceeding 1,024 unless values are tiny. Watch for the 2 MB boundary — leave at least 100 KB headroom.

### Amazon DynamoDB (limit: 400 KB)

This is the **most restrictive** provider. Plan very carefully.

```
MaxLeafKeys × (31 + avgKeySize + avgValueSize) + 1024 < 380 KB (389,120 bytes)
```

| Scenario | Avg key | Avg value | Recommended `MaxLeafKeys` | Estimated max leaf size |
|---|---|---|---|---|
| Small values | 36 B | 8 B | 512 | ~39 KB ✅ |
| Medium values | 50 B | 500 B | 256 | ~149 KB ✅ |
| Large values | 50 B | 2 KB | 128 | ~267 KB ✅ |
| Very large values | 50 B | 4 KB | 64 | ~262 KB ✅ |

**Recommendation:** For DynamoDB, reduce `MaxLeafKeys` aggressively for larger values. The default of 128 works for values up to ~2 KB. For larger values, drop to 64 or 32. Internal nodes with the default `MaxInternalChildren = 128` are fine (~18 KB).

### ADO.NET / SQL (limit: practical ~10 MB)

Relational providers have very large theoretical limits but performance degrades with large binary columns.

**Recommendation:** Treat as similar to Blob Storage. The defaults are conservative and safe. You can increase `MaxLeafKeys` to 512–1,024 for small values. Keep total leaf state under 5–10 MB for responsive queries.

### Redis (limit: practical ~5 MB)

**Recommendation:** Similar to Blob Storage but with tighter practical limits due to single-threaded processing. Keep leaf state under 1–2 MB for responsive operations. Defaults are safe.

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

The defaults are safe for **all providers** with values up to ~2.5 KB — covering most typical use cases (JSON documents, serialized DTOs, small binary payloads).

## Calculating Your Limits

Use this formula to determine the maximum `MaxLeafKeys` for your workload:

```
MaxLeafKeys = floor((ProviderLimit - 1024) / (31 + avgKeySize + avgValueSize))
```

Where:
- `ProviderLimit` is the usable provider limit in bytes (e.g., 921,600 for Table Storage)
- `avgKeySize` is the average key length in bytes (UTF-8 encoded)
- `avgValueSize` is the average value length in bytes
- `1024` accounts for fixed leaf overhead
- `31` accounts for per-entry serialization overhead

**Example:** Azure Table Storage with 100-byte keys and 1 KB values:

```
MaxLeafKeys = floor((921,600 - 1,024) / (31 + 100 + 1,024)) = floor(920,576 / 1,155) = 797
```

Apply a **25% safety margin** (for tombstones, version vector growth, and serialization variance):

```
SafeMaxLeafKeys = floor(797 × 0.75) = 597
```

## Internal Node Sizing

Internal nodes are much smaller than leaf nodes because they only store separator keys and `GrainId` references — no values. The formula is:

```
InternalStateSize ≈ 200 + MaxInternalChildren × (90 + avgSeparatorKeySize)
```

With the default `MaxInternalChildren = 128` and 50-byte separator keys:

```
InternalStateSize ≈ 200 + 128 × 140 = ~18 KB
```

This fits comfortably within **all** storage providers, including DynamoDB (400 KB). You would need separator keys averaging over 2 KB *and* `MaxInternalChildren` over 128 before internal nodes become a sizing concern. In practice, **you do not need to tune `MaxInternalChildren` for storage limits** — only for tree depth vs. fan-out trade-offs.

## Key Trade-Offs

| Direction | Effect |
|---|---|
| **Increase `MaxLeafKeys`** | Fewer grains, shallower tree, fewer splits, larger state per grain, higher risk of hitting provider limits, more data transferred per `ReadStateAsync`/`WriteStateAsync` |
| **Decrease `MaxLeafKeys`** | More grains, deeper tree, more splits, smaller state per grain, well within provider limits, faster individual reads/writes |
| **Increase `MaxInternalChildren`** | Shallower tree, fewer routing hops, slightly larger internal state (rarely a problem) |
| **Decrease `MaxInternalChildren`** | Deeper tree, more routing hops, smaller internal state |

## Resizing an Existing Tree

If you need to change `MaxLeafKeys` or `MaxInternalChildren` on a tree that already contains data, use the `ResizeAsync` API:

```csharp
var tree = grainFactory.GetGrain<ILattice>("my-tree");
await tree.ResizeAsync(newMaxLeafKeys: 256, newMaxInternalChildren: 64);
```

### How it works

Resize runs **online**: reads and writes remain available throughout. A whole-tree shape change cannot be done in place — every leaf and every internal node has to be re-paginated at the new fan-out — so `ResizeAsync` drains the source into a freshly-provisioned destination physical tree and then atomically swaps the registry alias.

1. **Provision destination** — `TreeResizeGrain` creates a destination physical tree ID (e.g. `my-tree/resized/{operationId}`) registered with the new `MaxLeafKeys` / `MaxInternalChildren`. The destination inherits the source's `ShardCount` and a copy of its `ShardMap`, so slot *x* on the source routes to the same physical shard index on the destination.
2. **Snapshot with shadow forwarding** — the source tree runs under `SnapshotMode.Online`. Before drain begins, every source `ShardRootGrain` is placed in `ShadowForwardPhase.Draining` via `BeginShadowForwardAsync`, and every mutation path (`SetAsync`, `SetManyAsync`, `SetManyAtomicAsync`, `DeleteAsync`, `DeleteManyAsync`, `DeleteRangeAsync`, `BulkLoadAsync`, and saga compensation) runs the local write and a parallel forward to the corresponding destination shard.
3. **Drain** — `TreeSnapshotGrain` reads each source shard's current entries and merges them into the destination via `IShardRootGrain.MergeManyAsync` (bounded by `LatticeOptions.MaxConcurrentDrains`, default 4). Raw `LwwEntry` payloads flow through verbatim: HLC timestamps, tombstone flags, and TTL (`ExpiresAtTicks`) are preserved. When every shard has finished draining, its shadow state transitions to `Drained`; live forwards continue until swap.
4. **Swap** — the registry entry is updated with the new sizing and `SetAliasAsync` atomically points the logical tree ID at the destination. Once complete, each source shard transitions to `ShadowForwardPhase.Rejecting` and subsequent writes against the old physical tree throw `StaleTreeRoutingException`. The stateless-worker `LatticeGrain` catches this exception, re-resolves via the registry, and retries transparently against the destination — callers never observe the transition.
5. **Cleanup** — the old physical tree is soft-deleted. It will be purged automatically after the configured `SoftDeleteDuration` (default 72 hours), leaving `UndoResizeAsync` viable until then.

Each phase transition is persisted. If the silo crashes mid-resize, reminder-anchored `TreeResizeGrain` and `TreeSnapshotGrain` reactivate and resume from the last completed phase; source shards retain their `ShadowForwardState` across activations, so live forwards continue uninterrupted.

### LWW convergence — why shadow forwarding is safe

Every entry carries a hybrid-logical-clock timestamp and all writes flow through a last-writer-wins comparator. That makes the shadow-forward path commutative: whether a live write arrives at the destination before or after the drain reader copies the same key, the destination converges to the entry with the higher HLC. Consequently:

- The parallel `local ∥ forward` write is **not** a two-phase commit. If local succeeds and forward fails, the client sees failure; the next idempotent retry lands on both trees. Writes that briefly land on the source only are captured by the drain reader and re-delivered with their original HLCs.
- The drain uses `MergeManyAsync` (not `BulkLoadRawAsync`) because shadow-forwarded writes can populate destination shards ahead of the drain batch. LWW merge absorbs the race; a bulk-load would error on non-empty destination shards. (The offline snapshot path still uses `BulkLoadRawAsync` — source shards are locked before drain so the destination is guaranteed empty.)

### Cache invalidation

Different physical trees produce different leaf grain IDs, which automatically create fresh `LeafCacheGrain` instances. No explicit cache flush is needed after the alias swap. See [Read Caching](caching.md#cache-invalidation-via-tree-aliasing) for details.

### Undo resize

During the soft-delete window the resize can be undone:

```csharp
var tree = grainFactory.GetGrain<ILattice>("my-tree");
await tree.UndoResizeAsync();
```

`UndoResizeAsync` is phase-aware:

- **Before swap** (`Phase == Snapshot`) — aborts the snapshot coordinator, clears every source shard's `ShadowForwardState`, deletes the half-built destination tree, and returns the source to a fully-writable state. No alias was ever set, so clients never observed the destination.
- **After swap** (`Phase ∈ { Swap, Reject, Cleanup }`) — recovers the old physical tree, removes the alias, restores the original registry configuration, clears any residual `Rejecting` phase on source shards, defensively aborts any post-swap snapshot still attached, and deletes the new snapshot tree.

Once the soft-delete window expires and the old tree is purged, the resize can no longer be undone.

### Important considerations

- **Availability:** reads and writes continue throughout. The per-shard `Rejecting` window at swap is absorbed by `LatticeGrain`'s `StaleTreeRoutingException` retry — callers observe at most one internal retry, not an error.
- **Storage:** both the old and new physical trees exist simultaneously until the old tree is purged. Plan for approximately 2× the tree's storage usage during this window.
- **Hot-path cost during drain:** every write between `BeginShadowForwardAsync` and swap pays one extra grain hop for the parallel forward. For same-cluster destinations this is in the millisecond range. Prefer off-peak windows for large resizes even though they are online.
- **Concurrency cap:** `LatticeOptions.MaxConcurrentDrains` (default 4) bounds the number of concurrent per-shard drains `TreeSnapshotGrain` dispatches. Mirrors `MaxConcurrentMigrations` for reshard.
- **Idempotency:** calling `ResizeAsync` again with the same parameters while a resize is in progress is a no-op. Calling with different parameters throws `InvalidOperationException`.
- **Registry is the source of truth:** the new sizing values are persisted on the `TreeRegistryEntry` and read through `LatticeOptionsResolver` by every structural grain. You do not need to update `LatticeOptions` in silo configuration separately — `LatticeOptions` no longer exposes `MaxLeafKeys` / `MaxInternalChildren` / `ShardCount`.
- **Empty-tree fast-path:** if the tree has no live entries yet, `ResizeAsync` and `ReshardAsync` update the registry pin in-place and return immediately without activating the coordinator machinery. This is the recommended way to start a tree with non-default sizing.
- **Interlocks:** while a resize is in flight, `HotShardMonitorGrain` suppresses autonomic splits on the tree and `ReshardAsync` throws `InvalidOperationException`. Run reshard first if you need both, then resize.
- **`ShardCount` cannot be resized via `ResizeAsync`.** Changing shard count requires re-hashing all keys, which `ResizeAsync` does not support. Use `ReshardAsync` for that; it runs online via its own shadow-write + swap primitive.

### Manual trigger (testing)

In integration tests, the existing test harnesses call `ITreeResizeGrain` directly to drive resize passes synchronously. This grain is **guarded by `InternalGrainGuardFilter`** — external callers (including application code and `dotnet tool` wrappers) cannot invoke it. Use `ILattice.ResizeAsync` for all non-test scenarios; it delegates to `ITreeResizeGrain` internally and exposes `ILattice.IsResizeCompleteAsync()` for progress polling.

## Summary

1. **Know your storage provider's hard limit** before tuning options.
2. **Estimate your average entry size** (key + value + 31 bytes overhead).
3. **Apply the formula** with a 25% safety margin.
4. **DynamoDB and Table Storage** are the most constrained — always verify fit.
5. **The defaults are safe for all providers** when values are under ~2.5 KB.
6. **Internal nodes rarely need tuning** for storage limits — only for depth trade-offs.
7. **Monitor grain state sizes** in production — tombstones, version vector growth, and value size variance can push actual sizes above estimates.
