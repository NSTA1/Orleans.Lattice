# Architecture

## High-Level Architecture

A request flows through five layers of Orleans grains. Writes go directly to the primary leaf; reads are served by a stateless cache that pulls deltas from the primary:

```mermaid
flowchart TD
    Client([Client])
    SR[LatticeGrain<br/>StatelessWorker]
    S0[ShardRootGrain<br/>Shard 0]
    S1[ShardRootGrain<br/>Shard 1]
    SN[ShardRootGrain<br/>Shard N]
    I0[InternalGrain]
    I1[InternalGrain]
    C0[LeafCacheGrain<br/>StatelessWorker]
    C1[LeafCacheGrain<br/>StatelessWorker]
    C2[LeafCacheGrain<br/>StatelessWorker]
    C3[LeafCacheGrain<br/>StatelessWorker]
    L0[LeafGrain]
    L1[LeafGrain]
    L2[LeafGrain]
    L3[LeafGrain]

    Client --> SR
    SR -->|"ShardMap.Resolve(XxHash32 % 4096)"| S0
    SR --> S1
    SR --> SN
    S0 --> I0
    S1 --> I1
    I0 -->|read| C0
    I0 -->|read| C1
    I1 -->|read| C2
    I1 -->|read| C3
    I0 -->|write| L0
    I0 -->|write| L1
    I1 -->|write| L2
    I1 -->|write| L3
    C0 -.->|"GetDeltaSinceAsync"| L0
    C1 -.->|"GetDeltaSinceAsync"| L1
    C2 -.->|"GetDeltaSinceAsync"| L2
    C3 -.->|"GetDeltaSinceAsync"| L3
    L0 -. "NextSibling" .-> L1
    L2 -. "NextSibling" .-> L3
```

1. **`LatticeGrain`** — a `[StatelessWorker]` grain (many concurrent activations). Resolves the key's virtual slot via `XxHash32(key) % VirtualShardCount`, looks up the physical shard index in the cached `ShardMap`, and forwards the request to the corresponding `ShardRootGrain`.
2. **`ShardRootGrain`** — one per shard (keyed `{treeId}/{shardIndex}`). Manages the root pointer for its sub-tree and handles root-level splits by creating new internal nodes above the old root. Routes reads through the cache layer.
3. **`BPlusInternalGrain`** — an internal node holding separator keys and child references. Routes a key to the correct child and accepts promoted splits from below. Split acceptance is idempotent — duplicate deliveries are detected and skipped.
4. **`LeafCacheGrain`** — a `[StatelessWorker]` read-through cache. Each silo may have its own activation. On a cache miss, it pulls a `StateDelta` from the primary leaf and merges entries using `LwwValue.Merge`. Because the merge is commutative and idempotent, stale entries are harmlessly overwritten without an invalidation protocol.
5. **`BPlusLeafGrain`** — a leaf node storing key → value entries in a sorted dictionary. Splits when the entry count exceeds the configured maximum. Maintains a `VersionVector` that is ticked on every write, enabling delta extraction for the cache layer.

## Sharding

Without sharding, every operation starts at a single root grain — a serialisation bottleneck. Sharding eliminates this by giving each key range its own independent sub-tree:

```mermaid
flowchart LR
    subgraph Router["LatticeGrain (stateless)"]
        H["XxHash32(key) % VirtualShardCount<br/>→ ShardMap.Resolve"]
    end

    subgraph Shard0["Shard 0"]
        R0[Root] --> LA[Leaf A]
        R0 --> LB[Leaf B]
    end

    subgraph Shard41["Shard 41"]
        R41[Root] --> LC[Leaf C]
    end

    subgraph Shard63["Shard 63"]
        R63[Root] --> LD[Leaf D]
        R63 --> LE[Leaf E]
    end

    H --> R0
    H --> R41
    H --> R63
```

The hash function (`XxHash32`) is **stable across processes** — unlike `string.GetHashCode()`, it will always route the same key to the same shard. The default shard count is 64, configurable at tree creation time.

**Shard map indirection.** Routing is two-stage: keys hash into a large fixed virtual space (`LatticeConstants.DefaultVirtualShardCount`, a compile-time constant fixed at 4096), and a per-tree `ShardMap` collapses ranges of virtual slots onto physical shards. The default map (`slot[i] = i % shardCount`) preserves the legacy `hash % shardCount` routing bit-for-bit when `VirtualShardCount % ShardCount == 0` (enforced by `ShardMap.CreateDefault` at use time). The shard map is persisted on the tree's registry entry, fetched lazily by `LatticeGrain` on first access, cached for the activation's lifetime, and invalidated alongside the physical-tree-ID cache when a shard signals a stale alias. This indirection decouples logical key routing from the physical shard count, enabling adaptive shard splitting without rehashing existing keys. The virtual shard count is not a `LatticeOptions` property because changing it would invalidate every persisted `ShardMap` (slots are referenced by integer index).

**Trade-off:** Keys in different shards have no ordering relationship. A global range scan requires a scatter-gather across all shards followed by a merge.

## Root Promotion

When a split cascades all the way up to the shard root, `ShardRootGrain` creates a new internal root above the old one via a **two-phase promotion**:

1. **Phase 1 (persist intent):** The `SplitResult` and a `RootWasLeaf` flag are saved to `ShardRootState.PendingPromotion` and persisted.
2. **Phase 2 (create root):** A new `BPlusInternalGrain` is created with a **deterministic `GrainId`** derived from the shard key and old root ID (`SHA-256` hash). The new root is initialised with the promoted key and left/right children, and `RootNodeId` is updated.

If the shard root crashes between phases, `ResumePendingPromotionAsync` (called at the start of every `GetAsync`, `SetAsync`, and `DeleteAsync`) detects the pending promotion and completes it. The deterministic `GrainId` ensures that re-executing Phase 2 targets the same grain — making the promotion idempotent.

## Bounded Retry

`ShardRootGrain` wraps `SetAsync` and `DeleteAsync` in a bounded retry loop (default: 3 attempts). If a grain call fails due to a transient error (e.g. storage fault, network partition), the request is retried. Orleans automatically deactivates a failed grain; the retry hits a fresh activation that runs any pending recovery logic before processing the request. This shields callers from transient infrastructure errors without requiring client-side retry code.

## Grain-to-Grain Mapping

### Data-path grains

These grains form the structural B+ tree and handle every read/write request:

| B+ Tree Concept | Orleans Grain | Key Format | Persistent State |
|---|---|---|---|
| Shard router | `LatticeGrain` (`[StatelessWorker]`) | `{treeId}` | None (stateless). Caches the resolved `ShardMap` in memory; invalidated on stale-routing detection. |
| Shard root | `ShardRootGrain` | `{treeId}/{shardIndex}` | `ShardRootState` — root node ID + leaf/internal flag + pending promotion + pending bulk graft + last completed bulk operation ID |
| Internal node | `BPlusInternalGrain` | `Guid` | `InternalNodeState` — sorted children + HLC + split state |
| Leaf node | `BPlusLeafGrain` | `Guid` | `LeafNodeState` — sorted LWW entries + sibling pointer + HLC + version vector + split state |
| Leaf cache | `LeafCacheGrain` (`[StatelessWorker]`) | `{leafGrainId}` | None (in-memory LWW-map + version vector) |

### Tree registry

| Grain | Key Format | Storage |
|---|---|---|
| `LatticeRegistryGrain` | `_lattice_trees` (the `LatticeConstants.RegistryTreeId` constant) | **Self-hosting** — stores its data in a Lattice tree keyed `_lattice_trees`, so registry reads/writes flow through the same `LatticeGrain → ShardRootGrain → LeafGrain` path as user data. |

The registry holds a `TreeRegistryEntry` per user tree, containing:

- **`ShardMap`** — the per-tree mapping from virtual slots to physical shard indices. `null` until the first topology change (adaptive split or explicit `SetShardMapAsync`); `LatticeGrain` falls back to `ShardMap.CreateDefault(LatticeConstants.DefaultVirtualShardCount, ShardCount)` when absent.
- **Structural pins** — per-tree `MaxLeafKeys`, `MaxInternalChildren`, and `ShardCount` seeded on first use from `LatticeConstants` (128 / 128 / 64). These are the sole source of structural truth, read by every grain through `LatticeOptionsResolver`. Mutable only through `ResizeAsync` (leaf / internal capacity) and `ReshardAsync` (shard count).
- **Tree alias** — an optional indirection from a logical tree name to a physical tree ID, used by `ResizeAsync` and `SnapshotAsync` to swap the backing tree atomically.
- **Soft-delete metadata** — deletion timestamp and retention window for `DeleteTreeAsync` / `RecoverTreeAsync`.

### Coordination grains

Long-running or multi-step operations are managed by dedicated coordination grains. Each persists its progress and registers an Orleans reminder so that a silo crash mid-operation is recovered automatically on the next reminder tick. All are internal — external callers interact only through methods on `ILattice`.

| Operation | Orleans Grain | Key Format | Persistent State | Reminder-driven |
|---|---|---|---|---|
| Adaptive shard split | `TreeShardSplitGrain` | `{treeId}/{shardIndex}` | `TreeShardSplitState` — source/dest shard, migrating slots, drain cursor, phase | Yes |
| Hot-shard monitoring | `HotShardMonitorGrain` | `{treeId}` | None (polls `ShardRootGrain.GetHotnessAsync` on each tick) | Yes |
| Tree merge | `TreeMergeGrain` | `{treeId}` | `TreeMergeState` — source tree, per-shard progress | Yes |
| Snapshot | `TreeSnapshotGrain` | `{treeId}` | `TreeSnapshotState` — destination tree, per-shard progress, phase | Yes |
| Resize | `TreeResizeGrain` | `{treeId}` | `TreeResizeState` — old/new tree IDs, sizing overrides, phase | Yes |
| Soft delete / purge | `TreeDeletionGrain` | `{treeId}` | `TreeDeletionState` — deletion timestamp, retention, purge progress | Yes |
| Tombstone compaction | `TombstoneCompactionGrain` | `{treeId}` | `TombstoneCompactionState` — per-shard compaction cursor | Yes |

### Interaction diagram

The following diagram shows how `ILattice` delegates to data-path and coordination grains, and how the registry self-hosts its own data through the same data path.

```mermaid
flowchart TD
    Client([Client]) --> ILattice

    subgraph "Data path"
        ILattice --> ShardRoot[ShardRootGrain]
        ShardRoot --> Internal[BPlusInternalGrain]
        Internal -->|write| Leaf[BPlusLeafGrain]
        Internal -->|read| Cache[LeafCacheGrain]
        Cache -.->|delta refresh| Leaf
    end

    subgraph "Coordination"
        ILattice --> Snapshot[TreeSnapshotGrain]
        ILattice --> Resize[TreeResizeGrain]
        ILattice --> Merge[TreeMergeGrain]
        ILattice --> Delete[TreeDeletionGrain]
        ILattice --> Compact[TombstoneCompactionGrain]
        Monitor[HotShardMonitorGrain] -->|poll hotness| ShardRoot
        Monitor -->|trigger| Split[TreeShardSplitGrain]
        Split -->|drain entries| ShardRoot
        Split -->|update shard map| Registry
    end

    subgraph "Registry (self-hosting)"
        ILattice -->|resolve tree config| Registry[LatticeRegistryGrain]
        Registry -->|read/write via| SelfLattice["ILattice(&quot;_lattice_trees&quot;)"]
        SelfLattice -.->|same data path| ShardRoot
    end

    Snapshot --> Registry
    Resize --> Registry
    Merge --> Registry
    Delete --> Registry
```

All coordination grain interfaces are declared `internal` — external callers interact only through methods on `ILattice`.

## Capacity and Depth

With the default branching factor of 128:

| Keys per shard | Tree depth | Total grains per shard |
|---|---|---|
| ≤ 128 | 1 (leaf only) | 2 (root + leaf) |
| ≤ 16,384 | 2 | ~130 |
| ≤ 2,097,152 | 3 | ~16,500 |

With 64 shards, the total tree supports **~134 million keys** at depth 3. A depth-3 lookup requires 3–4 grain calls from router to leaf; actual latency depends on cluster topology, network conditions, and storage provider performance.
