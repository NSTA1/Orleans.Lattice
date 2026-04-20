# Online Reshard

`ILattice.ReshardAsync(int newShardCount, CancellationToken)` grows a tree's physical shard count **online** — the tree continues to serve reads and writes throughout the migration, with no global cutover lock.

## What resharding is (and isn't)

Resharding is **key-space partitioning**, not capacity management. Each physical shard is a self-contained B+ tree rooted at a `ShardRootGrain` and can grow indefinitely in key count, node count, and depth — nothing about `ShardCount` bounds how much data a tree holds. What shard count actually controls is how many independent write paths the key space is spread across.

| Concern | Bounded by shard count | Not bounded by shard count |
|---|---|---|
| Total keys / bytes stored | | ✅ (per-shard tree grows indefinitely) |
| Write throughput | ✅ (one root grain + storage partition per shard) | |
| Point-read throughput | ✅ (hash-routes to one shard) | |
| Scan cost (`KeysAsync`, `EntriesAsync`, `CountAsync`, bulk-load) | ✅ (linear fan-out) | |
| Hot-key / hot-slot contention | ✅ (splits redistribute virtual slots) | |

You reshard when a shard's **write path** is saturated — single root grain bottleneck, single storage partition bottleneck, or a hot virtual slot concentrating traffic — not when a shard is "full". It can't be full.

## When to use it

- A tree's write throughput has outgrown its current shard fan-out (e.g. it was provisioned with the default `ShardCount = 64` and a handful of virtual slots are carrying most of the traffic).
- You want to increase parallelism without a maintenance window.
- You want shard growth to compose with `HotShardMonitorGrain` autonomic splitting — the reshard coordinator uses the same `ITreeShardSplitGrain` primitive, so the end state is identical to the one the monitor would have produced over time.

## Semantics

| Property | Value |
|---|---|
| Availability | Reads and writes served throughout. No global lock. |
| Direction | **Grow-only.** Shrink is not supported. |
| Target range | `2 ≤ newShardCount ≤ LatticeOptions.VirtualShardCount` (default 4096). Must be strictly greater than the current distinct-shard count. |
| Idempotence | Repeated calls with the same target while in progress are no-ops. |
| Concurrent target change | `InvalidOperationException` if a reshard with a different target is already in progress. |
| Crash-safety | Reminder-anchored coordinator (`reshard-keepalive`, 1 min keepalive). Resumes automatically on silo restart. |
| Completion signal | `ILattice.IsReshardCompleteAsync(CancellationToken)`. |

## Usage

```csharp
var tree = grainFactory.GetGrain<ILattice>("catalogue");

// Start the reshard — returns once the intent is persisted.
await tree.ReshardAsync(newShardCount: 16);

// Optionally wait for completion.
while (!await tree.IsReshardCompleteAsync())
    await Task.Delay(TimeSpan.FromSeconds(1));
```

## How it works

Internally, `ReshardAsync` routes through a dedicated `ITreeReshardGrain` coordinator keyed per tree (`{treeId}`). The coordinator drives a small phase machine:

1. **`Planning`** — persist the target shard count and operation ID.
2. **`Migrating`** — on each 2-second tick, read the current `ShardMap`, count virtual-slot ownership per physical shard, filter to eligible sources (owns ≥ 2 virtual slots and not already splitting), and dispatch up to `LatticeOptions.MaxConcurrentMigrations` (default 4) concurrent `ITreeShardSplitGrain.SplitAsync` calls against the largest-slot owners. Each underlying split atomically grows the map by one distinct physical shard via its own shadow-write + swap + reject phases, inheriting all of that mechanism's online-safety guarantees. Repeats until the map contains at least the target number of distinct physical shards.
3. **`Complete`** — clear in-progress state, unregister the keepalive, and deactivate.

Because each underlying split is itself an independent online operation, the tree never loses availability. Writes arriving during a migrating slot's drain phase are shadow-forwarded to the target shard; after the split's swap phase, the source enters a `StaleShardRoutingException`-emitting reject state and the client retries against the new owner.

## Interaction with the autonomic split monitor

`HotShardMonitorGrain` polls `ILattice.IsReshardCompleteAsync` and suppresses its own passes while a reshard is running. This prevents two coordinators from simultaneously dispatching splits against the same tree and racing on `ShardMap` updates.

## Tuning

| Option | Default | Effect |
|---|---|---|
| `LatticeOptions.MaxConcurrentMigrations` | 4 | Upper bound on the number of in-flight splits the reshard coordinator will dispatch per tick. Higher values migrate faster but increase drain I/O load. |
| `LatticeOptions.VirtualShardCount` | 4096 | Absolute ceiling on `newShardCount`. The reshard can never produce more distinct physical shards than there are virtual slots. |

### Practical size limits

The `VirtualShardCount = 4096` default is generous. The real ceiling on useful shard counts comes from scan fan-out and activation cost, not the map itself:

- **Scan fan-out is linear in distinct physical shards.** Every strongly-consistent scan issues one parallel grain call per shard. A 4096-shard tree issues 4096 concurrent calls per `KeysAsync` / `EntriesAsync` / `CountAsync`.
- **Activation cost scales with shards × trees × silos.** Each physical shard has one `ShardRootGrain` activation per active tree.
- **`ShardMap` storage is trivial** (`4 × VirtualShardCount` bytes — 16 KB at default) and never the limit.

Recommended ranges for steady-state operation:

| Physical shards | Suitability |
|---|---|
| 1 – 64 | Low-to-moderate write throughput. The out-of-box default. |
| 128 – 1024 | Sweet spot for large, write-heavy trees. Scans remain tractable. |
| 1024 – 4096 | Only when write throughput genuinely demands it. Prefer indexes over full-tree enumeration. |
| > 4096 | Raise `VirtualShardCount` first — but if you need this, you probably want multiple trees rather than one massive one. |

Splits halve the source shard's virtual-slot ownership. Starting from `ShardCount = 64` on the default map, each shard owns `4096 / 64 = 64` virtual slots and can be split 6 times (64 → 32 → 16 → 8 → 4 → 2 → 1) before hitting the `≥ 2 slots` eligibility floor, so the full 4096 ceiling is reachable by the reshard path.

## Limitations and future work

- **Grow-only.** Shrinking a tree (merging shards) is not supported. The underlying `ShardMap` primitive can represent any slot-to-shard mapping, but no coordinator currently implements the merge-and-drop flow.
- **Shard-count only.** The existing `ILattice.ResizeAsync(newMaxLeafKeys, newMaxInternalChildren)` tree-sizing path remains offline (snapshot + swap). An online version is planned as a follow-up.
- **Node-count policy is heuristic.** The coordinator picks the largest-slot owners as split sources; it does not currently read hotness counters. Hot-shard-aware source selection would fit neatly into the same loop.
