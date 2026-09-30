# Online Reshard

`ILattice.ReshardAsync(int newShardCount, CancellationToken)` grows or shrinks a tree's physical shard count **online** - the tree continues to serve reads and writes throughout the migration, with no global cutover lock. A larger count splits shards; a smaller count folds shards together and releases the retired shards' storage.

## What resharding is (and isn't)

Resharding is **key-space partitioning**, not capacity management. Each physical shard is a self-contained B+ tree rooted at its own per-shard root grain and can grow indefinitely in key count, node count, and depth - nothing about `ShardCount` bounds how much data a tree holds. What shard count actually controls is how many independent write paths the key space is spread across.

| Concern | Bounded by shard count | Not bounded by shard count |
|---|---|---|
| Total keys / bytes stored | | Yes (per-shard tree grows indefinitely) |
| Write throughput | Yes (one root grain + storage partition per shard) | |
| Point-read throughput | Yes (hash-routes to one shard) | |
| Scan cost (`ScanKeysAsync`, `ScanEntriesAsync`, `CountAsync`, bulk-load) | Yes (linear fan-out) | |
| Hot-key / hot-slot contention | Yes (splits redistribute virtual slots) | |

You reshard when a shard's **write path** is saturated - single root grain bottleneck, single storage partition bottleneck, or a hot virtual slot concentrating traffic - not when a shard is "full". It can't be full.

Shrinking is the same trade in the other direction. Fewer shards means fewer independent write paths, so the tree's write and point-read throughput ceilings drop and a hot key range saturates sooner. In return every whole-tree operation (scans, `CountAsync`, bulk load, snapshot, resize) fans out to fewer shards, the tree keeps fewer grain activations resident, and each shard owns more virtual slots, leaving more room for later splits.

## When to use it

- A tree's write throughput has outgrown its current shard fan-out (e.g. it was provisioned with the default `ShardCount = 64` and a handful of virtual slots are carrying most of the traffic).
- You want to increase parallelism without a maintenance window.
- You want shard growth to compose with the autonomic hot-shard split monitor - the reshard coordinator uses the same underlying shard-split primitive, so the end state is identical to the one the monitor would have produced over time.
- A tree is over-provisioned for its traffic - it was created with more shards than its write rate needs, or a past burst was resharded up - and you want cheaper scans and fewer resident activations. Shrinking uses the same online shard-consolidation primitive as [automatic over-split healing](configuration.md#shardhealingenabled), but runs to the count you ask for, under load, without waiting for healing's uniform-load and quiet-tree conditions.

## Semantics

| Property | Value |
|---|---|
| Availability | Reads and writes served throughout. No global lock. |
| Direction | **Grow or shrink.** A target above the current distinct-shard count splits shards; a target below it folds shards together. |
| Target range | `2 <= newShardCount <=` the smaller of 4096 and the tree's virtual slot count - the number of slots in its shard map, 4096 unless an installed app's manifest declared another `virtualShardCount`; a value outside it throws `ArgumentOutOfRangeException`. A target equal to the current distinct-shard count is a no-op. An observably empty tree is instead re-pinned in place to any in-range count, with no coordinator. |
| Cleanup | A shrink reports complete only once every fold it started has finished, including releasing the retired shards' storage (see [Shrinking](#shrinking)). |
| Idempotence | Repeated calls with the same target while in progress are no-ops. |
| Concurrent target change | `InvalidOperationException` if a reshard with a different target is already in progress. |
| Resize interlock | `InvalidOperationException` while a resize is in flight on the tree. |
| Crash-safety | Reminder-anchored coordinator (`reshard-keepalive`, 1 min keepalive). Resumes automatically on silo restart. |
| Completion signal | `ILattice.IsReshardCompleteAsync(CancellationToken)`. |

## Usage

```csharp verify
var tree = grainFactory.GetGrain<ILattice>("catalogue");

// Start the reshard - returns once the intent is persisted.
await tree.ReshardAsync(newShardCount: 16);

// Optionally wait for completion.
while (!await tree.IsReshardCompleteAsync())
    await Task.Delay(TimeSpan.FromSeconds(1));
```

Shrinking uses the same call with a smaller count:

```csharp verify
var tree = grainFactory.GetGrain<ILattice>("catalogue");

// Fold the tree down to 8 physical shards, online.
await tree.ReshardAsync(newShardCount: 8);

while (!await tree.IsReshardCompleteAsync())
    await Task.Delay(TimeSpan.FromSeconds(1));
```

## How it works

Internally, `ReshardAsync` routes through a dedicated per-tree reshard coordinator grain keyed per tree (`{treeId}`). The coordinator drives a small phase machine:

1. **Start** - `ReshardAsync` persists the target shard count, whether the reshard grows or shrinks the tree, and a fresh operation ID, and enters the migrating phase directly. (A coordinator persisted in the older planning phase is advanced to migrating on its next tick.)
2. **Migrating (grow)** - on each 2-second tick, read the current `ShardMap`, count virtual-slot ownership per physical shard, filter to eligible sources (owns >= 2 virtual slots and not already splitting), and dispatch up to `LatticeOptions.MaxConcurrentMigrations` (default 4) concurrent online shard-split operations against the largest-slot owners. Each underlying split atomically grows the map by one distinct physical shard via its own shadow-write, drain, swap and reject phases, inheriting all of that mechanism's online-safety guarantees. Repeats until the map contains at least the target number of distinct physical shards.
   **Migrating (shrink)** - on each tick, reconcile the folds already started, then start up to `LatticeOptions.MaxConcurrentMigrations` new online shard consolidations against the cheapest adjacent pairs of physical shards, planning against the map the running folds will leave and never folding a shard that is concurrently absorbing another. Repeats until the map holds at most the target number of distinct physical shards **and** every fold it started has finished.
3. **Complete** - re-pin the registry's `ShardCount` to the target, clear in-progress state, publish a reshard-completed tree event (when tree events are enabled), prompt a reconcile of any tag index covering the tree, unregister the keepalive, and deactivate.

Because each underlying split is itself an independent online operation, the tree never loses availability. Writes arriving during a migrating slot's drain phase are shadow-forwarded to the target shard; once the split has swapped the shard map, the source rejects operations on the moved slots with an internal stale shard-routing signal, and the tree's router refreshes its shard map and retries against the new owner.

### Shrinking

Each fold is the exact inverse of a split. The donor shard shadow-forwards writes on its slots to an adjacent survivor while a bounded background drain copies its entries across; then, in one freeze-and-flip step, the donor is sealed and frozen, a final drain runs over it, the survivor takes the slots back, and the shard map is re-pointed. The donor then rejects its old slots with the same stale shard-routing signal a split source uses.

When a fold commits, the retired donor's storage is released: every leaf and internal node it held is cleared, which also retires those leaves' write-ahead-log materialiser pins. Nothing is lost by this - each drained entry was appended to the survivor's own write-ahead log before the drain moved on - and keeping the leaves would hold the tree's WAL trim horizon back for good, because a retired leaf's pin can never advance. The donor's shard root stays behind as a small routing tombstone that remembers which slots moved where, so a router still holding a pre-shrink shard map is redirected rather than served; it never grows a new root.

A donor that the live map still routes a slot to, or that refuses retirement because another migration or an online resize holds it, keeps its storage, and the fold completes as a routing-only retirement.

Four rules keep a retired shard from ever being read or reused while something still depends on it:

- **A fold waits out a snapshot or merge before it releases anything.** Its final step, and with it the release of the donor's storage, is held while a snapshot of, or merge into, the tree runs, because those read the shards they recorded when they started. A shrink also starts no new fold while one runs. A resize is not waited for; a donor an online resize is forwarding refuses retirement and keeps its storage instead.
- **A merge notices a source shard that retired under it.** Before a merge completes it checks that every source shard it recorded is still in the source's routing map. If a fold retired one while the merge ran, it re-drains the source's current shards; every entry carries its original timestamp, so re-merging is harmless.
- **A retired index is never handed out again.** The fold's routing swap raises the tree's split allocation high-water past the donor, so a later grow or adaptive split allocates a fresh index rather than splitting into a tombstone.
- **The empty-tree path revives what it reuses.** An observably empty tree is re-pinned to an identity map over indices `0..n-1`, which may include retired shards; those are returned to service first, as empty shards, before the new map is published. Each keeps its routing tombstone for any slot the new map sends elsewhere, so a caller holding an older map is still redirected.

## Interaction with the autonomic split monitor

The autonomic hot-shard split monitor polls `ILattice.IsReshardCompleteAsync` and suppresses its own passes while a reshard is running. This prevents two coordinators from simultaneously dispatching splits against the same tree and racing on `ShardMap` updates. A reshard is not bound by `LatticeOptions.MaxPhysicalShardsPerTree` (default 256), which caps only autonomic growth; a tree resharded above that ceiling simply stops splitting autonomically.

Automatic over-split healing likewise admits no new fold while a reshard runs, and a shrink waits for any fold healing had already started before it plans its own. Because a completed reshard re-pins the registry's `ShardCount`, healing never folds a tree below a count a shrink chose, and never undoes a grow. After a shrink the split monitor behaves as it always does: if the smaller tree develops a hot shard, it splits it again.

## Tuning

| Option | Default | Effect |
|---|---|---|
| `LatticeOptions.MaxConcurrentMigrations` | 4 | Upper bound on the number of in-flight splits (grow) or folds (shrink) the reshard coordinator keeps running. Higher values migrate faster but increase drain I/O load. |
| Virtual shard count (internal) | 4096 by default | Bounds `newShardCount`, which may not exceed the slot count of the tree's own shard map (nor 4096). Only a source shard that owns at least two virtual slots can split, so the reshard can never produce more distinct physical shards than the map has slots, and a target above that count is refused up front rather than left in progress. An empty tree's rebuilt map keeps the tree's slot count. Not a runtime option - persisted shard maps index into the virtual space. |

### Practical size limits

The default 4096 virtual shard count is generous. The real ceiling on useful shard counts comes from scan fan-out and activation cost, not the map itself:

- **Scan fan-out is linear in distinct physical shards.** Every scan issues one parallel grain call per shard. A 4096-shard tree issues 4096 concurrent calls per `ScanKeysAsync` / `ScanEntriesAsync` / `CountAsync`. See [Consistency](consistency.md) for the guarantee these scans deliver.
- **Activation cost scales with shards x trees x silos.** Each physical shard has one per-shard root grain activation per active tree.
- **`ShardMap` storage is trivial** (`4 x 4096 = 16 KB`) and never the limit.

Recommended ranges for steady-state operation:

| Physical shards | Suitability |
|---|---|
| 1 - 64 | Low-to-moderate write throughput. The out-of-box default. |
| 128 - 1024 | Sweet spot for large, write-heavy trees. Scans remain tractable. |
| 1024 - 4096 | Only when write throughput genuinely demands it. Prefer indexes over full-tree enumeration. |
| > 4096 | Not supported in a single tree - use multiple trees instead. |

Splits halve the source shard's virtual-slot ownership. Starting from `ShardCount = 64` on the default map, each shard owns `4096 / 64 = 64` virtual slots and can be split 6 times (64 -> 32 -> 16 -> 8 -> 4 -> 2 -> 1) before hitting the `>= 2 slots` eligibility floor, so the full 4096 ceiling is reachable by the reshard path.

## Telemetry

Four instruments on the `orleans.lattice` meter track reshards, each tagged `tree` and `tenant`: `orleans.lattice.shard_root.reshard.initiated` and `orleans.lattice.shard_root.reshard.completed` count reshards that started and finished (the empty-tree fast path counts on both), `orleans.lattice.shard_root.reshard.rejected` counts requests refused before a coordinator started, with a `reason` tag (`argument_out_of_range_min`, `argument_out_of_range_max`, `already_in_progress`, `resize_in_flight`, `state_write_failed`), and the `orleans.lattice.shard_root.reshard.in_flight` histogram records `0` or `1` at every `ReshardAsync` entry. See [Metrics](metrics.md) for the full schema.

## Limitations and future work

- **No revert.** A reshard runs to its target once started; to go back, reshard again to the previous count.
- **Shards retired before this release keep their storage.** A fold only releases its donor's storage when the fold itself commits, so a donor retired by automatic healing on an earlier build keeps its leaves until the tree is purged.
- **Shard-count only.** Changing the B+ fan-out (`MaxLeafKeys` / `MaxInternalChildren`) uses a different path - see [Tree Sizing](tree-sizing.md#resizing-an-existing-tree).
- **Node-count policy is heuristic.** The coordinator picks the largest-slot owners as split sources; it does not currently read hotness counters. Hot-shard-aware source selection would fit neatly into the same loop.
