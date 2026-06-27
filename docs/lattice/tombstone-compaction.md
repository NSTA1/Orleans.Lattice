# Tombstone Compaction

Deleted keys are represented as **tombstones** - `LwwValue` entries with `IsTombstone = true`. Tombstones participate in LWW merge and delta replication like any other entry, so all replicas and caches eventually learn about the delete. However, tombstones are never removed by normal operations, leading to unbounded storage and scan overhead.

## How It Works

A single **`TombstoneCompactionGrain`** per tree owns one [grain reminder](https://learn.microsoft.com/dotnet/orleans/grains/timers-and-reminders) that fires at the configured grace-period interval. When the reminder fires, it starts a **grain timer** that processes one shard per tick (every 500 ms), avoiding a single long-running grain call that could hit Orleans timeouts for large trees:

1. The reminder tick persists `InProgress = true` and registers a **one-minute keepalive reminder**, then starts a grain timer at shard 0.
2. Each timer tick processes one shard:
   a. Calls `GetLeftmostLeafIdAsync` on the shard root to find the head of the leaf chain.
   b. Walks the doubly-linked leaf list via `GetNextSiblingAsync`, calling `CompactTombstonesAsync` on each leaf.
   c. Persists the updated `NextShardIndex` to durable state.
3. If a shard fails, it is retried once before being skipped.
4. After all shards are processed, the timer self-disposes, `InProgress` is set to `false`, and the keepalive reminder is unregistered.

**Recovery:** If the silo restarts mid-compaction, the keepalive reminder fires within one minute and the grain resumes from the persisted `NextShardIndex`. Once the pass completes, the keepalive is unregistered. If `InProgress` is already `false` when the keepalive fires, it simply unregisters itself.

Each leaf compares every tombstone's `HLC.WallClockTicks` against `now − gracePeriod`. Tombstones older than the cutoff are durably reaped: for each removed entry the leaf appends a single `LatticeMutation { Kind = MutationKind.Tombstone, IsMerge = true }` envelope to the per-shard WAL **before** the entry is removed from the in-memory `SortedDictionary`. The envelope's HLC is the existing tombstone's own timestamp (not a fresh tick) so activation-time replay can use a straight `existing.Timestamp <= mutation.Timestamp` dominance check to skip a reap when a fresher live rewrite has already landed. Expired live entries past the same grace period are reaped through the same envelope shape.

The entire `CompactTombstonesAsync` body runs under a `LatticeMaintenanceContext` scope, so every emitted envelope is stamped `Category = MutationCategory.Maintenance`. This classification is what keeps reap envelopes off the replication wire: the producer-side observer in `Orleans.Lattice.Replication` skips `MutationCategory.Maintenance` writes entirely before any per-key filter runs, and the change feed plus the outbound shipper apply a defence-in-depth filter that drops `MutationKind.Tombstone` envelopes if they ever reach those layers. Every converged peer reaps its own copy of the data independently against its own grace window; replicating reap events would inflate every peer's vector clock with edges the user never authored.

A `LastCompactionVersion` (a `VersionVector` snapshot) advances in memory after each pass and is folded into the next projection-checkpoint flush alongside the entries. Subsequent ticks **skip the scan entirely** when the in-memory `LastCompactionVersion` already dominates `Version` (no writes have occurred since the last compaction). If a silo restarts before the checkpoint flushes, the next activation simply re-scans once - no data is lost.

A pass only stamps `LastCompactionVersion` when *no* tombstones remained inside the grace window; if any tombstone was still in-grace, the version is left untouched so the next pass re-scans once the grace has elapsed.

```mermaid
sequenceDiagram
    participant R as Reminder Service
    participant C as TombstoneCompactionGrain
    participant S0 as ShardRootGrain (0)
    participant L0 as LeafGrain (leftmost)
    participant L1 as LeafGrain (next)
    participant S1 as ShardRootGrain (1)
    participant L2 as LeafGrain

    R->>C: ReceiveReminder("tombstone-compaction")
    C->>C: Persist InProgress = true, NextShardIndex = 0
    C->>R: Register keepalive reminder (1 min)
    C->>C: Start grain timer (2s ticks)

    Note over C: Timer tick 1 - shard 0
    C->>S0: GetLeftmostLeafIdAsync()
    S0-->>C: leafId₀
    C->>L0: CompactTombstonesAsync(gracePeriod)
    C->>L0: GetNextSiblingAsync()
    L0-->>C: leafId₁
    C->>L1: CompactTombstonesAsync(gracePeriod)
    C->>L1: GetNextSiblingAsync()
    L1-->>C: null (end of chain)
    C->>C: Persist NextShardIndex = 1

    Note over C: Timer tick 2 - shard 1
    C->>S1: GetLeftmostLeafIdAsync()
    S1-->>C: leafId₂
    C->>L2: CompactTombstonesAsync(gracePeriod)
    C->>L2: GetNextSiblingAsync()
    L2-->>C: null

    Note over C: Timer tick 3 - all shards done
    C->>C: Persist InProgress = false
    C->>R: Unregister keepalive reminder
    C->>C: Dispose timer
```

The reminder is registered lazily - `LatticeGrain` calls `EnsureReminderAsync` on the first `SetAsync` or `DeleteAsync` for a given tree. A per-activation `bool` field ensures this cross-grain call happens at most once per `LatticeGrain` activation.

For manual or on-demand compaction (e.g. maintenance scripts, integration tests), `LatticeGrain` invokes `ITombstoneCompactionGrain.RunCompactionPassAsync` internally on each reminder tick. The compaction grain is not part of the public API.

## Configuration

`TombstoneGracePeriod` follows the same named-options pattern as all other `LatticeOptions` properties:

```csharp verify
// Global default - applies to all trees.
siloBuilder.ConfigureLattice(o => o.TombstoneGracePeriod = TimeSpan.FromHours(12));

// Per-tree override.
siloBuilder.ConfigureLattice("my-tree", o => o.TombstoneGracePeriod = TimeSpan.FromDays(7));

// Disable compaction entirely for a specific tree.
siloBuilder.ConfigureLattice("archive-tree", o => o.TombstoneGracePeriod = Timeout.InfiniteTimeSpan);
```

The default grace period is **24 hours**. The reminder interval equals the grace period (clamped to a minimum of 1 minute, the Orleans reminder floor).

### `CompactionShardTickInterval`

A `TimeSpan` (default 500 ms, floor 100 ms). The compaction grain processes one shard per internal grain-timer tick during a pass, and waits this long between ticks so the grain returns control to the Orleans scheduler between shards. Without this gap a single grain call could span every shard in the tree, hit Orleans' grain-call timeout, and starve concurrent operator-initiated `RequestCompactionAsync` callers.

The cadence is a **scheduler-fairness knob, not a grain-deactivation knob.** Leaf activation lifetime is governed by the silo's `GrainCollectionOptions.CollectionAge` (default 15 minutes) and is independent of this value.

#### What a pass actually touches

Within a single shard the coordinator walks the leaf chain in batches of `CompactionLeafBatchSize` leaves (default 64) per timer tick, then yields for `CompactionShardTickInterval` before resuming from a persisted in-shard cursor. **The tick interval gates both the gap between shards *and* the gap between leaf batches inside a shard**, so peak concurrent leaf activations during a pass are bounded by:

```
peak ~= min(leaves walked in last CollectionAge, CompactionLeafBatchSize * (CollectionAge / CompactionShardTickInterval))
```

On a healthy default-configured tree (`CompactionLeafBatchSize = 64`, `CompactionShardTickInterval = 500 ms`, `CollectionAge = 15 min`) the second term caps at roughly `64 * 1800 = 115 200` activations, but the **dirty-leaves fast path** described below clamps the *actual* per-pass activation cost to `O(shards + dirty_leaves)` in the steady state. On a tree where most leaves have nothing to compact, a full pass activates only one grain per physical shard plus the small set of leaves that observed routed deletes since the previous pass.

Leaves that finish compacting fall idle and are collected after they've been idle for `CollectionAge`. With batching in place, **the leaf walk no longer activates the entire shard's leaf chain back-to-back**, so a pass that finishes inside one `CollectionAge` window does not necessarily activate the whole tree at once.

#### Tuning trade-off

Full-pass wall-clock scales linearly with shard count and tick interval, plus the batch yield within shards. The table below is for a tree with 1024 physical shards and ~50 leaves per shard (~50 000 leaves total). "Peak concurrent activations" is the number of leaves walked in the last 15 minutes of the pass, capped at the tree's leaf count and at the batch-yield bound. With the dirty-leaves fast path the *typical* pass walks far fewer leaves than the table headline suggests; the figures below describe the legacy chain-walk fallback (e.g. the first pass after upgrade, or a pass that landed an empty dirty snapshot for the shard).

| `CompactionShardTickInterval` | `CompactionLeafBatchSize` | Full-pass duration | Peak concurrent leaf activations (chain-walk fallback) |
|---|---|---|---|
| 500 ms (default) | 64 (default) | ~8.5 minutes | ~7 200 |
| 2 s | 64 (default) | ~34 minutes | ~28 800 |
| 2 s | 1024 (per-shard cap) | ~34 minutes | ~50 000 (entire tree) |
| 200 ms | 64 (default) | ~3.4 minutes | ~2 880 |
| 100 ms (floor) | 64 (default) | ~1.7 minutes | ~1 440 |
| 100 ms (floor) | 1 (floor) | ~1.7 minutes | ~22 (extreme yielding) |

The default settings spread activations across multiple `CollectionAge` windows so the directory and silo memory don't see the full leaf set simultaneously. Lower the cadence or raise the batch size only after measuring that your silo can absorb the resulting peak activation count, and prefer `ILattice.CompactShardAsync(shardIndex)` for "compact this one shard fast" operator triage - a scoped pass walks only one shard's leaves regardless of the tick interval.

Values below the 100 ms floor are clamped up to the floor with a one-shot warning per tree per process. The floor protects scheduler fairness; lower it only if you have a measured reason. The interval is snapshotted at the start of each pass, so changing the option mid-pass does not reshape the in-flight pass; the next pass picks up the new value.

```csharp verify
// Speed up compaction triage on a high-shard tree.
// Verify the silo can absorb the resulting peak activation count first.
siloBuilder.ConfigureLattice("high-shard-tree", o => o.CompactionShardTickInterval = TimeSpan.FromMilliseconds(500));
```

### CompactionLeafBatchSize

An `int` (default 64, floor 1). Caps how many leaves the coordinator visits within a single shard before yielding for one `CompactionShardTickInterval`. The leaf walk resumes on the next timer tick from the persisted `TombstoneCompactionState.NextLeafIdInShard` cursor, so progress survives silo crashes the same way `NextShardIndex` does. The cursor is cleared when the shard's leaf walk completes; a fresh pass on a different shard list always starts from the leftmost leaf.

The default 64 reproduces pre-batching behaviour exactly on shards with <= 64 leaves (the common case). Raising the batch size shortens pass wall-clock at the cost of higher peak concurrent activations; lowering it does the inverse. Values below 1 are clamped up to 1 with a one-shot warning per tree per process. The batch size is snapshotted at the start of each pass, so changing the option mid-pass does not reshape the in-flight pass; the next pass picks up the new value.

```csharp verify
// Cut peak concurrent leaf activations by yielding more aggressively
// within each shard. Trades pass wall-clock for activation headroom.
siloBuilder.ConfigureLattice("activation-sensitive-tree", o => o.CompactionLeafBatchSize = 16);
```

## Dirty-Leaves Fast Path

The shard root maintains a small per-shard "dirty leaves since last compaction" set, populated as it routes `Delete` mutations down to leaves. The compaction coordinator pulls this set at the start of each shard's first batch via `IShardRootGrain.GetDirtyLeavesSinceLastCompactionAsync()`, walks only the named leaves, and on shard completion drains the set up to an HLC watermark via `IShardRootGrain.ClearDirtyLeavesUpToAsync(advance)`. The clear is HLC-gated, so deletes that arrived during the in-flight pass are preserved for the next pass rather than silently dropped.

A pass on a tree with no recent deletes activates only the shard root grains (one per physical shard), not every leaf - activation cost drops from `O(leaves)` to `O(shards + dirty_leaves)`. On a 50 000-leaf, 1024-shard tree where 1% of leaves accumulated tombstones since the last pass, the fast path activates ~1 524 grains versus the ~50 000 the legacy chain walk would touch.

When a shard's dirty-leaves snapshot is empty (a fresh tree, an upgraded silo with no signal yet, or a shard whose deletes were all already drained), the coordinator falls back to the legacy leaf-chain walk for that shard so progress is never blocked by the absence of accumulated signal. The fast path takes over from the next pass forward.

The shard root dedupes repeated `Delete` mutations within a dirty-window in memory, so the persistence cost scales with "distinct leaves touched per window", not "deletes per window". Dirty-leaf state is local to each cluster; receiver-side leaves observe the same `Delete` mutations via the standard WAL replication transport, and receiver-cluster shard roots populate their own dirty set from that stream and run their own compaction passes against it.

The active path is reported on `orleans.lattice.compaction.leaves.visited` via the `path` tag (`walk` or `dirty-set`), and the `orleans.lattice.compaction.shard.dirty_leaves` histogram records the per-shard dirty-leaf count at the moment the coordinator enters a shard.

### `DirtyLeafFlushIntervalMs`

Coalescing window for persisting the shard-root dirty-leaves dictionary (default: `50` ms). The `Delete` hot path never writes to storage directly: `ShardRootGrain.MarkLeafDirtyAsync` max-merges the destination leaf into the in-memory `DirtyLeavesSinceLastCompaction` map with a monotonically-advancing HLC, sets a pending-flush flag, and arms a one-shot grain timer scoped to this interval. The timer's tick drains the flag with one `WriteStateAsync` per window regardless of how many distinct leaves were marked - the per-`Delete` shard-root storage write that previously raced concurrent `SetManyAsync` turns is replaced by at most one persist per window.

The compaction coordinator reads the in-memory dictionary directly via `IShardRootGrain.GetDirtyLeavesSinceLastCompactionAsync`, so an unpersisted mark is still routable within the same activation - the coalescing window matters only for crash survival. Admin-path flushes (`ClearDirtyLeavesUpToAsync`) and `OnDeactivateAsync` always drain pending marks in their own persist call, so clean shutdown loses nothing. An unclean silo crash that loses an in-memory mark causes the affected leaf to be re-discovered by the legacy chain-walk fallback on the next pass (the shard's empty post-restart snapshot triggers the fallback automatically), so the loss bound is one missed leaf per crashed activation per window - bounded and self-healing, never a correctness signal.

Set to `0` to disable coalescing entirely: each `MarkLeafDirtyAsync` call performs a synchronous best-effort flush, restoring the pre-coalescing behaviour of one `WriteStateAsync` per first-call-per-leaf-per-window. Tighten the window if shard-root crash survival is more valuable than coalescing the hot-path write; widen it if storage-side write amplification dominates over crash-recovery cost.

## Policy-Driven Triggers

Reminder-driven compaction handles the steady state. Bursty workloads can build a tombstone backlog **between** reminder ticks - either because the delete:write ratio spikes or because a leaf accumulates so many tombstones that scan latency degrades before the next reminder fires. Three optional policy controls let the leaf request an out-of-cycle pass without waiting for the next reminder.

### `MinTombstoneRatioForCompaction`

A `double` in the range `[0.0, 1.0]` (default `0.0` = disabled). When non-zero, every mutation samples the leaf's tombstone-to-live-entry ratio and emits it on the `orleans.lattice.leaf.tombstone.ratio` histogram. When the sampled ratio crosses the threshold, the leaf calls `ITombstoneCompactionGrain.RequestCompactionAsync(shardIndex, "ratio")` to schedule an out-of-cycle pass scoped to that single shard.

### `MaxLeafEntriesBeforeForcedCompaction`

An `int` (default `0` = disabled). When non-zero, the leaf requests an out-of-cycle pass once its total entry count (live + tombstones) exceeds the threshold, with trigger label `"size"`. This is the safety net for workloads where the tombstone ratio stays low but absolute entry count drifts up because deletes never quite outpace writes.

### `CompactionTriggerCooldown`

A `TimeSpan` (default 5 minutes). Per-shard cooldown gate that prevents a hot leaf from re-requesting compaction every mutation. The coordinator persists `LastTriggerAt` per pass; ratio/size requests inside the cooldown window are silently dropped. Operator-initiated requests via `ILattice.CompactShardAsync` bypass the cooldown by carrying the `"operator"` trigger label.

```csharp verify
// Enable both triggers with a 2-minute cooldown.
siloBuilder.ConfigureLattice("hot-tree", o =>
{
    o.MinTombstoneRatioForCompaction = 0.30;        // 30% tombstones triggers a pass.
    o.MaxLeafEntriesBeforeForcedCompaction = 50_000; // 50k entries triggers a pass.
    o.CompactionTriggerCooldown = TimeSpan.FromMinutes(2);
});
```

## Operator API

`ILattice.CompactShardAsync(int shardIndex, CancellationToken)` schedules an out-of-cycle pass scoped to a single physical shard, bypassing the cooldown gate. Returns `false` when compaction is disabled (`TombstoneGracePeriod = Timeout.InfiniteTimeSpan`) or when a pass is already in flight. The shard index must be a physical shard of the tree's `ShardMap`; an out-of-range value throws `ArgumentOutOfRangeException`.

```csharp verify
// Operator triage: force a compaction pass on shard 3.
var accepted = await lattice.CompactShardAsync(3, cancellationToken);
```

## Telemetry

Every compaction pass emits the following instruments. See [Metrics](metrics.md) for the full schema:

- `orleans.lattice.compaction.pass.duration` (histogram, ms) - tagged `tree`, `trigger`.
- `orleans.lattice.compaction.leaves.visited` (counter) - tagged `tree`, `outcome` (`reaped` / `noop`), `trigger` when a policy-trigger pass is in flight, and `path` (`walk` / `dirty-set`) per the active fast path.
- `orleans.lattice.compaction.shard.retries` (counter) - tagged `tree`.
- `orleans.lattice.compaction.shard.skipped` (counter) - tagged `tree`. **Any non-zero rate is alert-worthy.**
- `orleans.lattice.compaction.shard.dirty_leaves` (histogram) - tagged `tree`. Records the per-shard dirty-leaf snapshot size at the moment the coordinator enters a shard. Use it to capacity-plan the dirty-leaves fast path.
- `orleans.lattice.leaf.tombstone.ratio` (histogram) - tagged `tree`. Only emitted when `MinTombstoneRatioForCompaction` is enabled.

The bundled Grafana **Overview** dashboard ships compaction-focused panels for each of these (pass duration p95 by trigger, leaves visited by outcome, shard retries / skips, and tombstone-ratio p95).

## Design Considerations

| Concern | Approach |
|---|---|
| **Scalability** | One reminder per tree (not per leaf). The compaction grain uses a grain timer to process one shard per tick, avoiding long-running calls that could hit Orleans timeouts. |
| **Consistency** | Tombstones are only removed after the grace period, giving all caches and replicas time to observe the delete via delta replication. |
| **Durability of reaps** | Each reaped entry is committed to the per-shard WAL as a `MutationKind.Tombstone` envelope **before** in-memory removal, so activation-time replay re-applies the reap deterministically. The WAL is the sole durability boundary; grain state is never a fallback store for entry values. |
| **Idempotency** | `CompactTombstonesAsync` is safe to call multiple times. The `LastCompactionVersion` fast-path avoids redundant scans. Replay-time `ApplyTombstoneReap` re-runs the dominance check, so the same envelope applied twice is a no-op. |
| **Replication isolation** | Reap envelopes carry `MutationCategory.Maintenance`. The replication observer skips maintenance writes entirely, and the change feed plus outbound shipper apply a `MutationKind.Tombstone` filter as defence in depth. Every peer reaps independently against its own grace window. |
| **Durability of progress** | Compaction progress (`NextShardIndex`, `InProgress`) is persisted to grain storage. A one-minute keepalive reminder ensures the grain is reactivated after a silo restart to resume the in-flight pass. |
| **Fault tolerance** | If a shard fails during compaction, it is retried once before being skipped. The next reminder tick starts a fresh pass. |
| **Memory** | Leaves are compacted in batches of `CompactionLeafBatchSize` (default 64) per timer tick; both the *between-shard* gap and the *between-batch* gap are governed by `CompactionShardTickInterval`. The dirty-leaves fast path (see below) clamps the steady-state per-pass activation count to `O(shards + dirty_leaves)`. The legacy chain-walk fallback (first pass after upgrade, or a shard whose dirty snapshot was empty) is bounded by `min(leaves walked in last CollectionAge, CompactionLeafBatchSize * (CollectionAge / CompactionShardTickInterval))`. With the default 64-leaf batch, 500 ms tick, and 15 min `CollectionAge`, the chain-walk fallback caps at roughly 115 200 activations regardless of tree size; the fast path runs orders of magnitude lower on most trees. |
| **Scheduler fairness** | The compactor yields between shard walks for `CompactionShardTickInterval` (default 500 ms, floor 100 ms) so the grain returns control to the Orleans scheduler and concurrent `RequestCompactionAsync` callers are not starved. The cadence is configurable per tree and snapshotted at pass start. |
| **Disabling** | Set `TombstoneGracePeriod = Timeout.InfiniteTimeSpan` to disable compaction globally or per tree. |
