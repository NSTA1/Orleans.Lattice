# Adaptive Shard Splitting

Adaptive shard splitting allows a hot physical shard to split into two **at
runtime, fully online** — no shard is ever taken offline. Splits happen
automatically when an autonomic monitor detects a hot shard, or can be
triggered manually for diagnostics and tooling.

## Why

Lattice trees are sharded by hashing keys into a virtual slot space and
mapping virtual slots onto physical `ShardRootGrain` activations. With a
fixed shard count, a workload skewed toward a small set of keys will
saturate one shard while others sit idle. Adaptive splitting redistributes
hot virtual slots to a new physical shard so the load follows the data.

## How it works

A split is driven by the internal `TreeShardSplitGrain` coordinator through
five phases. The source shard *S* keeps serving reads and writes throughout;
the target shard *T* receives mirrored data and eventually owns the moved
slots.

```mermaid
stateDiagram-v2
    [*] --> BeginShadowWrite : SplitAsync(sourceShard)
    BeginShadowWrite --> Drain : S.BeginSplitAsync(target, slots, virt)
    Drain --> Swap : forward all moved-slot entries (live + tombstones) to T
    Swap --> Reject : registry.SetShardMapAsync(newMap)
    Reject --> Complete : S.EnterRejectPhaseAsync()
    Complete --> [*] : final drain pass + S.CompleteSplitAsync()
```

1. **BeginShadowWrite** — Coordinator persists intent and calls
   `S.BeginSplitAsync(targetShardIndex, movedSlots, virtualShardCount)`. From
   this point on, every successful write *S* applies to a key in a moved
   virtual slot is mirrored to *T* via `T.MergeManyAsync`, preserving the
   original HLC. CRDT LWW guarantees correct convergence regardless of how
   the foreground write and the background drain interleave.
2. **Drain** — Coordinator walks *S*'s leaf chain and forwards moved-slot
   entries (including tombstones) to *T* with their original HLC timestamps.
   The drain is **chunked** and **leaf-side filtered**: each leaf returns
   only entries whose virtual slot is in the moved-slot set via
   `IBPlusLeafGrain.GetDeltaSinceForSlotsAsync`, and the coordinator flushes
   to *T* in batches of `SplitDrainBatchSize` (default 1024) entries. This
   bounds peak memory on the coordinator regardless of source shard size,
   and avoids transferring non-moved entries over the wire. Idempotent under
   retry — re-running merges only converges to the same state.
3. **Swap** — Coordinator persists a new `ShardMap` in the registry that
   redirects moved slots to *T*. New `LatticeGrain` activations immediately
   route the moved slots to *T*; stale activations still cache the old map.
4. **Reject** — Coordinator calls `S.EnterRejectPhaseAsync()`. From this
   point any read or write to *S* for a moved-slot key throws
   `StaleShardRoutingException`. `LatticeGrain` catches the exception,
   invalidates its cached map, fetches the fresh map from the registry, and
   retries against *T* — a single transparent retry per call.
5. **Complete** — Coordinator runs one final drain pass to capture any
   tombstones written during shadow that were not mirrored on the hot path,
   then calls `S.CompleteSplitAsync()` and clears its own state.
   `CompleteSplitAsync` also promotes the just-completed split's moved
   slots into a permanent `MovedAwaySlots` set on `S`, so even after the
   active reject-phase state is cleared, every subsequent operation on a
   moved-slot key continues to throw `StaleShardRoutingException`. This
   guarantees that stale `[StatelessWorker]` `LatticeGrain` activations
   (which may have cached the pre-split shard map) always trigger a map
   refresh on first use rather than silently returning orphan data.

The coordinator state is persisted before any side effect, so a silo crash
mid-split is recovered by the keepalive reminder: `RunSplitPassAsync`
resumes from the last persisted phase, and every phase method is
idempotent.

## Scan semantics during a split

Point reads and writes (`GetAsync`, `SetAsync`, `DeleteAsync`,
`SetIfVersionAsync`, `GetOrSetAsync`, etc.) are **fully consistent**
throughout the split: every successful write is mirrored to the new owner
during shadow phase and the post-swap reject phase guarantees stale
activations transparently retry against the correct shard. The post-Complete
permanent `MovedAwaySlots` rejection extends this guarantee for the lifetime
of the source shard.

Scans (`KeysAsync`, `EntriesAsync`, `CountAsync`) are **strongly consistent**
across topology changes — including any number of concurrent splits. They
return the exact live key set as observed at the moment each scan starts,
with no missing or double-counted entries even when the shard map mutates
mid-scan.

### How strong consistency is achieved

Each scan uses a per-slot reconciliation algorithm coordinated against the
registry's monotonically-incrementing `ShardMap.Version`:

1. **Pass 1 — fan out to current owners.** The orchestrator reads the shard
   map, computes the current physical owner of every virtual slot, and asks
   each owner for its data. Each shard root reports back two things:
   * the keys/entries (or count) of all keys *not* in its
     `MovedAwaySlots` table (entries it no longer authoritatively owns), and
   * the set of `MovedAwaySlots` virtual slots it observed during the
     traversal (used as a topology-stability hint).
2. **Stability check.** The orchestrator re-reads `ShardMap.Version` from
   the registry. If the version moved during pass 1, a concurrent split
   committed its swap mid-scan and the per-shard view is potentially
   inconsistent — the whole pass is discarded and retried. Bounded by
   `LatticeOptions.MaxScanRetries` (default 3); throws
   `InvalidOperationException` on exhaustion. Each retry re-fetches only
   the virtual slots that actually moved, so the cost per retry is
   O(moved slots), not O(all shards). Under the default configuration
   (`MaxConcurrentAutoSplits` = 2, `HotShardSplitCooldown` = 2 minutes),
   exhausting 3 retries would require an unusually sustained burst of
   concurrent topology changes.
3. **Pass 2 — reconcile (Keys/Entries only).** For `KeysAsync` and
   `EntriesAsync`, the orchestrator compares the shard map at the start of
   pass 1 against the current map and asks the new owners of any slots
   that changed hands for the missing entries, using the slot-filtered
   variants `GetSortedKeysBatchForSlotsAsync` /
   `GetSortedEntriesBatchForSlotsAsync`. A `HashSet<string>` deduplicator
   ensures no key is emitted twice. `CountAsync` does not need pass 2 —
   pass 1 already polled every current owner and the source's
   `MovedAwaySlots` filter prevents double counting.

### Trade-offs

* **Order**: Keys/Entries are streamed in sorted (or reverse) order
  during pass 1. Reconciled entries from pass 2, when present, are
  appended at the tail in unspecified order — breaking the lexicographic
  ordering guarantee for the affected scan. Callers requiring strict
  global ordering across topology changes should sort the output, or
  scan again after `IsSplittingAsync()` returns `false` on every shard.
  Tracked by **F-032** (scan ordering preservation under topology change).
* **Memory**: scans allocate a `HashSet<string>` for dedup that grows
  with the number of distinct keys observed in pass 1. For very large
  trees, prefer the range-bounded overload of `KeysAsync` /
  `EntriesAsync` to bound memory.
* **Latency**: when no split has ever occurred, scans take the same
  fast path as before (one round-trip per shard). The reconciliation
  passes only run when a shard actually reports moved slots.
* **System trees**: the lattice registry tree itself bypasses the
  reconciliation path (it never participates in adaptive splits, and
  reading its own shard map would deadlock). It uses the simple
  fan-out-and-sum count instead.

## Autonomic detection

The per-tree `HotShardMonitorGrain` is started lazily on the first write and
re-anchored by a keepalive reminder. On each tick (default every 30 s) it:

1. Polls every physical shard's `GetHotnessAsync()` in parallel (F-013).
2. Computes ops/sec = `(reads + writes) / window.TotalSeconds`.
3. Counts the number of in-flight splits **for this tree** by polling every
   physical shard's `IsSplittingAsync()`. If that count is already
   `MaxConcurrentAutoSplits`, the pass returns without triggering anything.
   Because `HotShardMonitorGrain` is keyed per-tree, the cap is enforced
   independently per tree — in a multi-tree cluster each tree may have up
   to `MaxConcurrentAutoSplits` concurrent splits running simultaneously.
4. Selects the top-`(MaxConcurrentAutoSplits − inFlight)` hottest shards
   whose rate exceeds `HotShardOpsPerSecondThreshold` (default 200 ops/s),
   skipping any shard already splitting, on cooldown, or owning a single
   virtual slot.
5. Triggers `ITreeShardSplitGrain.SplitAsync` on each selected shard in
   parallel via `Task.WhenAll` and starts a per-shard cooldown.

Each split runs in its own coordinator activation: the
`ITreeShardSplitGrain` key format is **`{treeId}/{sourceShardIndex}`**,
so independent splits of different source shards within the same tree do
not contend on a single coordinator. Concurrent target-index allocation is
made collision-free by a registry-side atomic counter
(`ILatticeRegistry.AllocateNextShardIndexAsync`), and concurrent shard-map
swaps are made composition-safe by re-reading the current map inside the
swap phase before persisting the diff. Both atomicity guarantees rely on
the singleton `LatticeRegistryGrain` being non-reentrant.

A split is **suppressed** (whole pass skipped) or a candidate is **skipped
individually** when:

| Suppression rule | Scope | Mechanism |
|---|---|---|
| `AutoSplitEnabled = false` | Whole pass | Returns early. |
| Tree younger than `AutoSplitMinTreeAge` (since monitor activation, default 60 s) | Whole pass | Returns early. |
| Resize / merge / snapshot in progress | Whole pass | `ILattice.IsResize/Merge/SnapshotCompleteAsync()` returns `false`. |
| Any shard has a pending bulk graft | Whole pass | `IShardRootGrain.HasPendingBulkOperationAsync()` returns `true`. |
| In-flight splits already at `MaxConcurrentAutoSplits` | Whole pass | Sum of `IsSplittingAsync()` results. |
| Shard already splitting | Per shard | Excluded from candidate set. |
| Per-shard cooldown active (default 2 min) | Per shard | In-memory cooldown timestamp. |
| Shard owns a single virtual slot | Per shard | Cannot be subdivided further. |

## Tunables (`LatticeOptions`)

| Option | Default | Description |
|---|---|---|
| `AutoSplitEnabled` | `true` | Master switch for autonomic splits. Manual splits via `ITreeShardSplitGrain` remain available even when `false`. |
| `HotShardOpsPerSecondThreshold` | `200` | Operations/second above which a shard is considered hot. Intentionally low so splits occur before throughput degrades. |
| `HotShardSampleInterval` | `30 s` | How often the monitor polls hotness counters. |
| `HotShardSplitCooldown` | `2 min` | Minimum interval between consecutive splits of the same physical shard. |
| `MaxConcurrentAutoSplits` | `2` | Maximum concurrent splits per tree. Each split runs in its own per-shard coordinator activation; the cap bounds aggregate storage I/O. |
| `SplitDrainBatchSize` | `1024` | Maximum number of moved-slot entries the drain accumulates in memory before flushing to the target shard. Caps coordinator allocation regardless of source shard size. |
| `AutoSplitMinTreeAge` | `60 s` | Minimum tree age before autonomic splits are allowed; absorbs startup bursts. |
| `MaxScanRetries` | `3` | Maximum bounded retries that a strongly-consistent scan (`CountAsync`, `KeysAsync`, `EntriesAsync`) performs when `ShardMap.Version` keeps moving mid-scan due to concurrent splits. Throws `InvalidOperationException` on exhaustion. Increase if scans run during very-high split churn. |

## Convergence guarantees

* **No data loss** — every write committed to *S* is either drained,
  shadow-mirrored, or both, and `MergeManyAsync` is idempotent under LWW.
* **No duplicate authority** — after the swap, only *T* is reachable for
  moved slots via the public API; orphan entries on *S* are unreachable
  and reclaimed on tree purge.
* **Geometric convergence on a single hot slot** — if all heat is in one
  virtual slot, successive autonomic splits subdivide *S*'s slot set in
  half each pass, isolating the hot slot in `O(log virtualSlotsPerShard)`
  splits.

## Manual control

For tooling and tests, `ITreeShardSplitGrain` exposes:

| Method | Purpose |
|---|---|
| `SplitAsync(int sourceShardIndex)` | Initiate a split of the given shard. Idempotent for matching parameters. |
| `RunSplitPassAsync()` | Synchronously drive the in-progress split to completion. |
| `IsCompleteAsync()` | `true` when no split is in progress. |

Both grains are internal infrastructure (`[EditorBrowsable(Never)]`) and are
not exposed on `ILattice`. The intent is that splits remain an
autonomic concern; the manual interface exists to support tests and
operators investigating routing topology.
