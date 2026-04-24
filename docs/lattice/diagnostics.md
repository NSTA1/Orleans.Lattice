# Diagnostics

`ILattice.DiagnoseAsync` returns a point-in-time health snapshot of a tree. It is an admin-rate API intended for dashboards, health probes, and post-mortem investigation — not for hot-path application logic.

```csharp verify
var report = await tree.DiagnoseAsync(deep: true, cancellationToken);
Console.WriteLine($"Tree {report.TreeId}: {report.TotalLiveKeys} live / {report.TotalTombstones} tombstones across {report.ShardCount} shards.");
foreach (var shard in report.Shards)
{
    Console.WriteLine($"  shard {shard.ShardIndex}: depth={shard.Depth}, live={shard.LiveKeys}, ops/s={shard.OpsPerSecond:F1}");
}
```

## Report shape

`TreeDiagnosticReport` aggregates per-shard structural and runtime metrics plus a bounded ring buffer of recent adaptive-split events:

| Field | Description |
|---|---|
| `TreeId` | Logical tree identifier. |
| `ShardCount` | Number of physical shards currently owning virtual slots. |
| `VirtualShardCount` | Fixed-size virtual slot space (4096 for all current trees). |
| `TotalLiveKeys` | Sum of live keys across all shards. |
| `TotalTombstones` | Sum of tombstones across all shards (always `0` when `deep: false`). |
| `Shards` | Per-shard reports, ordered by `ShardIndex`. |
| `RecentSplits` | Most recent adaptive-split events (oldest first, capped at 32). |
| `SampledAt` | UTC timestamp when the report was assembled. |
| `Deep` | Whether the report includes tombstone counts. |

Each `ShardDiagnosticReport` carries structural, volume, and hotness fields:

| Field | Description |
|---|---|
| `ShardIndex` | Zero-based physical shard index. |
| `Depth` | B+ tree depth — `1` when the root is a leaf, `2` with one internal level, etc. `0` when the shard is empty. |
| `RootIsLeaf` | Whether the shard's root is currently a leaf. |
| `LiveKeys` / `Tombstones` | Live and tombstoned entry counts. `Tombstones` is `0` unless `deep: true`. |
| `TombstoneRatio` | `Tombstones / (LiveKeys + Tombstones)`; `0` when the shard is empty or `deep: false`. |
| `OpsPerSecond` | `(Reads + Writes) / HotnessWindow.TotalSeconds` since shard activation. |
| `Reads` / `Writes` | Volatile counters; reset on shard-grain deactivation. |
| `HotnessWindow` | Wall-clock duration over which `Reads` and `Writes` accumulated. |
| `SplitInProgress` | Whether the shard is participating in an adaptive split as the source. |
| `BulkOperationPending` | Whether a bulk-load graft is pending on this shard. |

## Shallow vs deep

The `deep` parameter controls whether tombstones are counted:

- **`deep: false`** (default) — fans out one RPC per shard (`IShardRootGrain.GetDiagnosticsAsync`). Each shard computes `LiveKeys` from its persisted shard-level count and skips a full leaf walk. Fast.
- **`deep: true`** — each shard walks its entire leaf chain to count tombstones exactly. Cost is proportional to the number of leaves per shard; expect this to be slower on large trees.

Use shallow reports for routine health probing; reach for deep reports when you suspect tombstone bloat or are diagnosing a compaction issue.

## Caching

Repeat callers within `LatticeOptions.DiagnosticsCacheTtl` (default: 5 seconds) share the same snapshot — identical `SampledAt` timestamps are returned and no fan-out is performed. Shallow and deep reports are cached independently, so a shallow-then-deep sequence will always produce two distinct samples.

The cache is invalidated immediately when an adaptive split commits, ensuring the next `DiagnoseAsync` call after a topology change returns a fresh report rather than serving a stale pre-split view.

Set `DiagnosticsCacheTtl = TimeSpan.Zero` to disable caching entirely — every call assembles a new report. See [Configuration](configuration.md#diagnosticscachettl) for details.

## Recent splits

`RecentSplits` is a bounded (32-entry) ring buffer of the most recent adaptive-split commits observed by the diagnostics grain. Each entry carries the source `ShardIndex` and the UTC commit time. The buffer survives grain activations for the lifetime of the diagnostics grain's activation and is useful for correlating shard-count changes with recent traffic bursts.

Splits are pushed to the diagnostics grain on a best-effort, fire-and-forget basis from the split-coordinator's commit path, so a split may occasionally be missing from the buffer if the push RPC fails — the split itself still completes and is reflected in `ShardCount`/`Shards` on the next full report.

## Cancellation

`DiagnoseAsync` honours its `CancellationToken` cooperatively. A pre-cancelled token throws `OperationCanceledException` immediately; cancellation during fan-out aborts outstanding shard RPCs at the next Orleans turn boundary.

## Operational notes

- `DiagnoseAsync` is safe to call at any time, including during an ongoing resize or reshard — per-shard reports whose fan-out fails are returned as empty entries rather than failing the whole report.
- The returned DTOs are `readonly record struct` types with `[Immutable]` serialization — they are cheap to copy and log.
- Do not call `DiagnoseAsync` on a hot path. For routing or capacity decisions inside the data plane, use the dedicated public APIs (`GetRoutingAsync`, `CountAsync`) rather than decoding a diagnostics report.
