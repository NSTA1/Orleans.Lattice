# Troubleshooting

A symptom-driven guide to the problems Lattice trees actually hit in
production: storage-provider failures on write, slowdowns caused by an
in-flight shard split, scans that take far longer than expected, and reads
that look stale. Each section is laid out as **symptom -> likely cause -> how
to confirm -> how to fix**.

The centre of the guide is [Reading a `DiagnoseAsync`
report](#reading-a-diagnoseasync-report). Nearly every investigation starts by
taking a report and following the field that looks wrong into one of the
sections below.

This guide deliberately does not restate the reference material it points at.
Field-by-field definitions live in [Diagnostics](diagnostics.md), the
instrument catalog lives in [Metrics](metrics.md), and every option named here
is described in [Configuration](configuration.md).

---

## Take a diagnostic report first

`ILattice.DiagnoseAsync` returns a `TreeDiagnosticReport`: a per-tree health
snapshot assembled by fanning out to every physical shard.

```csharp verify
var report = await tree.DiagnoseAsync(deep: true, cancellationToken);

Console.WriteLine(
    $"tree={report.TreeId} shards={report.ShardCount}/{report.VirtualShardCount} " +
    $"live={report.TotalLiveKeys} tombstones={report.TotalTombstones} " +
    $"deep={report.Deep} sampledAt={report.SampledAt:O}");

foreach (var shard in report.Shards)
{
    Console.WriteLine(
        $"  shard {shard.ShardIndex}: depth={shard.Depth} rootIsLeaf={shard.RootIsLeaf} " +
        $"live={shard.LiveKeys} tombstones={shard.Tombstones} " +
        $"ratio={shard.TombstoneRatio:P1} ops/s={shard.OpsPerSecond:F1} " +
        $"reads={shard.Reads} writes={shard.Writes} window={shard.HotnessWindow} " +
        $"split={shard.SplitInProgress} bulk={shard.BulkOperationPending}");
}

foreach (var split in report.RecentSplits)
{
    Console.WriteLine($"  recent split: shard {split.ShardIndex} at {split.AtUtc:O}");
}
```

`deep: false` is the cheap mode and is safe to poll. `deep: true` is the mode
that produces tombstone counts; see [the traps below](#traps-when-reading-a-report)
before you draw a conclusion from a shallow report.

---

## Reading a `DiagnoseAsync` report

### Tree-level fields

| Field | What a healthy value looks like | What an unhealthy value points at |
|---|---|---|
| `ShardCount` | The tree's physical shard count. Stable between reshards. | A value you did not expect means you are looking at a different tree than you think, or a reshard changed the topology. See [Online reshard](online-reshard.md). |
| `VirtualShardCount` | A compile-time constant (4096). Persisted shard maps reference virtual slots by index, so it is not configurable. | Nothing to act on - it is the routing map's resolution, not a load signal. See [Tree sizing](tree-sizing.md). |
| `TotalLiveKeys` | Tracks your expected working-set size. | Growth that outruns your model points at [Slow scans](#slow-scans) and admission headroom. Compare against `LatticeOptions.MaxLiveKeys` if you have set one. |
| `TotalTombstones` | Small relative to `TotalLiveKeys`. | A large share of the total points at [tombstone bloat](#slow-scans). Only populated when `deep: true`. |
| `SampledAt` | Within `DiagnosticsCacheTtl` (default 5 s) of now. | Older than the TTL means you are reading a cached report; see [the traps](#traps-when-reading-a-report). |
| `Deep` | Echoes the argument you passed. | If it is `false`, ignore every tombstone field in the report. |
| `Shards` | One entry per physical shard, ordered by `ShardIndex`. | See the per-shard table below. |
| `RecentSplits` | Empty on a stable tree; a short list after adaptive splits. | A steady stream of entries points at [Concurrent split activity](#concurrent-split-activity). |

### Per-shard fields

| Field | What a healthy value looks like | What an unhealthy value points at |
|---|---|---|
| `Depth` | `0` for a shard with no root yet, `1` while the root is still a leaf, and small (single digits) once the shard has internal levels. | Depth climbing across shards means the shards hold far more keys than the tree was sized for. See [Tree sizing](tree-sizing.md) and [Tree structure](tree-structure.md). |
| `RootIsLeaf` | `true` on a small or new shard, `false` once the shard has grown past one leaf. | Not a fault on its own; read it alongside `Depth`. |
| `LiveKeys` | Roughly even across shards. | A shard carrying many times its peers' keys is a key-distribution problem, not a load problem. Review your key design; see [Tree structure](tree-structure.md). |
| `Tombstones` / `TombstoneRatio` | Low. `0.0` on a tree that never deletes. | A high ratio means deleted rows are still being walked on every scan. See [Slow scans](#slow-scans) and [Tombstone compaction](tombstone-compaction.md). |
| `OpsPerSecond` | Comparable across shards. | One shard far above its peers is a hot shard - the exact condition adaptive splitting exists to relieve. See [Concurrent split activity](#concurrent-split-activity). |
| `Reads` / `Writes` | The raw counters `OpsPerSecond` is derived from, over `HotnessWindow`: `(Reads + Writes) / HotnessWindow.TotalSeconds`. | A read-heavy shard and a write-heavy shard need different remedies; the split between the two counters tells you which you have. |
| `HotnessWindow` | The sampling window the counters cover. | `TimeSpan.Zero` makes `OpsPerSecond` meaningless (it is reported as `0.0`); the shard has not accumulated a window yet. |
| `SplitInProgress` | `false` on a stable tree. | `true` means this shard is the source of an in-flight adaptive split. Normal if transient; see [Concurrent split activity](#concurrent-split-activity). |
| `BulkOperationPending` | `false` on a stable tree. | `true` means the shard is holding a pending bulk graft. Normal during a bulk load; see [Bulk loading](bulk-loading.md). |

### A worked reading

A tree with a hot shard mid-split, and a second shard that needs compaction:

```text
tree=orders shards=4/4096 live=812433 tombstones=196022 deep=True sampledAt=2025-...
  shard 0: depth=3 live=201110 tombstones=1204  ratio=0.6%  ops/s=48.2  split=False bulk=False
  shard 1: depth=3 live=198740 tombstones=1190  ratio=0.6%  ops/s=51.7  split=False bulk=False
  shard 2: depth=4 live=210301 tombstones=192455 ratio=47.8% ops/s=44.9  split=False bulk=False
  shard 3: depth=3 live=202282 tombstones=1173  ratio=0.6%  ops/s=502.4 split=True  bulk=False
  recent split: shard 3 at 2025-...
```

Read it in this order:

1. **`LiveKeys` is even across every shard.** Key distribution is fine, so
   whatever is wrong is not a hashing problem.
2. **Shard 3 carries roughly ten times its peers' `OpsPerSecond`.** That is a
   hot shard: even load by key count, very uneven load by request rate. Its
   `SplitInProgress` is `true` and it appears in `RecentSplits`, so the
   autonomic splitter has already noticed and is acting. Nothing to do unless
   the flag stays set - go to [Concurrent split activity](#concurrent-split-activity).
3. **Shard 2 has a `TombstoneRatio` of 47.8 percent** while its peers sit under
   one percent. Nearly half of what a scan of that shard walks is deleted rows.
   That is the compaction signal - go to [Slow scans](#slow-scans).
4. **Shard 2's `Depth` is one greater than its peers'**, which is consistent
   with the tombstone bloat rather than a separate problem: the shard is
   holding more physical entries than its peers for the same live-key count.

### Traps when reading a report

These are the report behaviours that most often lead to a wrong conclusion.

- **A shallow report always reports zero tombstones.** The shallow path never
  asks leaves for tombstone counts, so `Tombstones` and `TombstoneRatio` come
  back `0` (and `TotalTombstones` with them) regardless of what is on disk.
  Check `report.Deep` before believing a zero. Only `deep: true` produces
  tombstone figures.
- **An all-zero shard entry can mean the fan-out failed.** When a shard's
  diagnostics call throws, the aggregator logs a warning
  (`Diagnostics fan-out failed for shard {ShardIndex} in tree {TreeId}`) and
  substitutes an entry carrying only the shard index - every other field is
  its default. An entry with `Depth = 0`, `LiveKeys = 0`, and
  `HotnessWindow = TimeSpan.Zero` is therefore either a genuinely empty shard
  or an unreachable one, and the silo log is the only way to tell them apart.
  If one shard reads as empty on a tree you know holds data, check the log
  before concluding the data is gone.
- **Reports are cached per mode.** Shallow and deep results are cached
  independently for `DiagnosticsCacheTtl` (default 5 s), so a shallow poll
  never refreshes the deep report and vice versa. `SampledAt` tells you how old
  the report actually is. To make a report unconditionally fresh during an
  investigation, set the TTL to zero for that tree:

  ```csharp verify
  siloBuilder.ConfigureLattice("orders", options =>
  {
      options.DiagnosticsCacheTtl = TimeSpan.Zero;
  });
  ```

  A zero TTL means every call fans out to every shard. Use it for triage, not
  for a steady-state dashboard poll.
- **`RecentSplits` is activation state, not history.** It is a bounded ring
  buffer (32 entries) on the per-tree stats grain. It is emptied if that grain
  deactivates, and it is trimmed once 32 splits have accumulated. An empty
  `RecentSplits` does not prove no split happened; the
  `orleans.lattice.shard.splits_committed` counter in [Metrics](metrics.md) is
  the record that survives a deactivation.
- **A split commit invalidates both cached reports.** Recording a split clears
  the shallow and deep caches, so the report you take immediately after a split
  is always freshly fanned out.

### What the report does not tell you

`DiagnoseAsync` is a structural and hotness snapshot. It carries no storage
bytes, no latency percentiles, and no cache hit ratio. For those:

| Question | Where to look |
|---|---|
| How many bytes is this tree holding? | `ILattice.GetStorageUsageAsync`; see [Tree storage](tree-storage.md). |
| How slow are reads and writes? | The `orleans.lattice.get.duration` / `set.duration` histograms in [Metrics](metrics.md). |
| Is the read cache helping? | `orleans.lattice.cache.hits` and `orleans.lattice.cache.misses`. |
| Is the WAL backing up? | [WAL saturation signal](wal-saturation-signal.md) and [WAL tuning](wal-tuning.md). |
| Is compaction keeping up? | `orleans.lattice.compaction.*`; see [Tombstone compaction](tombstone-compaction.md). |

---

## Storage-provider exceptions on write

### Symptom

A write fails with an exception thrown by the Orleans storage provider rather
than by Lattice, surfacing from the provider's `WriteStateAsync`. The failure
is usually reproducible for a particular key or leaf and unaffected by retry.

### Likely cause

Lattice persists a tree across several storage rows, and each one is bounded
independently by the provider's per-row or per-blob limit:

- **The WAL row**, written once per mutation. Its size grows with the key and
  value you wrote, plus causal metadata.
- **The leaf snapshot blob**, capturing a leaf's live entries. Its size grows
  with the number of entries the leaf holds, which is bounded by that tree's
  pinned `MaxLeafKeys`.
- **The leaf state row**, which no longer scales with `MaxLeafKeys`.

An oversized single value pushes the WAL row over the limit; too many entries
per leaf pushes the snapshot blob over it. [Tree storage](tree-storage.md)
carries the per-provider limit table, the row-size formulas, and the sizing
arithmetic for choosing `MaxLeafKeys` against a given provider.

Two things this is *not*:

- It is **not** `LatticeQuotaExceededException`. That is Lattice's own opt-in
  admission control (`LatticeOptions.MaxLiveKeys` / `MaxEstimatedBytes`, both
  `null` by default) refusing a write because the tree hit a configured
  ceiling, and it carries `TreeId`, `Dimension`, `Current`, and `Limit` so you
  can act on it without parsing a message.
- It is **not** Orleans's `InconsistentStateException`, which signals an etag
  conflict - a concurrent writer to the same state row - not a size problem.

### How to confirm

1. Read the provider exception itself. It names the limit it enforced. Lattice
   does not translate a provider size failure into a Lattice exception type,
   so the provider's own error is the primary evidence.
2. Take a storage-usage report and compare the surfaces against the provider's
   limit from [Tree storage](tree-storage.md):

   ```csharp verify
   var usage = await tree.GetStorageUsageAsync(cancellationToken);

   Console.WriteLine(
       $"tree={usage.TreeId} wal={usage.WalRetainedBytes} snapshot={usage.SnapshotBytes} " +
       $"leafState={usage.LeafStateBytes} total={usage.TotalBytes} partial={usage.Partial}");
   ```

   `Partial` set to `true` means at least one surface could not be sampled, so
   the totals are a floor rather than an exact figure.
3. Check the silo log for the leaf snapshot warning. A snapshot capture driven
   by the activation advisory is best-effort: it is caught and logged
   (`Proactive snapshot capture for leaf {GrainId} failed; will retry on next
   periodic recheck or reactivation.`) rather than surfaced to the caller. A
   snapshot that is permanently too large to write therefore shows up as a
   repeating warning and no snapshot coverage, not as a failed request.

### How to fix

- **Bound what callers can write, at the edge.** Both guards are opt-in and
  both default to `null`; each throws an `ArgumentException` before the write
  reaches storage, which is a far better failure than a provider error deep in
  the persistence path:

  ```csharp verify
  siloBuilder.ConfigureLattice("orders", options =>
  {
      options.MaxKeyLength = 512;
      options.MaxValueSizeBytes = 256 * 1024;
  });
  ```

- **Keep large payloads out of the tree.** Store the blob in blob storage and
  put its identifier in the tree. This is the only fix that scales - a value
  large enough to threaten a WAL row will threaten the next provider too.
- **Lower the tree's `MaxLeafKeys`** so each snapshot blob covers fewer
  entries. `MaxLeafKeys` is **not** a `LatticeOptions` property: it is pinned
  per tree in the registry (defaulting to 128) and changed only through
  `ILattice.ResizeAsync`. See [Tree sizing](tree-sizing.md) for the resize
  procedure and [Tree storage](tree-storage.md) for how to pick the value.
- **Move the WAL to a higher-capacity backend.** The WAL has its own storage
  seam (`IWalStorageProvider`), so it can be pointed at a backend with a larger
  per-row limit independently of the grain-storage provider the rest of the
  tree uses; see [WAL storage providers](wal-storage-providers.md). Note this
  does not help the snapshot blob: leaf snapshots are persisted through the
  same named grain-storage provider as the rest of the tree, so the only levers
  there are `MaxLeafKeys`, the value sizes you write, and swapping that
  provider.
- **Turn on admission control** so the tree refuses writes with a typed,
  actionable `LatticeQuotaExceededException` before it grows into provider
  limits, rather than after. See `MaxLiveKeys`, `MaxEstimatedBytes`, and the
  non-enforcing `AdmissionAdvisoryLiveKeys` / `AdmissionAdvisoryBytes`
  dry-run ceilings in [Configuration](configuration.md).

---

## Concurrent split activity

### Symptom

One or more shards report `SplitInProgress = true`. Latency on the affected
shard is elevated, and `RecentSplits` shows entries appearing regularly.

### Likely cause

`SplitInProgress` means the shard is the **source** of an in-flight adaptive
split: Lattice has detected a hot shard and is moving part of its virtual-shard
range to a new physical shard. The autonomic splitter watches per-shard
throughput and triggers when a shard's observed operations per second exceed
`HotShardOpsPerSecondThreshold` (default 200). That figure is computed as
`(reads + writes) / window.TotalSeconds` - the same quantity the report
surfaces as `OpsPerSecond`, so the report shows you exactly what the splitter
is reacting to. `AutoSplitEnabled` defaults to `true`.

`BulkOperationPending` is the analogous flag for a pending bulk graft, set
while a [bulk load](bulk-loading.md) is staged but not yet committed.

**A split in progress is normal operation, not a fault.** Callers do not see
topology exceptions during one: routing staleness is caught and retried inside
Lattice, so a read or write that races the topology change is retried against
the new owner transparently. See [Shard splitting](shard-splitting.md) for the
phase-by-phase description of what the split actually does.

### How to confirm

Poll the flags and correlate them against split history:

```csharp verify
var report = await tree.DiagnoseAsync(deep: false, cancellationToken);

foreach (var shard in report.Shards.Where(s => s.SplitInProgress || s.BulkOperationPending))
{
    Console.WriteLine(
        $"shard {shard.ShardIndex}: split={shard.SplitInProgress} " +
        $"bulk={shard.BulkOperationPending} ops/s={shard.OpsPerSecond:F1}");
}

var totalOps = report.Shards.Sum(s => s.OpsPerSecond);
var hottest = report.Shards.OrderByDescending(s => s.OpsPerSecond).First();
var share = totalOps > 0 ? hottest.OpsPerSecond / totalOps : 0;

Console.WriteLine($"hottest shard {hottest.ShardIndex} carries {share:P0} of observed ops/s");
```

Then decide which case you are in:

| Observation | Reading |
|---|---|
| Flag set, clears within a few report intervals, `RecentSplits` gains one entry | Normal. The split committed. |
| Flag set on several shards at once | Also normal if it is bounded: `MaxConcurrentAutoSplits` (default 2) caps in-flight splits per tree, and `MaxClusterConcurrentAutoSplits` (default `null`, disabled) adds a cluster-wide ceiling on top of it. |
| Flag set on the same shard across many reports, no new `RecentSplits` entry, no throughput recovery | Stuck. Treat as a fault. |
| Flag never set even though one shard is obviously hot | The candidate is being suppressed. |

For the last two cases, the metrics tell you which: `orleans.lattice.split.in_flight`
shows what is actually running, `orleans.lattice.split.candidates_suppressed`
shows candidates rejected by a suppression rule, and
`orleans.lattice.split.admission.deferred` shows splits deferred by the
concurrency caps, and `orleans.lattice.shard.splits_committed` counts the
splits that completed. See [Metrics](metrics.md).

### How to fix

- **A transient flag needs no action.** Let it complete.
- **Splits never triggering on an obviously hot shard**: check the suppression
  rules in [Shard splitting](shard-splitting.md). The common causes are
  `AutoSplitMinTreeAge` (default 60 s) holding off splits on a young tree,
  `HotShardSplitCooldown` (default 2 minutes) rate-limiting repeat splits on
  the same shard, and `AutoSplitEnabled` having been turned off.
- **Splits triggering too eagerly on a bursty workload**: raise
  `HotShardOpsPerSecondThreshold`, or lengthen `HotShardSampleInterval`
  (default 30 s) so a short burst does not look like sustained load.
- **The split itself is too disruptive**: lower `SplitDrainBatchSize`
  (default 1024) to make each drain step smaller, at the cost of a longer
  overall split.
- **Splitting cannot fix an uneven `LiveKeys` distribution.** Splitting
  redistributes virtual shards to relieve *request* load. If one shard holds
  many times its peers' keys, the key design is skewed and no amount of
  splitting will even it out.

---

## Slow scans

### Symptom

A range scan or full enumeration takes much longer than the key count
suggests it should, or its latency degrades over time on a tree whose live-key
count is flat.

### Likely cause

Four things dominate scan cost:

1. **Tombstone bloat.** A deleted key leaves a tombstone that a scan still
   walks. A shard at 50 percent `TombstoneRatio` does twice the work per live
   result. This is the classic case of a scan getting slower while
   `TotalLiveKeys` stays flat.
2. **Shard fan-out.** Keys are routed to shards by hash (a virtual slot derived
   from `XxHash32` of the key), so a scan fans out to *every* physical shard no
   matter how narrow the range you ask for. The scan's wall-clock is therefore
   bounded by its slowest shard - one hot or bloated shard slows every scan of
   the tree.
3. **Leaf-chain walk depth.** Within a shard, a scan walks the leaf chain one
   leaf at a time. More entries per shard means more leaves to hop.
4. **Page size and round trips.** `KeysPageSize` (default 512) sets how many
   keys each shard returns per page; a small page size on a large scan
   multiplies round trips.

### How to confirm

- **Take a deep report and read `TombstoneRatio` per shard.** This is the
  fastest discriminator: a high ratio on the shards you scan is the answer.
  Remember that a shallow report reports zero tombstones unconditionally.
- **Watch `orleans.lattice.leaf.scan.duration`** to see whether the time is
  going into per-leaf work, and `orleans.lattice.leaf.tombstone.ratio` for the
  live per-leaf view. See [Metrics](metrics.md).
- **Compare per-shard figures in the report.** Because every scan fans out to
  every shard, the slowest shard sets the pace. Look for the shard whose
  `TombstoneRatio` or `OpsPerSecond` is the outlier and treat that shard as the
  scan's bottleneck.

### How to fix

- **Compact the bloated shards.** `MinTombstoneRatioForCompaction` (default
  `0.0`, meaning ratio-triggered compaction is off) and
  `MaxLeafEntriesBeforeForcedCompaction` (default `0`, off) are the policy
  triggers; `CompactionTriggerCooldown` (default 5 minutes) rate-limits them.
  `TombstoneGracePeriod` (default 24 hours) is how long a tombstone must age
  before it can be reaped. For a one-off, drive a shard directly:

  ```csharp verify
  var accepted = await tree.CompactShardAsync(shardIndex: 2, cancellationToken);

  Console.WriteLine(accepted
      ? "compaction pass accepted"
      : "shard declined the request (already compacting, or nothing to do)");
  ```

  See [Tombstone compaction](tombstone-compaction.md) for the full policy and
  its telemetry.
- **Bound the range.** Prefer the `startInclusive` / `endExclusive` overloads
  over an unbounded enumeration. A bound does not reduce the number of shards
  visited - routing is by hash, so every shard is asked - but it cuts the work
  each shard does and keeps the client-side dedup set small, since that set
  grows with the number of distinct keys the scan has yielded.
- **Use the resilient streaming API for long scans.** `ScanKeysAsync` and
  `ScanEntriesAsync` transparently reconnect and resume when an enumeration is
  aborted mid-flight, with a default budget of 8 reconnect attempts
  (overridable per call via `maxAttempts`). `ILattice.KeysAsync` is for short,
  single-page reads.
- **Turn on prefetch for scans you will consume in full.** `PrefetchKeysScan`
  and `PrefetchEntriesScan` both default to `false`; a per-call `prefetch: true`
  argument overrides them for a single scan. Prefetch fetches each shard's next
  page in parallel while the current page is being consumed, hiding per-shard
  grain-call latency. Prefetched pages are held in memory until consumed, so a
  caller that aborts early (a `Take(n)`, say) pays for pages it never reads.
- **Raise `KeysPageSize`** (default 512) if you are paging a large result set
  and the round-trip count dominates.

If a scan does not just run slowly but *fails*, that is a different problem.
Strongly-consistent scans (`CountAsync`, `KeysAsync`, `EntriesAsync`) reconcile
against shard-map changes that land mid-scan, bounded by `MaxScanRetries`
(default 3); exhausting the budget throws an `InvalidOperationException` whose
message names the operation and tells you to raise `MaxScanRetries` or reduce
concurrent split activity. If you see it, read [Concurrent split
activity](#concurrent-split-activity) first - the scan is a symptom of the
topology churn, not the cause. See [Consistency](consistency.md) for the
enumeration guarantees.

---

## Stale reads and cache behaviour

### Symptom

A read returns a value you believe was already overwritten or deleted, or two
reads issued close together disagree.

### Likely cause

Lattice serves `GetAsync`, `ExistsAsync`, and `GetManyAsync` through a
per-silo read-through cache. Whether that cache can return a stale value is
entirely determined by `CacheTtl`:

- **`CacheTtl = TimeSpan.Zero` (the default).** Every read performs a delta
  refresh against the primary leaf before answering. The version-vector
  comparison is cheap, but the round trip still happens - so the default
  configuration does **not** serve stale values.
- **`CacheTtl` set to a non-zero value.** The cache may answer from its local
  dictionary without contacting the primary for up to that interval. This is
  the trade you opted into: lower read latency, staleness bounded by the TTL.

So on a default-configured tree, a surprising read is almost never the read
cache. See [Caching](caching.md) for the refresh protocol and
[Consistency](consistency.md) for the guarantees each read path gives.

### How to confirm

Compare a cacheable read against a read that cannot be served from cache.
`GetWithVersionAsync` bypasses the cache deliberately, because compare-and-swap
callers need the authoritative version:

```csharp verify
var cached = await tree.GetAsync("orders/42", cancellationToken);
var authoritative = await tree.GetWithVersionAsync("orders/42", cancellationToken);

Console.WriteLine($"cached-path bytes: {cached?.Length ?? -1}");
Console.WriteLine($"authoritative bytes: {authoritative.Value?.Length ?? -1}");
```

- **They agree.** The cache is not involved; the value really is what the tree
  holds. Look at the writer instead.
- **They disagree and `CacheTtl` is non-zero.** Expected staleness, bounded by
  the TTL you configured.
- **They disagree and `CacheTtl` is `TimeSpan.Zero`.** That is not ordinary
  cache staleness. Capture both results and treat it as a consistency issue.

Two secondary checks:

- `orleans.lattice.cache.hits` and `orleans.lattice.cache.misses` in
  [Metrics](metrics.md) tell you whether the cache is answering at all.
- If you have set `MaxCacheValueBytes`, large payloads are evicted **value
  first** while their metadata envelope stays resident; a read landing on an
  evicted payload transparently fetches from the primary and counts as a miss.
  That costs an RPC but cannot return a stale value.

### How to fix

- **Set `CacheTtl` to `TimeSpan.Zero`** for a tree whose reads must always
  reflect the latest committed write:

  ```csharp verify
  siloBuilder.ConfigureLattice("orders", options =>
  {
      options.CacheTtl = TimeSpan.Zero;
  });
  ```

- **Use `GetWithVersionAsync` for read-modify-write.** It bypasses the cache
  and returns the version a conditional write needs. Never build a
  compare-and-swap on top of `GetAsync`.
- **Do not confuse the two caches.** `CacheTtl` governs value reads;
  `DiagnosticsCacheTtl` (default 5 s) governs `DiagnoseAsync` reports only.
  A stale *report* is a diagnostics-cache artefact and says nothing about read
  freshness - which is exactly why a caller that needs an authoritative live
  count should use `CountAsync` rather than reading `TotalLiveKeys` out of a
  possibly-cached report.
- **After a resize or reshard, no cache flush is needed.** The new physical
  tree has different leaf grain identities, so reads land on fresh cache
  activations. See [Tree sizing](tree-sizing.md) and [Online
  reshard](online-reshard.md).

---

## Symptom index

| Symptom | Section |
|---|---|
| Provider exception out of `WriteStateAsync` | [Storage-provider exceptions on write](#storage-provider-exceptions-on-write) |
| `LatticeQuotaExceededException` on write | [Storage-provider exceptions on write](#storage-provider-exceptions-on-write) (admission control, not a provider limit) |
| Repeating "Proactive snapshot capture ... failed" warning | [Storage-provider exceptions on write](#storage-provider-exceptions-on-write) |
| One shard far hotter than its peers | [Concurrent split activity](#concurrent-split-activity) |
| `SplitInProgress` stuck on for a long time | [Concurrent split activity](#concurrent-split-activity) |
| `BulkOperationPending` stuck on | [Concurrent split activity](#concurrent-split-activity) and [Bulk loading](bulk-loading.md) |
| Scan latency climbing while live keys stay flat | [Slow scans](#slow-scans) |
| `InvalidOperationException` naming `MaxScanRetries` | [Slow scans](#slow-scans), then [Concurrent split activity](#concurrent-split-activity) |
| High `TombstoneRatio` | [Slow scans](#slow-scans) and [Tombstone compaction](tombstone-compaction.md) |
| Read returns an overwritten value | [Stale reads and cache behaviour](#stale-reads-and-cache-behaviour) |
| One shard reports all zeros on a tree that holds data | [Traps when reading a report](#traps-when-reading-a-report) |
| Report is older than expected | [Traps when reading a report](#traps-when-reading-a-report) |
| `Depth` growing across shards | [Tree sizing](tree-sizing.md) |

---

## See also

- [Diagnostics](diagnostics.md) - the field-by-field definition of
  `TreeDiagnosticReport` and `ShardDiagnosticReport`.
- [Metrics](metrics.md) - the full instrument catalog behind every metric
  named here.
- [Configuration](configuration.md) - every `LatticeOptions` member, with
  defaults and tuning guidance.
- [Tree storage](tree-storage.md) - per-provider size limits and the row-size
  arithmetic.
- [Tree sizing](tree-sizing.md) - choosing and changing `MaxLeafKeys`.
- [Shard splitting](shard-splitting.md) - the split protocol, suppression
  rules, and tunables.
- [Tombstone compaction](tombstone-compaction.md) - compaction policy,
  operator API, and telemetry.
- [Caching](caching.md) - the read-through cache and its refresh protocol.
- [Consistency](consistency.md) - the guarantees each read and scan path
  gives.
