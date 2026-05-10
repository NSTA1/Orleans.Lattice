# Metrics

Orleans.Lattice publishes runtime telemetry through [`System.Diagnostics.Metrics`](https://learn.microsoft.com/dotnet/core/diagnostics/metrics), so any
OpenTelemetry-compatible exporter (Prometheus, OTLP, Azure Monitor, Datadog,
etc.) can subscribe once to the library's meter and receive every instrument
without per-metric wiring.

## Meter

All instruments are owned by a single static `Meter` exposed via
`Orleans.Lattice.LatticeMetrics`:

| Member | Value |
|---|---|
| `LatticeMetrics.MeterName` | `orleans.lattice` |
| `LatticeMetrics.Meter` | the `Meter` instance (exposed for reference-based subscription in tests) |

The name is pinned by a regression test (`LatticeMetrics_meter_name_is_orleans_lattice`) so it cannot drift.

## Tag conventions

Every Lattice instrument carries a consistent set of low-cardinality tags:

| Tag key | Applies to | Value |
|---|---|---|
| `tree` | every instrument | Logical tree id (`ILattice.TreeId`) |
| `shard` | shard-level instruments only | Physical shard index as an `int` |
| `operation` | `orleans.lattice.leaf.scan.duration` only | `keys` or `entries` |
| `step` | `orleans.lattice.leaf.commit.duration` only | `wal`, `apply`, or `observer` |
| `outcome` | `orleans.lattice.atomic_write.completed`, `orleans.lattice.atomic_write.duration`, `orleans.lattice.atomic_write.batch_size` | `committed`, `compensated`, or `failed` |
| `kind` | `orleans.lattice.coordinator.completed`, `orleans.lattice.tree.lifecycle`, `orleans.lattice.events.published` | Discriminator — see each instrument below |
| `reason` | `orleans.lattice.events.dropped` | `missing_provider` or `publish_error` |
| `config` | `orleans.lattice.config.changed` | Configuration dimension name (e.g. `publish_events`) |

Leaf grain ids are **not** emitted as a tag — in a large tree they would produce
unbounded tag cardinality. All leaf instruments are aggregated to the tree level.

## Instrument catalog

### Shard-level (sourced from `ShardRootGrain`)

| Name | Kind | Unit | Description |
|---|---|---|---|
| `orleans.lattice.shard.reads` | `Counter<long>` | `{op}` | Read operations served by a shard root (`GetAsync`, `ExistsAsync`, scan, count, etc.). |
| `orleans.lattice.shard.writes` | `Counter<long>` | `{op}` | Write operations served by a shard root (`SetAsync`, `DeleteAsync`, `MergeManyAsync`, etc.). |
| `orleans.lattice.shard.splits_committed` | `Counter<long>` | `{split}` | Adaptive shard-split commits — fired once per successful `ShardMap` swap from `TreeShardSplitGrain.FinaliseAsync`. |

To derive ops/sec, compute the rate of `shard.reads + shard.writes` at the
collector; the same underlying counters back the internal hotness monitor that
drives autonomic splitting.

### Leaf-level (sourced from `BPlusLeafGrain`)

| Name | Kind | Unit | Description |
|---|---|---|---|
| `orleans.lattice.leaf.write.duration` | `Histogram<double>` | `ms` | Duration of `IPersistentState.WriteStateAsync` calls from a leaf grain — i.e. storage-provider write latency. |
| `orleans.lattice.leaf.commit.duration` | `Histogram<double>` | `ms` | Per-step latency on the `BPlusLeafGrain` commit path. Tagged `step=wal` (commit-log-writer append), `step=apply` (in-memory projection apply + state persist), or `step=observer` (`IMutationObserver` fan-out). Emitted from every commit-path code path (single-key `SetAsync` / `DeleteAsync`, batched `MergeManyAsync`, prepared-write commit) so operators can attribute latency between durability, projection, and observer overhead independently. |
| `orleans.lattice.leaf.scan.duration` | `Histogram<double>` | `ms` | Duration of leaf-level range scans. Tagged `operation=keys` (from `GetKeysAsync`) or `operation=entries` (from `GetEntriesAsync`). |
| `orleans.lattice.leaf.compaction.duration` | `Histogram<double>` | `ms` | Duration of `CompactTombstonesAsync` on a single leaf. |
| `orleans.lattice.leaf.tombstones.reaped` | `Counter<long>` | `{tombstone}` | Tombstones (from explicit `DeleteAsync` / `DeleteRangeAsync`) permanently removed by compaction. |
| `orleans.lattice.leaf.tombstones.created` | `Counter<long>` | `{tombstone}` | Tombstones newly written by `DeleteAsync` (1) or `DeleteRangeAsync` (N). |
| `orleans.lattice.leaf.tombstones.expired` | `Counter<long>` | `{tombstone}` | Live entries reaped by compaction because their per-entry TTL (set via `SetAsync(key, value, TimeSpan)`) elapsed past the configured grace period. Separate from `reaped` so operators can distinguish TTL churn from explicit-delete throughput. |
| `orleans.lattice.leaf.splits` | `Counter<long>` | `{split}` | Leaf-node splits triggered by `MaxLeafKeys` overflow. |
| `orleans.lattice.leaf.replay.duration` | `Histogram<double>` | `ms` | Activation-time leaf-projection replay duration. Tagged `outcome=tail` (caught up by replaying the slice `(checkpoint, head]`), `outcome=snapshot_then_wal` (a fall-off-log trigger fired and the snapshot-then-WAL recovery path was taken), or `outcome=full_rebuild` (`ProjectionRebuildPolicy.FullRebuildFromWal`). Emitted on every activation that performs WAL replay. See [Projection Rebuild](projection-rebuild.md) and [Write-Ahead Log](wal.md). |
| `orleans.lattice.leaf.replay.entries` | `Counter<long>` | `{entry}` | Mutations seen by activation-time leaf-projection replay. Tagged `outcome=applied` (fed to `ILeafProjection.Apply`) or `outcome=skipped` (filtered by the leaf's key-range responsibility before reaching `Apply`). |

### Cache (sourced from `LeafCacheGrain`)

| Name | Kind | Unit | Description |
|---|---|---|---|
| `orleans.lattice.cache.hits` | `Counter<long>` | `{hit}` | `LeafCacheGrain` reads served by a live, cached entry. |
| `orleans.lattice.cache.misses` | `Counter<long>` | `{miss}` | `LeafCacheGrain` reads that did not find a live cached entry. |

### Saga / coordinator / lifecycle

Long-running maintenance and atomicity primitives each emit a completion
signal so operators can alert on stalls, compensation spikes, or missing
coordinator progress independently of whether the event stream is enabled.

| Name | Kind | Unit | Description |
|---|---|---|---|
| `orleans.lattice.atomic_write.completed` | `Counter<long>` | `{saga}` | Terminal transition of a `SetManyAtomicAsync` saga. Tagged `outcome=committed` (all writes applied), `compensated` (rolled back via LWW), or `failed` (post-compensation surrogate failure). |
| `orleans.lattice.atomic_write.duration` | `Histogram<double>` | `ms` | End-to-end `SetManyAtomicAsync` saga duration, captured from the first `Prepare` to `Completed` and persisted across reminder-driven recovery so the recorded ms reflects true wall-clock cost (including any time the saga was suspended across silo restarts). Tagged with the same `outcome` values as `atomic_write.completed`; emitted alongside it on every terminal transition. Pair with `atomic_write.completed` to derive sustained atomic-write throughput and SLO percentiles. |
| `orleans.lattice.atomic_write.batch_size` | `Histogram<int>` | `{entry}` | Entry count of each `SetManyAtomicAsync` saga at terminal transition. Tagged with the same `outcome` values as `atomic_write.completed`; emitted alongside it. Lets operators correlate p99 saga duration with batch size and detect distribution shifts in caller batch sizing. |
| `orleans.lattice.coordinator.completed` | `Counter<long>` | `{operation}` | Successful completion of a long-running coordinator. Tagged `kind=snapshot`, `resize`, `reshard`, `merge`, or `compaction`. |
| `orleans.lattice.tree.lifecycle` | `Counter<long>` | `{event}` | Tree-lifecycle transition from `TreeDeletionGrain`. Tagged `kind=deleted`, `recovered`, or `purged`. Emitted **unconditionally** — regardless of the tree's `PublishEvents` setting. |

### Event publisher health

These counters are emitted only when event publication is enabled on at least
one tree. They let operators detect a misconfigured stream provider or a
failing downstream queue before it starts consuming silo resources.

| Name | Kind | Unit | Description |
|---|---|---|---|
| `orleans.lattice.events.published` | `Counter<long>` | `{event}` | `LatticeTreeEvent` instances successfully dispatched to the configured stream provider. Tagged `kind` = the `LatticeTreeEventKind` name (e.g. `Set`, `SnapshotCompleted`). |
| `orleans.lattice.events.dropped` | `Counter<long>` | `{event}` | Events dropped by the publisher. Tagged `reason=missing_provider` (no stream provider by the configured name is registered on this silo) or `publish_error` (the stream provider threw during dispatch). A non-zero rate on `missing_provider` means [`LatticeOptions.PublishEvents`](configuration.md#publishevents) is `true` but the corresponding `AddMemoryStreams` / `AddEventHubStreams` call is missing on the silo. |

### Configuration

Runtime overrides applied through `ILattice` that mutate per-tree behaviour
emit a lightweight change counter so operators can audit policy changes on
the same pipeline as the traffic they affect.

| Name | Kind | Unit | Description |
|---|---|---|---|
| `orleans.lattice.config.changed` | `Counter<long>` | `{change}` | A per-tree configuration change was applied. Tagged `config` = the configuration dimension (currently `publish_events` from `ILattice.SetPublishEventsEnabledAsync`). |

## OpenTelemetry registration

Register the meter by name — this is the same pattern used for any other
`System.Diagnostics.Metrics` source:

```csharp
// In your silo host's Program.cs or similar composition root.
using OpenTelemetry.Metrics;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddOpenTelemetry()
    .WithMetrics(metrics => metrics
        .AddMeter("orleans.lattice")
        .AddPrometheusExporter()); // or AddOtlpExporter, AddAzureMonitorMetricExporter, etc.
```

The meter is created at assembly load time, so adding it before the silo starts
is sufficient — every subsequently-activated grain publishes into the already-
subscribed pipeline.

## Bundled Grafana dashboards

The companion `Orleans.Lattice.Dashboards` package ships ready-to-import
dashboards keyed by `LatticeDashboardKind`:

| Kind | Title | Focus |
|---|---|---|
| `Overview` | Orleans.Lattice — Overview | Per-tree throughput, leaf-write percentiles, cache hit-rate, tombstone churn, splits, atomic-write outcomes (rate), coordinator completions, tree-lifecycle, event publish/drop, runtime config changes. The dashboard now also includes a horizontal row of three atomic-write panels: saga duration p50/p95/p99, batch-size p50/p95/p99, and a dedicated saga-failure-rate panel. |
| `CommitPath` | Orleans.Lattice — Commit Path | Dual-durability commit-path promotion, leaf shadow-write percentiles, activation-time replay duration / entries by recovery outcome. |
| `Replication` | Orleans.Lattice.Replication — Operator | Cross-cluster ship/apply/lag, WAL append vs trim, dead-letter churn, FIFO violations, fall-off-log events, per-peer cursor lag. Sources the `orleans.lattice.replication` meter. |
| `AtomicWrites` | Orleans.Lattice — Atomic Writes | Dedicated `SetManyAtomicAsync` saga deep-dive: outcome rate (stacked area), saga duration p50/p95/p99 and p95 by outcome, batch size p50/p95/p99 and p95 by outcome, per-tree committed throughput, range-window non-committed saga count, and a separate saga-failure-rate panel with 1% / 5% threshold lines. |

The `Overview` dashboard's atomic-write row is intentionally a teaser surface
suitable for at-a-glance operator scanning; the dedicated `AtomicWrites`
dashboard is the right home for incident triage and SLO drill-down.

Resolve the dashboard JSON at runtime:

```csharp
using Orleans.Lattice.Dashboards;

string atomicWritesJson = LatticeDashboards.GetGrafanaDashboardJson(
    LatticeDashboardKind.AtomicWrites);
```

The same dashboards are used by the in-repo benchmark cockpit
(`benchmark/grafana/dashboards/`) and by the persistent benchmark-history
cockpit (`benchmark/history/grafana/dashboards/`). The history cockpit
additionally ships a `Performance: Atomic Writes` view with headline KPI
tiles (ops/sec, p99 ns, mean ns, alloc/op B), trend timeseries, per-run
barcharts, and a stat + trend pair for the saga failure rate sourced from
the live cluster's Prometheus → VictoriaMetrics scrape.

## Performance

All instruments are zero-allocation on the hot path: counters use primitive
`Add`, and histograms use `Stopwatch.GetTimestamp()` deltas rather than
`Stopwatch` instances. When no listener is attached, the measurement callbacks
are elided by the runtime.

## Relationship to `DiagnoseAsync`

`ILattice.DiagnoseAsync` ([docs/lattice/diagnostics.md](diagnostics.md)) returns a
point-in-time snapshot intended for operator inspection and troubleshooting.
The metrics pipeline described here is the **continuous** telemetry feed for
dashboards and alerting. Both are sourced from the same underlying grain state
(shard hotness counters, leaf statistics) — they are complementary, not
redundant.