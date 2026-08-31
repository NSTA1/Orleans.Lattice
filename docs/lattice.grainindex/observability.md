# Observability

Every grain index publishes metrics on the shared `orleans.lattice` meter and
exposes an administrative surface, `IGrainIndexAdmin`, for status and control.

## Metrics

The package adds no meter of its own: its instruments sit on `orleans.lattice`
alongside the core's, so a host that already collects Lattice metrics picks these
up with no configuration change.

### Tags

| Tag | Values | Applies to |
|---|---|---|
| `index` | the logical index name | every instrument |
| `path` | `activation`, `backfill`, `outbox` | `grains_enrolled`, `write_failures` |

The `path` tag names the route that did the work: `activation` is the activation
and mutation path that physically writes a grain's entries, `backfill` is the
background crawl, and `outbox` is a deferred or retried index write.

### Instruments

| Instrument | Type | Unit | What it reports |
|---|---|---|---|
| `orleans.lattice.grainindex.grains_enrolled` | `Counter<long>` | `{grain}` | Grains onboarded into an index, by the route that onboarded them. |
| `orleans.lattice.grainindex.entries` | `UpDownCounter<long>` | `{entry}` | Net change in the number of entries an index holds, so the running sum is the current entry count. |
| `orleans.lattice.grainindex.write_failures` | `Counter<long>` | `{failure}` | Failures to publish a grain's index entries, by the route that failed. |
| `orleans.lattice.grainindex.projection.duration` | `Histogram<double>` | `ms` | Time to project one grain's state into index entries and diff it against the stored projection. |
| `orleans.lattice.grainindex.backfill.processed` | `ObservableGauge<long>` | `{grain}` | Keys a background backfill has taken from its key source. |
| `orleans.lattice.grainindex.backfill.total` | `ObservableGauge<long>` | `{grain}` | Best-effort size of the population a backfill has to cover. |
| `orleans.lattice.grainindex.backfill.percent_complete` | `ObservableGauge<double>` | `%` | How far through its population a backfill has reached. |
| `orleans.lattice.grainindex.backfill.state` | `ObservableGauge<int>` | `{state}` | Lifecycle state of a backfill, as the numeric `GrainIndexBackfillState`. |

The four observable gauges read a frozen snapshot published by the backfill
grain, so a scrape never recomputes progress and only the silo hosting a crawl's
activation - the one that knows where the crawl has reached - publishes its
series.

`backfill.total` and `backfill.percent_complete` publish a series only for the
indexes whose key source implements `TryGetApproximateCountAsync`. Without that
denominator they stay silent rather than reporting a misleading figure. See
[The optional count](backfill.md#the-optional-count).

### Backfill state values

`backfill.state` reports the numeric `GrainIndexBackfillState`:

| Value | State |
|---|---|
| `0` | `NotStarted` |
| `1` | `Running` |
| `2` | `Paused` |
| `3` | `Completed` |
| `4` | `Failed` |

### What to alert on

- **`write_failures` rising** on the `activation` path means state writes are
  succeeding but their index projection is not. The entries are not lost - they
  are queued in the [outbox](architecture.md#the-outbox) - but the index is
  stale for those grains until the drain catches up. A sustained rise on the
  `outbox` path means the drain itself is failing.
- **`backfill.state` at `4` (`Failed`)** means a crawl stopped and will not
  resume without intervention.
- **`backfill.percent_complete` flat** while the state is `Running` means the
  crawl is making no progress, typically a key source that is throwing or
  yielding nothing.
- **`projection.duration` p99 climbing** means the projection path is becoming a
  latency contributor to the grain's own write path under the default
  synchronous projection mode.

### Dashboards

A Grafana dashboard covering these instruments ships in
[`src/lattice.dashboards/Grafana`](../lattice.dashboards/README.md), and each
instrument's panel is listed in the
[metrics-to-panel map](../lattice.dashboards/metrics-to-panel-map.md).

Prometheus mangles the OTel names: dots become underscores and counters gain a
`_total` suffix, so `orleans.lattice.grainindex.grains_enrolled` is scraped as
`orleans_lattice_grainindex_grains_enrolled_total`.

## `IGrainIndexAdmin`

Resolve it from the silo's service provider to inspect and control the declared
indexes.

| Member | What it does |
|---|---|
| `IReadOnlyList<string> DeclaredIndexes` | The names of every index declared in this silo. |
| `Task<GrainIndexStatus> GetStatusAsync(string indexName, CancellationToken)` | One index's status: its definition, entry count, drift state, and backfill progress. |
| `Task<IReadOnlyList<GrainIndexStatus>> ListStatusAsync(CancellationToken)` | The same for every declared index. |
| `Task<GrainIndexBackfillStatus> PauseBackfillAsync(string indexName, CancellationToken)` | Stops scheduling passes, keeping the checkpoint. |
| `Task<GrainIndexBackfillStatus> ResumeBackfillAsync(string indexName, CancellationToken)` | Resumes from the checkpoint. |
| `Task<GrainIndexBackfillStatus> RebuildAsync(string indexName, CancellationToken)` | Restarts the crawl from the beginning of the key range. |
| `Task<GrainIndexBackfillBatchResult> RunBackfillPassAsync(string indexName, CancellationToken)` | Runs exactly one pass now, whatever the schedule says. |

```csharp verify
using Orleans.Lattice.GrainIndex;

public static class IndexHealth
{
    public static async Task ReportAsync(
        IGrainIndexAdmin admin,
        CancellationToken cancellationToken)
    {
        foreach (var status in await admin.ListStatusAsync(cancellationToken))
        {
            Console.WriteLine($"{status.IndexName}: {status.EntryCount} entries");
        }
    }
}
```

An unknown index name throws `GrainIndexNotDeclaredException`.

`RunBackfillPassAsync` is the primitive that makes a backfill testable: combined
with `BackfillEnabled = false` it lets a test drive the crawl one pass at a time
instead of waiting on a schedule.

## See also

- [Backfill](backfill.md) - the crawl these gauges report on.
- [Configuration](configuration.md) - the options the admin surface reflects.
- [Architecture](architecture.md#the-outbox) - what `write_failures` implies.
