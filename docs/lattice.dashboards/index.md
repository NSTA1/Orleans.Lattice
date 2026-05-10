# Orleans.Lattice.Dashboards

`Orleans.Lattice.Dashboards` is a sibling package that ships pre-built Grafana dashboards and provisioning templates for the `orleans.lattice` and `orleans.lattice.replication` meters. Install it when you want operator dashboards bundled with the library version — the core library has no dependency on it.

> **Note:** v3.4.0 takes a direct project reference on `Orleans.Lattice.Replication` so the Replication dashboard always resolves. Splitting that into an optional reference (so local-only deployments can skip the replication meter entirely) is planned for a future minor version once both packages are published to NuGet.

## Install

```xml
<PackageReference Include="Orleans.Lattice.Dashboards" Version="3.4.0" />
```

## Wire up the meters

```csharp
builder.Services.AddOpenTelemetry()
    .WithMetrics(b => b
        .AddMeter("orleans.lattice")
        .AddMeter("orleans.lattice.replication")  // omit if no replication
        .AddPrometheusExporter());
```

## Get the dashboard JSON

```csharp
using Orleans.Lattice.Dashboards;

var overview     = LatticeDashboards.GetGrafanaDashboardJson(LatticeDashboardKind.Overview);
var commitPath   = LatticeDashboards.GetGrafanaDashboardJson(LatticeDashboardKind.CommitPath);
var replication  = LatticeDashboards.GetGrafanaDashboardJson(LatticeDashboardKind.Replication);
var atomicWrites = LatticeDashboards.GetGrafanaDashboardJson(LatticeDashboardKind.AtomicWrites);
```

Either import each JSON via Grafana's *Dashboards → New → Import* UI, or write the strings to a provisioning directory referenced by `Provisioning/dashboards.yaml`.

## What's covered

| Dashboard | Source meter | Focus |
|-----------|---------------|-------|
| `Overview` | `orleans.lattice` | Throughput, leaf-write percentiles, cache hit-rate, tombstone churn, splits, atomic-write outcomes, coordinator completions, tree-lifecycle, events, runtime config changes. The dashboard now also includes a horizontal row of three atomic-write panels (saga duration p50/p95/p99, batch size p50/p95/p99, and a dedicated saga-failure-rate panel with 1% / 5% threshold lines). |
| `CommitPath` | `orleans.lattice` | WAL-only commit path: per-step latency (`wal` / `apply` / `observer`), activation replay duration and entries by recovery outcome, storage-provider IOPS contribution, compaction. |
| `Replication` | `orleans.lattice.replication` | Ship / apply / lag percentiles, WAL append vs trim throughput, dead-letter queue churn, apply FIFO and causal violations, dependency-wait histogram, fell-off-log events, per-peer entries / bytes behind, last contact, consecutive errors. |
| `AtomicWrites` | `orleans.lattice` | Dedicated `SetManyAtomicAsync` saga deep-dive: outcome rate (stacked area), saga duration p50/p95/p99 and p95 by outcome, batch size p50/p95/p99 and p95 by outcome, per-tree committed throughput, range-window non-committed saga count, and a separate saga-failure-rate panel. The right home for incident triage and SLO drill-down on the atomic-write surface; the `Overview` row is the at-a-glance teaser. |

See [`metrics-to-panel-map.md`](metrics-to-panel-map.md) for the per-instrument coverage table.

## Drift guard

The companion test project `Orleans.Lattice.Dashboards.Tests` parses every embedded dashboard JSON, extracts every metric name referenced in panel `expr` strings, and asserts each name resolves to a live instrument on `LatticeMetrics.Meter` or `LatticeReplicationMetrics.Meter`. A rename in either meter fails CI before the dashboard ships stale.

## Provisioning templates

The package also ships two Grafana provisioning yaml templates under `Provisioning/` that are copied into the NuGet `contentFiles` directory at install time:

- `datasources.yaml` — Prometheus data source declaration.
- `dashboards.yaml` — file-system dashboard provider that picks up the JSONs your host writes to its dashboard directory.

Both are templates — review and customise the URLs and paths for your environment before applying.
