# Orleans.Lattice.Dashboards

`Orleans.Lattice.Dashboards` is an optional sibling package that ships pre-built Grafana dashboards and provisioning templates for the `orleans.lattice` and `orleans.lattice.replication` meters. Install it only when you want operator dashboards bundled with the library version — the core library has no dependency on it.

## Install

```xml
<PackageReference Include="Orleans.Lattice.Dashboards" Version="0.1.0" />
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

var overview    = LatticeDashboards.GetGrafanaDashboardJson(LatticeDashboardKind.Overview);
var commitPath  = LatticeDashboards.GetGrafanaDashboardJson(LatticeDashboardKind.CommitPath);
var replication = LatticeDashboards.GetGrafanaDashboardJson(LatticeDashboardKind.Replication);
```

Either import each JSON via Grafana's *Dashboards → New → Import* UI, or write the strings to a provisioning directory referenced by `Provisioning/dashboards.yaml`.

## What's covered

| Dashboard | Source meter | Focus |
|-----------|---------------|-------|
| `Overview` | `orleans.lattice` | Throughput, leaf-write percentiles, cache hit-rate, tombstone churn, splits, atomic-write outcomes, coordinator completions, tree-lifecycle, events, runtime config changes. |
| `CommitPath` | `orleans.lattice` | Dual-durability commit path: per-step latency (`wal` / `apply` / `shadow` / `observer`), shadow-write tail (must collapse to zero once `LeafShadowWrites=false`), activation replay duration and entries by recovery outcome, storage-provider IOPS contribution, compaction. |
| `Replication` | `orleans.lattice.replication` | Ship / apply / lag percentiles, WAL append vs trim throughput, dead-letter queue churn, apply FIFO and causal violations, dependency-wait histogram, fell-off-log events, per-peer entries / bytes behind, last contact, consecutive errors. |

See [`metrics-to-panel-map.md`](metrics-to-panel-map.md) for the per-instrument coverage table.

## Drift guard

The companion test project `Orleans.Lattice.Dashboards.Tests` parses every embedded dashboard JSON, extracts every metric name referenced in panel `expr` strings, and asserts each name resolves to a live instrument on `LatticeMetrics.Meter` or `LatticeReplicationMetrics.Meter`. A rename in either meter fails CI before the dashboard ships stale.

## Provisioning templates

The package also ships two Grafana provisioning yaml templates under `Provisioning/` that are copied into the NuGet `contentFiles` directory at install time:

- `datasources.yaml` — Prometheus data source declaration.
- `dashboards.yaml` — file-system dashboard provider that picks up the JSONs your host writes to its dashboard directory.

Both are templates — review and customise the URLs and paths for your environment before applying.
