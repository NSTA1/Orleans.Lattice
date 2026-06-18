# Orleans.Lattice.Dashboards

`Orleans.Lattice.Dashboards` is a sibling package that ships pre-built Grafana dashboards and provisioning templates for the `orleans.lattice` and `orleans.lattice.replication` meters. Install it when you want operator dashboards bundled with the library version - the core library has no dependency on it.

## What is it?

The package is a thin, dependency-light delivery vehicle for operator dashboards. Each dashboard is an embedded Grafana JSON resource, retrieved by a typed kind through a single public accessor, so the dashboards travel with the exact library version that emits the metrics they chart.

- **Bundled, version-pinned dashboards.** Overview, commit-path, replication, atomic-write, and materialised-views dashboards ship as embedded JSON keyed by metric name. They move in lockstep with the library version, so a dashboard never references an instrument the installed library does not emit.
- **No replication dependency.** The package takes a runtime dependency only on `Orleans.Lattice` (the core library). The replication dashboard's queries reference instruments on the `orleans.lattice.replication` meter, but the package does not link against `Orleans.Lattice.Replication`; that meter is only emitted when the replication package is registered on the silo separately. Local-only deployments install the dashboards without pulling in replication and simply omit the replication dashboard.
- **Drift-guarded coverage.** Every metric name a dashboard references resolves to a live instrument, and every live instrument is referenced by at least one panel - both directions are enforced by a CI test so a rename or a new unpaneled instrument fails the build before it ships stale.

## Core Properties

- **Self-contained.** Dashboards are embedded resources; retrieving one is a synchronous in-process call with no I/O and no external service.
- **Import-ready.** Each accessor returns a complete Grafana dashboard model (panels, templating, time range) suitable for direct import or file-system provisioning.
- **Operator-oriented.** The kinds map to focused operator workflows - at-a-glance overview, commit-path latency triage, cross-cluster replication lag, atomic-write saga deep-dive, and materialised-view freshness.

## Features

| Dashboard | Source meter | Focus |
|---|---|---|
| `Overview` | `orleans.lattice` | Throughput, leaf-write percentiles, cache hit-rate, tombstone churn, splits, atomic-write outcomes, coordinator completions, tree-lifecycle, events, runtime config changes, and top-of-stack read-path latency envelopes. Includes a row of atomic-write panels (saga duration p50/p95/p99, batch size p50/p95/p99, and a saga-failure-rate panel with 1% / 5% threshold lines). |
| `CommitPath` | `orleans.lattice` | WAL-only commit path: per-step latency (`wal` / `apply` / `observer`), activation replay duration and entries by recovery outcome, storage-provider IOPS contribution, compaction. |
| `Replication` | `orleans.lattice.replication` | Ship / apply / lag percentiles, WAL append vs trim throughput, dead-letter queue churn, apply FIFO and causal violations, dependency-wait histogram, fell-off-log events, per-peer entries / bytes behind, last contact, consecutive errors. |
| `AtomicWrites` | `orleans.lattice` | Dedicated `SetManyAtomicAsync` saga deep-dive: outcome rate, saga duration p50/p95/p99 and p95 by outcome, batch size p50/p95/p99 and p95 by outcome, per-tree committed throughput, range-window non-committed saga count, and a separate saga-failure-rate panel. The right home for incident triage and SLO drill-down; the `Overview` row is the at-a-glance teaser. |
| `MaterialisedViews` | `orleans.lattice` | Cluster-wide materialised-view health: apply-lag and drain-backlog-depth percentiles, filter / re-project and aggregation apply throughput, and warning panels for lag-budget evictions, re-key collisions, atomic-staging backstop fall-backs, and cross-tree joint-atomicity violations. Keyed by view name (and cluster); deliberately offers no per-silo filter, because a view's maintainer is a single grain activation that migrates between silos, so the dashboard aggregates across the whole cluster. Needs only the core meter, not the replication package. |

## Quick Start

Install the package:

```xml
<PackageReference Include="Orleans.Lattice.Dashboards" Version="<X.Y.Z>" />
```

Wire up the meters the dashboards read:

```csharp
builder.Services.AddOpenTelemetry()
    .WithMetrics(b => b
        .AddMeter("orleans.lattice")
        .AddMeter("orleans.lattice.replication")  // omit if no replication
        .AddPrometheusExporter());
```

Retrieve the dashboard JSON by kind:

```csharp
using Orleans.Lattice.Dashboards;

var overview     = LatticeDashboards.GetGrafanaDashboardJson(LatticeDashboardKind.Overview);
var commitPath   = LatticeDashboards.GetGrafanaDashboardJson(LatticeDashboardKind.CommitPath);
var replication  = LatticeDashboards.GetGrafanaDashboardJson(LatticeDashboardKind.Replication);
var atomicWrites = LatticeDashboards.GetGrafanaDashboardJson(LatticeDashboardKind.AtomicWrites);
var views        = LatticeDashboards.GetGrafanaDashboardJson(LatticeDashboardKind.MaterialisedViews);
```

Either import each JSON via Grafana's *Dashboards -> New -> Import* UI, or write the strings to a provisioning directory referenced by `Provisioning/dashboards.yaml`.

## Reference

For day-to-day use:

- [API Reference](api.md) - the public `LatticeDashboards` accessor and `LatticeDashboardKind` kinds.
- [Configuration](configuration.md) - meter registration, dashboard selection, and the Grafana provisioning templates.
- [Metric-to-panel map](metrics-to-panel-map.md) - the per-instrument coverage table across both meters.

For internals (the "how"):

- [Architecture](architecture.md) - embedded JSON resources, the bidirectional drift guard, and the provisioning-template layout.

## Feature tracking

The dashboards are tracked alongside the core and replication packages on [GitHub Issues](https://github.com/NSTA1/Orleans.Lattice/issues). The [core feature index](../lattice/features.md) and [replication feature index](../lattice.replication/features.md) link the metrics-observability items to their issues.