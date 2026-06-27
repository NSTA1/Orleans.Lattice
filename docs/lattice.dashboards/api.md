# Dashboards Public API Reference

This document is the contract for the public `Orleans.Lattice.Dashboards` surface. The package exposes a single accessor type and a kind enum; everything else (the embedded JSON resources, the resource-name resolution) is an internal detail described by behaviour in [Architecture](architecture.md).

## Retrieving a dashboard

```csharp
using Orleans.Lattice.Dashboards;

string json = LatticeDashboards.GetGrafanaDashboardJson(LatticeDashboardKind.Overview);
```

| Type | Kind | Purpose | Key public members |
|---|---|---|---|
| `LatticeDashboards` | static class | Retrieves the bundled Grafana dashboard JSON. | `GetGrafanaDashboardJson(LatticeDashboardKind)`, `All` |
| `LatticeDashboardKind` | enum | Identifies one bundled dashboard, each resolving to a focused operator workflow. | `Overview`, `CommitPath`, `Replication`, `AtomicWrites`, `MaterialisedViews` |

### `LatticeDashboards`

| Member | Semantics |
|---|---|
| `string GetGrafanaDashboardJson(LatticeDashboardKind kind)` | Returns the Grafana dashboard JSON for `kind` as a UTF-8 string. The result is a complete Grafana dashboard model (panels, templating, time range) suitable for import. Throws `ArgumentOutOfRangeException` when `kind` is not a defined value. The call is synchronous and performs no I/O beyond reading an embedded resource. |
| `IReadOnlyList<LatticeDashboardKind> All` | Every dashboard kind shipped with the package, in declaration order. Use it to enumerate and export all dashboards in one pass. |

### `LatticeDashboardKind`

| Value | Source meter | Operator workflow |
|---|---|---|
| `Overview` | `orleans.lattice` | At-a-glance per-tree throughput, latency envelopes, cache hit-rate, tombstone churn, splits, atomic-write outcomes, lifecycle, and runtime configuration changes. Does not require the replication package. |
| `CommitPath` | `orleans.lattice` | WAL-first commit pipeline: per-step commit latency, storage-provider write latency, compaction latency, and activation-time replay duration and entries by recovery outcome. |
| `Replication` | `orleans.lattice.replication` | Cross-cluster operator view: ship / apply / lag durations, WAL append vs trim throughput, dead-letter churn, apply FIFO and causal violations, fall-off-log events, and per-peer cursor lag. Useful only when the replication package is registered on the silo. |
| `AtomicWrites` | `orleans.lattice` | `SetManyAtomicAsync` saga deep-dive: outcome rate, saga duration and batch-size percentiles, per-tree committed throughput, and a dedicated saga-failure-rate panel. |
| `MaterialisedViews` | `orleans.lattice` | Cluster-wide materialised-view health: apply-lag and drain-backlog-depth percentiles, filter / re-project and aggregation apply throughput, and warning panels for lag-budget evictions, re-key collisions, atomic-staging backstop fall-backs, and cross-tree joint-atomicity violations. Keyed by view name (and cluster); no per-silo filter, because a view's maintainer is a single grain activation that migrates between silos. Does not require the replication package. |

## Enumerating every dashboard

```csharp
using Orleans.Lattice.Dashboards;

foreach (LatticeDashboardKind kind in LatticeDashboards.All)
{
    string json = LatticeDashboards.GetGrafanaDashboardJson(kind);
    // write json to a provisioning directory, POST to the Grafana API, etc.
}
```

## See also

- [Configuration](configuration.md) - registering the meters the dashboards read and the provisioning templates.
- [Architecture](architecture.md) - how the JSON is embedded and drift-guarded.
- [Metric-to-panel map](metrics-to-panel-map.md) - the per-instrument coverage table.
