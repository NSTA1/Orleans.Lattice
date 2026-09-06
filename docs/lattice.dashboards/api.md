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
| `LatticeDashboardKind` | enum | Identifies one bundled dashboard, each resolving to a focused operator workflow. | `Overview`, `CommitPath`, `Replication`, `AtomicWrites`, `MaterialisedViews`, `Authorization`, `Backup`, `Scaling`, `ReplicationGrpc`, `Tenancy`, `GrainIndex` |

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
| `Authorization` | `orleans.lattice.auth`, `orleans.lattice.membership` | Identity and authorization operator view: enforcement-gate decision throughput (by effect and operation), decision-latency percentiles, compiled-snapshot rebuild rate and the snapshot epoch / age gauges, plus subject-resolution cache hit-ratio and hit / miss throughput. Useful only when the authentication / authorization packages are registered on the silo. |
| `Backup` | `orleans.lattice.backup` | Backup / restore operator view: capture / restore throughput and duration percentiles, per-backup size / artifact / entry distributions, cumulative processed throughput, retention reclaim and prune rates, incremental lag behind the base cut, capture / restore failure rates by reason, scheduler skipped-run and overrun counters, cross-tree-consistent fence selection / drain counters, and the inventory gauges. Useful only when the backup package is registered on the silo. |
| `Scaling` | `orleans.lattice.scaling` | Autoscaling-signal operator view: the smoothed scale-in-gated and raw instantaneous scale-value gauges, the three normalised compute-pressure dimensions (activation / host-resource / WAL-dispatch), the recommended silo replica count, and the storage-axis stats. Useful only when the scaling package is registered on the silo. |
| `ReplicationGrpc` | `orleans.lattice.replication.grpc` | Replication gRPC transport-security view: the insecure (plaintext) channel construction counter as a cumulative total and a per-second rate, broken out by peer cluster id and transport (push / saga_control / snapshot), so an accidental plaintext downgrade under `AllowPlaintextEndpoints` is visible. Useful only when the gRPC replication transport is registered on the silo. |
| `Tenancy` | `orleans.lattice.tenancy` | Per-tenant observability view: the registered-tenant count (cluster aggregate) and, dimensioned by tenant, the usage series (stored bytes, live keys, resident memory, owned trees), the quota ceilings and burst-headroom percentage, and the durable metered overage series (bytes / keys / memory / trees). A templated `tenant` variable scopes every panel to one tenant or to all tenants. Useful only when the tenancy package is registered on the silo. |
| `GrainIndex` | `orleans.lattice` | Grain-index operator view: each index's backfill lifecycle state and percent complete, its processed-versus-total crawl progress, its live entry count, onboarding throughput split by route (activation versus backfill), projection-latency percentiles, and index-write failure rates by route. A templated `index` variable scopes every panel to one index or to all of them. Sources the shared core meter - the grain-index package publishes no meter of its own - but the series appear only once the grain-index package is registered on the silo. |

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
