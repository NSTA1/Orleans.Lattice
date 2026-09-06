# Orleans.Lattice.Dashboards

`Orleans.Lattice.Dashboards` is a sibling package that ships pre-built Grafana dashboards and provisioning templates for the `orleans.lattice`, `orleans.lattice.replication`, `orleans.lattice.replication.grpc`, `orleans.lattice.auth`, `orleans.lattice.membership`, `orleans.lattice.backup`, `orleans.lattice.scaling`, and `orleans.lattice.tenancy` meters. Install it when you want operator dashboards bundled with the library version - the core library has no dependency on it.

## What is it?

The package is a thin, dependency-light delivery vehicle for operator dashboards. Each dashboard is an embedded Grafana JSON resource, retrieved by a typed kind through a single public accessor, so the dashboards travel with the exact library version that emits the metrics they chart.

- **Bundled, version-pinned dashboards.** Operator dashboards for the overview, commit path, replication, replication gRPC transport security, atomic writes, materialised views, identity/authorization, backup/restore, autoscaling-signal, per-tenant observability, and grain-index surfaces ship as embedded Grafana JSON, each retrieved by a typed kind. They move in lockstep with the library version, so a dashboard never references an instrument the installed library does not emit.
- **No replication dependency.** The package takes a runtime dependency only on `Orleans.Lattice` (the core library). The replication dashboard's queries reference instruments on the `orleans.lattice.replication` meter, but the package does not link against `Orleans.Lattice.Replication`; that meter is only emitted when the replication package is registered on the silo separately. Local-only deployments install the dashboards without pulling in replication and simply omit the replication dashboard.
- **Drift-guarded coverage.** Every metric name a dashboard references resolves to a live instrument, and every instrument the guard can discover is referenced by at least one panel - both directions are enforced by a CI test so a rename or a new unpaneled instrument fails the build before it ships stale. The forward direction reaches only instruments declared on the two static metric classes; see [Architecture](architecture.md#the-bidirectional-drift-guard) for the one gap this leaves and the instruments currently outside it.

## Core Properties

- **Self-contained.** Dashboards are embedded resources; retrieving one is a synchronous in-process call with no I/O and no external service.
- **Import-ready.** Each accessor returns a complete Grafana dashboard model (panels, templating, time range) suitable for direct import or file-system provisioning.
- **Operator-oriented.** The kinds map to focused operator workflows - for example at-a-glance overview, commit-path latency triage, cross-cluster replication lag, atomic-write saga deep-dive, materialised-view freshness, identity/authorization enforcement, per-tenant usage against quota, and grain-index backfill progress.

## Features

| Dashboard | Source meter | Focus |
|---|---|---|
| `Overview` | `orleans.lattice` | Throughput, leaf-write percentiles, cache hit-rate, tombstone churn, splits, atomic-write outcomes, coordinator completions, tree-lifecycle, events, runtime config changes, and top-of-stack read-path latency envelopes. Includes a row of atomic-write panels (saga duration p50/p95/p99, batch size p50/p95/p99, and a saga-failure-rate panel with 1% / 5% threshold lines). |
| `CommitPath` | `orleans.lattice` | WAL-only commit path: per-step latency (`wal` / `apply` / `observer`), activation replay duration and entries by recovery outcome, storage-provider IOPS contribution, compaction. |
| `Replication` | `orleans.lattice.replication` | Ship / apply / lag percentiles, WAL append vs trim throughput, dead-letter queue churn, apply FIFO and causal violations, dependency-wait histogram, fell-off-log events, per-peer entries / bytes behind, last contact, consecutive errors. |
| `AtomicWrites` | `orleans.lattice` | Dedicated `SetManyAtomicAsync` saga deep-dive: outcome rate, saga duration p50/p95/p99 and p95 by outcome, batch size p50/p95/p99 and p95 by outcome, per-tree committed throughput, range-window non-committed saga count, and a separate saga-failure-rate panel. The right home for incident triage and SLO drill-down; the `Overview` row is the at-a-glance teaser. |
| `MaterialisedViews` | `orleans.lattice` | Cluster-wide materialised-view health: apply-lag and drain-backlog-depth percentiles, filter / re-project and aggregation apply throughput, and warning panels for lag-budget evictions, re-key collisions, atomic-staging backstop fall-backs, and cross-tree joint-atomicity violations. Keyed by view name (and cluster); deliberately offers no per-silo filter, because a view's maintainer is a single grain activation that migrates between silos, so the dashboard aggregates across the whole cluster. Needs only the core meter, not the replication package. |
| `Authorization` | `orleans.lattice.auth`, `orleans.lattice.membership` | Identity and authorization operator view: enforcement-gate decision throughput (by `effect` and `operation`), decision-latency percentiles, compiled-snapshot rebuild rate and the snapshot `epoch` / `age` gauges, alongside the subject-resolution cache hit-ratio and hit / miss throughput. Useful only when the authentication / authorization packages are registered on the silo. |
| `Backup` | `orleans.lattice.backup` | Backup / restore operator view: capture / restore throughput and duration percentiles, per-backup size / artifact / entry distributions, cumulative processed throughput, retention reclaim and prune rates, incremental lag (entries and age behind the base cut), capture / restore failure rates by reason, scheduler skipped-run and overrun counters, the cross-tree-consistent fence selection / drain counters, and the inventory gauges (tracked count, max chain depth, catalog bytes, oldest / newest age, and per-scope last-run status and last-success age). Useful only when the backup package is registered on the silo. |
| `Scaling` | `orleans.lattice.scaling` | Autoscaling-signal operator view: the two scale-value gauges (the smoothed, scale-in-gated value an autoscaler acts on and the raw, un-smoothed instantaneous demand), the three normalised compute-pressure dimensions (activation / host-resource / WAL-dispatch), the recommended silo replica count, and the storage-axis stats (WAL catalogue keys over the advisory threshold and whether a WAL rebalance is recommended). Useful only when the scaling package is registered on the silo. |
| `ReplicationGrpc` | `orleans.lattice.replication.grpc` | Replication gRPC transport-security view: the insecure (plaintext) channel construction counter as a cumulative total and a per-second rate, broken out by peer cluster id and transport (`push` / `saga_control` / `snapshot`), so an accidental production plaintext downgrade under `AllowPlaintextEndpoints` is visible rather than silent. Useful only when the gRPC replication transport is registered on the silo. |
| `Tenancy` | `orleans.lattice.tenancy` | Per-tenant observability operator view: the registered-tenant count (cluster aggregate) and, dimensioned by tenant, the usage series (stored bytes, live keys, resident memory, owned trees), the quota ceilings and burst-headroom percentage, and the durable metered overage series (bytes / keys / memory / trees), so a burst or a sustained overage is attributable to a tenant. A templated `tenant` variable scopes every panel to one tenant (a tenant's own view) or to all tenants (the platform-operator view). Useful only when the tenancy package is registered on the silo. |
| `GrainIndex` | `orleans.lattice` | Grain-index operator view: each index's backfill lifecycle state and percent complete, its processed-versus-total crawl progress, its live entry count, onboarding throughput split by route (activation versus backfill), projection-latency percentiles, and index-write failure rates by route. A templated `index` variable scopes every panel to one index or to all of them. Sources the shared core meter - the grain-index package publishes no meter of its own - so the series appear under an existing lattice subscription, but only once the grain-index package is registered on the silo. |

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
var authz        = LatticeDashboards.GetGrafanaDashboardJson(LatticeDashboardKind.Authorization);
var backup       = LatticeDashboards.GetGrafanaDashboardJson(LatticeDashboardKind.Backup);
var scaling      = LatticeDashboards.GetGrafanaDashboardJson(LatticeDashboardKind.Scaling);
var replGrpc     = LatticeDashboards.GetGrafanaDashboardJson(LatticeDashboardKind.ReplicationGrpc);
var tenancy      = LatticeDashboards.GetGrafanaDashboardJson(LatticeDashboardKind.Tenancy);
var grainIndex   = LatticeDashboards.GetGrafanaDashboardJson(LatticeDashboardKind.GrainIndex);
```

`LatticeDashboards.All` enumerates every kind in declaration order, so a provisioning loop stays complete as new dashboards are added.

Either import each JSON via Grafana's *Dashboards -> New -> Import* UI, or write the strings to a provisioning directory referenced by `Provisioning/dashboards.yaml`.

## Reference

For day-to-day use:

- [API Reference](api.md) - the public `LatticeDashboards` accessor and `LatticeDashboardKind` kinds.
- [Configuration](configuration.md) - meter registration, dashboard selection, and the Grafana provisioning templates.
- [Metric-to-panel map](metrics-to-panel-map.md) - the per-instrument coverage table across every meter the dashboards chart.

For internals (the "how"):

- [Architecture](architecture.md) - embedded JSON resources, the bidirectional drift guard, and the provisioning-template layout.