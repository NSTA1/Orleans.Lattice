# Orleans.Lattice.Dashboards

Pre-built Grafana dashboards and provisioning templates for `Orleans.Lattice` and `Orleans.Lattice.Replication` telemetry, and for the add-on packages that chart their own surfaces (the gRPC replication transport, auth, membership, backup, scaling, tenancy and grain index). Sibling package - install when you want operator dashboards bundled with the library version.

## What's in the box

Grafana dashboards (Grafana schema v39, Prometheus data source) shipped as embedded resources and accessed via `LatticeDashboards.GetGrafanaDashboardJson(kind)`:

| Kind | Focus |
|------|-------|
| `Overview` | Per-tree throughput, leaf-write percentiles, cache hit-rate, tombstone churn, splits, compaction, shard consolidation and healing, atomic-write outcomes, coordinator completions and phase-tick failures, tree-lifecycle, events, runtime config changes, read-path latency envelopes, storage footprint, WAL compression and saturation, snapshot replay, write admission, the distributed-lock, atomic-action and grain-call contention rows, tree-registry fan-in, leaf-division health, and the deployed build. |
| `CommitPath` | Foreground commit path: `leaf.commit.duration{step=wal\|apply\|observer\|digest}` percentiles, commit concurrency and per-observer latency, `SetAsync` / `SetManyAsync` envelopes, the WAL append and writer pipeline, storage-provider write, commit, retry and timeout panels, compaction, scan-page coalescing and shard-root wedge guards, and leaf lifecycle diagnostics (materialiser pin path, snapshot capture and hydration, WAL replay permit gate, deferred-terminal ledger refusals, resident working set, span fail-open commits, WAL GC blocked consumers). |
| `Replication` | Cross-cluster replication: ship / apply / lag, ship ack latency and batch size, apply parallelism, WAL ship-vs-trim and producer append-vs-ship, dead-letter churn, apply violations, causal apply buffer and dependency wait, fell-off-log, per-peer cursor lag, batches in flight and wire version, anti-entropy, bootstrap, shipping optimisations, coordinated restore, and the core meter's WAL GC, compaction and recovery-discard panels. |
| `AtomicWrites` | `SetManyAtomicAsync` saga deep-dive: outcome rate, saga duration and batch-size percentiles, per-tree committed throughput, a dedicated saga-failure-rate panel, saga phase and per-key breakdowns, and cross-tree atomic-write outcome, failure-rate, duration and fan-out panels. |
| `MaterialisedViews` | Cluster-wide materialised-view health: apply-lag and drain-backlog-depth percentiles, filter / re-project and aggregation apply throughput, and warning panels for lag-budget evictions, re-key collisions, atomic-staging backstop fall-backs, cross-tree joint-atomicity violations, source back-pressure self-throttling, and aggregation reserved-key rejections. |
| `Authorization` | Identity and authorization: enforcement-gate decision throughput (by effect and operation), decision-latency percentiles, compiled-snapshot rebuild rate and epoch / age / subjects gauges, plus subject-resolution cache hit-ratio and hit / miss throughput and identity-directory search latency and hit / miss panels. Sources the `orleans.lattice.auth` and `orleans.lattice.membership` meters. |
| `Backup` | Backup and restore: capture / restore throughput and duration percentiles, per-backup size / artifact / entry distributions, processed throughput, retention reclaim and prune rates, incremental lag (entries and age), capture / restore failure rates by reason, capture retries, scheduler skipped / overrun / failure counters, cross-tree fence selection / drain / retry counters and drain wait, and inventory gauges (tracked count, chain depth, catalog bytes, oldest / newest age, per-scope last-run status and last-success age). Sources the `orleans.lattice.backup` meter. |
| `Scaling` | Autoscaling signal: smoothed vs raw scale value, the three normalised compute-pressure dimensions (activation / resource / WAL-dispatch), recommended replica count, and the storage-axis stats (WAL accounts over threshold, rebalance recommended). Sources the `orleans.lattice.scaling` meter. |
| `ReplicationGrpc` | Replication transport (gRPC) security: insecure (plaintext) channel construction as a cluster-wide total and as a per-second rate broken out by peer cluster id and transport (push / saga_control / snapshot), surfacing an accidental `AllowPlaintextEndpoints` downgrade. Sources the `orleans.lattice.replication.grpc` meter. |
| `Tenancy` | Per-tenant observability: the registered-tenant count and, dimensioned by tenant, usage (stored bytes, live keys, resident memory, owned trees), quota ceilings and burst headroom, and the metered overage series. Sources the `orleans.lattice.tenancy` meter. |
| `GrainIndex` | Grain-index operator view: backfill lifecycle state and percent complete, processed-versus-total crawl progress, live entry count, onboarding throughput by route, projection-latency percentiles, and index-write failure rates by route. Sources the shared `orleans.lattice` meter, which the grain-index package publishes onto. |

Plus Grafana provisioning templates under `Provisioning/`:

- `datasources.yaml` - Prometheus data source.
- `dashboards.yaml` - file-system dashboard provider.

## Drift guard

The companion test project `Orleans.Lattice.Dashboards.Tests` parses every embedded dashboard JSON, extracts every metric name referenced in panel `expr` and `query` strings (templating queries included), and asserts each name resolves to an instrument declared in the library source on whichever meter owns it, and that every instrument it can observe on `LatticeMetrics.Meter` and `LatticeReplicationMetrics.Meter` is referenced by at least one panel, apart from a short allow-list left uncharted on purpose. The add-on meters' coverage, and that of the grain-index package's own instruments on the core meter, is enforced from their owning packages' tests. A future rename fails CI before the dashboard ships stale.

## Why a separate package

- Dashboards have a different release cadence from the library and may be updated independently.
- Asymmetric coupling: the dashboards consume the meter; the meter does not depend on the dashboards.

See the [Orleans.Lattice repository](https://github.com/NSTA1/Orleans.Lattice) and the documentation under `docs/lattice.dashboards/`.
