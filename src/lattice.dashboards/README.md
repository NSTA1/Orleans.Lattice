# Orleans.Lattice.Dashboards

Pre-built Grafana dashboards and provisioning templates for `Orleans.Lattice` and `Orleans.Lattice.Replication` telemetry. Sibling package - install when you want operator dashboards bundled with the library version.

## What's in the box

Three Grafana dashboards (Grafana schema v39, Prometheus data source) shipped as embedded resources and accessed via `LatticeDashboards.GetGrafanaDashboardJson(kind)`:

| Kind | Focus |
|------|-------|
| `Overview` | Per-tree throughput, leaf-write percentiles, cache hit-rate, tombstone churn, splits, atomic-write outcomes, coordinator completions, tree-lifecycle, events, runtime config changes. |
| `CommitPath` | Foreground commit path: `leaf.commit.duration{step=wal|apply|observer}` percentiles and activation `leaf.replay.duration / .entries{outcome=tail|rebuild}`. |
| `Replication` | Cross-cluster replication: ship / apply / lag, WAL append-vs-trim, dead-letter churn, apply violations, dependency wait, fell-off-log, per-peer cursor lag, cross-cluster atomic-batch staging. |

Plus Grafana provisioning templates under `Provisioning/`:

- `datasources.yaml` - Prometheus data source.
- `dashboards.yaml` - file-system dashboard provider.

## Drift guard

The companion test project `Orleans.Lattice.Dashboards.Tests` parses every embedded dashboard JSON, extracts every metric name referenced in panel `expr` strings, and asserts each name resolves to a live instrument on `LatticeMetrics.Meter` (or `LatticeReplicationMetrics.Meter`). A future rename in either meter fails CI before the dashboard ships stale.

## Why a separate package

- Dashboards have a different release cadence from the library and may be updated independently.
- Asymmetric coupling: the dashboards consume the meter; the meter does not depend on the dashboards.

> **Note:** v3.4.0 takes a direct project reference on `Orleans.Lattice.Replication` so the Replication dashboard always resolves. Splitting that into an optional reference (so local-only deployments can skip the replication meter entirely) is planned for a future minor version once both packages are published to NuGet.

See the [Orleans.Lattice repository](https://github.com/NSTA1/Orleans.Lattice) and the documentation under `docs/lattice.dashboards/`.
