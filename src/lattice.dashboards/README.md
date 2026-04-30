# Orleans.Lattice.Dashboards

Pre-built Grafana dashboards and provisioning templates for `Orleans.Lattice` and `Orleans.Lattice.Replication` telemetry. Optional sibling package — install only when you want operator dashboards bundled with the library version.

## What's in the box

Three Grafana dashboards (Grafana schema v39, Prometheus data source) shipped as embedded resources and accessed via `LatticeDashboards.GetGrafanaDashboardJson(kind)`:

| Kind | Focus |
|------|-------|
| `Overview` | Per-tree throughput, leaf-write percentiles, cache hit-rate, tombstone churn, splits, atomic-write outcomes, coordinator completions, tree-lifecycle, events, runtime config changes. |
| `CommitPath` | Dual-durability commit path: `leaf.commit.duration{step=...}`, `leaf.shadow_write.duration` percentiles, activation `leaf.replay.duration / .entries{outcome=...}`. |
| `Replication` | Cross-cluster replication: ship / apply / lag, WAL append-vs-trim, dead-letter churn, apply violations, dependency wait, fell-off-log, per-peer cursor lag. |

Plus Grafana provisioning templates under `Provisioning/`:

- `datasources.yaml` — Prometheus data source.
- `dashboards.yaml` — file-system dashboard provider.

## Drift guard

The companion test project `Orleans.Lattice.Dashboards.Tests` parses every embedded dashboard JSON, extracts every metric name referenced in panel `expr` strings, and asserts each name resolves to a live instrument on `LatticeMetrics.Meter` (or `LatticeReplicationMetrics.Meter`). A future rename in either meter fails CI before the dashboard ships stale.

## Why a separate package

- The replication meter is optional for local-only deployments; the Replication dashboard is gated on importing it from the package, but the Overview / Commit Path dashboards work with `Orleans.Lattice` alone.
- Dashboards have a different release cadence from the library and may be updated independently.
- Asymmetric coupling: the dashboards consume the meter; the meter does not depend on the dashboards.

See the [Orleans.Lattice repository](https://github.com/NSTA1/Orleans.Lattice) and the documentation under `docs/lattice.dashboards/`.
