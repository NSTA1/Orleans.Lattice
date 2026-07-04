# Orleans.Lattice.Dashboards

Pre-built Grafana dashboards for [Orleans.Lattice](https://github.com/NSTA1/Orleans.Lattice) telemetry. Bundles ready-to-import JSON dashboards covering every instrument on the `orleans.lattice` and `orleans.lattice.replication` meters.

## What it gives you

- **Focused dashboards** - `Overview`, `CommitPath`, `Replication`, `AtomicWrites`, `MaterialisedViews`, and `Authorization`, selectable via the `LatticeDashboardKind` enum.
- **Full instrument coverage** - every metric published on the `orleans.lattice` and `orleans.lattice.replication` meters maps to at least one panel, and the add-on `orleans.lattice.auth` / `orleans.lattice.membership` meters are covered by the `Authorization` dashboard; drift guards in the test suite assert the coverage stays complete in both directions.
- **Programmatic access** - `LatticeDashboards.GetGrafanaDashboardJson(kind)` returns the raw dashboard JSON for importing or writing to a Grafana provisioning directory.
- **OpenTelemetry-ready** - designed for a Prometheus-exported OpenTelemetry pipeline; no bespoke agent or exporter required.

For the underlying instruments and what each one measures, see the [metrics reference](https://github.com/NSTA1/Orleans.Lattice/blob/main/docs/lattice/metrics.md).

## Quick start

1. Wire the meters into an OpenTelemetry pipeline and export to Prometheus:

   ```csharp
   builder.Services.AddOpenTelemetry()
       .WithMetrics(b => b
           .AddMeter("orleans.lattice")
           .AddMeter("orleans.lattice.replication")
           .AddPrometheusExporter());
   ```

2. Import the dashboard JSON into Grafana, or write it to a provisioning directory:

   ```csharp
   var json = LatticeDashboards.GetGrafanaDashboardJson(LatticeDashboardKind.Overview);
   File.WriteAllText("/var/lib/grafana/dashboards/orleans-lattice/overview.json", json);
   ```

See the [`docs/lattice.dashboards/`](https://github.com/NSTA1/Orleans.Lattice/tree/main/docs/lattice.dashboards) directory for usage and the metric-to-panel map.
