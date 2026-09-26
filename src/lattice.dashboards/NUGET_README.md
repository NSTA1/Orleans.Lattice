# Orleans.Lattice.Dashboards

Pre-built Grafana dashboards for [Orleans.Lattice](https://github.com/NSTA1/Orleans.Lattice) telemetry. Bundles ready-to-import JSON dashboards covering almost every instrument on the `orleans.lattice`, `orleans.lattice.replication`, `orleans.lattice.replication.grpc`, `orleans.lattice.auth`, `orleans.lattice.membership`, `orleans.lattice.backup`, `orleans.lattice.scaling`, and `orleans.lattice.tenancy` meters.

## What it gives you

- **Focused dashboards** - `Overview`, `CommitPath`, `Replication`, `AtomicWrites`, `MaterialisedViews`, `Authorization`, `Backup`, `Scaling`, `ReplicationGrpc`, `Tenancy`, and `GrainIndex`, selectable via the `LatticeDashboardKind` enum and enumerable through `LatticeDashboards.All`.
- **Near-complete instrument coverage** - every metric published on the `orleans.lattice` and `orleans.lattice.replication` meters maps to at least one panel, apart from a few listed as not charted in the metric-to-panel map (most left off on purpose, one family not yet paneled), and each add-on meter (`orleans.lattice.auth` / `orleans.lattice.membership`, `orleans.lattice.backup`, `orleans.lattice.scaling`, `orleans.lattice.replication.grpc`, `orleans.lattice.tenancy`) is covered by its own dashboard; drift guards in the test suite assert the coverage in both directions.
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
           .AddMeter("Microsoft.Orleans")  // Orleans runtime: activations, activation latency
           .AddMeter("System.Runtime")     // .NET runtime: GC heap, working set, thread pool
           .AddPrometheusExporter());
   ```

   `AddMeter` matches a meter name exactly and does not cascade, so each add-on dashboard also needs its own meter registered by name: `orleans.lattice.auth` and `orleans.lattice.membership` for `Authorization`, `orleans.lattice.backup` for `Backup`, `orleans.lattice.scaling` for `Scaling`, `orleans.lattice.replication.grpc` for `ReplicationGrpc`, and `orleans.lattice.tenancy` for `Tenancy`. See the [configuration guide](https://github.com/NSTA1/Orleans.Lattice/blob/main/docs/lattice.dashboards/configuration.md).

2. Import the dashboard JSON into Grafana, or write it to a provisioning directory:

   ```csharp
   var json = LatticeDashboards.GetGrafanaDashboardJson(LatticeDashboardKind.Overview);
   File.WriteAllText("/var/lib/grafana/dashboards/orleans-lattice/overview.json", json);
   ```

See the [`docs/lattice.dashboards/`](https://github.com/NSTA1/Orleans.Lattice/tree/main/docs/lattice.dashboards) directory for usage and the metric-to-panel map.
