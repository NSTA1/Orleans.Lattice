# Orleans.Lattice.Dashboards

Pre-built Grafana dashboards for [Orleans.Lattice](https://github.com/NSTA1/Orleans.Lattice) telemetry. Bundles five ready-to-import JSON dashboards covering every instrument on the `orleans.lattice` and `orleans.lattice.replication` meters.

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
