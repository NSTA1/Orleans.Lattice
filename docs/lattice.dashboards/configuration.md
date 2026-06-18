# Dashboards Configuration

The dashboards package has no options type of its own - it is a delivery vehicle for JSON. "Configuring" the dashboards means three things: registering the meters whose instruments the panels query, choosing which dashboards to surface, and wiring the JSON into Grafana (by import or by provisioning template).

## 1. Register the meters

A dashboard only charts data if the matching meter is exported to the backend Grafana reads from. Register the meters with OpenTelemetry on the silo:

```csharp
builder.Services.AddOpenTelemetry()
    .WithMetrics(b => b
        .AddMeter("orleans.lattice")              // Overview, CommitPath, AtomicWrites, MaterialisedViews
        .AddMeter("orleans.lattice.replication")  // Replication (only if the replication package is registered)
        .AddPrometheusExporter());
```

| Meter | Emitted by | Dashboards that need it |
|---|---|---|
| `orleans.lattice` | the core library, always | `Overview`, `CommitPath`, `AtomicWrites`, `MaterialisedViews` |
| `orleans.lattice.replication` | the replication package, only when registered on the silo | `Replication` |

If you do not register the replication package, omit the replication meter and do not import the `Replication` dashboard - its panels would resolve to no data.

## 2. Choose which dashboards to surface

Retrieve only the kinds relevant to a deployment. A local-only silo typically imports `Overview`, `CommitPath`, and `AtomicWrites` (and `MaterialisedViews` if it registers any views); a multi-cluster deployment adds `Replication`.

```csharp
using Orleans.Lattice.Dashboards;

var kinds = new[]
{
    LatticeDashboardKind.Overview,
    LatticeDashboardKind.CommitPath,
    LatticeDashboardKind.AtomicWrites,
    LatticeDashboardKind.MaterialisedViews, // add when materialised views are registered
    // LatticeDashboardKind.Replication, // add when replication is registered
};

foreach (var kind in kinds)
{
    string json = LatticeDashboards.GetGrafanaDashboardJson(kind);
    File.WriteAllText($"./grafana/dashboards/{kind}.json", json);
}
```

## 3. Wire the JSON into Grafana

### Manual import

In Grafana, open *Dashboards -> New -> Import*, paste a JSON string returned by `GetGrafanaDashboardJson`, and select your Prometheus data source.

### File-system provisioning

Write the JSON files to a directory and point Grafana at it with a provisioning template. A matching pair of templates ships in the package's `Provisioning/` folder:

```yaml
# Provisioning/datasources.yaml
apiVersion: 1
datasources:
  - name: Prometheus
    type: prometheus
    access: proxy
    uid: prometheus
    url: http://prometheus:9090
    isDefault: true
    editable: false
```

```yaml
# Provisioning/dashboards.yaml
apiVersion: 1
providers:
  - name: orleans-lattice
    orgId: 1
    folder: Orleans.Lattice
    type: file
    disableDeletion: true
    editable: false
    updateIntervalSeconds: 30
    allowUiUpdates: false
    options:
      path: /var/lib/grafana/dashboards/orleans-lattice
      foldersFromFilesStructure: false
```

Mount the rendered JSON files at the `path` above and Grafana loads them at startup. The templates are a convenience baseline - adjust the data-source URL, folder, and provider name to match your environment.

## See also

- [API Reference](api.md) - the accessor and kinds you call to fetch the JSON.
- [Architecture](architecture.md) - how the JSON and provisioning templates are packaged.
- [Metric-to-panel map](metrics-to-panel-map.md) - which instruments each panel queries, so you can confirm your meter registration is complete.
