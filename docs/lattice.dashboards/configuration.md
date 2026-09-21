# Dashboards Configuration

The dashboards package has no options type of its own - it is a delivery vehicle for JSON. "Configuring" the dashboards means three things: registering the meters whose instruments the panels query, choosing which dashboards to surface, and wiring the JSON into Grafana (by import or by provisioning template).

## 1. Register the meters

A dashboard only charts data if the matching meter is exported to the backend Grafana reads from. Register the meters with OpenTelemetry on the silo:

```csharp
builder.Services.AddOpenTelemetry()
    .WithMetrics(b => b
        .AddMeter("orleans.lattice")              // Overview, CommitPath, AtomicWrites, MaterialisedViews
        .AddMeter("orleans.lattice.replication")  // Replication (only if the replication package is registered)
        .AddMeter("orleans.lattice.replication.grpc") // ReplicationGrpc (only if the gRPC replication transport is registered)
        .AddMeter("orleans.lattice.auth")         // Authorization (only if the auth package is registered)
        .AddMeter("orleans.lattice.membership")   // Authorization (only if the membership package is registered)
        .AddMeter("orleans.lattice.backup")       // Backup (only if the backup package is registered)
        .AddMeter("orleans.lattice.scaling")      // Scaling (only if the scaling package is registered)
        .AddMeter("Microsoft.Orleans")            // Orleans runtime: activations, activation latency, directory, messaging
        .AddMeter("System.Runtime")               // .NET runtime: GC heap, allocation, pause time, working set, thread pool
        .AddPrometheusExporter());
```

`AddMeter` matches a meter name **exactly** and does not cascade to child
namespaces, so each name above has to be registered in its own right. A silo
that registers only `orleans.lattice` exports none of its siblings, and one that
registers only the `orleans.lattice` family exports no runtime telemetry at all.

| Meter | Emitted by | Dashboards that need it |
|---|---|---|
| `orleans.lattice` | the core library, always | `Overview`, `CommitPath`, `AtomicWrites`, `MaterialisedViews` |
| `orleans.lattice.replication` | the replication package, only when registered on the silo | `Replication` |
| `orleans.lattice.replication.grpc` | the gRPC replication transport, only when registered on the silo | `ReplicationGrpc` |
| `orleans.lattice.auth`, `orleans.lattice.membership` | the auth / membership packages, only when registered on the silo | `Authorization` |
| `orleans.lattice.backup` | the backup package, only when registered on the silo | `Backup` |
| `orleans.lattice.scaling` | the scaling package, only when registered on the silo | `Scaling` |
| `Microsoft.Orleans` | the Orleans runtime, always | none of the bundled dashboards; registered for operational diagnosis |
| `System.Runtime` | the .NET runtime, always | none of the bundled dashboards; registered for operational diagnosis |

If you do not register the replication package, omit the replication meter and do not import the `Replication` dashboard - its panels would resolve to no data.

### Why register the two runtime meters

Neither runtime meter backs a bundled dashboard panel, so it is tempting to leave
both out. Do not. They are what turns an unexplained restart into a diagnosable
one, and their absence is indistinguishable on the endpoint from an instrument
that was never declared:

| Meter | Exact name | What it carries |
|---|---|---|
| Orleans runtime | `Microsoft.Orleans` | grain activation counts and activation **latency** (`orleans-catalog-activation-latency`), activation collection, grain directory, scheduler, messaging |
| .NET runtime | `System.Runtime` | GC heap size and total allocation, GC pause time, process working set, thread-pool depth and queue length, lock contention |

Without `System.Runtime` the endpoint carries **no heap or process-memory series
whatsoever**, so a silo in an out-of-memory restart loop has to be diagnosed by
inferring heap composition from activation counts. Without `Microsoft.Orleans`
there is no direct measure of activation cost, only a grain-call-duration proxy
that cannot separate activation from call.

Two details are worth getting right rather than assuming:

- **`System.Runtime` is built into .NET from .NET 9** and needs no package
  reference. The older `OpenTelemetry.Instrumentation.Runtime` package and its
  `AddRuntimeInstrumentation()` call publish an equivalent, differently-named
  `process.runtime.dotnet.*` series set; on a modern target framework prefer the
  built-in meter and skip the dependency.
- **`Microsoft.Orleans.Runtime`, `Microsoft.Orleans.Application` and their
  siblings are `ActivitySource` names for tracing, not meter names.** Passing one
  of those to `AddMeter` matches nothing, throws nothing, and leaves the endpoint
  looking exactly as it did before. The meter name is the bare
  `Microsoft.Orleans`. Re-verify it on an Orleans major upgrade.

### Cardinality cost

Measured on a single silo rather than estimated: `Microsoft.Orleans` produced 54
distinct series and `System.Runtime` 29.

Every `System.Runtime` dimension is a fixed enumeration (GC generation, CPU
mode), so its cost does not grow with load at all. The only Orleans dimension
that grows is the `type` tag on `orleans-grains` and `orleans-system-targets`,
which is bounded by the number of grain **types** in the image - a compile-time
constant - not by the number of grain activations. Neither meter carries a
per-key, per-tree, or per-activation tag, so neither is a cardinality risk on a
large keyspace.

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
    // LatticeDashboardKind.ReplicationGrpc, // add when the gRPC replication transport is registered
    // LatticeDashboardKind.Authorization, // add when the auth / membership packages are registered
    // LatticeDashboardKind.Backup, // add when the backup package is registered
    // LatticeDashboardKind.Scaling, // add when the scaling package is registered
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
