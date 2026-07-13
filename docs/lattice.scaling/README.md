# Orleans.Lattice.Scaling

Opt-in autoscaling signal for Orleans.Lattice: a read-only, cluster-aggregate,
two-axis (compute and storage) pressure snapshot that an external autoscaler can
scrape to size the silo pool.

The package answers one question - *how many silo replicas does this cluster
need right now?* - and answers a second, advisory one alongside it - *is any WAL
storage account hot enough that its partitions should be spread across more
accounts?* The first drives replica autoscaling on the compute axis; the second
is signal-only and never moves a replica.

## Why it exists

Orleans clusters that back a B+ tree store scale on two independent axes:

- **Compute**: grain-activation working set, host CPU and memory, and the
  write-ahead-log dispatch pipeline. When these run hot the fix is more silos.
- **Storage**: retained WAL bytes and per-account backend write throughput. When
  a single storage account tops out, adding silos does not help; the fix is to
  spread WAL partitions across more accounts.

A generic CPU-based autoscaler conflates the two and either over-provisions
compute to relieve a storage bottleneck or ignores a genuine compute shortfall.
`Orleans.Lattice.Scaling` separates them: it publishes a single compute-axis
replica-demand scalar that an external autoscaler (KEDA, an HPA custom metric,
or an Azure Container Apps custom scale rule) consumes directly, and it reports
storage-axis pressure as an operator recommendation that maps onto the
`ILatticeAdmin` WAL-move workflow.

## The two axes

- **Compute axis (`ComputePressure`)** - normalised activation, host-resource,
  and WAL-dispatch pressure (each `0.0` idle to `1.0` saturated), plus the
  worst-case `WalSaturationState` observed across every tree and partition. The
  dominant dimension times the current replica count is the raw replica demand.
- **Storage axis (`StoragePressure`)** - whether aggregate retained WAL bytes
  crossed the configured threshold, the aggregate retained bytes, a
  per-catalogue-key breakdown (`WalAccountPressure`) that classifies each
  account as throughput-bound or capacity-bound, and an optional
  `WalRebalanceRecommendation`.

Both axes roll up into a single `ScalingSignal` carrying the smoothed,
scale-in-gated `ScaleValue` (in replica-units) an autoscaler should act on, a
concrete `RecommendedReplicas` count, the raw `RawScaleValue` before smoothing,
a human-readable `Reason`, and a `SampledAt` timestamp. **The storage axis never
contributes to `ScaleValue`** - it is reported for operator action only.

## Quick start

Register the signal on the silo builder, then expose it over an HTTP endpoint and
an ASP.NET Core health check on the co-hosted web host:

```csharp verify
using Orleans.Lattice.Scaling;

siloBuilder.AddLatticeScalingSignal(options =>
{
    options.MinReplicas = 2;
    options.SampleInterval = TimeSpan.FromSeconds(5);
});
```

```csharp verify
using Orleans.Lattice.Scaling;

var app = WebApplication.Create();

// KEDA / ACA scrape target - serves the ScalingSignal as JSON with a top-level
// scaleValue property the autoscaler reads.
app.MapLatticeScalingSignal();
```

Resolve `ILatticeScalingSignal` anywhere in the container to read the current
snapshot directly:

```csharp verify
using System.Threading;
using Orleans.Lattice.Scaling;

async Task InspectAsync(ILatticeScalingSignal signal, CancellationToken cancellationToken)
{
    ScalingSignal current = await signal.GetScalingSignalAsync(cancellationToken);
    Console.WriteLine($"scaleValue={current.ScaleValue} replicas={current.RecommendedReplicas} reason={current.Reason}");
}
```

## Documentation

| Document | What it covers |
|---|---|
| [Architecture](architecture.md) | Collectors, aggregation, EWMA smoothing, asymmetric scale-in gating, cluster-aggregate answering, and the storage-axis-never-scales-replicas invariant. |
| [Configuration](configuration.md) | Every `LatticeScalingSignalOptions` and `LatticeScalingHealthCheckOptions` knob, its default, and guidance. |
| [API](api.md) | `ILatticeScalingSignal`, the snapshot DTOs, and the registration and endpoint extension methods. |
| [KEDA on Azure Container Apps](keda-aca.md) | End-to-end ACA walkthrough: the metrics-api custom scale rule, `targetValue`, min/max replicas, and polling versus EWMA. |
| [KEDA and HPA on AKS](aks.md) | A KEDA `ScaledObject` and the HPA custom-metric alternative on Kubernetes. |
| [Storage pressure](storage-pressure.md) | How per-account WAL pressure maps to the multi-account fan-out remediation and the `ILatticeAdmin` move workflow (signal-only in this release). |
| [Observability](observability.md) | The `orleans.lattice.scaling` meter instruments and the bundled Grafana dashboard. |

## Sample

[`samples/ClusterScaling`](../../samples/ClusterScaling/README.md) is a deployable
Azure Container Apps sample: a multi-silo Orleans cluster on real Azure Storage
clustering and WAL via managed identity, co-hosting the gRPC data API and the
scaling endpoint, with a bundled load driver that drives the compute axis so KEDA
scales the replica count out.

## Related

- Package overview: [`src/lattice.scaling/NUGET_README.md`](../../src/lattice.scaling/NUGET_README.md).
- Bundled dashboards: [`Orleans.Lattice.Dashboards`](../lattice.dashboards/README.md).
