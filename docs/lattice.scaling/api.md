# API

The public surface of `Orleans.Lattice.Scaling`.

## `ILatticeScalingSignal`

The read-only facade. Resolve it from the silo's service provider (registered by
`AddLatticeScalingSignal`) and call `GetScalingSignalAsync` for the current
cluster-aggregate snapshot. It is cheap to call repeatedly - it returns the
cached sample - so it can back a per-scrape HTTP endpoint.

```csharp verify
using System.Threading;
using Orleans.Lattice.Scaling;

async Task<double> ReadScaleValueAsync(ILatticeScalingSignal signal, CancellationToken cancellationToken)
{
    ScalingSignal snapshot = await signal.GetScalingSignalAsync(cancellationToken);
    return snapshot.ScaleValue;
}
```

## Snapshot DTOs

All are immutable, serializable value types.

### `ScalingSignal`

| Member | Type | Meaning |
|---|---|---|
| `ScaleValue` | `double` | Smoothed, scale-in-gated replica-demand scalar an autoscaler should act on. `0.0` means no additional demand. |
| `RecommendedReplicas` | `int` | Concrete recommended replica count derived from `ScaleValue` and the configured floor. |
| `Compute` | `ComputePressure` | The compute-axis component. |
| `Storage` | `StoragePressure` | The storage-axis component. |
| `Reason` | `string` | Human-readable explanation of how the signal was derived. |
| `SampledAt` | `DateTimeOffset` | UTC instant the snapshot was sampled. |
| `RawScaleValue` | `double` | Un-smoothed instantaneous demand, before EWMA smoothing and scale-in gating. |

### `ComputePressure`

| Member | Type | Meaning |
|---|---|---|
| `Activation` | `double` | Normalised grain-activation pressure (0..1). |
| `Resource` | `double` | Normalised host-resource pressure (0..1), worst-case CPU and memory. |
| `WalDispatch` | `double` | Normalised WAL-dispatch pressure (0..1). |
| `WalSaturation` | `WalSaturationState` | Worst-case WAL saturation across every tree and partition. |

### `StoragePressure`

| Member | Type | Meaning |
|---|---|---|
| `OverThreshold` | `bool` | Aggregate retained WAL bytes crossed the configured threshold. |
| `WalRetainedBytes` | `long` | Total retained WAL bytes across every catalogue key. |
| `Accounts` | `IReadOnlyList<WalAccountPressure>` | Per-catalogue-key breakdown; never `null`. |
| `Recommendation` | `WalRebalanceRecommendation?` | Optional rebalance suggestion, or `null`. |

### `WalAccountPressure`

| Member | Type | Meaning |
|---|---|---|
| `ProviderKey` | `string` | The catalogue key (the "account"). |
| `WalRetainedBytes` | `long` | Retained WAL bytes against this key. |
| `Saturation` | `WalSaturationState` | Worst-case saturation on partitions backed by this key. |
| `Classification` | `WalPressureClassification` | Throughput-bound, capacity-bound, or none. |
| `OverThreshold` | `bool` | Retained bytes crossed the advisory fraction (the capacity-bound trigger). |

### `WalRebalanceRecommendation`

| Member | Type | Meaning |
|---|---|---|
| `Tree` | `string` | Tree whose WAL partition the recommendation applies to. |
| `Partition` | `int` | Partition index to relocate. |
| `CurrentProviderKey` | `string` | Key that backs the partition today. |
| `TargetProviderKey` | `string` | Suggested target key when `HasHeadroom`; empty otherwise. |
| `Rationale` | `string` | Why the move is recommended. |
| `HasHeadroom` | `bool` | `true` when a registered key has spare headroom; `false` when every account is hot. |
| `Classification` | `WalPressureClassification` | Why the current account is hot. |

### `WalPressureClassification`

`None`, `ThroughputBound`, or `CapacityBound`. See [storage pressure](storage-pressure.md#throughput-bound-versus-capacity-bound).

## Registration and endpoint extensions

### `AddLatticeScalingSignal`

```
ISiloBuilder AddLatticeScalingSignal(this ISiloBuilder builder, Action<LatticeScalingSignalOptions>? configure = null)
```

Registers the facade and its hosted collector on the silo. Optional `configure`
callback binds `LatticeScalingSignalOptions`.

### `MapLatticeScalingSignal`

```
IEndpointConventionBuilder MapLatticeScalingSignal(this IEndpointRouteBuilder endpoints, string? path = null)
```

Maps the scrape endpoint on the co-hosted web host. Serves the `ScalingSignal` as
JSON with a stable, camelCase top-level `scaleValue` property (plus
`rawScaleValue`, the compute and storage breakdown, and the reason). `path`
overrides `LatticeScalingSignalOptions.EndpointPath` for this mapping. The
endpoint is unauthenticated by design: it discloses only aggregate pressure,
never data.

### `AddLatticeScalingHealthCheck`

```
IHealthChecksBuilder AddLatticeScalingHealthCheck(this IHealthChecksBuilder builder, string? name = null, HealthStatus? failureStatus = null, IEnumerable<string>? tags = null)
```

Registers the health check that projects the signal onto a single `HealthStatus`.
`name` defaults to `LatticeScalingHealthCheckOptions.DefaultName`. See
[configuration](configuration.md#latticescalinghealthcheckoptions).

## Metrics

`LatticeScalingMetrics` exposes the `orleans.lattice.scaling` meter and its
instrument-name constants. See [observability](observability.md).
