---
agent_spec: "docs/agents/deployment.yaml"
---

# API

The public surface of `Orleans.Lattice.Scaling`.

## `ILatticeScalingSignal`

The read-only facade. Resolve it from the silo's service provider (registered by
`AddLatticeScalingSignal`) and call `GetScalingSignalAsync` for the current
cluster-aggregate snapshot. It is cheap to call repeatedly - it returns the
cached sample - so it can back a per-scrape HTTP endpoint.

```
Task<ScalingSignal> GetScalingSignalAsync(CancellationToken cancellationToken = default)
```

A token that is already cancelled yields a cancelled task. Before the first
sample completes it returns the warming-up signal described under
`ScalingSignal` below.

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
| `ScaleValue` | `double` | Smoothed, scale-in-gated replica-demand scalar an autoscaler should act on, in replica-units and never below `MinReplicas` once the first sample lands. It reads `0.0` while the signal is still warming up, and otherwise only when there is no pressure and `MinReplicas` is `0`. It is the dominant compute pressure (`0.0` to `1.0`) times the current replica count, so it never exceeds that count except at the `MinReplicas` floor, or while the scale-in gate holds - or the smoothing releases - an earlier, higher value; see [the custom scale rule](keda-aca.md#the-custom-scale-rule) for what that means for an autoscaler's `targetValue`. |
| `RecommendedReplicas` | `int` | `max(MinReplicas, ceil(ScaleValue))`: the replicas needed to carry the current demand at full per-replica pressure, so, like `ScaleValue`, it never exceeds the current count except at the floor or while an earlier, higher value is held or released. Before the first sample it reads `MinReplicas`. |
| `Compute` | `ComputePressure` | The compute-axis component. |
| `Storage` | `StoragePressure` | The storage-axis component. |
| `Reason` | `string` | Human-readable explanation of how the signal was derived. |
| `SampledAt` | `DateTimeOffset` | UTC instant the snapshot was sampled. |
| `RawScaleValue` | `double` | Un-smoothed instantaneous demand, before EWMA smoothing and scale-in gating. |

### `ComputePressure`

| Member | Type | Meaning |
|---|---|---|
| `Activation` | `double` | Normalised grain-activation pressure (0..1): the worst silo's activation count against `ActivationWorkingSetTarget`. A cluster aggregate. |
| `Resource` | `double` | Normalised host-resource pressure (0..1), worst-case CPU and memory, taken from the worst silo. A cluster aggregate. |
| `WalDispatch` | `double` | Normalised WAL-dispatch pressure (0..1): a step mapping of `WalSaturation` - `0.0` for `Healthy`, `0.5` for `Throttled`, `1.0` for `Saturated` - so, like it, it reflects the answering silo only. |
| `WalSaturation` | `WalSaturationState` | Worst-case WAL saturation across every tree the answering silo has observed (the silo-local WAL saturation signal); `Healthy` when the core `AddLattice` registration is absent. |

### `StoragePressure`

| Member | Type | Meaning |
|---|---|---|
| `OverThreshold` | `bool` | The retained WAL bytes of the trees that declare a `WalMaxRetainedBytes` ceiling reached `RetainedBytesAdvisoryRatio` of their summed ceilings. A tree with no ceiling contributes neither bytes nor budget, so the flag stays `false` when no tree declares one. |
| `WalRetainedBytes` | `long` | Total retained WAL bytes across every catalogue key, over the trees whose figure was complete. |
| `Accounts` | `IReadOnlyList<WalAccountPressure>` | Per-catalogue-key breakdown; never `null`. |
| `Recommendation` | `WalRebalanceRecommendation?` | Optional rebalance suggestion, or `null`. |
| `ExcludedPartialTrees` | `int` | Trees left out of the byte figures and capacity comparisons because their storage-usage report was partial (a lower bound - see [storage pressure](storage-pressure.md#what-it-measures)). Their saturation still counts toward throughput classification. |

### `WalAccountPressure`

| Member | Type | Meaning |
|---|---|---|
| `ProviderKey` | `string` | The catalogue key (the "account"). |
| `WalRetainedBytes` | `long` | Retained WAL bytes against this key. |
| `Saturation` | `WalSaturationState` | Worst-case saturation of the trees with partitions backed by this key, as the answering silo's WAL saturation signal reports them (tree-level, not per partition). |
| `Classification` | `WalPressureClassification` | Throughput-bound, capacity-bound, or none. |
| `OverThreshold` | `bool` | The retained bytes of the trees on this key that declare a `WalMaxRetainedBytes` ceiling crossed the advisory fraction of those trees' ceilings attributed to this key (the capacity-bound trigger); always `false` when no tree on the key declares a ceiling. |

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

Declared on `LatticeScalingServiceCollectionExtensions`. Registers the facade and its hosted collector on the silo. Optional `configure`
callback binds `LatticeScalingSignalOptions`.

### `MapLatticeScalingSignal`

```
IEndpointConventionBuilder MapLatticeScalingSignal(this IEndpointRouteBuilder endpoints, string? path = null)
```

Declared on `LatticeScalingEndpointRouteBuilderExtensions`. Maps a `GET` scrape endpoint on the co-hosted web host. Serves the `ScalingSignal` as
JSON with a stable, camelCase top-level `scaleValue` property (plus
`rawScaleValue`, `recommendedReplicas`, the compute and storage breakdown, the
`reason` and `sampledAt`; enums are serialized as strings). `path` overrides `LatticeScalingSignalOptions.EndpointPath`
for this mapping; with no `path` and no bound options it falls back to
`LatticeScalingSignalOptions.DefaultEndpointPath`. The endpoint is unauthenticated by default because it is a
scrape target: it serves no tree data, though the storage breakdown does name WAL
provider keys and, in a recommendation, a tree id. The returned
`IEndpointConventionBuilder` composes with the host pipeline like any other
mapped endpoint, so chain `RequireAuthorization()` (or rate limiting) to restrict
it.

### `AddLatticeScalingHealthCheck`

```
IHealthChecksBuilder AddLatticeScalingHealthCheck(this IHealthChecksBuilder builder, string? name = null, HealthStatus? failureStatus = null, IEnumerable<string>? tags = null)
```

Declared on `LatticeScalingServiceCollectionExtensions`. Registers the health
check that projects the signal onto a single `HealthStatus`. It reads the
`ILatticeScalingSignal` facade, so call it on a host that also calls
`AddLatticeScalingSignal`. `name` defaults to
`LatticeScalingHealthCheckOptions.DefaultName`. See
[configuration](configuration.md#latticescalinghealthcheckoptions).

## Options types

- `LatticeScalingSignalOptions` - the signal's knobs, bound by the `configure`
  callback of `AddLatticeScalingSignal`, with a public `Default*` constant or
  static field for every default.
- `LatticeScalingHealthCheckOptions` - the health check's knobs, bound as named
  options under the check's registered name, plus the `DefaultName` and
  `DefaultComputePressure` defaults and the nested
  `LatticeScalingHealthCheckOptions.DoubleTier(double Degraded, double Unhealthy)`
  record struct that carries the tiered compute bound.

Every member, its default and guidance are in [configuration](configuration.md).

## Serialization aliases

`ScalingTypeAliases` holds the stable Orleans serialization aliases for the snapshot DTOs above: `ScalingSignal` (`ol.scs`), `ComputePressure` (`ol.scp`), `StoragePressure` (`ol.stp`), `WalAccountPressure` (`ol.wap`), `WalRebalanceRecommendation` (`ol.wrr`), and `WalPressureClassification` (`ol.wpc`). They are part of the wire format and never change.

## Metrics

`LatticeScalingMetrics` exposes the `orleans.lattice.scaling` meter name
(`MeterName`), the `Meter` instance itself (so a subscriber can match by
reference), and the eight instrument-name constants. See
[observability](observability.md).
