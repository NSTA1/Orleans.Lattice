# Configuration

Every knob on `LatticeScalingSignalOptions` (bound through `AddLatticeScalingSignal`)
and `LatticeScalingHealthCheckOptions` (bound through `AddLatticeScalingHealthCheck`),
with its default and guidance.

## `LatticeScalingSignalOptions`

Configure it with the `Action<LatticeScalingSignalOptions>` overload of
`AddLatticeScalingSignal`:

```csharp verify
using Orleans.Lattice.Scaling;

siloBuilder.AddLatticeScalingSignal(options =>
{
    options.EndpointPath = "/lattice/scale";
    options.MinReplicas = 2;
    options.SampleInterval = TimeSpan.FromSeconds(5);
    options.EwmaHalfLife = TimeSpan.FromSeconds(30);
    options.ScaleInGateWindow = TimeSpan.FromMinutes(2);
    options.ActivationScaleInThreshold = 0.25;
    options.ResourceScaleInThreshold = 0.25;
    options.WalDispatchScaleInThreshold = 0.25;
    options.ActivationWorkingSetTarget = 100_000;
    options.RetainedBytesAdvisoryRatio = 0.8;
    options.AccountSaturationWindow = TimeSpan.FromSeconds(30);
    options.StorageRecommendationsEnabled = true;
});
```

### Endpoint and floor

| Option | Type | Default | Guidance |
|---|---|---|---|
| `EndpointPath` | `string` | `/lattice/scale` | The HTTP path `MapLatticeScalingSignal` serves the signal from. `MapLatticeScalingSignal(path)` can override it per-call; keep this and the mapped path in sync. |
| `MinReplicas` | `int` | `0` | Lower bound applied to `RecommendedReplicas` - the recommendation is never reported below this floor. Set it to your cluster's minimum viable silo count so scale-in never suggests dropping below quorum. |

### Compute axis

| Option | Type | Default | Guidance |
|---|---|---|---|
| `SampleInterval` | `TimeSpan` | `5s` | How often the silo recomputes the signal. The per-scrape facade reads the cached result, so this is the freshness bound, not the scrape cost. Keep it well below your autoscaler's polling interval. |
| `EwmaHalfLife` | `TimeSpan` | `30s` | Half-life of the exponentially-weighted moving average applied to the scalar on the scale-in (release) side. Longer damps noise and makes scale-in more conservative; scale-out reacts immediately regardless. |
| `ScaleInGateWindow` | `TimeSpan` | `2m` | How long every scale-in precondition (all compute dimensions low, WAL healthy, no shard split in flight) must hold continuously before the scalar is allowed to fall. Any break resets the window. |
| `ActivationScaleInThreshold` | `double` | `0.25` | Activation-pressure level (0..1) at or above which the activation dimension is too hot to permit scale-in. |
| `ResourceScaleInThreshold` | `double` | `0.25` | Resource-pressure level (0..1) at or above which the resource dimension is too hot to permit scale-in. |
| `WalDispatchScaleInThreshold` | `double` | `0.25` | WAL-dispatch-pressure level (0..1) at or above which the WAL-dispatch dimension is too hot to permit scale-in. |
| `ActivationWorkingSetTarget` | `int` | `100000` | Per-silo grain-activation count treated as full activation saturation. Activation pressure is `activationCount / target`, clamped to 0..1. Size it to the activation count at which a silo's memory or scheduler starts to strain. |
| `SplitAwareScaleIn` | `bool` | `true` | Whether the scale-in gate is suppressed while any adaptive shard split is in flight cluster-wide. Reads `ILatticeAdmin.GetSplitActivityAsync` once per `SampleInterval` - a single call to the split-admission singleton, never a fan-out. Set to `false` to make the axis inert (a deployment with autonomic splitting disabled, where the query is pure overhead). Scale-**out** is never influenced either way. See [split-aware scale-in](architecture.md#split-aware-scale-in). |

### Storage axis

The storage axis is report-only: none of these knobs affect the compute
`ScaleValue`.

| Option | Type | Default | Guidance |
|---|---|---|---|
| `RetainedBytesAdvisoryRatio` | `double` | `0.8` | Fraction of `LatticeOptions.WalMaxRetainedBytes` at or above which retained WAL bytes count as capacity pressure. Clamped to `(0, 1]`. Ignored when `WalMaxRetainedBytes` is `null` (no ceiling configured). |
| `AccountSaturationWindow` | `TimeSpan` | `30s` | How long a provider key must be continuously observed saturated before the collector classifies it throughput-bound and recommends a move. Debounces a transient blip. A non-positive value classifies on the first saturated sample. |
| `StorageRecommendationsEnabled` | `bool` | `true` | Master switch for emitting a `WalRebalanceRecommendation`. When `false` the collector still reports `OverThreshold` and the per-account breakdown but leaves `Recommendation` `null`. |

## `LatticeScalingHealthCheckOptions`

`AddLatticeScalingHealthCheck` registers an ASP.NET Core health check that
projects the signal onto a single `HealthStatus`. Bind the named options under
the check's registered name (default `orleans.lattice.scaling`):

```csharp verify
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Scaling;

var services = new ServiceCollection();

services.AddHealthChecks().AddLatticeScalingHealthCheck(tags: new[] { "ready" });

services.Configure<LatticeScalingHealthCheckOptions>(
    LatticeScalingHealthCheckOptions.DefaultName,
    options =>
    {
        options.ComputePressure = new LatticeScalingHealthCheckOptions.DoubleTier(0.85, 0.95);
        options.UnhealthyOnWalSaturated = true;
        options.DegradeOnWalThrottled = true;
        options.DegradeOnStorageOverThreshold = true;
    });
```

| Option | Type | Default | Guidance |
|---|---|---|---|
| `ComputePressure` | `DoubleTier?` | `0.85` / `0.95` | Tiered bound on the worst normalised compute dimension: at or above the soft bound reports `Degraded`, at or above the hard bound reports `Unhealthy`. Set to `null` to disable the tiered compute signal. |
| `UnhealthyOnWalSaturated` | `bool` | `true` | When `true`, a `Saturated` worst-case WAL state reports `Unhealthy` regardless of the compute ratios. |
| `DegradeOnWalThrottled` | `bool` | `true` | When `true`, a `Throttled` worst-case WAL state contributes `Degraded`. |
| `DegradeOnStorageOverThreshold` | `bool` | `true` | When `true`, an over-threshold storage axis contributes `Degraded`. The storage axis never escalates past `Degraded` because it is advisory and not wired to the replica recommendation. |

The `failureStatus` and `tags` arguments to `AddLatticeScalingHealthCheck` are
the standard ASP.NET Core health-check registration parameters: `failureStatus`
is the status reported when the check throws (defaults to `Unhealthy`), and
`tags` let a host filter the check into a readiness or liveness probe group.

## See also

- [Architecture](architecture.md) for how the compute knobs shape the scalar.
- [Storage pressure](storage-pressure.md) for how the storage knobs classify accounts.
