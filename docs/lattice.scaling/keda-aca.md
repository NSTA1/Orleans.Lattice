# KEDA on Azure Container Apps

An end-to-end walkthrough for autoscaling an Orleans.Lattice cluster on Azure
Container Apps (ACA) using the scaling signal as a KEDA custom metric. The
[`ClusterScaling` sample](../../samples/ClusterScaling/README.md) is a complete,
deployable implementation of everything below.

## The shape

ACA scales container-app replicas with KEDA under the hood. The
`metrics-api` KEDA scaler polls an HTTP endpoint, reads a numeric value out of the
JSON response, and drives the replica count toward a target. The scaling
endpoint is exactly that: a GET endpoint returning a JSON body with a top-level
`scaleValue`.

```
   KEDA metrics-api scaler                Orleans.Lattice silo (each replica)
   ----------------------                 -----------------------------------
   GET https://<app>/lattice/scale  --->  MapLatticeScalingSignal()
        reads $.scaleValue          <---  { "scaleValue": 3.4, ... }
        targetValue: 1
        desiredReplicas = ceil(scaleValue / targetValue)
```

Because the signal is a cluster-aggregate, KEDA can poll any replica and read a
coherent whole-cluster demand.

## Host wiring

Register the signal on the silo, map the endpoint, and listen on the ACA ingress
target port:

```csharp verify
using Orleans.Lattice.Scaling;

siloBuilder.AddLatticeScalingSignal(options =>
{
    // Never recommend fewer than the minimum viable cluster.
    options.MinReplicas = 2;
    // Refresh well inside KEDA's poll interval.
    options.SampleInterval = TimeSpan.FromSeconds(5);
});
```

```csharp verify
using Orleans.Lattice.Scaling;

var app = WebApplication.Create();
app.MapLatticeScalingSignal(); // GET /lattice/scale
```

## The custom scale rule

Add a `custom` scale rule of type `metrics-api` to the container app. In bicep:

```bicep
scale: {
  minReplicas: 2
  maxReplicas: 20
  rules: [
    {
      name: 'lattice-scale'
      custom: {
        type: 'metrics-api'
        metadata: {
          url: 'https://${containerApp.properties.configuration.ingress.fqdn}/lattice/scale'
          valueLocation: 'scaleValue'
          targetValue: '1'
          activationTargetValue: '1'
        }
      }
    }
  ]
}
```

- `valueLocation: 'scaleValue'` - the JSON path KEDA reads. The endpoint emits it
  as a stable, camelCase top-level property.
- `targetValue: '1'` - the scale value is already expressed in replica-units, so a
  target of `1` means "provision one replica per replica-unit of demand". KEDA
  computes `desiredReplicas = ceil(currentValue / targetValue)`.
- `activationTargetValue: '1'` - the threshold above which KEDA activates the app
  from zero (if you allow scale-to-zero). Keep it aligned with your `MinReplicas`.
- `minReplicas` / `maxReplicas` - the ACA replica envelope. Set `minReplicas` to
  your quorum floor and `maxReplicas` to your capacity ceiling.

## Polling and stabilization versus EWMA

KEDA polls on its own interval (`pollingInterval`, default 30s) and ACA applies
its own cooldown before scaling in. These stack on top of the signal's own
smoothing:

- **Scale-out** is fast on both sides: the signal snaps up immediately, and KEDA
  scales out on its next poll.
- **Scale-in** is deliberately slow: the signal only lets the scalar fall after
  every scale-in precondition has held for `ScaleInGateWindow` (default 2m), and
  KEDA/ACA then apply their own cooldown on top. Tune `EwmaHalfLife` and
  `ScaleInGateWindow` for how conservative you want scale-in, and leave the
  KEDA/ACA cooldown to guard against poll-to-poll flapping.

Keep `SampleInterval` (signal freshness) well below `pollingInterval` (KEDA read
cadence) so KEDA never reads a stale sample.

## Health probes

Point the ACA health probes at the scaling health check so a silo whose compute
axis is saturated is reported unhealthy:

```csharp verify
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Scaling;

var services = new ServiceCollection();
services.AddHealthChecks().AddLatticeScalingHealthCheck(tags: new[] { "ready" });
```

Map a readiness endpoint filtered to the `ready` tag and set it as the ACA
readiness probe path.

## See also

- [AKS](aks.md) for the Kubernetes `ScaledObject` and HPA alternatives.
- [Configuration](configuration.md) for every knob the walkthrough tunes.
- [`ClusterScaling` sample](../../samples/ClusterScaling/README.md) for the full deployable implementation.
