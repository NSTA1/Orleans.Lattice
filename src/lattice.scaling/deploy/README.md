# Orleans.Lattice.Scaling reference autoscaler manifests

These files are reference deployment assets shipped with the
`Orleans.Lattice.Scaling` package. They wire an external autoscaler to the
HTTP scaling endpoint that `MapLatticeScalingSignal()` maps (default route
`/lattice/scale`). Copy and adapt them; they are inert assets, not compiled.

| File | Target | What it does |
|------|--------|--------------|
| `aca-scale-rule.json` | Azure Container Apps | A `custom` scale rule of type `metrics-api`, inside a `properties.template.scale` JSON fragment (with `minReplicas` and `maxReplicas`) for a `Microsoft.App/containerApps` resource. |
| `aca-scale-rule.bicep` | Azure Container Apps | The same rule as a Bicep fragment producing a `scale` object. |
| `keda-scaledobject.yaml` | AKS / Kubernetes | A KEDA `ScaledObject` using the `metrics-api` scaler, plus a note on the plain-HPA custom-metric alternative. |

## How the scalar is read

The endpoint returns JSON whose top-level `scaleValue` property is the scalar
the autoscaler tracks. KEDA (which also backs Azure Container Apps custom scale
rules) reads it via `valueLocation: "scaleValue"` and, with the default
`AverageValue` metric type these files use, derives
`desiredReplicas = ceil(scaleValue / targetValue)`. `scaleValue` is the dominant
compute pressure (`0.0` to `1.0`) times the current replica count, so it never
exceeds that count except at the `MinReplicas` floor or while the scale-in gate
holds an earlier value. `targetValue` is therefore the per-replica pressure the
autoscaler holds the pool at, and it must be below `1` for the pool to grow. All
three files here set `targetValue: "1"`, which asks for at most the current
replica count and so can only hold or shrink the pool. The
[`ClusterScaling` sample](../../../samples/ClusterScaling/README.md) deploys
`0.5`, which asks for twice the current count at full saturation.

## Important operator notes

- **`maxReplicas` / `maxReplicaCount` is the hard ceiling.** The autoscaler
  never scales past it regardless of how high `scaleValue` climbs. Size it to
  your cluster's capacity, not to the signal.
- **The storage axis is advisory and NOT wired to replica count.** The
  `storage` breakdown in the response (retained WAL bytes, over-threshold flag)
  is reported for observability and health-check gating only. Only the
  compute-derived `scaleValue` drives replica count. Relieving storage pressure
  is an operational action (rebalancing WAL partitions), not an autoscaling one.
- **Match the scrape URL path to `LatticeScalingSignalOptions.EndpointPath`**
  (default `/lattice/scale`). Point the URL at an in-cluster address of a silo
  that mapped the endpoint.
- **Polling cadence vs smoothing.** Keep the autoscaler polling interval above
  `LatticeScalingSignalOptions.SampleInterval` (default 5 seconds), the cadence
  at which the cached signal refreshes. The `scaleValue` it reads is already
  smoothed and gated on the producer side - it rises immediately, and falls only
  through an EWMA (`EwmaHalfLife`) once the scale-in gate (`ScaleInGateWindow`)
  opens - so do not stack a long scale-in stabilization window on top of them, or
  scale-in lags twice.
