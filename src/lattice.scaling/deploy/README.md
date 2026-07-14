# Orleans.Lattice.Scaling reference autoscaler manifests

These files are reference deployment assets shipped with the
`Orleans.Lattice.Scaling` package. They wire an external autoscaler to the
HTTP scaling endpoint that `MapLatticeScalingSignal()` maps (default route
`/lattice/scale`). Copy and adapt them; they are inert assets, not compiled.

| File | Target | What it does |
|------|--------|--------------|
| `aca-scale-rule.json` | Azure Container Apps | A `custom` scale rule of type `metrics-api` as a JSON fragment for `properties.template.scale.rules[]`. |
| `aca-scale-rule.bicep` | Azure Container Apps | The same rule as a Bicep fragment producing a `scale` object. |
| `keda-scaledobject.yaml` | AKS / Kubernetes | A KEDA `ScaledObject` using the `metrics-api` scaler, plus a note on the plain-HPA custom-metric alternative. |

## How the scalar is read

The endpoint returns JSON whose top-level `scaleValue` property is the scalar
the autoscaler tracks. KEDA (which also backs Azure Container Apps custom scale
rules) reads it via `valueLocation: "scaleValue"` and divides it by
`targetValue` to derive the desired replica count. Because `scaleValue` is
already expressed in replica-units, `targetValue: "1"` makes the autoscaler
track it directly.

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
- **Polling cadence vs EWMA smoothing.** Keep the autoscaler polling interval
  at or above the signal's producer-side EWMA smoothing window so it samples a
  settled value. Do not stack a long scale-in stabilization window on top of a
  long EWMA, or scale-in lags twice.
