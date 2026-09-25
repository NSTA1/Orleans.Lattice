# KEDA and HPA on AKS

Autoscaling an Orleans.Lattice cluster on Azure Kubernetes Service (AKS), or any
Kubernetes cluster, using the scaling signal. Two options: a KEDA `ScaledObject`
(recommended) or a native Horizontal Pod Autoscaler (HPA) against a custom metric.

The host wiring is identical to the [ACA walkthrough](keda-aca.md#host-wiring):
`AddLatticeScalingSignal` on the silo, `MapLatticeScalingSignal` on the web host,
and the endpoint served on the pod's container port.

## Option 1: KEDA `ScaledObject` (recommended)

Install KEDA in the cluster, then apply a `ScaledObject` with a `metrics-api`
trigger pointed at the in-cluster service:

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: lattice-scaledobject
spec:
  scaleTargetRef:
    name: lattice-silo            # the Deployment to scale
  minReplicaCount: 2
  maxReplicaCount: 20
  pollingInterval: 15             # seconds; keep above SampleInterval
  cooldownPeriod: 120             # seconds; scale-in cooldown on top of the gate
  triggers:
    - type: metrics-api
      metadata:
        url: "http://lattice-silo.default.svc.cluster.local/lattice/scale"
        valueLocation: "scaleValue"
        targetValue: "1"
```

- `url` targets the headless or ClusterIP service in front of the silo pods; KEDA
  polls one pod and reads its snapshot, whose activation and resource dimensions
  are cluster aggregates (the WAL-dispatch dimension and the smoothing state are
  that pod's own - see
  [cluster-aggregate answering](architecture.md#cluster-aggregate-answering)).
- `valueLocation: "scaleValue"` and `targetValue: "1"` behave exactly as in the
  [ACA rule](keda-aca.md#the-custom-scale-rule): demand is in replica-units, so a
  target of `1` means one pod per replica-unit.
- `pollingInterval` should stay above `LatticeScalingSignalOptions.SampleInterval`
  so KEDA never reads a stale sample; `cooldownPeriod` stacks on the signal's own
  `ScaleInGateWindow`.

KEDA creates and manages the underlying HPA for you.

## Option 2: HPA against a custom metric

If you prefer a native HPA, expose the scale value as an external metric through
the Prometheus adapter (scrape the [`orleans.lattice.scaling` meter](observability.md)
via the OpenTelemetry Prometheus exporter, so `scaleValue` is available as
`orleans_lattice_scaling_scale_value`), then:

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: lattice-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: lattice-silo
  minReplicas: 2
  maxReplicas: 20
  metrics:
    - type: External
      external:
        metric:
          name: orleans_lattice_scaling_scale_value
        target:
          type: AverageValue
          averageValue: "1"
  behavior:
    scaleDown:
      stabilizationWindowSeconds: 120
```

Use an `External` metric, not a `Pods` one. The value is a cluster-wide demand
in replica-units that every silo exports, so an `External` metric with an
`AverageValue` target of `1` makes the HPA divide it by the current pod count
and settle on `ceil(scaleValue)` replicas - the same arithmetic as the KEDA rule.
A `Pods` metric would instead average the near-identical per-pod values and
multiply by the current pod count, overshooting by roughly that factor. For the
same reason, have the adapter's external-metric query aggregate the per-pod
series with `max` or `avg` rather than `sum`.

The KEDA route is preferred because the `metrics-api` trigger reads the endpoint
directly and needs no Prometheus-adapter plumbing; the HPA route is useful when
you already run the Prometheus adapter and want a single autoscaling mechanism.

## Readiness

The [scaling health check](configuration.md#latticescalinghealthcheckoptions)
can back a probe, but it is not a per-pod signal: it reads the same cached
snapshot the endpoint serves, so its activation and resource inputs are the
cluster's worst-silo values and every pod reports the same verdict for them
(only the WAL inputs are the pod's own). Wired into a `readinessProbe`, it
therefore takes every pod out of rotation together once the cluster's hottest
silo crosses the `Unhealthy` bound - a `Degraded` result still answers `200` on
the default ASP.NET Core status mapping. If that is the behaviour you want, wire
it like this; otherwise map it on its own endpoint (or keep it out of the
readiness tag group) and use it for alerting:

```yaml
readinessProbe:
  httpGet:
    path: /readyz
    port: 8080
  periodSeconds: 10
```

## See also

- [KEDA on Azure Container Apps](keda-aca.md) for the managed-ACA equivalent.
- [Observability](observability.md) for the meter the HPA route scrapes.
