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
  polls one pod and reads the cluster-aggregate value.
- `valueLocation: "scaleValue"` and `targetValue: "1"` behave exactly as in the
  [ACA rule](keda-aca.md#the-custom-scale-rule): demand is in replica-units, so a
  target of `1` means one pod per replica-unit.
- `pollingInterval` should stay above `LatticeScalingSignalOptions.SampleInterval`
  so KEDA never reads a stale sample; `cooldownPeriod` stacks on the signal's own
  `ScaleInGateWindow`.

KEDA creates and manages the underlying HPA for you.

## Option 2: HPA against a custom metric

If you prefer a native HPA, expose the scale value as a custom metric through the
Prometheus adapter (scrape the [`orleans.lattice.scaling` meter](observability.md)
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
    - type: Pods
      pods:
        metric:
          name: orleans_lattice_scaling_scale_value
        target:
          type: AverageValue
          averageValue: "1"
  behavior:
    scaleDown:
      stabilizationWindowSeconds: 120
```

The KEDA route is preferred because the `metrics-api` trigger reads the endpoint
directly and needs no Prometheus-adapter plumbing; the HPA route is useful when
you already run the Prometheus adapter and want a single autoscaling mechanism.

## Readiness

Wire the [scaling health check](configuration.md#latticescalinghealthcheckoptions)
into a `readinessProbe` so Kubernetes stops routing to a pod whose compute axis is
saturated, and let the autoscaler add capacity:

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
