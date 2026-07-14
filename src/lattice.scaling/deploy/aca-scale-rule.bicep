// Azure Container Apps scale rule for Orleans.Lattice.Scaling (issue #1188),
// expressed as a Bicep fragment. Drop the `scale` block into the `template`
// of a Microsoft.App/containerApps resource. The `custom` rule of type
// `metrics-api` polls the app's /lattice/scale endpoint and reads the scalar
// at valueLocation 'scaleValue'; ACA (backed by KEDA) divides the scraped
// value by targetValue to derive the desired replica count.

@description('Minimum replica count. Scale-in never drops below this floor.')
param minReplicas int = 1

@description('Maximum replica count. This is the hard ceiling; the autoscaler never scales past it regardless of scaleValue.')
param maxReplicas int = 10

@description('In-cluster URL of a silo that mapped MapLatticeScalingSignal(). Path must match LatticeScalingSignalOptions.EndpointPath (default /lattice/scale).')
param scaleSignalUrl string = 'http://localhost:8080/lattice/scale'

// Example placement inside a container app template. Merge `scale` into your
// existing resource rather than declaring a second containerApp.
var latticeScale = {
  minReplicas: minReplicas
  maxReplicas: maxReplicas
  rules: [
    {
      name: 'lattice-scale'
      custom: {
        type: 'metrics-api'
        metadata: {
          // scaleValue is expressed in replica-units, so targetValue '1'
          // tracks it directly (desiredReplicas = ceil(scaleValue / 1)).
          url: scaleSignalUrl
          valueLocation: 'scaleValue'
          targetValue: '1'
        }
      }
    }
  ]
}

// pollingInterval (KEDA default 30s) should sit at or above the signal's
// producer-side EWMA smoothing window so the autoscaler samples a settled
// value. Scale-in stabilization (default 300s) can be shorter than usual
// because scaleValue is already smoothed; avoid stacking a long stabilization
// window on top of a long EWMA or scale-in lags twice.
output scale object = latticeScale
