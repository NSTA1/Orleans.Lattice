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
          // scaleValue is the dominant pressure (0.0 to 1.0) times the current
          // replica count, so it never exceeds that count. desiredReplicas =
          // ceil(scaleValue / targetValue), and targetValue must therefore be
          // below 1 for the pool to grow: '1' could only hold or shrink it.
          // '0.5' holds each replica at half load and asks for twice the
          // current count at full saturation.
          url: scaleSignalUrl
          valueLocation: 'scaleValue'
          targetValue: '0.5'
        }
      }
    }
  ]
}

// pollingInterval (KEDA default 30s) should sit at or above
// LatticeScalingSignalOptions.SampleInterval so repeated scrapes do not just read
// the same cached sample. Scale-in stabilization (default 300s) should be chosen
// deliberately: scaleValue is already EWMA-smoothed and gated on the producer
// side, so stacking a long HPA window on top of a long Lattice scale-in gate makes
// scale-in lag twice.
output scale object = latticeScale
