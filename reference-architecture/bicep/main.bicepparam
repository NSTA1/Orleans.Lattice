// =============================================================================
// main.bicepparam - example parameter set for the compute foundation.
// -----------------------------------------------------------------------------
// Demonstrates deploying N regions from ONE parameter set: add or remove an
// entry in the `regions` array and every per-region compute stack follows. A
// single-region deployment is just a one-element array (see params/single-region.bicepparam).
// =============================================================================

using './main.bicep'

param baseName = 'lattice'

// Two regions here; the same file scales to N by editing this array only.
param regions = [
  {
    location: 'westeurope'
    regionCode: 'weu'
  }
  {
    location: 'eastus2'
    regionCode: 'eus2'
  }
]

param imageTag = '1.0.0'

// OBSERVABILITY-SUBISSUE SEAM: left empty here so this compute-only set builds
// and deploys standalone. When the observability sub-issue lands, set this to
// the managed-Prometheus query endpoint to activate the silo KEDA scale rule.
param prometheusQueryEndpoint = ''
