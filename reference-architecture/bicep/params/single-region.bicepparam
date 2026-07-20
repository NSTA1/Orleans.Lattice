// =============================================================================
// single-region.bicepparam - proves the same main.bicep deploys ONE region
// from a one-element region list (no template change, only the parameter set).
// =============================================================================

using '../main.bicep'

param baseName = 'lattice'

param regions = [
  {
    location: 'westeurope'
    regionCode: 'weu'
  }
]

param imageTag = '1.0.0'

param prometheusQueryEndpoint = ''
