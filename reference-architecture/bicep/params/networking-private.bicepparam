// =============================================================================
// networking-private.bicepparam - PRIVATE replication transport example.
// -----------------------------------------------------------------------------
// Provisions a VNet per region with an ACA-delegated infrastructure subnet and
// full-mesh global peering, so the replication transport is never publicly
// reachable. `infrastructureSubnetId` from perRegionPrivate feeds compute's
// VNet-integration seam with `internalEnvironment = true`. Per-region address
// space is derived non-overlapping by region index (override via the explicit
// prefix parameters if the estate needs specific CIDRs).
// =============================================================================

using '../modules/networking.bicep'

param deploymentOption = 'private'

param regions = [
  {
    location: 'westeurope'
    regionCode: 'weu'
  }
  {
    location: 'eastus'
    regionCode: 'eus'
  }
]

// Region i gets 10.{10 + i}.0.0/16 with a 10.{10 + i}.0.0/23 infra subnet.
param privateVnetSupernetSecondOctetBase = 10
