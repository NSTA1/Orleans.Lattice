// =============================================================================
// vnet.bicepparam - PRIVATE-option network foundation example.
// -----------------------------------------------------------------------------
// Provisions a VNet per region with an ACA-delegated infrastructure subnet and
// full-mesh global peering, so the replication transport is never publicly
// reachable. `infrastructureSubnetId` from perRegionPrivate feeds compute's
// VNet-integration seam (main.bicep sets `internalEnvironment = true` for the
// private option). Per-region address space is derived non-overlapping by region
// index (override via the explicit prefix parameters if the estate needs
// specific CIDRs). Deployed only for the private option; the public option does
// not provision these resources.
// =============================================================================

using '../modules/vnet.bicep'

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
