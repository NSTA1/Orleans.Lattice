// =============================================================================
// vnet.bicepparam - per-region network foundation example.
// -----------------------------------------------------------------------------
// Provisions a VNet per region with an ACA-delegated infrastructure subnet and
// (by default) full-mesh global peering. `infrastructureSubnetId` from
// perRegionNetwork feeds compute's VNet-integration seam so the environment is
// VNet-injected (a prerequisite for zone redundancy). Both deployment options
// provision these VNets; the private option additionally enables peering
// (`enablePeering = true`) and internal-only ingress, while the public option sets
// `enablePeering = false` and keeps external ingress. Per-region address space is
// derived non-overlapping by region index (override via the explicit prefix
// parameters if the estate needs specific CIDRs).
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
