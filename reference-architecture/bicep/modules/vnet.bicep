// =============================================================================
// vnet.bicep - private-option network foundation (per-region VNets + peering)
// -----------------------------------------------------------------------------
// Sub-issue F-190 (Reference Architecture epic): the PRIVATE-option network
// foundation, split out of networking.bicep so it can be provisioned BEFORE the
// compute module.
//
// Why a separate module: the ACA managed environment (compute.bicep) consumes an
// infrastructure subnet via its `infrastructureSubnetId` seam, so the subnet must
// exist before the environment is created. The public option's Key Vault, by
// contrast, consumes each region's workload-identity principalId, which only
// exists AFTER compute has run. Keeping both concerns in one module would make
// compute and networking mutually dependent (a compile-time cycle). Splitting the
// VNet foundation into this pre-compute module makes the ordering one-directional:
//
//     vnet.bicep  ->  compute.bicep  ->  networking.bicep (public Key Vault)
//
// This module is invoked ONLY for the private option; for the public option it is
// not deployed and compute receives an empty infrastructure subnet (public env).
//
// It creates a per-region VNet with a single infrastructure subnet delegated to
// Microsoft.App/environments, plus full-mesh (global) VNet peering between every
// region so the internal replication transport reaches its peers over private
// address space only. It creates NO container apps and NO Key Vaults.
// =============================================================================

targetScope = 'resourceGroup'

@description('Region list, in the SAME order as compute.bicep\'s region list. Each item: { location, regionCode }. One entry or many, from the same parameter set.')
@minLength(1)
param regions array

@description('The second octet from which each region\'s non-overlapping VNet space is carved by region index. Region i gets 10.{base+i}.0.0/16 by default. Override `regionVnetAddressPrefixes` for explicit control.')
param privateVnetSupernetSecondOctetBase int = 10

@description('Explicit per-region VNet address prefix, index-aligned with `regions`. Empty derives 10.{privateVnetSupernetSecondOctetBase + i}.0.0/16 per region (guaranteed non-overlapping so global peering is valid).')
param regionVnetAddressPrefixes array = []

@description('Explicit per-region ACA infrastructure subnet prefix, index-aligned with `regions`. Empty derives a /23 at the start of the region VNet (ACA consumption environments require a /23 or larger, non-overlapping infrastructure subnet).')
param regionInfrastructureSubnetPrefixes array = []

// =============================================================================
// Derived values
// =============================================================================

// Non-overlapping address plan per region (index-derived unless overridden).
var vnetAddressPrefixes = [for (region, i) in regions: empty(regionVnetAddressPrefixes) ? '10.${privateVnetSupernetSecondOctetBase + i}.0.0/16' : regionVnetAddressPrefixes[i]]
var infraSubnetPrefixes = [for (region, i) in regions: empty(regionInfrastructureSubnetPrefixes) ? '10.${privateVnetSupernetSecondOctetBase + i}.0.0/23' : regionInfrastructureSubnetPrefixes[i]]

// Full-mesh ordered peering pairs (i -> j) as a single flat loop. The pair index
// k maps to (from = k / N, to = k % N); i == j is skipped at the resource.
var regionCount = length(regions)
var peeringPairs = [for k in range(0, regionCount * regionCount): {
  fromIndex: k / regionCount
  toIndex: k % regionCount
}]

// =============================================================================
// Per-region VNet, delegated infra subnet, full-mesh peering
// =============================================================================

resource vnet 'Microsoft.Network/virtualNetworks@2023-11-01' = [for (region, i) in regions: {
  name: '${region.regionCode}-lattice-vnet'
  location: region.location
  properties: {
    addressSpace: {
      addressPrefixes: [
        vnetAddressPrefixes[i]
      ]
    }
    subnets: [
      {
        // ACA workload/consumption environments require a dedicated subnet
        // delegated to Microsoft.App/environments.
        name: 'aca-infra'
        properties: {
          addressPrefix: infraSubnetPrefixes[i]
          delegations: [
            {
              name: 'aca-environment-delegation'
              properties: {
                serviceName: 'Microsoft.App/environments'
              }
            }
          ]
          privateEndpointNetworkPolicies: 'Enabled'
          privateLinkServiceNetworkPolicies: 'Enabled'
        }
      }
    ]
  }
}]

// Global full-mesh peering: every region VNet peers with every other so the
// internal replication transport reaches its peers over private address space
// only. i == j is skipped; both directions of each pair are created.
resource peering 'Microsoft.Network/virtualNetworks/virtualNetworkPeerings@2023-11-01' = [for pair in peeringPairs: if (pair.fromIndex != pair.toIndex) {
  parent: vnet[pair.fromIndex]
  name: 'peer-${regions[pair.fromIndex].regionCode}-to-${regions[pair.toIndex].regionCode}'
  properties: {
    remoteVirtualNetwork: {
      id: vnet[pair.toIndex].id
    }
    allowVirtualNetworkAccess: true
    allowForwardedTraffic: true
    allowGatewayTransit: false
    useRemoteGateways: false
  }
}]

// =============================================================================
// Outputs - the infrastructure subnet per region feeds compute.bicep's
// `infrastructureSubnetId` seam (which makes the environment internal-only).
// =============================================================================

@description('Per-region private network seams, in region-list order. Feed `infrastructureSubnetId` into compute.bicep\'s VNet-integration seam per region.')
output perRegionPrivate array = [for (region, i) in regions: {
  regionCode: region.regionCode
  location: region.location
  vnetId: vnet[i].id
  infrastructureSubnetId: '${vnet[i].id}/subnets/aca-infra'
}]
