// =============================================================================
// Private DNS lane - cross-region resolution of internal environment FQDNs.
// -----------------------------------------------------------------------------
// The private deployment option runs every ACA environment with INTERNAL ingress
// inside a per-region VNet, and full-mesh VNet peering gives IP reachability
// between regions. But an internal Container Apps environment injected into a
// customer VNet does NOT get an automatic private DNS zone, so each region's
// environment default domain (`<hash>.<region>.azurecontainerapps.io`) resolves
// nowhere. Cross-region replication dials the peer silo by its internal head
// FQDN, so without DNS the peer name never resolves and replication cannot reach
// across regions (the peering-provided IP path is unusable without resolution).
//
// This module closes that gap declaratively: it creates one customer-managed
// private DNS zone per environment default domain, publishes a wildcard A record
// pointing every `*.<defaultDomain>` at that environment's static inbound IP, and
// links every zone to EVERY region VNet. The result: any region can resolve any
// region's internal head FQDNs (silo / MCP / Explorer), so cross-region
// replication converges over the peered private network.
//
// Used only by the private option (the public option keeps external ingress with
// public DNS and a global Front Door, so no private zones are needed).
// =============================================================================

targetScope = 'resourceGroup'

@description('One entry per region environment: its ACA default domain and the environment static inbound IP the wildcard A record resolves to.')
param environmentZones array

@description('Resource ids of every region VNet. Each private DNS zone is linked to all of them so any region can resolve any region\'s internal environment FQDNs.')
param vnetResourceIds array

// One private DNS zone per environment default domain (global resources).
resource zones 'Microsoft.Network/privateDnsZones@2020-06-01' = [for (zone, i) in environmentZones: {
  name: zone.defaultDomain
  location: 'global'
}]

// Wildcard A record so every app FQDN under the environment default domain
// (`<app>.<defaultDomain>`) resolves to the environment's static inbound IP.
resource wildcardA 'Microsoft.Network/privateDnsZones/A@2020-06-01' = [for (zone, i) in environmentZones: {
  parent: zones[i]
  name: '*'
  properties: {
    ttl: 300
    aRecords: [
      {
        ipv4Address: zone.staticIp
      }
    ]
  }
}]

// Full mesh of virtual-network links: every zone linked to every region VNet.
// Flattened with a single index loop (Bicep does not support nested resource
// loops, and rejects a nested for-expression here): iterate the cartesian product
// zones x vnets as one range and recover each pair by integer div / modulo.
var vnetCount = length(vnetResourceIds)
var zoneVnetLinks = [for pairIndex in range(0, length(environmentZones) * vnetCount): {
  zoneIndex: pairIndex / vnetCount
  vnetIndex: pairIndex % vnetCount
  linkKey: '${pairIndex / vnetCount}-${pairIndex % vnetCount}'
}]

resource vnetLinks 'Microsoft.Network/privateDnsZones/virtualNetworkLinks@2020-06-01' = [for link in zoneVnetLinks: {
  parent: zones[link.zoneIndex]
  name: 'link-${link.linkKey}'
  location: 'global'
  properties: {
    registrationEnabled: false
    virtualNetwork: {
      id: vnetResourceIds[link.vnetIndex]
    }
  }
}]
