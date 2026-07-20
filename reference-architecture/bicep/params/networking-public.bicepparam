// =============================================================================
// networking-public.bicepparam - PUBLIC replication transport example.
// -----------------------------------------------------------------------------
// Provisions a per-region Key Vault holding the shared replication key; the
// replication transport rides the ACA-managed ingress FQDN (server TLS). The
// replication key is a @secure() value the deployer supplies at deploy time
// (for example: --parameters replicationKey=$(openssl rand -base64 48)); it is
// intentionally left empty here so no secret is committed.
// =============================================================================

using '../modules/networking.bicep'

param deploymentOption = 'public'

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

// Map from compute outputs in main.bicep:
//   [for (r, i) in regions: compute[i].outputs.managedIdentityPrincipalId]
param regionManagedIdentityPrincipalIds = [
  '00000000-0000-0000-0000-000000000000'
  '00000000-0000-0000-0000-000000000000'
]

// Supplied at deploy time - never committed. See header.
param replicationKey = ''

param ingressAllowedCidrs = [
  '203.0.113.0/24'
]
