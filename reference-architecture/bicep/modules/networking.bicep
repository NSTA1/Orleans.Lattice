// =============================================================================
// networking.bicep - per-estate replication transport + endpoint security
// -----------------------------------------------------------------------------
// Sub-issue F-190 (Reference Architecture epic): the NETWORKING lane.
//
// A single `deploymentOption` parameter selects the cross-region replication
// transport and provisions ONLY the resources that option needs:
//
//   'public'  - replication rides the ACA-managed ingress FQDN (server TLS, no
//               custom certificate lifecycle). The endpoint is AUTHENTICATED by
//               Lattice's per-cluster replication key, held in a per-region Key
//               Vault (RBAC-authorized, soft-delete + purge-protection ON) and
//               read by the region's workload identity via least-privilege
//               "Key Vault Secrets User". An ingress allow-list seam is exposed
//               for the coordinator to wire into the ACA ingress / front door.
//
//   'private' - a VNet per region with an infrastructure subnet delegated to
//               Microsoft.App/environments, full-mesh (global) VNet peering
//               between every region, and an internal-only environment seam so
//               the replication transport is never publicly reachable. Emits
//               `infrastructureSubnetId` (+ `internalEnvironment = true`) to feed
//               compute.bicep's VNet-integration seam.
//
// This module owns the NETWORKING lane only. It creates NO container apps and
// NO managed environments (compute.bicep owns those) - it feeds compute via the
// named outputs at the bottom and is fed the per-region workload-identity
// principalIds via the parameters at the top.
//
// Security posture (hard requirement):
//   - Key Vault uses AZURE RBAC authorization (enableRbacAuthorization: true),
//     NOT vault access policies. Soft-delete and purge-protection are ON.
//   - The replication key is ONLY ever a @secure() parameter written into a Key
//     Vault secret. It is NEVER a plain parameter, NEVER an output.
//   - Access is granted by the built-in "Key Vault Secrets User" role scoped to
//     the individual vault (least privilege), to the region MANAGED IDENTITY.
//   - The private option's internal environment has no public ingress, so the
//     replication transport carries no public exposure.
// =============================================================================

targetScope = 'resourceGroup'

// --- Deployment option -------------------------------------------------------

@description('Selects the cross-region replication transport. "public" provisions per-region Key Vaults for replication-key auth over server-TLS public ingress; "private" provisions per-region VNets, delegated infrastructure subnets and full-mesh peering for a transport that is never publicly reachable. Only the selected option\'s resources are provisioned.')
@allowed([
  'public'
  'private'
])
param deploymentOption string

// --- Region list (N-region parameterisation) ---------------------------------

@description('Region list, in the SAME order as compute.bicep\'s region list. Each item: { location, regionCode }. One entry or many, from the same parameter set.')
@minLength(1)
param regions array

@description('Per-region workload-identity principalIds, index-aligned with `regions`. In main.bicep map this from compute: [for (r, i) in regions: compute[i].outputs.managedIdentityPrincipalId]. Used to grant least-privilege Key Vault access in the public option; ignored by the private option.')
param regionManagedIdentityPrincipalIds array = []

// --- Public option: replication key + Key Vault ------------------------------

@description('The per-cluster Lattice replication key/secret, matched across EVERY region. The deployer generates it once and passes it here. Written verbatim into each region\'s Key Vault secret; never emitted as an output. Used only by the public option.')
@secure()
param replicationKey string = ''

@description('Name of the Key Vault secret that holds the replication key. Must be identical across regions so the silo config references one stable name.')
@minLength(1)
@maxLength(127)
param replicationKeySecretName string = 'lattice-replication-key'

@description('Public option ingress allow-list seam: CIDR ranges permitted to reach the region ingress (and the front door). Empty means "no restriction applied here" - the coordinator wires these into the ACA ingress ipSecurityRestrictions / AFD WAF. Surfaced as an output; this module provisions no container ingress itself.')
param ingressAllowedCidrs array = []

// --- Private option: VNet address planning -----------------------------------

@description('Private option: the /8-or-larger supernet from which each region\'s non-overlapping VNet space is carved by region index. Region i gets 10.{base+i}.0.0/16 by default. Override `regionVnetAddressPrefixes` for explicit control.')
param privateVnetSupernetSecondOctetBase int = 10

@description('Private option: explicit per-region VNet address prefix, index-aligned with `regions`. Empty derives 10.{privateVnetSupernetSecondOctetBase + i}.0.0/16 per region (guaranteed non-overlapping so global peering is valid).')
param regionVnetAddressPrefixes array = []

@description('Private option: explicit per-region ACA infrastructure subnet prefix, index-aligned with `regions`. Empty derives a /23 at the start of the region VNet (ACA consumption environments require a /23 or larger, non-overlapping infrastructure subnet).')
param regionInfrastructureSubnetPrefixes array = []

// =============================================================================
// Derived values
// =============================================================================

var isPublic = deploymentOption == 'public'
var isPrivate = deploymentOption == 'private'

// Built-in "Key Vault Secrets User" role - read secret contents only.
var keyVaultSecretsUserRoleId = '4633458b-17de-408a-b874-0445c86b69e6'

// Key Vault names are globally unique 3-24 char DNS labels. A uniqueString
// suffix keeps them collision-free across estates while staying in bounds.
var vaultNames = [for (region, i) in regions: take(toLower('${replace(region.regionCode, '-', '')}kv${uniqueString(resourceGroup().id, region.regionCode)}'), 24)]

// Non-overlapping address plan per region (index-derived unless overridden).
var vnetAddressPrefixes = [for (region, i) in regions: empty(regionVnetAddressPrefixes) ? '${privateVnetSupernetSecondOctetBase + i}.0.0/16' : regionVnetAddressPrefixes[i]]
var infraSubnetPrefixes = [for (region, i) in regions: empty(regionInfrastructureSubnetPrefixes) ? '${privateVnetSupernetSecondOctetBase + i}.0.0/23' : regionInfrastructureSubnetPrefixes[i]]

// Full-mesh ordered peering pairs (i -> j) as a single flat loop. The pair
// index k maps to (from = k / N, to = k % N); i == j is skipped at the resource.
var regionCount = length(regions)
var peeringPairs = [for k in range(0, regionCount * regionCount): {
  fromIndex: k / regionCount
  toIndex: k % regionCount
}]

// Option-scoped region lists keep the output for-loops empty (and their
// conditional-resource dereferences unreached) when the option is inactive.
var publicRegions = isPublic ? regions : []
var privateRegions = isPrivate ? regions : []

// =============================================================================
// PUBLIC OPTION - per-region Key Vault holding the replication key
// =============================================================================

resource vault 'Microsoft.KeyVault/vaults@2023-07-01' = [for (region, i) in regions: if (isPublic) {
  name: vaultNames[i]
  location: region.location
  properties: {
    tenantId: subscription().tenantId
    sku: {
      family: 'A'
      name: 'standard'
    }
    // RBAC authorization (not access policies) is the hard requirement.
    enableRbacAuthorization: true
    // Recoverability guarantees for the estate's only secret.
    enableSoftDelete: true
    softDeleteRetentionInDays: 90
    enablePurgeProtection: true
    // Key Vault enforces TLS 1.2 as its floor; deployment/template access is
    // disabled. Managed-identity reads from ACA arrive over the public endpoint
    // (ACA egress IPs are dynamic), so publicNetworkAccess stays Enabled but the
    // network ACL trusts only Azure services by default.
    publicNetworkAccess: 'Enabled'
    enabledForDeployment: false
    enabledForTemplateDeployment: false
    enabledForDiskEncryption: false
    networkAcls: {
      bypass: 'AzureServices'
      defaultAction: 'Allow'
    }
  }
}]

// The replication key secret - the ONLY place the @secure() key is materialised.
resource replicationKeySecret 'Microsoft.KeyVault/vaults/secrets@2023-07-01' = [for (region, i) in regions: if (isPublic) {
  parent: vault[i]
  name: replicationKeySecretName
  properties: {
    value: replicationKey
  }
}]

// Least-privilege grant: the region workload identity may READ secrets from its
// own vault and nothing else. Scoped to the individual vault resource.
resource kvSecretsUser 'Microsoft.Authorization/roleAssignments@2022-04-01' = [for (region, i) in regions: if (isPublic) {
  name: guid(vault[i].id, regionManagedIdentityPrincipalIds[i], keyVaultSecretsUserRoleId)
  scope: vault[i]
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', keyVaultSecretsUserRoleId)
    principalId: regionManagedIdentityPrincipalIds[i]
    principalType: 'ServicePrincipal'
  }
}]

// =============================================================================
// PRIVATE OPTION - per-region VNet, delegated infra subnet, full-mesh peering
// =============================================================================

resource vnet 'Microsoft.Network/virtualNetworks@2023-11-01' = [for (region, i) in regions: if (isPrivate) {
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
resource peering 'Microsoft.Network/virtualNetworks/virtualNetworkPeerings@2023-11-01' = [for pair in peeringPairs: if (isPrivate && pair.fromIndex != pair.toIndex) {
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
// Outputs - seams the coordinator wires into compute / silo config
// =============================================================================

@description('The selected deployment option, echoed for downstream conditionals.')
output deploymentOption string = deploymentOption

@description('Feeds compute.bicep\'s `internalEnvironment` seam. True for the private option (internal-only ingress), false for the public option.')
output internalEnvironment bool = isPrivate

@description('PUBLIC option per-region Key Vault seams, in region-list order. Wire `replicationKeySecretUri` (+ the region managed identity) into the silo\'s Key Vault secret reference. Contains NO secret material - only the resolvable URI of the secret. Empty for the private option.')
output perRegionPublic array = [for (region, i) in publicRegions: {
  regionCode: region.regionCode
  location: region.location
  keyVaultName: vault[i]!.name
  keyVaultUri: vault[i]!.properties.vaultUri
  replicationKeySecretName: replicationKeySecretName
  replicationKeySecretUri: '${vault[i]!.properties.vaultUri}secrets/${replicationKeySecretName}'
}]

@description('PRIVATE option per-region network seams, in region-list order. Feed `infrastructureSubnetId` into compute.bicep\'s VNet-integration seam per region. Empty for the public option.')
output perRegionPrivate array = [for (region, i) in privateRegions: {
  regionCode: region.regionCode
  location: region.location
  vnetId: vnet[i]!.id
  infrastructureSubnetId: '${vnet[i]!.id}/subnets/aca-infra'
}]

@description('PUBLIC option ingress allow-list, echoed for the coordinator to apply to the ACA ingress ipSecurityRestrictions / front-door WAF. This module provisions no container ingress itself.')
output ingressAllowedCidrs array = ingressAllowedCidrs
