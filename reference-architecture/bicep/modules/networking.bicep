// =============================================================================
// networking.bicep - public-option replication endpoint security (Key Vault)
// -----------------------------------------------------------------------------
// Sub-issue F-190 (Reference Architecture epic): the PUBLIC-option half of the
// NETWORKING lane. The private-option network foundation (per-region VNets +
// full-mesh peering) lives in the sibling module vnet.bicep, which is provisioned
// BEFORE compute; this module holds the public option's Key Vault and is
// provisioned AFTER compute (it consumes each region's workload-identity
// principalId). See vnet.bicep's header for why the two concerns are split (it
// keeps the compute <-> networking module ordering one-directional and avoids a
// compile-time dependency cycle).
//
//   'public'  - replication rides the ACA-managed ingress FQDN (server TLS, no
//               custom certificate lifecycle). The endpoint is AUTHENTICATED by
//               Lattice's per-cluster replication key, held in a per-region Key
//               Vault (RBAC-authorized, soft-delete + purge-protection ON) and
//               read by the region's workload identity via least-privilege
//               "Key Vault Secrets User". An ingress allow-list seam is exposed
//               for the coordinator to wire into the ACA ingress / front door.
//
//   'private' - see vnet.bicep. This module deploys nothing for the private
//               option (its resources are gated on the public option); private
//               replication is isolated by the VNet transport, not a Key Vault.
//
// This module owns the public Key Vault only. It creates NO container apps and
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
// =============================================================================

targetScope = 'resourceGroup'

// --- Deployment option -------------------------------------------------------

@description('Selects the cross-region replication transport. "public" provisions per-region Key Vaults for replication-key auth over server-TLS public ingress. "private" provisions nothing here (the VNet transport in vnet.bicep is the isolation boundary). Only the public option\'s resources are provisioned by this module.')
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

@description('Per-region ACA infrastructure subnet ids, index-aligned with `regions` (from vnet.bicep, which enables the Microsoft.KeyVault service endpoint on each). Added to the public option Key Vault firewall as a virtualNetworkRule so the vault data plane trusts ONLY the region workload subnet (with defaultAction Deny). Ignored by the private option.')
param infrastructureSubnetIds array = []

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

// =============================================================================
// Derived values
// =============================================================================

var isPublic = deploymentOption == 'public'

// Built-in "Key Vault Secrets User" role - read secret contents only.
var keyVaultSecretsUserRoleId = '4633458b-17de-408a-b874-0445c86b69e6'

// Key Vault names are globally unique 3-24 char DNS labels. A uniqueString
// suffix keeps them collision-free across estates while staying in bounds.
var vaultNames = [for (region, i) in regions: take(toLower('${replace(region.regionCode, '-', '')}kv${uniqueString(resourceGroup().id, region.regionCode)}'), 24)]

// Option-scoped region list keeps the output for-loop empty (and its
// conditional-resource dereferences unreached) when the public option is inactive.
var publicRegions = isPublic ? regions : []

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
    // disabled. Managed-identity reads arrive from the region's ACA workload over
    // the aca-infra subnet's Microsoft.KeyVault service endpoint, so the firewall
    // denies by default and trusts ONLY that subnet (virtualNetworkRule below).
    // bypass: AzureServices lets the Key Vault resource provider write the secret
    // during template deployment (a trusted service) without opening the data
    // plane to arbitrary networks.
    publicNetworkAccess: 'Enabled'
    enabledForDeployment: false
    enabledForTemplateDeployment: false
    enabledForDiskEncryption: false
    networkAcls: {
      bypass: 'AzureServices'
      defaultAction: 'Deny'
      virtualNetworkRules: [
        {
          id: infrastructureSubnetIds[i]
        }
      ]
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
// Outputs - seams the coordinator wires into compute / silo config
// =============================================================================

@description('The selected deployment option, echoed for downstream conditionals.')
output deploymentOption string = deploymentOption

@description('PUBLIC option per-region Key Vault seams, in region-list order. Wire `replicationKeySecretUri` (+ the region managed identity) into the silo\'s Key Vault secret reference. Contains NO secret material - only the resolvable URI of the secret. Empty for the private option.')
output perRegionPublic array = [for (region, i) in publicRegions: {
  regionCode: region.regionCode
  location: region.location
  keyVaultName: vault[i]!.name
  keyVaultUri: vault[i]!.properties.vaultUri
  replicationKeySecretName: replicationKeySecretName
  replicationKeySecretUri: '${vault[i]!.properties.vaultUri}secrets/${replicationKeySecretName}'
}]

@description('PUBLIC option ingress allow-list, echoed for the coordinator to apply to the ACA ingress ipSecurityRestrictions / front-door WAF. This module provisions no container ingress itself.')
output ingressAllowedCidrs array = ingressAllowedCidrs

