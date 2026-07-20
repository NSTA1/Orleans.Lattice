// =============================================================================
// main.bicep - Reference Architecture compute orchestrator (sub-issue F-188)
// -----------------------------------------------------------------------------
// Stands up the estate-wide COMPUTE foundation from a single parameter set:
//   - ONE global Azure Container Registry (shared by every region, as per the
//     design: three chiseled images pulled by every region). Justification for a
//     single registry: the images are identical across regions, a single
//     registry removes N-way image replication/version skew, and cross-region
//     pull latency at start-up is not on the request hot path. Registry
//     geo-replication (Premium) is a documented upgrade seam, not the baseline.
//   - The per-region compute module (modules/compute.bicep) looped over an
//     arbitrary region list. One region or N regions deploy from the same set.
//   - One least-privilege AcrPull role assignment per region identity, scoped to
//     the registry only. NO registry admin user, NO passwords.
//
// Storage, networking, observability, and AFD are SEPARATE sub-issues; this
// orchestrator exposes named outputs (per-region identities, environments,
// head FQDNs, the registry) for them to consume.
// =============================================================================

targetScope = 'resourceGroup'

@description('Lowercase base name shared by every region and the registry (for example "lattice").')
@minLength(3)
@maxLength(16)
param baseName string

@description('Region list. Each entry deploys a full per-region compute stack. One entry or many, from the same parameter set. Each item: { location, regionCode }.')
@minLength(1)
param regions array

@description('Location for the single global registry. Defaults to the first region location.')
param registryLocation string = regions[0].location

@description('Shared image tag applied to all three built images across every region.')
param imageTag string

@description('Silo host image repository name within the registry.')
param siloImageRepository string = 'lattice-silo'

@description('MCP host image repository name within the registry.')
param mcpImageRepository string = 'lattice-mcp'

@description('Explorer host image repository name within the registry.')
param explorerImageRepository string = 'lattice-explorer'

@description('Orleans service id, stable across the estate (each region is its own Orleans cluster; the cluster id is derived per region).')
param orleansServiceId string = baseName

@description('Daily Log Analytics ingestion cap in GB per region.')
@minValue(1)
param logAnalyticsDailyQuotaGb int = 1

@description('Log Analytics retention in days per region.')
@minValue(7)
@maxValue(730)
param logAnalyticsRetentionInDays int = 30

@description('Silo minimum replica count (MUST be >= 1).')
@minValue(1)
param siloMinReplicas int = 1

@description('Silo maximum replica count.')
@minValue(1)
param siloMaxReplicas int = 10

@description('OBSERVABILITY-SUBISSUE SEAM: managed-Prometheus query endpoint the silo KEDA scaler scrapes. Empty leaves every silo at its min-replica floor (the module still deploys).')
param prometheusQueryEndpoint string = ''

// AcrPull built-in role definition id.
var acrPullRoleId = '7f951dda-4ed3-4680-a7ca-43fe172d538d'

// ACR names are globally unique DNS labels, so a uniqueString suffix keeps the
// registry name collision-free across subscriptions/estates and guarantees the
// 5-50 char length constraint.
var registryName = toLower('${replace(baseName, '-', '')}acr${uniqueString(resourceGroup().id)}')

// =============================================================================
// Global Azure Container Registry
// -----------------------------------------------------------------------------
// adminUserEnabled is explicitly false: image pull is by managed identity +
// AcrPull RBAC only. No password ever leaves this template.
// =============================================================================

resource registry 'Microsoft.ContainerRegistry/registries@2023-11-01-preview' = {
  name: registryName
  location: registryLocation
  sku: {
    name: 'Standard'
  }
  properties: {
    adminUserEnabled: false
    publicNetworkAccess: 'Enabled'
    anonymousPullEnabled: false
  }
}

// =============================================================================
// Per-region compute stack
// =============================================================================

module compute 'modules/compute.bicep' = [for region in regions: {
  name: 'compute-${region.regionCode}'
  params: {
    location: region.location
    regionCode: region.regionCode
    baseName: baseName
    acrLoginServer: registry.properties.loginServer
    imageTag: imageTag
    siloImageRepository: siloImageRepository
    mcpImageRepository: mcpImageRepository
    explorerImageRepository: explorerImageRepository
    logAnalyticsDailyQuotaGb: logAnalyticsDailyQuotaGb
    logAnalyticsRetentionInDays: logAnalyticsRetentionInDays
    // Each region is its own Orleans cluster; replication (a separate sub-issue)
    // connects them. Cluster id is region-scoped, service id is estate-wide.
    orleansClusterId: '${baseName}-${region.regionCode}'
    orleansServiceId: orleansServiceId
    siloMinReplicas: siloMinReplicas
    siloMaxReplicas: siloMaxReplicas
    prometheusQueryEndpoint: prometheusQueryEndpoint
  }
}]

// =============================================================================
// AcrPull role assignments - least privilege, scoped to the registry
// -----------------------------------------------------------------------------
// One assignment per region identity. Scope is the registry resource only (not
// the resource group or subscription), so each identity can pull images and do
// nothing else on the registry.
// =============================================================================

resource acrPull 'Microsoft.Authorization/roleAssignments@2022-04-01' = [for (region, i) in regions: {
  name: guid(registry.id, region.regionCode, acrPullRoleId)
  scope: registry
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', acrPullRoleId)
    principalId: compute[i].outputs.managedIdentityPrincipalId
    principalType: 'ServicePrincipal'
  }
}]

// =============================================================================
// Outputs - estate-wide seams for storage / networking / observability / AFD
// =============================================================================

@description('Login server of the shared registry.')
output acrLoginServer string = registry.properties.loginServer

@description('Resource id of the shared registry.')
output acrId string = registry.id

@description('Per-region compute seams (identity, environment, Log Analytics, and head FQDNs) in region-list order.')
output perRegion array = [for (region, i) in regions: {
  regionCode: region.regionCode
  location: region.location
  environmentId: compute[i].outputs.environmentId
  environmentDefaultDomain: compute[i].outputs.environmentDefaultDomain
  managedIdentityId: compute[i].outputs.managedIdentityId
  managedIdentityPrincipalId: compute[i].outputs.managedIdentityPrincipalId
  managedIdentityClientId: compute[i].outputs.managedIdentityClientId
  logAnalyticsWorkspaceId: compute[i].outputs.logAnalyticsWorkspaceId
  siloAppName: compute[i].outputs.siloAppName
  siloStateApiFqdn: compute[i].outputs.siloStateApiFqdn
  mcpFqdn: compute[i].outputs.mcpFqdn
  explorerFqdn: compute[i].outputs.explorerFqdn
}]
