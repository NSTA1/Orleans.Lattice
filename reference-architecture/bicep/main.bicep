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

@description('regionCode of the single backup-PRIMARY region whose silo runs the scheduled-backup writer. That region gets Storage Blob Data Contributor on the shared backup sink; every other region is restore-read only. Must equal one regions[].regionCode. Defaults to the first region.')
param backupPrimaryRegionCode string = regions[0].regionCode

@description('Cross-region replication transport option. "public" (default, cost-optimized) rides the ACA-managed ingress FQDN over server TLS, authenticated by a replication key held in a per-region Key Vault. "private" provisions per-region VNets with delegated ACA infrastructure subnets and full-mesh peering so the transport is never publicly reachable. Private is default-OFF (opt-in).')
@allowed([
  'public'
  'private'
])
param deploymentOption string = 'public'

@description('PUBLIC option: the per-cluster Lattice replication key, matched across EVERY region. The deployer generates it once and passes it at deploy time (never committed). Written only into each region Key Vault secret by the networking module; never emitted as an output.')
@secure()
param replicationKey string = ''

@description('PUBLIC option ingress allow-list seam: CIDR ranges permitted to reach the region ingress / front door. Empty applies no restriction here. Echoed by the networking module for the AFD sub-issue to apply.')
param ingressAllowedCidrs array = []

@description('Grafana admin password for the per-region self-hosted Grafana head. Supply via a Key Vault reference or a secure pipeline variable at deploy time (never committed). Stored only as an ACA secret. Required whenever observability is deployed.')
@secure()
param grafanaAdminPassword string = ''

@description('Azure Front Door id (GUID) threaded to every client-facing head so it rejects inbound traffic that bypasses the global ingress (X-Azure-FDID origin lock). Empty on the first deploy pass (heads unlocked, Front Door not yet created); the deployer runs a SECOND compute pass supplying the frontdoor module\'s frontDoorId output. Threading frontdoor.outputs.frontDoorId here directly would form a compile cycle because Front Door consumes the head FQDNs compute exports.')
param frontDoorId string = ''

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
// Private-option network foundation (VNets + full-mesh peering) - F-190.
// -----------------------------------------------------------------------------
// Provisioned BEFORE compute (and only for the private option) so the ACA
// environment can consume its delegated infrastructure subnet. Keeping the VNet
// foundation ahead of compute - and the public Key Vault after compute - makes
// the compute <-> networking ordering one-directional (no compile-time cycle).
// =============================================================================

module vnet 'modules/vnet.bicep' = if (deploymentOption == 'private') {
  name: 'vnet'
  params: {
    regions: regions
  }
}

// =============================================================================
// Per-region compute stack
// =============================================================================

module compute 'modules/compute.bicep' = [for (region, i) in regions: {
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
    // Global-ingress origin lock. Empty on pass 1 (Front Door not yet created);
    // the deployer's second compute pass supplies frontdoor.outputs.frontDoorId.
    // Threading that output here directly would cycle (Front Door consumes the
    // head FQDNs compute exports).
    frontDoorId: frontDoorId
    // Private option: the environment is VNet-integrated and internal-only; the
    // subnet is provisioned by the vnet module above. Public option: empty
    // subnet -> public baseline environment.
    infrastructureSubnetId: deploymentOption == 'private' ? vnet!.outputs.perRegionPrivate[i].infrastructureSubnetId : ''
    internalEnvironment: deploymentOption == 'private'
    // Keyless storage endpoints are DETERMINISTIC functions of
    // (resourceGroup().id, baseName, regionCode) matching the names the storage
    // module creates, so compute is fed strings and there is no module cycle.
    // WAL and Orleans clustering share the per-region account by design.
    walTableEndpoint: 'https://st${uniqueString(resourceGroup().id, baseName, region.regionCode)}.table.${environment().suffixes.storage}/'
    clusteringTableEndpoint: 'https://st${uniqueString(resourceGroup().id, baseName, region.regionCode)}.table.${environment().suffixes.storage}/'
    backupBlobEndpoint: 'https://stbk${uniqueString(resourceGroup().id, baseName)}.blob.${environment().suffixes.storage}/'
    backupIsPrimary: region.regionCode == backupPrimaryRegionCode
  }
}]

// =============================================================================
// Storage lane (durable WAL, Orleans clustering, shared backup sink) - F-189.
// -----------------------------------------------------------------------------
// Invoked AFTER the compute loop because it consumes each region identity's
// principal id for least-privilege data-plane RBAC. The endpoints compute is fed
// above are the deterministic names this module creates, so the ordering is
// one-directional (no cycle). Keyless throughout: no keys/SAS/connection strings.
// =============================================================================

module storage 'modules/storage.bicep' = {
  name: 'storage'
  params: {
    baseName: baseName
    backupPrimaryRegionCode: backupPrimaryRegionCode
    regions: [for (region, i) in regions: {
      regionCode: region.regionCode
      location: region.location
      managedIdentityPrincipalId: compute[i].outputs.managedIdentityPrincipalId
    }]
  }
}

// =============================================================================
// Public-option replication endpoint security (per-region Key Vault) - F-190.
// -----------------------------------------------------------------------------
// Invoked AFTER the compute loop because it grants Key Vault Secrets User to each
// region's workload identity (compute owns the identities). The silo consumes the
// replication key via its Key Vault secret reference; that cross-region silo env
// wiring (peer list, wire merge mode, receiver enrollment, and the KV secret ref)
// is applied symmetrically by the deployer sub-issue, which computes the per-region
// peer FQDN set. For the private option this module provisions nothing (the VNet
// transport is the isolation boundary).
// =============================================================================

module networking 'modules/networking.bicep' = {
  name: 'networking'
  params: {
    deploymentOption: deploymentOption
    regions: regions
    regionManagedIdentityPrincipalIds: [for (region, i) in regions: compute[i].outputs.managedIdentityPrincipalId]
    replicationKey: replicationKey
    ingressAllowedCidrs: ingressAllowedCidrs
  }
}

// =============================================================================
// Observability lane (managed Prometheus + Grafana) - F-191.
// -----------------------------------------------------------------------------
// One shared metrics pipeline per region: an Azure Monitor workspace (managed
// Prometheus) feeding both the silo KEDA scaler and a scale-to-zero Grafana head.
// Invoked AFTER the compute loop (it consumes each region's environment/identity).
//
// TWO-PASS Prometheus endpoint: to keep the compute <-> observability seam
// one-directional (no compile cycle), compute's `prometheusQueryEndpoint` stays
// empty in this template, so every silo deploys at its min-replica floor. The
// deployer sub-issue (#1280) runs a SECOND compute pass that feeds
// observability[i].outputs.prometheusQueryEndpoint back into compute to activate
// the silo KEDA scaler. The endpoint is stable across redeploys.
// =============================================================================

module observability 'modules/observability.bicep' = [for (region, i) in regions: {
  name: 'observability-${region.regionCode}'
  params: {
    location: region.location
    regionCode: region.regionCode
    baseName: baseName
    environmentId: compute[i].outputs.environmentId
    managedIdentityId: compute[i].outputs.managedIdentityId
    managedIdentityPrincipalId: compute[i].outputs.managedIdentityPrincipalId
    managedIdentityClientId: compute[i].outputs.managedIdentityClientId
    grafanaAdminPassword: grafanaAdminPassword
  }
}]

// =============================================================================
// Global ingress lane (Azure Front Door Standard) - F-194.
// -----------------------------------------------------------------------------
// One global Front Door profile latency-routes users to the nearest healthy
// region across every client-facing head (Explorer, MCP, State API), failing
// over on a health-probe failure. Public option only (the private option keeps
// heads on internal ingress behind the VNet). Consumes the per-region head FQDNs
// compute exports, so it runs AFTER the compute loop and is strictly
// one-directional: compute -> frontdoor. The reverse edge (locking origins to
// the Front Door id) is applied by the deployer's second compute pass via the
// top-level frontDoorId param, avoiding a compile cycle.
// =============================================================================

module frontdoor 'modules/frontdoor.bicep' = if (deploymentOption == 'public') {
  name: 'frontdoor'
  params: {
    baseName: baseName
    origins: [for (region, i) in regions: {
      regionCode: region.regionCode
      explorerFqdn: compute[i].outputs.explorerFqdn
      mcpFqdn: compute[i].outputs.mcpFqdn
      siloStateApiFqdn: compute[i].outputs.siloStateApiFqdn
    }]
  }
}

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

@description('Per-region storage seams (keyless table endpoint backing the WAL + Orleans clustering) in region-list order.')
output perRegionStorage array = storage.outputs.perRegionStorage

@description('Keyless blob endpoint of the shared global backup sink (feeds LATTICE_BACKUP_BLOB_ENDPOINT on every silo).')
output backupBlobEndpoint string = storage.outputs.backupBlobEndpoint

@description('Name of the shared global backup blob account (single source of truth for cold-restore).')
output backupAccountName string = storage.outputs.backupAccountNameOut

@description('Selected cross-region replication transport option ("public" or "private").')
output deploymentOption string = deploymentOption

@description('PUBLIC option per-region Key Vault seams (keyless secret URIs, no secret material) for the deployer to wire the silo replication-key secret reference. Empty for the private option.')
output perRegionReplicationKeyVault array = networking.outputs.perRegionPublic

@description('PRIVATE option per-region VNet / infrastructure-subnet seams. Empty for the public option.')
output perRegionPrivateNetwork array = deploymentOption == 'private' ? vnet!.outputs.perRegionPrivate : []

@description('Per-region observability seams in region-list order. prometheusQueryEndpoint is the single feed for the silo KEDA scaler (deployer second pass) and the MCP telemetry add-on; grafanaFqdn is the scale-to-zero dashboard head.')
output perRegionObservability array = [for (region, i) in regions: {
  regionCode: region.regionCode
  prometheusQueryEndpoint: observability[i].outputs.prometheusQueryEndpoint
  azureMonitorWorkspaceId: observability[i].outputs.azureMonitorWorkspaceId
  dataCollectionRuleId: observability[i].outputs.dataCollectionRuleId
  grafanaFqdn: observability[i].outputs.grafanaFqdn
}]

@description('PUBLIC option global Front Door id (GUID). The deployer feeds this back into the top-level frontDoorId param on the second compute pass so every head locks its ingress to this Front Door (X-Azure-FDID). Empty for the private option.')
output frontDoorId string = deploymentOption == 'public' ? frontdoor!.outputs.frontDoorId : ''

@description('PUBLIC option public HTTPS hostnames of the global Front Door endpoints (*.azurefd.net) for Explorer, MCP and State API. Empty for the private option.')
output frontDoorEndpoints object = deploymentOption == 'public' ? {
  explorer: frontdoor!.outputs.explorerHostName
  mcp: frontdoor!.outputs.mcpHostName
  state: frontdoor!.outputs.stateHostName
} : {}
