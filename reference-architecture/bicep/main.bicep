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

@description('Location for the single global registry. Defaults to the resource group location, matching bootstrap.bicep so a raw-Bicep deploy (without the deploy script) converges both templates onto the same immutable registry resource. The deploy script pins this to the first region location on both passes.')
param registryLocation string = resourceGroup().location

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

@description('Silo maximum replica count. Capped at 3 by default to bound data-plane scale-out (runaway KEDA scale-up was observed at the previous ceiling of 10).')
@minValue(1)
param siloMaxReplicas int = 3

@description('OBSERVABILITY-SUBISSUE SEAM: managed-Prometheus query endpoint the silo KEDA scaler scrapes. Empty leaves every silo at its min-replica floor (the module still deploys).')
param prometheusQueryEndpoint string = ''

@description('regionCode of the single backup-PRIMARY region whose silo runs the scheduled-backup writer. That region gets Storage Blob Data Contributor on the shared backup sink; every other region is restore-read only. Must equal one regions[].regionCode. Defaults to the first region.')
param backupPrimaryRegionCode string = regions[0].regionCode

@description('Deployment option controlling ingress visibility and the cross-region replication transport. Both options provision per-region VNets (so every managed environment is VNet-injected and zone-redundancy capable). "public" (default, cost-optimized) keeps an EXTERNAL environment ingress and rides the ACA-managed FQDN over server TLS for replication, authenticated by a per-region Key Vault replication key. "private" (opt-in) makes the ingress INTERNAL-only and adds full-mesh VNet peering so the replication transport is never publicly reachable.')
@allowed([
  'public'
  'private'
])
param deploymentOption string = 'public'

@description('When true, each region managed environment is zone-redundant (replicas spread across availability zones). Every deployment option is VNet-injected, so this applies to both public and private estates. Defaults to true so estates are zone-redundant out of the box; set false to opt out (for example single-zone dev estates).')
param zoneRedundant bool = true

@description('The per-cluster Lattice replication key, matched across EVERY region and used by BOTH options (public authenticates over public ingress; private layers it on the VNet transport as defense in depth). The deployer generates it once and passes it at deploy time (never committed). Written only into each region Key Vault secret by the networking module; never emitted as an output.')
@secure()
param replicationKey string = ''

@description('PUBLIC option ingress allow-list seam: CIDR ranges permitted to reach the region ingress / front door. Empty applies no restriction here. Echoed by the networking module for the AFD sub-issue to apply.')
param ingressAllowedCidrs array = []

@description('Grafana admin password for the per-region self-hosted Grafana head. Supply via a Key Vault reference or a secure pipeline variable at deploy time (never committed). Stored only as an ACA secret. Required (no default) so a deploy cannot silently stand up an internet-facing Grafana with a blank admin password; the minimum length rejects an empty value.')
@secure()
@minLength(1)
param grafanaAdminPassword string

@description('Azure Front Door id (GUID) threaded to every client-facing head so it rejects inbound traffic that bypasses the global ingress (X-Azure-FDID origin lock). Empty on the first deploy pass (heads unlocked, Front Door not yet created); the deployer runs a SECOND compute pass supplying the frontdoor module\'s frontDoorId output. Threading frontdoor.outputs.frontDoorId here directly would form a compile cycle because Front Door consumes the head FQDNs compute exports.')
param frontDoorId string = ''

// --- Estate-wide host application configuration (bound to the reference host
//     IConfiguration contract under reference-architecture/hosts). Secure by
//     default; a throwaway dev cluster relaxes these explicitly. ---

@description('Authorization default effect for every region. Deny-by-default is the secure baseline; set "Allow" ONLY for a throwaway open dev cluster.')
@allowed([
  'Deny'
  'Allow'
])
param authDefaultEffect string = 'Deny'

@description('Whether the State/auth gRPC surfaces and the MCP endpoint require authorization. Secure default true.')
param requireApiAuthorization bool = true

@description('Runtime per-tree replication control plane, estate-wide. Secure default OFF: no sys-replication-config tree, no silo replication control gRPC binding, no MCP lattice_replication_* tools. When true the control plane is co-hosted but stays fail-closed behind the deny-by-default LatticeOperation.Replication gate (an explicit grant is required to enable/disable; not even Admin confers it).')
param enableReplicationControl bool = false

@description('MCP backup control surface, estate-wide. The silo Orleans.Lattice.Api.Backup facade is always co-hosted; this gates only whether the MCP head advertises the backup tool group (read plus the mutating capture/restore/delete verbs). Fail-closed behind the deny-by-default LatticeOperation.Backup gate when on.')
param enableBackupControl bool = false

@description('Cross-cluster anti-entropy (digest probe + Merkle-walk drift localisation + bounded automatic remediation), estate-wide. Quiet default OFF: a healthy estate converges via the forward change feed; this fallback heals divergence introduced out-of-band or after a peer outage past WAL retention. Applied symmetrically to every region.')
param enableDigestAntiEntropy bool = false

@description('Optional digest-probe cadence override in seconds when enableDigestAntiEntropy is on. 0 keeps the package default.')
param digestProbeIntervalSeconds int = 0

@description('Whether Entra authentication is enabled on the exposed facades and heads across the estate.')
param entraEnabled bool = false

@description('Entra tenant id (required when entraEnabled).')
param entraTenantId string = ''

@description('Entra application (client) id for the exposed facades / heads (required when entraEnabled).')
param entraClientId string = ''

@description('Comma-separated additional Entra token audiences accepted by the silo facades. Empty lets the host derive {clientId, api://{clientId}}.')
param entraAudiences string = ''

@description('Application (client) id of the Explorer console\'s OWN confidential web-app registration (the entra.bicep explorerClientId output) - the app that holds the OIDC redirect URIs. Distinct from entraClientId (the silo facade audience). Fed to the Explorer head Entra:WebClientId; the deployer supplies it after the entra deployment reports the output. Empty leaves the Explorer head without web sign-in.')
param explorerWebClientId string = ''

@description('Downstream State API scope the Explorer console requests on the signed-in operator\'s behalf (on-behalf-of), for example api://{tenantId}/{baseName}-silo/user_impersonation. Fed to the Explorer head Entra:Scopes. Empty lets the console resolve the scope at sign-in from the audience the State API advertises.')
param explorerAuthScope string = ''

@description('Externally visible public origin (scheme + host) operators reach the Explorer console at - the global Front Door endpoint (frontDoorEndpoints.explorer). Fed to the Explorer head Explorer:PublicOrigin so OpenID Connect builds sign-in redirect URIs against the public host, not the Front-Door-locked Container Apps origin. Azure-assigned, so the deployer threads it on a later pass. Empty leaves request scheme/host untouched.')
param explorerPublicOrigin string = ''

@description('Externally visible public URL (the resource identifier) MCP clients reach the MCP endpoint at - the global Front Door endpoint (frontDoorEndpoints.mcp). Fed to the MCP head Mcp:PublicUrl so it serves OAuth 2.0 Protected Resource Metadata (RFC 9728) discovery pointing at the Entra authorization server. Azure-assigned, so the deployer threads it on a later pass. Empty advertises no discovery document.')
param mcpPublicUrl string = ''

@description('The delegated silo scope an MCP client should request (api://{tenantId}/{baseName}-silo/user_impersonation - the same scope the Explorer console requests). Fed to the MCP head Mcp:Oauth:Scopes and emitted as the discovery document scopes_supported. Empty omits scopes_supported.')
param mcpAuthScope string = ''

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
// Per-region network foundation (VNets + optional full-mesh peering) - F-190.
// -----------------------------------------------------------------------------
// Provisioned BEFORE compute (for EVERY option) so the ACA environment can consume
// its delegated infrastructure subnet and be VNet-injected (a prerequisite for
// zone redundancy). Full-mesh peering is enabled only for the private option (its
// internal replication transport rides private space); the public option gets the
// same per-region VNets with external ingress and no peering. Keeping the VNet
// foundation ahead of compute - and the public Key Vault after compute - makes the
// compute <-> networking ordering one-directional (no compile-time cycle).
// =============================================================================

module vnet 'modules/vnet.bicep' = {
  name: 'vnet'
  params: {
    regions: regions
    enablePeering: deploymentOption == 'private'
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
    acrName: registry.name
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
    // Estate-wide host application configuration (bound to the reference host
    // IConfiguration contract). Secure-by-default: deny-by-default authorization
    // and an authorization-required API; Entra opt-in.
    authDefaultEffect: authDefaultEffect
    requireApiAuthorization: requireApiAuthorization
    // Runtime replication control plane, secure default off (fail-closed when on).
    enableReplicationControl: enableReplicationControl
    // MCP backup control surface (silo backup facade always co-hosted; gates only
    // MCP advertisement of the backup tool group). Fail-closed behind Backup gate.
    enableBackupControl: enableBackupControl
    // Cross-cluster anti-entropy: digest probe + Merkle-walk + auto-remediation.
    enableDigestAntiEntropy: enableDigestAntiEntropy
    digestProbeIntervalSeconds: digestProbeIntervalSeconds
    entraEnabled: entraEnabled
    entraTenantId: entraTenantId
    entraClientId: entraClientId
    entraAudiences: entraAudiences
    // Explorer hosted-web OIDC sign-in: the console's own confidential web-app
    // client id and the downstream State API scope it requests on-behalf-of the
    // signed-in operator. Both are empty until the entra deployment reports the
    // explorerClientId (the deployer threads them on a later pass, exactly like
    // the head client ids / redirect URIs).
    explorerWebClientId: explorerWebClientId
    explorerAuthScope: explorerAuthScope
    explorerPublicOrigin: explorerPublicOrigin
    // MCP OAuth 2.0 Protected Resource Metadata (RFC 9728) discovery: the head's
    // public resource URL (the Front Door MCP endpoint) and the silo scope a
    // client should request. Both empty until the Front Door hostname is known
    // (the deployer threads them on a later pass, like explorerPublicOrigin).
    mcpPublicUrl: mcpPublicUrl
    mcpAuthScope: mcpAuthScope
    // Every option is VNet-injected (the subnet exists for both) so the
    // environment can be zone-redundant. Public keeps external ingress; private
    // is internal-only.
    infrastructureSubnetId: vnet.outputs.perRegionNetwork[i].infrastructureSubnetId
    internalEnvironment: deploymentOption == 'private'
    // Zone-redundant compute (both options are VNet-injected).
    zoneRedundant: zoneRedundant
    // Keyless storage endpoints are DETERMINISTIC functions of
    // (resourceGroup().id, baseName, regionCode) matching the names the storage
    // module creates, so compute is fed strings and there is no module cycle.
    // The per-region table account backs the WAL, clustering, grain state and
    // reminders; the shared global blob account backs backup.
    walTableEndpoint: 'https://st${uniqueString(resourceGroup().id, baseName, region.regionCode)}.table.${environment().suffixes.storage}/'
    backupBlobEndpoint: 'https://stbk${uniqueString(resourceGroup().id, baseName)}.blob.${environment().suffixes.storage}/'
    // Blob endpoint of the SAME per-region account (deterministic, matching the
    // storage module's account name) backing the Explorer head token cache.
    tokenCacheBlobEndpoint: 'https://st${uniqueString(resourceGroup().id, baseName, region.regionCode)}.blob.${environment().suffixes.storage}/'
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
// Replication endpoint security (per-region Key Vault) - F-190.
// -----------------------------------------------------------------------------
// Invoked AFTER the compute loop because it grants Key Vault Secrets User to each
// region's workload identity (compute owns the identities). The silo consumes the
// replication key via its Key Vault secret reference; that cross-region silo env
// wiring (peer list, wire merge mode, receiver enrollment, and the KV secret ref)
// is applied symmetrically by the deployer sub-issue, which computes the per-region
// peer FQDN set. BOTH options provision the Key Vault: the public option
// authenticates replication over server-TLS public ingress with the key, and the
// private option layers the same key on top of the VNet transport as defense in
// depth.
// =============================================================================

module networking 'modules/networking.bicep' = {
  name: 'networking'
  params: {
    deploymentOption: deploymentOption
    regions: regions
    regionManagedIdentityPrincipalIds: [for (region, i) in regions: compute[i].outputs.managedIdentityPrincipalId]
    // Per-region ACA infrastructure subnet ids (Key Vault service endpoint enabled
    // in vnet.bicep) so the Key Vault firewall trusts only the region workload's
    // subnet egress (defaultAction Deny + virtualNetworkRule), for both options.
    infrastructureSubnetIds: [for (region, i) in regions: vnet.outputs.perRegionNetwork[i].infrastructureSubnetId]
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
    // Metrics ingestion: point the in-environment collector at the silo's
    // internal-network /metrics address so managed Prometheus receives real
    // series (the KEDA scaler + MCP telemetry + Grafana feed). compute exposes
    // the silo HTTP/1 port external:false and emits this host:port seam.
    siloScrapeTarget: compute[i].outputs.siloMetricsScrapeTarget
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
// Private DNS lane (cross-region internal-FQDN resolution) - private option only.
// -----------------------------------------------------------------------------
// The public option uses public DNS + Front Door, so it needs no private zones.
// The private option runs internal-ingress environments in peered VNets that have
// no automatic private DNS zone; this module publishes one customer-managed zone
// per environment default domain (wildcard A -> environment static IP) and links
// every zone to every region VNet, so cross-region replication can resolve the
// peer silo's internal FQDN. Runs AFTER the compute loop (needs each environment's
// default domain + static IP) and consumes the vnet ids, so it is one-directional.
// =============================================================================

module privateDns 'modules/privatedns.bicep' = if (deploymentOption == 'private') {
  name: 'privateDns'
  params: {
    environmentZones: [for (region, i) in regions: {
      defaultDomain: compute[i].outputs.environmentDefaultDomain
      staticIp: compute[i].outputs.environmentStaticIp
    }]
    vnetResourceIds: [for (region, i) in regions: vnet.outputs.perRegionNetwork[i].vnetId]
  }
}

// =============================================================================
// AcrPull role assignments
// -----------------------------------------------------------------------------
// Each region identity's AcrPull assignment is declared INSIDE compute.bicep
// (scoped to this shared registry, passed in by name) so the role is effective
// before that region's container-app revisions are provisioned. Declaring it
// here keyed off a compute output would order it after the apps and the first
// revision would fail to pull its image.
// =============================================================================

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

@description('Per-region Key Vault seams (keyless secret URIs, no secret material) for the deployer to wire the silo replication-key secret reference. Populated for BOTH options.')
output perRegionReplicationKeyVault array = networking.outputs.perRegionPublic

@description('Per-region VNet / infrastructure-subnet seams (every option is VNet-injected).')
output perRegionNetwork array = vnet.outputs.perRegionNetwork

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
