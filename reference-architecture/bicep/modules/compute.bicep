// =============================================================================
// compute.bicep - per-region compute + autoscaling foundation
// -----------------------------------------------------------------------------
// Sub-issue F-188 (Reference Architecture epic): the BASE compute module.
// Provisions, for ONE region:
//   - a user-assigned managed identity (the region's workload identity)
//   - a Log Analytics workspace capped at 1 GB/day ingestion
//   - an Azure Container Apps managed environment wired to that workspace
//   - three container apps: silo (min 1 / max 10), MCP and Explorer (min 0)
//
// This module owns the COMPUTE lane only. Storage (WAL + clustering table),
// networking (VNet / Front Door / replication transport), and observability
// (managed Prometheus + Grafana) are SEPARATE sub-issues. Named outputs at the
// bottom of this file are the seams those modules consume; named parameters at
// the top are the seams they feed in (prometheusQueryEndpoint,
// infrastructureSubnetId, key-vault / storage endpoints).
//
// Security posture (hard requirement):
//   - Image pull uses the user-assigned managed identity + AcrPull RBAC. There
//     is NO registry admin user, NO password, NO connection string here.
//   - Nothing secret-shaped is a plain parameter. The only deploy-time secret
//     touched is the Log Analytics shared key, obtained via listKeys() at
//     deployment (never stored as a parameter) because the ACA environment
//     app-logs binding requires it.
//   - Container apps run the non-root chiseled images; no privileged options.
// =============================================================================

@description('Azure region for every resource in this module (for example "westeurope").')
param location string

@description('Short lowercase region moniker used in resource names (for example "weu"). Must be unique per region in the estate.')
@minLength(2)
@maxLength(8)
param regionCode string

@description('Lowercase base name shared by every region (for example "lattice"). Combined with regionCode to name resources.')
@minLength(3)
@maxLength(16)
param baseName string

// --- Container registry seam (the registry itself is a global resource in main.bicep) ---

@description('Login server of the shared Azure Container Registry (for example "latticeacr.azurecr.io").')
param acrLoginServer string

@description('Name of the shared Azure Container Registry. Used to scope the AcrPull role assignment for this region identity to the registry resource so image pulls are authorized before the revisions are provisioned.')
param acrName string

// --- Image seams: each head references its image by repository + shared tag ---

@description('Shared image tag applied to all three built images (for example a build number or git sha).')
param imageTag string

@description('Silo host image repository name within the registry (for example "lattice-silo").')
param siloImageRepository string

@description('MCP host image repository name within the registry (for example "lattice-mcp").')
param mcpImageRepository string

@description('Explorer host image repository name within the registry (for example "lattice-explorer").')
param explorerImageRepository string

// --- Storage seams (the storage sub-issue provisions the accounts; these are
// deterministic keyless endpoint strings so there is no compute<->storage module
// dependency cycle). All access is managed-identity + RBAC; no keys/SAS here. ---

@description('STORAGE-SUBISSUE SEAM: keyless table endpoint backing the durable Azure Table WAL, Orleans clustering, grain state and reminders (one per-region account). Bound to the host Storage:TableServiceUri; consumed via AZURE_CLIENT_ID managed identity.')
param walTableEndpoint string = ''

@description('STORAGE-SUBISSUE SEAM: keyless blob endpoint of the shared global backup sink consumed by Orleans.Lattice.Backup.AzureBlob. Bound to the host Storage:BlobServiceUri.')
param backupBlobEndpoint string = ''

@description('STORAGE-SUBISSUE SEAM: true only for the single backup-PRIMARY region whose silo runs the scheduled-backup writer; standbys are restore-read only.')
param backupIsPrimary bool = false

// --- Log Analytics cost controls ---

@description('Daily Log Analytics ingestion cap in GB. Bounds ACA container-log cost; managed-Prometheus metrics are unaffected.')
@minValue(1)
param logAnalyticsDailyQuotaGb int = 1

@description('Log Analytics retention in days.')
@minValue(7)
@maxValue(730)
param logAnalyticsRetentionInDays int = 30

// --- Orleans clustering / endpoint configuration ---

@description('Orleans cluster id. Replicas of the silo app cluster under this id via Azure Table membership.')
param orleansClusterId string

@description('Orleans service id (stable across deployments of the same logical service).')
param orleansServiceId string

@description('Orleans silo-to-silo (inter-silo) port. Reached replica-to-replica within the ACA environment.')
param orleansSiloPort int = 11111

@description('Orleans gateway (client) port.')
param orleansGatewayPort int = 30000


// --- Silo autoscaling (lattice.scaling KEDA bridge) ---

@description('Silo minimum replica count. MUST be >= 1: the data plane and a membership quorum never scale to zero.')
@minValue(1)
param siloMinReplicas int = 1

@description('Silo maximum replica count. Capped at 3 by default to bound the data-plane fan-out: the KEDA Prometheus scaler can otherwise ramp the silo well past the useful working set under load, which was observed to leave regions pinned at a high replica count (runaway scale-out) long after demand subsided. Raise deliberately for a genuinely larger keyspace.')
@minValue(1)
param siloMaxReplicas int = 3

@description('Managed-Prometheus query endpoint the KEDA scaler scrapes the lattice.scaling signal from. OBSERVABILITY-SUBISSUE SEAM: empty string leaves the silo at its min-replica floor with no external scale rule wired, so this module builds and deploys standalone before observability lands.')
param prometheusQueryEndpoint string = ''

@description('Azure Front Door id (GUID) the client-facing heads assert on inbound X-Azure-FDID to reject traffic that bypasses the global ingress. FRONT-DOOR-SUBISSUE SEAM: empty string leaves the heads unlocked so this module deploys standalone before Front Door exists; the deployer runs a second compute pass supplying frontdoor.outputs.frontDoorId to activate the origin lock (Front Door consumes the head FQDNs this module outputs, so the value cannot be wired in one static pass without a cycle).')
param frontDoorId string = ''

@description('PromQL query returning the compute-axis replica-demand scalar (lattice.scaling scaleValue) aggregated across silo replicas.')
param siloScaleQuery string = 'max(lattice_scaling_scale_value{app="__SILO__"})'

@description('KEDA prometheus scaler threshold: target value of the query per replica.')
param siloScaleThreshold string = '1'

// --- Scale-to-zero heads ---

@description('Maximum replicas for the MCP head (minimum is always 0). Capped at 3 by default to match the silo ceiling and bound scale-out for this small admin surface.')
@minValue(1)
param mcpMaxReplicas int = 3

@description('Maximum replicas for the Explorer head (minimum is always 0).')
@minValue(1)
param explorerMaxReplicas int = 3

@description('Concurrent-request threshold that scales an idle (zero) MCP/Explorer head up from zero.')
param headHttpConcurrency int = 20

// --- Graceful scale-in tuning (respecting LatticeShuttingDownException) ---

@description('Seconds a draining silo replica is given to complete or hand off in-flight shard transfers before the platform force-terminates it. Must exceed the host activation-cooldown so LatticeShuttingDownException drains cleanly.')
@minValue(30)
@maxValue(600)
param siloTerminationGracePeriodSeconds int = 120

// --- Host application configuration (matches the reference host IConfiguration
//     contract: the silo/MCP/Explorer projects under reference-architecture/hosts
//     read these keys, so the ACA env var names use the .NET double-underscore
//     form for exact binding) ---

@description('Silo gRPC (HTTP/2) port serving the read-only State API and the auth-admin control plane. This is the client-facing surface fronted by the global ingress. Matches the host Silo:GrpcPort.')
param siloGrpcPort int = 8081

@description('Silo plain-HTTP (HTTP/1) port serving the liveness probe, the lattice.scaling signal and the Prometheus /metrics scrape. Matches the host Silo:HttpPort.')
param siloHttpPort int = 8080

@description('Deny-by-default authorization is the secure default for every deployed region. Set to "Allow" ONLY for a throwaway open dev cluster. Bound to the host Auth:DefaultEffect.')
@allowed([
  'Deny'
  'Allow'
])
param authDefaultEffect string = 'Deny'

@description('Whether the State/auth gRPC surfaces require authorization. Secure default true; the local compose harness sets false. Bound to the host StateApi:RequireAuthorization and the MCP Mcp:RequireAuthorization.')
param requireApiAuthorization bool = true

@description('Whether the read-write Data API surface is exposed. Enabled by default: the write-capable data-API gRPC binding is co-hosted on the silo gRPC endpoint (same origin as the read-only State API) and the MCP head advertises its write tools. Real enforcement is the deny-by-default per-tree/per-key access gate keyed on the caller subject. Set false to withhold the write surface. Bound to the host DataApi:Enabled and the MCP Mcp:EnableDataWrites.')
param dataApiEnabled bool = true

@description('Whether Entra authentication is enabled on the exposed facades and heads. Bound to the host Entra:Enabled on all three heads.')
param entraEnabled bool = false

@description('Entra tenant id (required when entraEnabled).')
param entraTenantId string = ''

@description('Entra application (client) id for the exposed facades / heads (required when entraEnabled).')
param entraClientId string = ''

@description('Comma-separated additional Entra token audiences accepted by the silo facades. When empty the host derives {clientId, api://{clientId}}.')
param entraAudiences string = ''

@description('EXPLORER WEB-OIDC SEAM: application (client) id of the Explorer console\'s own confidential web-app registration (the app holding the OIDC redirect URIs). Bound to the Explorer head Entra:WebClientId. Distinct from entraClientId (the silo facade audience). Empty leaves the Explorer head without hosted-web Entra sign-in.')
param explorerWebClientId string = ''

@description('EXPLORER WEB-OIDC SEAM: downstream State API scope the Explorer console requests on-behalf-of the signed-in operator (for example api://{tenantId}/{baseName}-silo/user_impersonation). Bound to the Explorer head Entra:Scopes. Empty lets the console resolve the scope at sign-in from the advertised audience.')
param explorerAuthScope string = ''

@description('EXPLORER WEB-OIDC SEAM: keyless blob endpoint of the per-region storage account backing the Explorer Microsoft.Identity.Web distributed token cache. Bound to the Explorer head Entra:TokenCache:BlobServiceUri; consumed via the AZURE_CLIENT_ID managed identity (container-scoped Storage Blob Data Contributor). Empty falls back to an in-memory token cache.')
param tokenCacheBlobEndpoint string = ''

@description('EXPLORER WEB-OIDC SEAM: blob container on the per-region account backing the Explorer token cache. Bound to the Explorer head Entra:TokenCache:ContainerName. Must match the storage module tokenCacheContainerName.')
param tokenCacheContainerName string = 'explorer-token-cache'

@description('EXPLORER WEB-OIDC SEAM: externally visible public origin (scheme + host) operators reach the Explorer console at - the global Front Door endpoint, for example https://{base}-explorer-{hash}.z01.azurefd.net. Bound to the Explorer head Explorer:PublicOrigin so OpenID Connect builds sign-in redirect URIs against the public host rather than the internal Container Apps origin (which is Front-Door-locked). Empty leaves request scheme/host untouched (dev/compose).')
param explorerPublicOrigin string = ''

@description('MCP OAUTH-DISCOVERY SEAM: externally visible public URL (the resource identifier) clients reach the MCP endpoint at - the global Front Door endpoint, for example https://{base}-mcp-{hash}.z01.azurefd.net. Bound to the MCP head Mcp:PublicUrl. When set (with Entra on) the head serves an OAuth 2.0 Protected Resource Metadata document (RFC 9728) at /.well-known/oauth-protected-resource and hints it on 401 challenges, so a standard MCP client discovers the Entra authorization server and signs in itself instead of needing a pre-pasted token. Empty (dev/compose, or before the Front Door hostname is known) advertises nothing.')
param mcpPublicUrl string = ''

@description('MCP OAUTH-DISCOVERY SEAM: the delegated silo scope a client should request so the token it obtains carries the audience the MCP head validates and forwards (for example api://{tenantId}/{baseName}-silo/user_impersonation - the same scope the Explorer console requests). Bound to the MCP head Mcp:Oauth:Scopes and emitted as the metadata document scopes_supported. Empty omits scopes_supported.')
param mcpAuthScope string = ''

@description('Comma-separated Entra object ids (oid claim) seeded as the estate administrators - the root of trust the deny-by-default access gate honours. The deployer sets this to the single security administrator (the deploying user by default); every other caller is refused until this administrator grants access at runtime through the Explorer Access tab. Bound to the host Auth:BootstrapAdministrators.')
param bootstrapAdministrators string = ''

@description('DEPLOYER SEAM: comma-separated clusterId=endpoint replication peers for THIS region (every OTHER region), applied symmetrically. Empty until the deployer resolves the peer FQDNs post-provision. Bound to the host Replication:Peers.')
param replicationPeers string = ''

@description('DEPLOYER SEAM: comma-separated treeName=MergeMode wire-merge-mode map, identical estate-wide. Empty until the deployer supplies it. Bound to the host Replication:Trees.')
param replicationTrees string = ''

@description('DEPLOYER SEAM: Key Vault secret URI holding the per-cluster replication key. When non-empty a managed-identity-backed ACA secret is created and surfaced to the silo as LATTICE_REPLICATION_SECRET; empty leaves it unset so the module deploys standalone before the Key Vault secret exists. Never a plaintext key.')
param replicationKeySecretUri string = ''

@description('Runtime per-tree replication control plane. Secure default OFF: leaves the sys-replication-config CRDT tree un-enrolled, the silo ILatticeReplicationControl gRPC binding un-hosted, and the MCP lattice_replication_* tools unadvertised, so a deployed estate carries no replication control surface until an operator opts in. When true the control plane is co-hosted but stays FAIL-CLOSED behind the deny-by-default LatticeOperation.Replication gate - which no other capability, not even Admin, confers - so enabling/disabling replication still requires an explicitly authored Replication grant. Bound to the silo Replication:EnableRuntimeConfig and the MCP Mcp:ReplicationEndpoint + Mcp:EnableReplicationControl.')
param enableReplicationControl bool = false

@description('MCP backup control surface. The silo Orleans.Lattice.Api.Backup gRPC facade is ALWAYS co-hosted (the scheduled writer runs only on the backup-primary region); this flag gates only whether the MCP head advertises it. When true the head points Mcp:BackupEndpoint at the silo and advertises the backup tool group (read plus the mutating capture/restore/delete verbs via Mcp:EnableBackupControl); the silo re-validates the forwarded Entra JWT and the deny-by-default LatticeOperation.Backup gate enforces per-subject. False leaves the group unadvertised. Bound to the MCP Mcp:BackupEndpoint + Mcp:EnableBackupControl.')
param enableBackupControl bool = false

@description('Cross-cluster anti-entropy: the periodic digest probe + Merkle-walk drift localisation + bounded automatic remediation that re-ships divergent key ranges to a lagging peer. Secure/quiet default OFF: a healthy estate converges via the forward change feed, so this is a fallback that heals divergence introduced out-of-band (rows written before a tree was brought into replication at runtime, or a peer offline past its WAL retention). Set symmetrically across regions. Bound to the silo Replication:EnableDigestAntiEntropy.')
param enableDigestAntiEntropy bool = false

@description('Optional override (seconds) for the digest-probe cadence when enableDigestAntiEntropy is on. 0 keeps the package default. A shorter interval reconciles drift faster at the cost of more digest traffic. Bound to the silo Replication:DigestProbeIntervalSeconds.')
param digestProbeIntervalSeconds int = 0

@description('DEPLOYER SEAM: PromQL backend address the MCP cluster-telemetry tools proxy (the managed Prometheus query endpoint). Empty leaves the telemetry tool group off (the host skips it when unset). Bound to the host Mcp:Telemetry:BackendAddress.')
param mcpTelemetryBackendAddress string = ''

@description('DEPLOYER SEAM: backend auth mode for the MCP telemetry proxy. Empty/None leaves the backend unauthenticated (local dev Prometheus). Set to DynamicBearer for an Azure Monitor managed-Prometheus endpoint, which the MCP head then queries with a rotating managed-identity Entra token (no secret). Bound to the host Mcp:Telemetry:AuthMode.')
@allowed([
  ''
  'None'
  'DynamicBearer'
])
param mcpTelemetryAuthMode string = ''


// --- Container sizing ---

@description('vCPU cores per silo replica.')
param siloCpu string = '1.0'

@description('Memory per silo replica (for example "2Gi").')
param siloMemory string = '2Gi'

@description('vCPU cores per head replica.')
param headCpu string = '0.5'

@description('Memory per head replica (for example "1Gi").')
param headMemory string = '1Gi'

// --- Networking seam (private option; the networking sub-issue owns the VNet) ---

@description('ACA infrastructure subnet resource id for the VNet-injected managed environment. In the reference architecture main.bicep supplies this for EVERY deployment option (each region has a per-region VNet), so the environment is VNet-injected and zone-redundancy capable regardless of ingress visibility. An empty string is still honoured (module stays usable standalone) and yields a non-VNet, single-zone environment.')
param infrastructureSubnetId string = ''

@description('When true, the environment ingress is internal-only (private option); when false, external (public option). Only meaningful together with an infrastructureSubnetId.')
param internalEnvironment bool = false

@description('When true, the managed environment spreads replicas across availability zones (zone-redundant compute). Azure Container Apps only supports zone redundancy on a VNet-injected environment, so this flag is honoured ONLY when an infrastructureSubnetId is supplied; without a subnet the environment is single-zone regardless. Defaults to true.')
param zoneRedundant bool = true

// =============================================================================
// Naming
// =============================================================================

var namePrefix = '${baseName}-${regionCode}'
var identityName = '${namePrefix}-id'
var lawName = '${namePrefix}-law'
var envName = '${namePrefix}-env'
var siloAppName = '${namePrefix}-silo'
var mcpAppName = '${namePrefix}-mcp'
var explorerAppName = '${namePrefix}-explorer'

var siloImage = '${acrLoginServer}/${siloImageRepository}:${imageTag}'
var mcpImage = '${acrLoginServer}/${mcpImageRepository}:${imageTag}'
var explorerImage = '${acrLoginServer}/${explorerImageRepository}:${imageTag}'

var enableSiloScaleRule = !empty(prometheusQueryEndpoint)
// Substitute the concrete silo app name into the scale query placeholder so the
// same parameter default works for every region without per-region editing.
var resolvedSiloScaleQuery = replace(siloScaleQuery, '__SILO__', siloAppName)

// =============================================================================
// User-assigned managed identity (the region's workload identity)
// -----------------------------------------------------------------------------
// One identity per region. This module wires it to ACR pull (via the AcrPull
// role assignment declared in main.bicep, scoped to the registry). The storage
// and networking sub-issues attach their own least-privilege role assignments
// (Storage Table Data Contributor, Key Vault Secrets User) to this same
// identity using the principalId output below.
// =============================================================================

resource identity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' = {
  name: identityName
  location: location
}

// =============================================================================
// AcrPull role assignment - least privilege, scoped to the shared registry
// -----------------------------------------------------------------------------
// Assigned INSIDE this module (before the container apps below) so the region
// identity already holds AcrPull when each revision is first provisioned.
// Declaring it in the parent keyed off a compute output would order it AFTER the
// apps, so the very first revision would fail to pull the image ("unable to pull
// image using Managed identity"). The assignment name is a deterministic guid()
// over the registry id + identity id + role id, so re-runs are idempotent. Each
// container app takes an explicit dependsOn to make the ordering deterministic.
// =============================================================================

var acrPullRoleId = '7f951dda-4ed3-4680-a7ca-43fe172d538d'

resource acrRef 'Microsoft.ContainerRegistry/registries@2023-11-01-preview' existing = {
  name: acrName
}

resource acrPull 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(acrRef.id, identity.id, acrPullRoleId)
  scope: acrRef
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', acrPullRoleId)
    principalId: identity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

// =============================================================================
// Log Analytics workspace (ACA container logs, capped at 1 GB/day)
// =============================================================================

resource law 'Microsoft.OperationalInsights/workspaces@2023-09-01' = {
  name: lawName
  location: location
  properties: {
    sku: {
      name: 'PerGB2018'
    }
    retentionInDays: logAnalyticsRetentionInDays
    // Hard daily ingestion cap to bound ACA log cost. Managed-Prometheus
    // metrics (the KEDA + Grafana feed) are a separate pipeline and unaffected.
    workspaceCapping: {
      dailyQuotaGb: logAnalyticsDailyQuotaGb
    }
    features: {
      enableLogAccessUsingOnlyResourcePermissions: true
    }
  }
}

// =============================================================================
// ACA managed environment
// -----------------------------------------------------------------------------
// The Log Analytics shared key is read at deploy time via listKeys(); it is
// never a parameter and never stored in the template. VNet integration is driven
// by the infrastructureSubnetId seam: the reference architecture supplies a
// subnet for every option, so the environment is VNet-injected and can be
// zone-redundant; absent a subnet the module falls back to a non-VNet, single-
// zone environment.
// =============================================================================

resource environment 'Microsoft.App/managedEnvironments@2024-03-01' = {
  name: envName
  location: location
  properties: {
    appLogsConfiguration: {
      destination: 'log-analytics'
      logAnalyticsConfiguration: {
        customerId: law.properties.customerId
        sharedKey: law.listKeys().primarySharedKey
      }
    }
    vnetConfiguration: empty(infrastructureSubnetId) ? null : {
      infrastructureSubnetId: infrastructureSubnetId
      internal: internalEnvironment
    }
    // Zone redundancy requires a VNet-injected environment, so it is honoured only
    // when a subnet is supplied (the reference architecture supplies one for both
    // options) and forced off for the fallback no-subnet case. Spreads replicas
    // across availability zones once scaled > 1.
    zoneRedundant: empty(infrastructureSubnetId) ? false : zoneRedundant
  }
}

// =============================================================================
// Silo container app (the Orleans cluster; min 1 / max 10)
// -----------------------------------------------------------------------------
// Intra-region clustering model:
//   - Azure Table membership (storage sub-issue) provides Orleans membership;
//     every replica registers in the per-region clustering table.
//   - Same-revision replica-to-replica connectivity lets replicas reach each
//     other directly on the inter-silo port. The two additionalPortMappings
//     below (external:false) expose the Orleans silo and gateway ports on the
//     environment's internal network so replicas of THIS revision address each
//     other. This is genuine multi-silo clustering, not a single-silo fallback.
//
// Orleans endpoint env: the host binds 0.0.0.0 on the silo/gateway ports and
// advertises its own replica IP (resolved by the host at start-up); the port
// numbers below must match the host's ConfigureEndpoints call.
//
// Autoscaling: driven by the lattice.scaling compute-axis signal through a
// KEDA prometheus scaler (see the scale block). minReplicas is pinned >= 1.
// =============================================================================

resource siloApp 'Microsoft.App/containerApps@2024-03-01' = {
  name: siloAppName
  location: location
  // AcrPull must be effective before the first revision is provisioned.
  dependsOn: [
    acrPull
  ]
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${identity.id}': {}
    }
  }
  properties: {
    managedEnvironmentId: environment.id
    configuration: {
      activeRevisionsMode: 'Single'
      // Per-cluster replication key, sourced from Key Vault via the region's
      // managed identity (no plaintext secret in the template). Present only when
      // the deployer has supplied the secret URI; empty otherwise so the module
      // deploys standalone before the Key Vault secret exists.
      secrets: empty(replicationKeySecretUri) ? [] : [
        {
          name: 'replication-secret'
          keyVaultUrl: replicationKeySecretUri
          identity: identity.id
        }
      ]
      // Image pull via managed identity - no admin user, no password secret.
      registries: [
        {
          server: acrLoginServer
          identity: identity.id
        }
      ]
      ingress: {
        // State API + auth-admin control plane (read-only / control gRPC over
        // HTTP/2) is the silo's client-facing surface, fronted by the global
        // Front Door and locked to it via the X-Azure-FDID assertion. The plain
        // HTTP/1 health+metrics+scaling port is internal-only (probes below).
        external: true
        targetPort: siloGrpcPort
        transport: 'http2'
        allowInsecure: false
        // Orleans replica-to-replica ports on the environment-internal network,
        // plus the HTTP/1 health+metrics+scaling port (8080) exposed
        // internal-only so the in-environment metrics collector (observability
        // module) can scrape /metrics over the environment network. The port
        // stays external:false - it is never reachable from outside the env.
        additionalPortMappings: [
          {
            external: false
            targetPort: orleansSiloPort
            exposedPort: orleansSiloPort
          }
          {
            external: false
            targetPort: orleansGatewayPort
            exposedPort: orleansGatewayPort
          }
          {
            external: false
            targetPort: siloHttpPort
            exposedPort: siloHttpPort
          }
        ]
      }
    }
    template: {
      // Platform-enforced graceful drain: ACA waits this many seconds after
      // SIGTERM before force-terminating a scaled-in replica (default is 30s).
      // Set to the shard-transfer drain budget so the host's
      // LatticeShuttingDownException path completes or hands off in-flight
      // transfers before the platform SIGKILLs the replica.
      terminationGracePeriodSeconds: siloTerminationGracePeriodSeconds
      containers: [
        {
          name: 'silo'
          image: siloImage
          resources: {
            cpu: json(siloCpu)
            memory: siloMemory
          }
          // Health probes on the plain HTTP/1 port so the platform can probe
          // without a shell and without negotiating HTTP/2. Startup gates the
          // other two until the host is up; Readiness gates traffic so a single-
          // revision rollout only cuts over once the new replica actually serves
          // /health; Liveness restarts a hung replica.
          probes: [
            {
              type: 'Startup'
              httpGet: {
                path: '/health'
                port: siloHttpPort
              }
              initialDelaySeconds: 2
              periodSeconds: 5
              failureThreshold: 24
            }
            {
              type: 'Readiness'
              httpGet: {
                path: '/health'
                port: siloHttpPort
              }
              periodSeconds: 10
              failureThreshold: 3
            }
            {
              type: 'Liveness'
              httpGet: {
                path: '/health'
                port: siloHttpPort
              }
              initialDelaySeconds: 15
              periodSeconds: 30
            }
          ]
          env: concat([
            // Cluster identity: region-scoped cluster, estate-wide service id.
            { name: 'Cluster__Id', value: orleansClusterId }
            { name: 'Cluster__ServiceId', value: orleansServiceId }
            { name: 'Replication__ClusterId', value: orleansClusterId }
            // Kestrel dual-port: HTTP/1 health+metrics+scaling, HTTP/2 gRPC.
            { name: 'Silo__HttpPort', value: string(siloHttpPort) }
            { name: 'Silo__GrpcPort', value: string(siloGrpcPort) }
            { name: 'Silo__SiloPort', value: string(orleansSiloPort) }
            { name: 'Silo__GatewayPort', value: string(orleansGatewayPort) }
            // Managed-identity client id -> DefaultAzureCredential for keyless
            // Azure Storage / Key Vault access. No account keys, no SAS.
            { name: 'AZURE_CLIENT_ID', value: identity.properties.clientId }
            // Keyless storage: the per-region table account backs Orleans
            // clustering, grain state, reminders and the durable WAL; the shared
            // global blob sink backs backup. Both via managed identity + RBAC.
            { name: 'Storage__TableServiceUri', value: walTableEndpoint }
            { name: 'Storage__BlobServiceUri', value: backupBlobEndpoint }
            // Exactly one region owns the backup schedule; standbys restore-only.
            { name: 'Backup__Primary', value: string(backupIsPrimary) }
            // Compute-axis scaling floor for the lattice.scaling signal.
            { name: 'Scaling__MinReplicas', value: string(siloMinReplicas) }
            // Secure-by-default control plane: deny-by-default authorization and
            // an authorization-required State/auth API; TLS-only replication.
            { name: 'Auth__DefaultEffect', value: authDefaultEffect }
            // Sole seeded administrator (root of trust) - the single security
            // admin the deployer supplies. Deny-by-default refuses everyone else
            // until this admin grants access via the Explorer Access tab.
            { name: 'Auth__BootstrapAdministrators', value: bootstrapAdministrators }
            { name: 'StateApi__RequireAuthorization', value: string(requireApiAuthorization) }
            // Read-write Data API surface, co-hosted on the silo gRPC endpoint.
            // Enabled by default; withheld when dataApiEnabled is false.
            { name: 'DataApi__Enabled', value: string(dataApiEnabled) }
            { name: 'Replication__AllowPlaintext', value: 'false' }
            // Symmetric cross-region replication topology (deployer supplies the
            // peer/tree maps post-provision; empty here so the module deploys
            // standalone before the peer FQDNs are known).
            { name: 'Replication__Peers', value: replicationPeers }
            { name: 'Replication__Trees', value: replicationTrees }
            // Runtime per-tree replication control plane (sys-replication-config
            // CRDT tree + ILatticeReplicationControl facade + its gRPC binding).
            // Secure default off; when on it is fail-closed behind the deny-by-
            // default LatticeOperation.Replication gate (an explicit grant is
            // required to enable/disable - not even Admin confers it).
            { name: 'Replication__EnableRuntimeConfig', value: string(enableReplicationControl) }
            // Cross-cluster anti-entropy: digest probe + Merkle-walk drift
            // localisation + bounded automatic remediation. Quiet default off;
            // heals divergence the forward change feed cannot (out-of-band or
            // post-WAL-retention). Set symmetrically across regions.
            { name: 'Replication__EnableDigestAntiEntropy', value: string(enableDigestAntiEntropy) }
            { name: 'Replication__DigestProbeIntervalSeconds', value: string(digestProbeIntervalSeconds) }
            // Entra authentication for the exposed facades.
            { name: 'Entra__Enabled', value: string(entraEnabled) }
            { name: 'Entra__TenantId', value: entraTenantId }
            { name: 'Entra__ClientId', value: entraClientId }
            { name: 'Entra__Audiences', value: entraAudiences }
            // Secret-less app-only Microsoft Graph group resolver: when Entra is on,
            // the silo authenticates to Graph with its region user-assigned managed
            // identity (federated credential on the app registration) instead of a
            // client secret. DefaultAzureCredential resolves the MI via AZURE_CLIENT_ID.
            { name: 'Entra__Graph__UseManagedIdentity', value: string(entraEnabled) }
            // Global-ingress origin lock: the head asserts inbound X-Azure-FDID
            // matches this id (deployer's second pass supplies it). Empty = off.
            { name: 'LATTICE_FRONT_DOOR_ID', value: frontDoorId }
          ], empty(replicationKeySecretUri) ? [] : [
            // Per-cluster replication key, read by the host's
            // EnvironmentVariableSecretSource. Sourced from Key Vault via a
            // managed-identity-backed ACA secret; never a plaintext value here.
            { name: 'LATTICE_REPLICATION_SECRET', secretRef: 'replication-secret' }
          ])
        }
      ]
      scale: {
        minReplicas: siloMinReplicas
        maxReplicas: siloMaxReplicas
        // KEDA prometheus scaler bridging the lattice.scaling WAL-pressure
        // signal to the silo replica count. Authenticated to managed Prometheus
        // with the region's workload identity (no scaler secret). When the
        // observability sub-issue has not yet supplied a Prometheus endpoint the
        // rule is omitted and the silo holds at its min-replica floor.
        //
        // Graceful scale-in: KEDA/ACA select a replica to drain; the platform
        // sends SIGTERM and waits terminationGracePeriodSeconds. The host's
        // LatticeShuttingDownException path completes or hands off in-flight
        // shard transfers inside that window so scale-in never severs a transfer.
        rules: enableSiloScaleRule ? [
          {
            name: 'lattice-scaling-wal-pressure'
            custom: {
              type: 'prometheus'
              metadata: {
                serverAddress: prometheusQueryEndpoint
                query: resolvedSiloScaleQuery
                threshold: siloScaleThreshold
                // Azure Monitor managed Prometheus is reached with the region's
                // user-assigned identity (workload identity), not a bearer secret.
                authModes: 'azure-workload'
                identityOwner: 'workload'
              }
            }
          }
        ] : []
      }
    }
  }
}

// =============================================================================
// MCP head (stateless remote MCP server; scale to zero)
// =============================================================================

resource mcpApp 'Microsoft.App/containerApps@2024-03-01' = {
  name: mcpAppName
  location: location
  // AcrPull must be effective before the first revision is provisioned.
  dependsOn: [
    acrPull
  ]
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${identity.id}': {}
    }
  }
  properties: {
    managedEnvironmentId: environment.id
    configuration: {
      activeRevisionsMode: 'Single'
      registries: [
        {
          server: acrLoginServer
          identity: identity.id
        }
      ]
      ingress: {
        // Client-facing MCP endpoint; locked to the global Front Door by the
        // AFD sub-issue.
        external: true
        targetPort: 8080
        transport: 'auto'
        allowInsecure: false
      }
    }
    template: {
      containers: [
        {
          name: 'mcp'
          image: mcpImage
          resources: {
            cpu: json(headCpu)
            memory: headMemory
          }
          // Health probes against /health on the ingress port. Readiness gates
          // Front Door / single-revision cutover so traffic only reaches a warm
          // replica; Startup gives a cold scale-to-zero replica time to come up
          // before liveness applies. Probes act only on running replicas, so they
          // do not defeat scale-to-zero (an idle app has nothing to probe).
          probes: [
            {
              type: 'Startup'
              httpGet: {
                path: '/health'
                port: 8080
              }
              initialDelaySeconds: 2
              periodSeconds: 5
              failureThreshold: 24
            }
            {
              type: 'Readiness'
              httpGet: {
                path: '/health'
                port: 8080
              }
              periodSeconds: 10
              failureThreshold: 3
            }
            {
              type: 'Liveness'
              httpGet: {
                path: '/health'
                port: 8080
              }
              initialDelaySeconds: 15
              periodSeconds: 30
            }
          ]
          env: [
            // Silo gRPC State + auth-admin facades, dialed over server TLS by the
            // internal ACA FQDN. One endpoint serves both surfaces.
            { name: 'Mcp__StateEndpoint', value: 'https://${siloApp.properties.configuration.ingress.fqdn}' }
            { name: 'Mcp__AuthEndpoint', value: 'https://${siloApp.properties.configuration.ingress.fqdn}' }
            // Read-write Data API: the write facade rides the same silo gRPC
            // endpoint as State/auth. When enabled, point the head at it and
            // advertise the mutating tool verbs; the silo re-validates the
            // forwarded Entra JWT and the access gate enforces per-subject.
            { name: 'Mcp__DataEndpoint', value: dataApiEnabled ? 'https://${siloApp.properties.configuration.ingress.fqdn}' : '' }
            { name: 'Mcp__EnableDataWrites', value: string(dataApiEnabled) }
            // Replication control plane: the ILatticeReplicationControl facade rides
            // the same silo gRPC endpoint. Secure default off (endpoint empty, tools
            // unadvertised); when opted in, point the head at it and advertise the
            // mutating enable/disable tools. The silo re-validates the forwarded
            // Entra JWT and the deny-by-default Replication gate enforces per-subject
            // (an explicit Replication grant is required - not even Admin confers it).
            { name: 'Mcp__ReplicationEndpoint', value: enableReplicationControl ? 'https://${siloApp.properties.configuration.ingress.fqdn}' : '' }
            { name: 'Mcp__EnableReplicationControl', value: string(enableReplicationControl) }
            // Backup control plane: the Orleans.Lattice.Api.Backup facade is always
            // co-hosted on the silo gRPC endpoint (the scheduled writer runs only on
            // the backup-primary region). This gates only MCP advertisement: when
            // opted in, point the head at the silo and advertise the backup tool
            // group (read plus the mutating capture/restore/delete verbs). The silo
            // re-validates the forwarded Entra JWT and the deny-by-default Backup
            // gate enforces per-subject.
            { name: 'Mcp__BackupEndpoint', value: enableBackupControl ? 'https://${siloApp.properties.configuration.ingress.fqdn}' : '' }
            { name: 'Mcp__EnableBackupControl', value: string(enableBackupControl) }
            // Cluster-telemetry MCP tools proxy a PromQL backend. Empty leaves the
            // group off (the host skips it) - the deployer wires the managed
            // Prometheus query endpoint (and the DynamicBearer auth mode) once the
            // observability lane is active.
            { name: 'Mcp__Telemetry__BackendAddress', value: mcpTelemetryBackendAddress }
            // Backend auth mode: DynamicBearer makes the head mint a rotating
            // managed-identity Entra token for the managed-Prometheus scope.
            { name: 'Mcp__Telemetry__AuthMode', value: mcpTelemetryAuthMode }
            // Secure-by-default: the MCP endpoint requires authorization and
            // validates the inbound Entra JWT (forwarded to the silo for re-check).
            { name: 'Mcp__RequireAuthorization', value: string(requireApiAuthorization) }
            // Stateless streamable-HTTP transport: this head sits behind an
            // active-active Front Door origin group with no session affinity, so
            // a stateful in-memory session would break ("Session not found") the
            // moment a follow-up request is routed to another region or replica.
            // Stateless makes every request self-contained while preserving the
            // per-request permission-scoped tool discovery. See hosts/Mcp/Program.cs.
            { name: 'Mcp__Stateless', value: 'true' }
            { name: 'Entra__Enabled', value: string(entraEnabled) }
            { name: 'Entra__TenantId', value: entraTenantId }
            { name: 'Entra__ClientId', value: entraClientId }
            // Validate the SAME token audience the silo facades accept, so the
            // JWT the MCP head validates and forwards is re-accepted downstream.
            // The MCP head validates a single audience (Entra:Audience); when this
            // is empty it falls back to the client id, matching the silo's own
            // {clientId, api://clientId} default. In the reference architecture this
            // carries the tenant-scoped facade identifier URI.
            { name: 'Entra__Audience', value: entraAudiences }
            // OAuth 2.0 Protected Resource Metadata (RFC 9728) discovery. When the
            // head's own public URL is set (and Entra is on), it advertises an
            // anonymous /.well-known/oauth-protected-resource document pointing at
            // the Entra authorization server and requests the silo scope, and hints
            // it on 401 challenges, so a standard MCP client (VS Code, Visual
            // Studio, Copilot) can sign in itself. Empty advertises nothing.
            { name: 'Mcp__PublicUrl', value: mcpPublicUrl }
            { name: 'Mcp__Oauth__Scopes', value: mcpAuthScope }
            { name: 'AZURE_CLIENT_ID', value: identity.properties.clientId }
            { name: 'ASPNETCORE_URLS', value: 'http://0.0.0.0:8080' }
            // Global-ingress origin lock (see silo head). Empty until pass 2.
            { name: 'LATTICE_FRONT_DOOR_ID', value: frontDoorId }
          ]
        }
      ]
      scale: {
        minReplicas: 0
        maxReplicas: mcpMaxReplicas
        rules: [
          {
            name: 'http-concurrency'
            http: {
              metadata: {
                concurrentRequests: string(headHttpConcurrency)
              }
            }
          }
        ]
      }
    }
  }
}

// =============================================================================
// Explorer head (standalone Blazor Server operator console; scale to zero).
// Isolated as its own container app so its SignalR session-affinity requirement
// stays contained to this low-traffic admin head and never taxes the silo
// cluster's scaling - the core reason the console is not co-hosted in the silo.
// =============================================================================

resource explorerApp 'Microsoft.App/containerApps@2024-03-01' = {
  name: explorerAppName
  location: location
  // AcrPull must be effective before the first revision is provisioned.
  dependsOn: [
    acrPull
  ]
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${identity.id}': {}
    }
  }
  properties: {
    managedEnvironmentId: environment.id
    configuration: {
      activeRevisionsMode: 'Single'
      registries: [
        {
          server: acrLoginServer
          identity: identity.id
        }
      ]
      ingress: {
        external: true
        targetPort: 8080
        transport: 'auto'
        allowInsecure: false
        // Blazor Server holds a stateful SignalR circuit per user, so pin each
        // client to the replica that owns its circuit when more than one replica
        // is warm.
        stickySessions: {
          affinity: 'sticky'
        }
      }
    }
    template: {
      containers: [
        {
          name: 'explorer'
          image: explorerImage
          resources: {
            cpu: json(headCpu)
            memory: headMemory
          }
          env: concat([
            // Remote silo State + auth gRPC endpoint the console dials (as a
            // gRPC / gRPC-web client) over server TLS. Seeds the console's
            // first-run connection via the Explorer env bootstrap.
            { name: 'LATTICE_EXPLORER_ENDPOINT', value: 'https://${siloApp.properties.configuration.ingress.fqdn}' }
            // Hosted-web Entra (OpenID Connect) sign-in when enabled. The browser
            // signs in against the console's OWN confidential web-app registration
            // (Entra__WebClientId), a downstream State API token is acquired
            // on-behalf-of the operator for Entra__Scopes, and the silo
            // authenticator re-validates it. The token cache is a distributed blob
            // cache over the per-region account so tokens are shared across warm
            // replicas and survive restart.
            { name: 'Entra__Enabled', value: string(entraEnabled) }
            { name: 'Entra__TenantId', value: entraTenantId }
            { name: 'Entra__WebClientId', value: explorerWebClientId }
            { name: 'Entra__Scopes', value: explorerAuthScope }
            { name: 'Entra__TokenCache__BlobServiceUri', value: tokenCacheBlobEndpoint }
            { name: 'Entra__TokenCache__ContainerName', value: tokenCacheContainerName }
            // Public origin operators reach the console at (the global Front Door
            // endpoint) so OpenID Connect builds sign-in redirect URIs against the
            // public host, not the Front-Door-locked Container Apps origin. Empty
            // until pass 2 (the Front Door hostname is Azure-assigned).
            { name: 'Explorer__PublicOrigin', value: explorerPublicOrigin }
            { name: 'AZURE_CLIENT_ID', value: identity.properties.clientId }
            { name: 'ASPNETCORE_URLS', value: 'http://0.0.0.0:8080' }
            // Global-ingress origin lock (see silo head). Empty until pass 2.
            { name: 'LATTICE_FRONT_DOOR_ID', value: frontDoorId }
          ], empty(frontDoorId) ? [] : [
            // The console dials the silo origin FQDN directly over native gRPC,
            // which Front Door cannot proxy, so it must present the origin-lock
            // header itself to pass the silo's X-Azure-FDID assertion. Seeds the
            // Explorer transport-header seam (sign-in-independent, non-secret).
            { name: 'LATTICE_EXPLORER_TRANSPORT_HEADERS', value: 'X-Azure-FDID=${frontDoorId}' }
          ])
        }
      ]
      scale: {
        minReplicas: 0
        maxReplicas: explorerMaxReplicas
        rules: [
          {
            name: 'http-concurrency'
            http: {
              metadata: {
                concurrentRequests: string(headHttpConcurrency)
              }
            }
          }
        ]
      }
    }
  }
}

// =============================================================================
// Outputs - module seams for the dependent sub-issues
// =============================================================================

@description('Resource id of the ACA managed environment (networking / observability / additional-app seam).')
output environmentId string = environment.id

@description('Default domain of the ACA managed environment.')
output environmentDefaultDomain string = environment.properties.defaultDomain

@description('Resource id of the region user-assigned managed identity (AcrPull role assignment target in main.bicep).')
output managedIdentityId string = identity.id

@description('Principal (object) id of the region managed identity. STORAGE / NETWORKING SEAM: target for Storage Table Data Contributor and Key Vault Secrets User role assignments.')
output managedIdentityPrincipalId string = identity.properties.principalId

@description('Client (application) id of the region managed identity, used by the hosts for token-based Azure access.')
output managedIdentityClientId string = identity.properties.clientId

@description('Resource id of the region Log Analytics workspace (observability seam).')
output logAnalyticsWorkspaceId string = law.id

@description('Silo container app name.')
output siloAppName string = siloApp.name

@description('Silo internal-network metrics scrape target (host:port). The observability module points its in-environment OpenTelemetry collector at this address to scrape the silo /metrics endpoint over the ACA environment network. Reachable only from inside the environment (the port is exposed external:false).')
output siloMetricsScrapeTarget string = '${siloApp.name}:${siloHttpPort}'

@description('Silo State API FQDN (AFD origin seam for the read surface).')
output siloStateApiFqdn string = siloApp.properties.configuration.ingress.fqdn

@description('MCP head FQDN (AFD origin seam).')
output mcpFqdn string = mcpApp.properties.configuration.ingress.fqdn

@description('Explorer head FQDN (AFD origin seam).')
output explorerFqdn string = explorerApp.properties.configuration.ingress.fqdn
