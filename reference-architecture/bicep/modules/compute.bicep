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

@description('Silo maximum replica count.')
@minValue(1)
param siloMaxReplicas int = 10

@description('Managed-Prometheus query endpoint the KEDA scaler scrapes the lattice.scaling signal from. OBSERVABILITY-SUBISSUE SEAM: empty string leaves the silo at its min-replica floor with no external scale rule wired, so this module builds and deploys standalone before observability lands.')
param prometheusQueryEndpoint string = ''

@description('Azure Front Door id (GUID) the client-facing heads assert on inbound X-Azure-FDID to reject traffic that bypasses the global ingress. FRONT-DOOR-SUBISSUE SEAM: empty string leaves the heads unlocked so this module deploys standalone before Front Door exists; the deployer runs a second compute pass supplying frontdoor.outputs.frontDoorId to activate the origin lock (Front Door consumes the head FQDNs this module outputs, so the value cannot be wired in one static pass without a cycle).')
param frontDoorId string = ''

@description('PromQL query returning the compute-axis replica-demand scalar (lattice.scaling scaleValue) aggregated across silo replicas.')
param siloScaleQuery string = 'max(lattice_scaling_scale_value{app="__SILO__"})'

@description('KEDA prometheus scaler threshold: target value of the query per replica.')
param siloScaleThreshold string = '1'

// --- Scale-to-zero heads ---

@description('Maximum replicas for the MCP head (minimum is always 0).')
@minValue(1)
param mcpMaxReplicas int = 5

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

@description('Whether Entra authentication is enabled on the exposed facades and heads. Bound to the host Entra:Enabled on all three heads.')
param entraEnabled bool = false

@description('Entra tenant id (required when entraEnabled).')
param entraTenantId string = ''

@description('Entra application (client) id for the exposed facades / heads (required when entraEnabled).')
param entraClientId string = ''

@description('Comma-separated additional Entra token audiences accepted by the silo facades. When empty the host derives {clientId, api://{clientId}}.')
param entraAudiences string = ''

@description('DEPLOYER SEAM: comma-separated clusterId=endpoint replication peers for THIS region (every OTHER region), applied symmetrically. Empty until the deployer resolves the peer FQDNs post-provision. Bound to the host Replication:Peers.')
param replicationPeers string = ''

@description('DEPLOYER SEAM: comma-separated treeName=MergeMode wire-merge-mode map, identical estate-wide. Empty until the deployer supplies it. Bound to the host Replication:Trees.')
param replicationTrees string = ''

@description('DEPLOYER SEAM: Key Vault secret URI holding the per-cluster replication key. When non-empty a managed-identity-backed ACA secret is created and surfaced to the silo as LATTICE_REPLICATION_SECRET; empty leaves it unset so the module deploys standalone before the Key Vault secret exists. Never a plaintext key.')
param replicationKeySecretUri string = ''

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

@description('NETWORKING-SUBISSUE SEAM: resource id of the ACA infrastructure subnet for VNet-integrated (private) environments. Empty string provisions a public managed environment (the baseline public option).')
param infrastructureSubnetId string = ''

@description('When true and an infrastructure subnet is supplied, the environment ingress is internal-only (private option).')
param internalEnvironment bool = false

@description('When true, the managed environment spreads replicas across availability zones (zone-redundant compute). Azure Container Apps only supports zone redundancy on a VNet-injected environment, so this flag is honoured ONLY when an infrastructureSubnetId is supplied; a public (non-VNet) environment is always single-zone regardless. Defaults to true so the private option is zone-redundant out of the box.')
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
// never a parameter and never stored in the template. VNet integration is a
// networking-sub-issue seam: absent an infrastructure subnet the environment is
// the public baseline.
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
    // Zone redundancy requires a VNet-injected environment, so it is forced off
    // for the public (no-subnet) baseline and honoured only under the private
    // option. Spreads replicas across availability zones once scaled > 1.
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
        // Orleans replica-to-replica ports on the environment-internal network.
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
          // Liveness on the plain HTTP/1 port so the platform can probe without
          // a shell and without negotiating HTTP/2.
          probes: [
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
            { name: 'StateApi__RequireAuthorization', value: string(requireApiAuthorization) }
            { name: 'Replication__AllowPlaintext', value: 'false' }
            // Symmetric cross-region replication topology (deployer supplies the
            // peer/tree maps post-provision; empty here so the module deploys
            // standalone before the peer FQDNs are known).
            { name: 'Replication__Peers', value: replicationPeers }
            { name: 'Replication__Trees', value: replicationTrees }
            // Entra authentication for the exposed facades.
            { name: 'Entra__Enabled', value: string(entraEnabled) }
            { name: 'Entra__TenantId', value: entraTenantId }
            { name: 'Entra__ClientId', value: entraClientId }
            { name: 'Entra__Audiences', value: entraAudiences }
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
          env: [
            // Silo gRPC State + auth-admin facades, dialed over server TLS by the
            // internal ACA FQDN. One endpoint serves both surfaces.
            { name: 'Mcp__StateEndpoint', value: 'https://${siloApp.properties.configuration.ingress.fqdn}' }
            { name: 'Mcp__AuthEndpoint', value: 'https://${siloApp.properties.configuration.ingress.fqdn}' }
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
            { name: 'Entra__Enabled', value: string(entraEnabled) }
            { name: 'Entra__TenantId', value: entraTenantId }
            { name: 'Entra__ClientId', value: entraClientId }
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
          env: [
            // Remote silo State + auth gRPC endpoint the console dials (as a
            // gRPC / gRPC-web client) over server TLS. Seeds the console's
            // first-run connection via the Explorer env bootstrap.
            { name: 'LATTICE_EXPLORER_ENDPOINT', value: 'https://${siloApp.properties.configuration.ingress.fqdn}' }
            // Interactive Entra sign-in when enabled; the acquired token is
            // attached to calls and re-validated by the silo authenticator.
            { name: 'Entra__Enabled', value: string(entraEnabled) }
            { name: 'Entra__TenantId', value: entraTenantId }
            { name: 'Entra__ClientId', value: entraClientId }
            { name: 'AZURE_CLIENT_ID', value: identity.properties.clientId }
            { name: 'ASPNETCORE_URLS', value: 'http://0.0.0.0:8080' }
            // Global-ingress origin lock (see silo head). Empty until pass 2.
            { name: 'LATTICE_FRONT_DOOR_ID', value: frontDoorId }
          ]
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

@description('Silo State API FQDN (AFD origin seam for the read surface).')
output siloStateApiFqdn string = siloApp.properties.configuration.ingress.fqdn

@description('MCP head FQDN (AFD origin seam).')
output mcpFqdn string = mcpApp.properties.configuration.ingress.fqdn

@description('Explorer head FQDN (AFD origin seam).')
output explorerFqdn string = explorerApp.properties.configuration.ingress.fqdn
