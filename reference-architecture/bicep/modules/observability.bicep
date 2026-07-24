// =============================================================================
// observability.bicep - per-region observability foundation (sub-issue F-191)
// -----------------------------------------------------------------------------
// Reference Architecture epic. Provisions, for ONE region, the SINGLE metrics
// pipeline that feeds both the silo KEDA autoscaler and the operator dashboards:
//
//   - an Azure Monitor workspace (Microsoft.Monitor/accounts) = managed
//     Prometheus, exposing a Prometheus-compatible query endpoint;
//   - a Data Collection Endpoint (DCE) + Data Collection Rule (DCR) carrying the
//     Microsoft-PrometheusMetrics stream into that workspace, so the silo, MCP
//     and Explorer container apps' scraped metrics land in one place;
//   - a least-privilege Monitoring Data Reader role assignment granting the
//     region's user-assigned managed identity read access to the workspace
//     (consumed by both the KEDA scaler and Grafana - no keys, no secrets);
//   - a self-hosted Grafana head: stock grafana/grafana-oss, deployed into the
//     region's ACA environment, min 0 / max 1 (scale to zero), stateless (NO
//     database, NO persistent volume), provisioned on start-up with the managed
//     Prometheus datasource (managed-identity auth) and the bundled
//     Orleans.Lattice.Dashboards via ephemeral secret-volume config.
//
// This module owns the OBSERVABILITY lane only and edits NOTHING outside this
// file + modules/grafana/*. It is per-region, mirroring modules/compute.bicep:
// the orchestrator loops it over the region list, feeding one region's compute
// seams in and wiring the prometheusQueryEndpoint output back into compute on a
// second pass.
//
// -----------------------------------------------------------------------------
// WIRING RECIPE (applied by the orchestrator / deployer in main.bicep)
// -----------------------------------------------------------------------------
// 1) main.bicep module call (per region, after the compute module):
//
//      module observability 'modules/observability.bicep' = [for (region, i) in regions: {
//        name: 'observability-${region.regionCode}'
//        params: {
//          location: region.location
//          regionCode: region.regionCode
//          baseName: baseName
//          environmentId: compute[i].outputs.environmentId
//          managedIdentityId: compute[i].outputs.managedIdentityId
//          managedIdentityPrincipalId: compute[i].outputs.managedIdentityPrincipalId
//          managedIdentityClientId: compute[i].outputs.managedIdentityClientId
//          grafanaAdminPassword: grafanaAdminPassword   // @secure() top-level param / Key Vault reference
//        }
//      }]
//
// 2) prometheusQueryEndpoint two-pass ordering. compute.bicep's
//    `prometheusQueryEndpoint` seam defaults to '' so compute deploys FIRST with
//    every silo pinned at its min-replica floor (no scale rule). observability
//    deploys SECOND (it consumes compute's environment/identity outputs). Then
//    RE-DEPLOY compute passing the endpoint so the silo KEDA scaler activates:
//
//      prometheusQueryEndpoint: observability[i].outputs.prometheusQueryEndpoint
//
//    (Two passes because compute and observability have a mutual seam: compute
//    provides the identity+environment observability needs; observability
//    provides the endpoint compute's scaler needs. The endpoint is stable across
//    redeploys, so the second pass only adds the scale rule.)
//
// 3) MCP telemetry endpoint (Orleans.Lattice.Api.Mcp.Telemetry) env recipe. Point
//    the MCP head's telemetry add-on at the SAME managed Prometheus query
//    endpoint so the telemetry tool returns live metrics:
//
//      LatticeApiMcpTelemetry__BackendAddress = <observability.outputs.prometheusQueryEndpoint>
//
//    Auth residual: the add-on's LatticeApiMcpTelemetryOptions currently supports
//    None / Bearer / Basic / MutualTls only. Azure Monitor managed Prometheus
//    requires a rotating AAD bearer token for the region managed identity, which
//    the add-on cannot mint itself today. Until an Azure-workload auth mode ships
//    for the telemetry add-on, the coordinator supplies the token out-of-band
//    (a token-injecting sidecar / short-lived bearer) OR points the add-on at a
//    same-environment reverse proxy that stamps the MSI token. Tracked as a
//    follow-up; the datasource + KEDA paths need no such shim because Grafana and
//    KEDA both speak native azure-workload identity to managed Prometheus.
//
// -----------------------------------------------------------------------------
// METRICS INGESTION (managed-Prometheus scrape -> remote-write)
// -----------------------------------------------------------------------------
// Azure Container Apps cannot natively scrape into an Azure Monitor Workspace:
// the ACA managed OpenTelemetry agent only targets App Insights / Datadog / OTLP
// (there is no managed-Prometheus destination), and ACA has no AKS-style DCR
// scrape association. So ingestion is completed IN-BAND by this module: it
// deploys a small per-region OpenTelemetry Collector (contrib) container app
// into the region's ACA environment that
//
//   - scrapes the silo's /metrics endpoint over the environment-internal
//     network (compute exposes the silo HTTP/1 port external:false and hands the
//     address in via `siloScrapeTarget`), and
//   - remote-writes the scraped series into this module's Azure Monitor
//     Workspace through its Data Collection Endpoint + Rule, authenticating with
//     the region managed identity via a co-located `aad-auth-proxy` sidecar
//     (the collector cannot mint the rotating Entra token itself).
//
// The region identity is granted BOTH Monitoring Data Reader (query, below) and
// Monitoring Metrics Publisher (ingest, scoped to the DCR). This closes the gap
// that previously left the workspace holding zero series - every telemetry read
// (`list_metrics`, `count(up)`, named queries) now resolves against real data.
// =============================================================================


targetScope = 'resourceGroup'

@description('Azure region for every resource in this module (for example "westeurope"). Must match the region\'s compute stack.')
param location string

@description('Short lowercase region moniker used in resource names (for example "weu"). Must match the region\'s compute stack.')
@minLength(2)
@maxLength(8)
param regionCode string

@description('Lowercase base name shared by every region (for example "lattice"). Combined with regionCode to name resources.')
@minLength(3)
@maxLength(16)
param baseName string

// --- Compute seams consumed from modules/compute.bicep (one region) ---

@description('Resource id of the region ACA managed environment (compute.outputs.environmentId). The Grafana head is deployed into it.')
param environmentId string

@description('Resource id of the region user-assigned managed identity (compute.outputs.managedIdentityId). Assigned to the Grafana head and granted Monitoring Data Reader on the Azure Monitor workspace.')
param managedIdentityId string

@description('Principal (object) id of the region managed identity (compute.outputs.managedIdentityPrincipalId). Target of the Monitoring Data Reader role assignment.')
param managedIdentityPrincipalId string

@description('Client (application) id of the region managed identity (compute.outputs.managedIdentityClientId). Grafana presents it for managed-identity auth to managed Prometheus.')
param managedIdentityClientId string

// --- Grafana head configuration ---

@description('Stock Grafana OSS image. Pinned by digest/tag; must remain the unmodified upstream image (provisioning is injected via config, never a custom build).')
param grafanaImage string = 'docker.io/grafana/grafana-oss:11.3.1'

@description('Grafana admin username.')
param grafanaAdminUser string = 'admin'

@description('Grafana admin password. Never plain text at rest: supply via a Key Vault reference or a secure pipeline variable. Stored only as an ACA secret and surfaced to Grafana via GF_SECURITY_ADMIN_PASSWORD. Must be non-empty (an empty value would leave the internet-facing Grafana head with a blank admin password).')
@secure()
@minLength(1)
param grafanaAdminPassword string

@description('Grafana head max replicas (scale ceiling). Min is fixed at 0 (scale to zero); 1 is sufficient for a stateless visualization head.')
@minValue(1)
@maxValue(3)
param grafanaMaxReplicas int = 1

@description('Grafana container CPU (cores, as a string for the json() cast).')
param grafanaCpu string = '0.5'

@description('Grafana container memory.')
param grafanaMemory string = '1Gi'

@description('Provision the bundled Orleans.Lattice.Dashboards JSON into Grafana via an ephemeral secret volume. Leave true for a turnkey head; set false to keep the ACA template lean and deliver dashboards out-of-band.')
param provisionDashboards bool = true

// --- Metrics scrape + remote-write collector (managed-Prometheus ingestion) ---

@description('Silo internal-network metrics scrape target (host:port) the in-environment OpenTelemetry collector scrapes /metrics from (compute.outputs.siloMetricsScrapeTarget). Empty leaves the collector - and therefore managed-Prometheus ingestion - OFF, so the module still deploys standalone before compute exposes the seam. This is the seam that closes the ingestion gap: without it managed Prometheus holds zero series.')
param siloScrapeTarget string = ''

@description('OpenTelemetry Collector (contrib) image for the scrape + remote-write agent. Pinned upstream image; the collector config is injected via an ephemeral secret volume, never a custom build.')
param otelCollectorImage string = 'otel/opentelemetry-collector-contrib:0.111.0'

@description('Azure Monitor aad-auth-proxy image. Co-located sidecar that stamps the region managed-identity Entra token on the collector remote-write requests and forwards them to the Data Collection Endpoint (the collector cannot mint the token itself).')
param aadAuthProxyImage string = 'mcr.microsoft.com/azuremonitor/auth-proxy/prod/aad-auth-proxy/images/aad-auth-proxy:0.1.0-main-04-10-2024-7067ac84'

@description('Metrics collector container CPU (cores, as a string for the json() cast).')
param collectorCpu string = '0.25'

@description('Metrics collector container memory.')
param collectorMemory string = '0.5Gi'

// --- Log Analytics retention for the managed Prometheus DCR is N/A: metrics
//     land in the Azure Monitor workspace, not Log Analytics (that cap lives in
//     compute.bicep and is unaffected here). ---

// Monitoring Data Reader built-in role - least privilege: read metrics from the
// Azure Monitor workspace and nothing else.
var monitoringDataReaderRoleId = 'b0d8363b-8ddd-447d-831f-62ca05bff136'

// Monitoring Metrics Publisher built-in role - the WRITE counterpart: lets the
// region managed identity ingest (remote-write) metrics through the Data
// Collection Rule. Scoped to the DCR only, not the resource group.
var monitoringMetricsPublisherRoleId = '3913510d-42f4-4e42-8a64-420c390055eb'

var monitorAccountName = '${baseName}-${regionCode}-amw'
var dceName = '${baseName}-${regionCode}-dce'
var dcrName = '${baseName}-${regionCode}-dcr'
var grafanaAppName = '${baseName}-${regionCode}-grafana'
var collectorAppName = '${baseName}-${regionCode}-otelcol'
var deployCollector = !empty(siloScrapeTarget)
// Local port the aad-auth-proxy sidecar listens on; the collector remote-writes
// to localhost:<port> and the proxy forwards to the DCE with the MSI token.
var collectorProxyPort = 8081

// =============================================================================
// Azure Monitor workspace = managed Prometheus
// -----------------------------------------------------------------------------
// The metrics.prometheusQueryEndpoint it exposes is the SINGLE feed consumed by
// the silo KEDA scaler (compute.bicep, second pass), the Grafana datasource, and
// the MCP telemetry add-on. One pipeline, not two.
// =============================================================================

resource monitorWorkspace 'Microsoft.Monitor/accounts@2023-04-03' = {
  name: monitorAccountName
  location: location
}

// =============================================================================
// Data Collection Endpoint + Rule (Microsoft-PrometheusMetrics stream)
// -----------------------------------------------------------------------------
// The DCE is the ingestion front door; the DCR forwards the
// Microsoft-PrometheusMetrics stream to the Azure Monitor workspace. The
// in-environment collector (below) remote-writes the silo's scraped metrics
// through this DCE/DCR, so the workspace receives real series with no external
// scrape association required.
// =============================================================================

resource dataCollectionEndpoint 'Microsoft.Insights/dataCollectionEndpoints@2023-03-11' = {
  name: dceName
  location: location
  kind: 'Linux'
  properties: {
    networkAcls: {
      publicNetworkAccess: 'Enabled'
    }
  }
}

resource dataCollectionRule 'Microsoft.Insights/dataCollectionRules@2023-03-11' = {
  name: dcrName
  location: location
  kind: 'Linux'
  properties: {
    dataCollectionEndpointId: dataCollectionEndpoint.id
    dataSources: {
      prometheusForwarder: [
        {
          name: 'PrometheusDataSource'
          streams: [
            'Microsoft-PrometheusMetrics'
          ]
          labelIncludeFilter: {}
        }
      ]
    }
    destinations: {
      monitoringAccounts: [
        {
          accountResourceId: monitorWorkspace.id
          name: 'MonitoringAccount1'
        }
      ]
    }
    dataFlows: [
      {
        streams: [
          'Microsoft-PrometheusMetrics'
        ]
        destinations: [
          'MonitoringAccount1'
        ]
      }
    ]
  }
}

// =============================================================================
// Monitoring Data Reader - least privilege, scoped to the workspace only
// -----------------------------------------------------------------------------
// Grants the region managed identity read access to the Azure Monitor workspace.
// Both KEDA (compute.bicep scaler, azure-workload auth) and Grafana (managed
// identity datasource auth) authenticate as this identity - no keys, no bearer
// secrets. Scope is the workspace resource, not the resource group.
// =============================================================================

resource monitoringDataReader 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(monitorWorkspace.id, managedIdentityPrincipalId, monitoringDataReaderRoleId)
  scope: monitorWorkspace
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', monitoringDataReaderRoleId)
    principalId: managedIdentityPrincipalId
    principalType: 'ServicePrincipal'
  }
}

// =============================================================================
// Monitoring Metrics Publisher - the ingestion (write) grant, scoped to the DCR
// -----------------------------------------------------------------------------
// The region managed identity presents this to remote-write scraped metrics
// through the Data Collection Rule (via the aad-auth-proxy sidecar). Query-side
// Monitoring Data Reader above is read-only; without this write grant the
// remote-write is rejected and the workspace stays empty. Scope is the DCR, not
// the resource group.
// =============================================================================

resource monitoringMetricsPublisher 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(dataCollectionRule.id, managedIdentityPrincipalId, monitoringMetricsPublisherRoleId)
  scope: dataCollectionRule
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', monitoringMetricsPublisherRoleId)
    principalId: managedIdentityPrincipalId
    principalType: 'ServicePrincipal'
  }
}

// =============================================================================
// Grafana provisioning payloads
// -----------------------------------------------------------------------------
// Grafana on ACA is stateless: no database, no persistent volume. Provisioning
// config is injected via EPHEMERAL secret volumes (ACA's mechanism for mounting
// arbitrary files without persistent storage), so the head is fully
// re-createable and scales to zero cleanly. The datasource YAML's query-endpoint
// placeholder is resolved to the concrete managed Prometheus endpoint at deploy
// time.
// =============================================================================

var datasourceYaml = replace(
  loadTextContent('grafana/datasources.yaml'),
  '__PROM_QUERY_ENDPOINT__',
  monitorWorkspace.properties.metrics.prometheusQueryEndpoint
)

var dashboardProviderYaml = loadTextContent('grafana/dashboards.yaml')

// The bundled Orleans.Lattice.Dashboards JSON, embedded at compile time. Each
// entry becomes an ACA secret mounted read-only into the dashboard-provider
// path, so Grafana loads them on start-up with no external fetch.
var dashboardFiles = [
  { secretName: 'dashboard-overview', fileName: 'overview.json', content: loadTextContent('../../../src/lattice.dashboards/Grafana/OrleansLatticeOverview.json') }
  { secretName: 'dashboard-commitpath', fileName: 'commit-path.json', content: loadTextContent('../../../src/lattice.dashboards/Grafana/OrleansLatticeCommitPath.json') }
  { secretName: 'dashboard-atomicwrites', fileName: 'atomic-writes.json', content: loadTextContent('../../../src/lattice.dashboards/Grafana/OrleansLatticeAtomicWrites.json') }
  { secretName: 'dashboard-materialisedviews', fileName: 'materialised-views.json', content: loadTextContent('../../../src/lattice.dashboards/Grafana/OrleansLatticeMaterialisedViews.json') }
  { secretName: 'dashboard-backup', fileName: 'backup.json', content: loadTextContent('../../../src/lattice.dashboards/Grafana/OrleansLatticeBackup.json') }
  { secretName: 'dashboard-authorization', fileName: 'authorization.json', content: loadTextContent('../../../src/lattice.dashboards/Grafana/OrleansLatticeAuthorization.json') }
  { secretName: 'dashboard-scaling', fileName: 'scaling.json', content: loadTextContent('../../../src/lattice.dashboards/Grafana/OrleansLatticeScaling.json') }
  { secretName: 'dashboard-replication', fileName: 'replication.json', content: loadTextContent('../../../src/lattice.dashboards/Grafana/OrleansLatticeReplication.json') }
  { secretName: 'dashboard-replicationgrpc', fileName: 'replication-grpc.json', content: loadTextContent('../../../src/lattice.dashboards/Grafana/OrleansLatticeReplicationGrpc.json') }
]

// Base secrets always present: admin password + the two provisioning YAMLs.
var baseSecrets = [
  { name: 'grafana-admin-password', value: grafanaAdminPassword }
  { name: 'datasource-yaml', value: datasourceYaml }
  { name: 'dashboard-provider-yaml', value: dashboardProviderYaml }
]

var dashboardSecrets = [for f in dashboardFiles: { name: f.secretName, value: f.content }]

var grafanaSecrets = provisionDashboards ? concat(baseSecrets, dashboardSecrets) : baseSecrets

// Secret volumes: datasource provisioning, dashboard-provider provisioning, and
// (optionally) the dashboard JSON. All ephemeral - no persistent storage.
var dashboardVolumeItems = [for f in dashboardFiles: { secretRef: f.secretName, path: f.fileName }]

var baseVolumes = [
  {
    name: 'provisioning-datasources'
    storageType: 'Secret'
    secrets: [
      { secretRef: 'datasource-yaml', path: 'datasources.yaml' }
    ]
  }
  {
    name: 'provisioning-dashboards'
    storageType: 'Secret'
    secrets: [
      { secretRef: 'dashboard-provider-yaml', path: 'dashboards.yaml' }
    ]
  }
]

var dashboardVolume = [
  {
    name: 'dashboard-json'
    storageType: 'Secret'
    secrets: dashboardVolumeItems
  }
]

var grafanaVolumes = provisionDashboards ? concat(baseVolumes, dashboardVolume) : baseVolumes

var baseVolumeMounts = [
  { volumeName: 'provisioning-datasources', mountPath: '/etc/grafana/provisioning/datasources' }
  { volumeName: 'provisioning-dashboards', mountPath: '/etc/grafana/provisioning/dashboards' }
]

var dashboardVolumeMount = [
  { volumeName: 'dashboard-json', mountPath: '/var/lib/grafana/dashboards/orleans-lattice' }
]

var grafanaVolumeMounts = provisionDashboards ? concat(baseVolumeMounts, dashboardVolumeMount) : baseVolumeMounts

// =============================================================================
// Grafana head - stateless, scale-to-zero visualization surface
// -----------------------------------------------------------------------------
// Stock grafana/grafana-oss (runs as the non-root grafana user, uid 472, by
// image default). Anonymous access is off, self-sign-up is off, and the admin
// password rides an ACA secret. Managed Prometheus is reached with the region
// user-assigned managed identity (GF_AZURE_MANAGED_IDENTITY_*), never a key.
// =============================================================================

resource grafanaApp 'Microsoft.App/containerApps@2024-03-01' = {
  name: grafanaAppName
  location: location
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${managedIdentityId}': {}
    }
  }
  properties: {
    managedEnvironmentId: environmentId
    configuration: {
      activeRevisionsMode: 'Single'
      secrets: grafanaSecrets
      ingress: {
        external: true
        targetPort: 3000
        transport: 'auto'
        allowInsecure: false
      }
    }
    template: {
      volumes: grafanaVolumes
      containers: [
        {
          name: 'grafana'
          image: grafanaImage
          resources: {
            cpu: json(grafanaCpu)
            memory: grafanaMemory
          }
          volumeMounts: grafanaVolumeMounts
          env: [
            { name: 'GF_SECURITY_ADMIN_USER', value: grafanaAdminUser }
            { name: 'GF_SECURITY_ADMIN_PASSWORD', secretRef: 'grafana-admin-password' }
            // No anonymous access; no open sign-up. Auth is admin-only unless a
            // deployment fronts Grafana with Front Door / Entra auth.
            { name: 'GF_AUTH_ANONYMOUS_ENABLED', value: 'false' }
            { name: 'GF_USERS_ALLOW_SIGN_UP', value: 'false' }
            // Managed-identity auth to Azure Monitor managed Prometheus. The
            // datasource declares authType: msi; these settings tell Grafana to
            // use the region user-assigned identity to mint the AAD token.
            { name: 'GF_AZURE_MANAGED_IDENTITY_ENABLED', value: 'true' }
            { name: 'GF_AZURE_MANAGED_IDENTITY_CLIENT_ID', value: managedIdentityClientId }
            { name: 'GF_AZURE_CLOUD', value: 'AzureCloud' }
            { name: 'GF_PATHS_PROVISIONING', value: '/etc/grafana/provisioning' }
          ]
        }
      ]
      scale: {
        // Scale to zero: no traffic, no cost. The visualization head is
        // stateless, so a cold start rebuilds it from provisioning config.
        minReplicas: 0
        maxReplicas: grafanaMaxReplicas
        rules: [
          {
            name: 'http-traffic'
            http: {
              metadata: {
                concurrentRequests: '10'
              }
            }
          }
        ]
      }
    }
  }
}

// =============================================================================
// Metrics scrape + remote-write collector - closes the ingestion gap
// -----------------------------------------------------------------------------
// A single per-region container app with two co-located containers sharing
// localhost:
//   - otelcol-contrib: prometheus receiver scrapes the silo /metrics over the
//     environment-internal network; prometheusremotewrite exporter posts to the
//     aad-auth-proxy on localhost.
//   - aad-auth-proxy: stamps the region managed-identity Entra token
//     (audience https://monitor.azure.com/.default) and forwards to the DCE.
// Internal-only (no ingress); scales 1..1 (a scrape agent must stay resident).
// Deployed only when a scrape target is supplied, so the module still builds and
// deploys standalone before compute exposes the silo metrics seam.
// =============================================================================

// The remote-write URL is addressed at the local proxy; the proxy rewrites the
// host to the DCE and adds auth. The DCR immutableId + Microsoft-PrometheusMetrics
// stream select the ingestion pipeline.
var collectorRemoteWriteEndpoint = 'http://localhost:${collectorProxyPort}/dataCollectionRules/${dataCollectionRule.properties.immutableId}/streams/Microsoft-PrometheusMetrics/api/v1/write?api-version=2023-04-24'

var collectorConfigYaml = replace(
  replace(
    loadTextContent('scraper/otel-collector-config.yaml'),
    '__SCRAPE_TARGET__',
    siloScrapeTarget
  ),
  '__REMOTE_WRITE_ENDPOINT__',
  collectorRemoteWriteEndpoint
)

resource collectorApp 'Microsoft.App/containerApps@2024-03-01' = if (deployCollector) {
  name: collectorAppName
  location: location
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${managedIdentityId}': {}
    }
  }
  properties: {
    managedEnvironmentId: environmentId
    configuration: {
      activeRevisionsMode: 'Single'
      secrets: [
        { name: 'collector-config-yaml', value: collectorConfigYaml }
      ]
    }
    template: {
      volumes: [
        {
          name: 'collector-config'
          storageType: 'Secret'
          secrets: [
            { secretRef: 'collector-config-yaml', path: 'config.yaml' }
          ]
        }
      ]
      containers: [
        {
          name: 'otelcol'
          image: otelCollectorImage
          resources: {
            cpu: json(collectorCpu)
            memory: collectorMemory
          }
          args: [
            '--config=/etc/otelcol-contrib/config.yaml'
          ]
          volumeMounts: [
            { volumeName: 'collector-config', mountPath: '/etc/otelcol-contrib' }
          ]
        }
        {
          name: 'aad-auth-proxy'
          image: aadAuthProxyImage
          resources: {
            cpu: json(collectorCpu)
            memory: collectorMemory
          }
          env: [
            // Forward to the region DCE metrics-ingestion endpoint, adding the
            // managed-identity bearer token for the Azure Monitor audience.
            { name: 'TARGET_HOST', value: dataCollectionEndpoint.properties.metricsIngestion.endpoint }
            { name: 'LISTENING_PORT', value: string(collectorProxyPort) }
            { name: 'IDENTITY_TYPE', value: 'userassigned' }
            { name: 'AAD_CLIENT_ID', value: managedIdentityClientId }
            { name: 'AUDIENCE', value: 'https://monitor.azure.com/.default' }
          ]
        }
      ]
      scale: {
        // A scrape agent must stay resident; no scale-to-zero.
        minReplicas: 1
        maxReplicas: 1
      }
    }
  }
}

// =============================================================================
// Outputs - the observability seams the orchestrator wires back
// =============================================================================

@description('Managed Prometheus query endpoint. THE single feed: pass to compute.bicep\'s prometheusQueryEndpoint (silo KEDA scaler, second pass) AND to the MCP telemetry add-on\'s BackendAddress. Also the Grafana datasource url (already baked in-module).')
output prometheusQueryEndpoint string = monitorWorkspace.properties.metrics.prometheusQueryEndpoint

@description('Resource id of the Azure Monitor workspace (managed Prometheus).')
output azureMonitorWorkspaceId string = monitorWorkspace.id

@description('Resource id of the Data Collection Endpoint (managed-Prometheus ingestion front door).')
output dataCollectionEndpointId string = dataCollectionEndpoint.id

@description('Resource id of the Data Collection Rule (Microsoft-PrometheusMetrics stream) the in-environment collector remote-writes through.')
output dataCollectionRuleId string = dataCollectionRule.id

@description('Fully qualified domain name of the self-hosted Grafana head.')
output grafanaFqdn string = grafanaApp.properties.configuration.ingress.fqdn
