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
// RESIDUAL DEPLOYER STEP (managed-Prometheus <-> ACA scrape association)
// -----------------------------------------------------------------------------
// Azure Monitor managed Prometheus SCRAPING of Azure Container Apps is not, at
// current API availability, cleanly expressible as a first-class Bicep
// association the way an AKS cluster's DCR association is. This module provisions
// everything that IS expressible - the workspace, the DCE, the
// Microsoft-PrometheusMetrics DCR, the RBAC, and the query endpoint - and leaves
// exactly one residual for the deployer/coordinator to apply on the ACA managed
// environment (which compute.bicep owns, so this module must not touch it):
//
//   Enable the managed environment's Prometheus scrape -> Azure Monitor workspace
//   integration by setting the environment's OpenTelemetry / Azure Monitor
//   metrics destination to this module's `azureMonitorWorkspaceId` output, or by
//   attaching the DCE/DCR (`dataCollectionEndpointId` / `dataCollectionRuleId`
//   outputs) to the environment once the ACA<->DCR association GA API is
//   available. The three heads already expose OpenMetrics on their scrape port;
//   only the environment-level destination wiring is outstanding.
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

// --- Log Analytics retention for the managed Prometheus DCR is N/A: metrics
//     land in the Azure Monitor workspace, not Log Analytics (that cap lives in
//     compute.bicep and is unaffected here). ---

// Monitoring Data Reader built-in role - least privilege: read metrics from the
// Azure Monitor workspace and nothing else.
var monitoringDataReaderRoleId = 'b0d8363b-8ddd-447d-831f-62ca05bff136'

var monitorAccountName = '${baseName}-${regionCode}-amw'
var dceName = '${baseName}-${regionCode}-dce'
var dcrName = '${baseName}-${regionCode}-dcr'
var grafanaAppName = '${baseName}-${regionCode}-grafana'

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
// Microsoft-PrometheusMetrics stream to the Azure Monitor workspace. Associating
// the ACA managed environment as a scrape source is the documented residual
// deployer step (see the header) - everything expressible in Bicep is here.
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
// Outputs - the observability seams the orchestrator wires back
// =============================================================================

@description('Managed Prometheus query endpoint. THE single feed: pass to compute.bicep\'s prometheusQueryEndpoint (silo KEDA scaler, second pass) AND to the MCP telemetry add-on\'s BackendAddress. Also the Grafana datasource url (already baked in-module).')
output prometheusQueryEndpoint string = monitorWorkspace.properties.metrics.prometheusQueryEndpoint

@description('Resource id of the Azure Monitor workspace (managed Prometheus). Residual-step target: the ACA environment\'s metrics destination.')
output azureMonitorWorkspaceId string = monitorWorkspace.id

@description('Resource id of the Data Collection Endpoint (managed-Prometheus ingestion front door).')
output dataCollectionEndpointId string = dataCollectionEndpoint.id

@description('Resource id of the Data Collection Rule (Microsoft-PrometheusMetrics stream). Residual-step target for the ACA scrape association.')
output dataCollectionRuleId string = dataCollectionRule.id

@description('Fully qualified domain name of the self-hosted Grafana head.')
output grafanaFqdn string = grafanaApp.properties.configuration.ingress.fqdn
