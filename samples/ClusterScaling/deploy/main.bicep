// ClusterScaling - Azure Container Apps deployment for the Orleans.Lattice.Scaling
// autoscaling sample. Provisions:
//
//   - a user-assigned managed identity (no keys, no connection strings),
//   - a Storage account (Tables only) for Orleans clustering + reminders +
//     grain state and the Lattice WAL, with shared-key access disabled,
//   - the Storage Table Data Contributor role for the identity on that account,
//   - a Log Analytics workspace + Container Apps managed environment,
//   - the container app running the ClusterScaling silo image, with:
//       * external ingress over the managed TLS certificate (HTTP/2 transport),
//       * the admin password hash as a container-app SECRET surfaced through the
//         LATTICE_DATA_USER_admin env var (never plaintext, never an image layer),
//       * a KEDA custom metrics-api scale rule that scrapes /lattice/scale and
//         reads the top-level scaleValue (targetValue '1', min/max guardrails).
//
// deploy.ps1 fills the parameters (it hashes the operator's plaintext password
// before passing adminPasswordHash). Live deployment is validated by the
// operator; this template is correct-by-inspection.

@description('Location for all resources.')
param location string = resourceGroup().location

@description('Base name used to derive every resource name. 2-16 lowercase alphanumerics.')
@minLength(2)
@maxLength(16)
param namePrefix string = 'latscale'

@description('Fully-qualified container image for the ClusterScaling silo (e.g. myregistry.azurecr.io/clusterscaling-silo:latest).')
param containerImage string

@description('Admin username the data API accepts. Must be an environment-variable-name-safe segment.')
param adminUsername string = 'admin'

@description('Salted PBKDF2-SHA256 hash of the admin password (pbkdf2-sha256$...). deploy.ps1 produces this from the plaintext; the plaintext never reaches the template.')
@secure()
param adminPasswordHash string

@description('CPU cores per replica.')
param cpu string = '1.0'

@description('Memory per replica.')
param memory string = '2Gi'

@description('Minimum replica count. Scale-in never drops below this floor (keep >= 1 so the scrape target stays reachable and the cluster keeps a silo).')
@minValue(1)
@maxValue(30)
param minReplicas int = 1

@description('Maximum replica count. The hard ceiling; the autoscaler never scales past it regardless of scaleValue.')
@minValue(1)
@maxValue(30)
param maxReplicas int = 10

@description('The scaleValue per desired replica. scaleValue is already expressed in replica-units, so "1" tracks it directly.')
param scaleTargetValue string = '1'

@description('Orleans cluster / service id shared by every replica.')
param clusterId string = 'clusterscaling'

var identityName = '${namePrefix}-id'
var storageName = take(toLower('st${replace(namePrefix, '-', '')}${uniqueString(resourceGroup().id)}'), 24)
var logAnalyticsName = '${namePrefix}-logs'
var environmentName = '${namePrefix}-env'
var appName = '${namePrefix}-app'
var httpTargetPort = 8080

// Storage Table Data Contributor built-in role.
var roleDefIdTableContributor = '0a9a7e1f-b9d0-4cc4-a60d-0319b160aaa3'

resource identity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' = {
  name: identityName
  location: location
}

resource storage 'Microsoft.Storage/storageAccounts@2024-01-01' = {
  name: storageName
  location: location
  sku: { name: 'Standard_LRS' }
  kind: 'StorageV2'
  properties: {
    accessTier: 'Hot'
    allowBlobPublicAccess: false
    allowSharedKeyAccess: false
    minimumTlsVersion: 'TLS1_2'
    supportsHttpsTrafficOnly: true
    publicNetworkAccess: 'Enabled'
    networkAcls: {
      defaultAction: 'Allow'
      bypass: 'AzureServices'
    }
  }
}

// Grant the container's managed identity Table data-plane access on the account.
resource raTable 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(storage.id, identity.id, roleDefIdTableContributor)
  scope: storage
  properties: {
    principalId: identity.properties.principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', roleDefIdTableContributor)
  }
}

resource logAnalytics 'Microsoft.OperationalInsights/workspaces@2023-09-01' = {
  name: logAnalyticsName
  location: location
  properties: {
    sku: { name: 'PerGB2018' }
    retentionInDays: 30
  }
}

resource environment 'Microsoft.App/managedEnvironments@2024-03-01' = {
  name: environmentName
  location: location
  properties: {
    appLogsConfiguration: {
      destination: 'log-analytics'
      logAnalyticsConfiguration: {
        customerId: logAnalytics.properties.customerId
        sharedKey: logAnalytics.listKeys().primarySharedKey
      }
    }
  }
}

// The app FQDN is deterministic from the app name + the environment's default
// domain, so the scale rule can point the metrics-api scraper at the app's own
// ingress without a circular self-reference.
var appFqdn = '${appName}.${environment.properties.defaultDomain}'
var scaleSignalUrl = 'https://${appFqdn}/lattice/scale'

resource containerApp 'Microsoft.App/containerApps@2024-03-01' = {
  name: appName
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
      ingress: {
        external: true
        targetPort: httpTargetPort
        // HTTP/2 backend transport so the gRPC data API works end to end; the
        // managed TLS certificate terminates external TLS at the ingress.
        transport: 'http2'
        allowInsecure: false
      }
      secrets: [
        {
          // The salted PBKDF2 hash, injected as a secret (never plaintext).
          name: 'admin-password-hash'
          value: adminPasswordHash
        }
      ]
    }
    template: {
      containers: [
        {
          name: 'silo'
          image: containerImage
          resources: {
            cpu: json(cpu)
            memory: memory
          }
          env: [
            // DefaultAzureCredential resolves this user-assigned identity.
            {
              name: 'AZURE_CLIENT_ID'
              value: identity.properties.clientId
            }
            {
              name: 'CLUSTERSCALING_TABLE_URI'
              value: storage.properties.primaryEndpoints.table
            }
            {
              name: 'CLUSTERSCALING_CLUSTER_ID'
              value: clusterId
            }
            {
              name: 'CLUSTERSCALING_SERVICE_ID'
              value: clusterId
            }
            {
              name: 'CLUSTERSCALING_HTTP_PORT'
              value: string(httpTargetPort)
            }
            // The admin credential hash, sourced from the secret above and read
            // by the data-API Basic authorizer as LATTICE_DATA_USER_<admin>.
            {
              name: 'LATTICE_DATA_USER_${adminUsername}'
              secretRef: 'admin-password-hash'
            }
          ]
        }
      ]
      scale: {
        minReplicas: minReplicas
        maxReplicas: maxReplicas
        rules: [
          {
            name: 'lattice-scale'
            custom: {
              type: 'metrics-api'
              metadata: {
                // scaleValue is in replica-units, so targetValue '1' tracks it
                // directly (desiredReplicas = ceil(scaleValue / targetValue)).
                url: scaleSignalUrl
                valueLocation: 'scaleValue'
                targetValue: scaleTargetValue
              }
            }
          }
        ]
      }
    }
  }
}

@description('The external ingress FQDN of the data API + scaling endpoint.')
output ingressFqdn string = containerApp.properties.configuration.ingress.fqdn

@description('The data-API gRPC base address (https, managed TLS).')
output dataApiAddress string = 'https://${containerApp.properties.configuration.ingress.fqdn}'

@description('The scaling-signal scrape URL the KEDA metrics-api rule reads.')
output scaleSignalUrl string = scaleSignalUrl

@description('The provisioned storage account name.')
output storageAccountName string = storage.name

@description('The storage Table endpoint injected into the silo.')
output storageTableEndpoint string = storage.properties.primaryEndpoints.table

@description('The container app name (used by drive-load.ps1 to poll replica counts).')
output containerAppName string = containerApp.name

@description('The user-assigned managed identity client id.')
output identityClientId string = identity.properties.clientId
