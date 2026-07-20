// =============================================================================
// storage.bicep - durable WAL, Orleans clustering, and the shared backup sink
// -----------------------------------------------------------------------------
// Sub-issue F-189 (Reference Architecture epic): the STORAGE lane.
//
// Provisions, from ONE parameter set spanning every region:
//   - A per-region Azure Storage account that backs BOTH the durable Azure Table
//     WAL (Orleans.Lattice.Storage.AzureTable) and Azure Table Orleans clustering
//     (membership). Per reference-architecture.md, both the WAL and the
//     clustering table live in the SAME per-region account, reached by the
//     region's user-assigned managed identity.
//   - A per-region "Storage Table Data Contributor" role assignment granting that
//     region's identity data-plane table access, scoped to THAT account only
//     (least privilege - the identity can touch its own region's tables and
//     nothing else).
//   - ONE global Azure Blob backup sink (single storage account + container)
//     shared by every region and consumed by Orleans.Lattice.Backup.AzureBlob.
//     It is the single source of truth for cold-restore.
//   - Backup-sink RBAC that encodes the backup-primary / standby split:
//       * the PRIMARY region identity gets "Storage Blob Data Contributor"
//         (it runs the scheduled-backup writer),
//       * every STANDBY region identity gets "Storage Blob Data Reader"
//         (a standby only ever RESTORE-READS the sink; it never writes it).
//
// -----------------------------------------------------------------------------
// KEYLESS-BY-DESIGN (hard security requirement)
// -----------------------------------------------------------------------------
// Every account sets allowSharedKeyAccess: false. All access is Entra-token /
// managed-identity + least-privilege data-plane RBAC. There are NO account keys,
// NO connection strings, and NO SAS anywhere in this template or its outputs.
//   - Orleans Azure Table clustering (Microsoft.Orleans.Clustering.AzureStorage)
//     accepts a TableServiceClient built from a TokenCredential, so membership
//     works without a shared key.
//   - The Orleans.Lattice.Storage.AzureTable WAL backend and
//     Orleans.Lattice.Backup.AzureBlob both accept a TokenCredential / endpoint
//     URI, so the WAL and the backup sink work without a shared key.
// Because every consumer supports managed identity, shared-key access is disabled
// outright rather than left on "just in case". If a future component genuinely
// required a key it would be surfaced through Key Vault + @secure() - never as a
// plain parameter or output here.
//
// -----------------------------------------------------------------------------
// DISASTER-RECOVERY / CAUSAL-FENCE SEMANTICS (see reference-architecture.md)
// -----------------------------------------------------------------------------
// The estate is active-active; losing a whole region is survivable:
//   - Live peers keep serving. Surviving regions accept reads and writes; the
//     front door fails user traffic to the next-nearest healthy region.
//   - Rebuild from the shared sink. A replacement region is redeployed from this
//     same Bicep and COLD-RESTORES the latest backup chain (full + incremental,
//     with its causal fence) from the global blob sink, then re-enrolls into
//     replication and converges with the live peers.
//   - Restore vs live peers. A restored value NEVER overwrites a causally newer
//     live value: convergence is by the same per-key HLC / LWW rule the live
//     replication path uses, so replaying an older backup onto a live active
//     estate is safe. The single backup-PRIMARY designation is what prevents two
//     regions racing to write (and fork) the shared chain - exactly why standby
//     identities get reader-only access to the sink.
//
// =============================================================================
// WIRING RECIPE (the coordinator applies this glue; this module does not edit
// main.bicep or compute.bicep - sibling sessions own those files in parallel).
// -----------------------------------------------------------------------------
// 1) main.bicep - add a backup-primary parameter and invoke this module ONCE,
//    AFTER the compute loop (this module consumes compute identity principalIds):
//
//      @description('regionCode of the single region whose silo runs the backup scheduler. Must match one regions[].regionCode.')
//      param backupPrimaryRegionCode string = regions[0].regionCode
//
//      module storage 'modules/storage.bicep' = {
//        name: 'storage'
//        params: {
//          baseName: baseName
//          backupPrimaryRegionCode: backupPrimaryRegionCode
//          regions: [for (region, i) in regions: {
//            regionCode: region.regionCode
//            location: region.location
//            managedIdentityPrincipalId: compute[i].outputs.managedIdentityPrincipalId
//          }]
//        }
//      }
//
// 2) compute.bicep - the storage endpoints are DETERMINISTIC (pure functions of
//    resourceGroup().id + baseName + regionCode), so there is NO module
//    dependency cycle: compute is fed endpoint STRINGS, this module creates the
//    matching resources with the identical names. Add these four params to
//    compute.bicep and append them to the silo container's `env` array:
//
//      param walTableEndpoint string          // -> LATTICE_WAL_TABLE_ENDPOINT
//      param clusteringTableEndpoint string   // -> ORLEANS_CLUSTERING_TABLE_ENDPOINT
//      param backupBlobEndpoint string        // -> LATTICE_BACKUP_BLOB_ENDPOINT
//      param backupIsPrimary bool             // -> LATTICE_BACKUP_IS_PRIMARY
//
//        { name: 'LATTICE_WAL_TABLE_ENDPOINT',       value: walTableEndpoint }
//        { name: 'ORLEANS_CLUSTERING_TABLE_ENDPOINT', value: clusteringTableEndpoint }
//        { name: 'LATTICE_BACKUP_BLOB_ENDPOINT',      value: backupBlobEndpoint }
//        { name: 'LATTICE_BACKUP_IS_PRIMARY',         value: string(backupIsPrimary) }
//
//    (AZURE_CLIENT_ID is already on the silo; the TokenCredential uses it.)
//
// 3) main.bicep - feed those compute params from the SAME naming functions this
//    module uses (copy the two helper expressions verbatim so the strings match):
//
//      // per-region account name (matches storageAccountName below)
//      // = toLower('st${uniqueString(resourceGroup().id, baseName, regionCode)}')
//      // global backup account name (matches backupAccountName below)
//      // = toLower('stbk${uniqueString(resourceGroup().id, baseName)}')
//
//      In the compute module loop add:
//        walTableEndpoint:        'https://st${uniqueString(resourceGroup().id, baseName, region.regionCode)}.table.${environment().suffixes.storage}'
//        clusteringTableEndpoint: 'https://st${uniqueString(resourceGroup().id, baseName, region.regionCode)}.table.${environment().suffixes.storage}'
//        backupBlobEndpoint:      'https://stbk${uniqueString(resourceGroup().id, baseName)}.blob.${environment().suffixes.storage}'
//        backupIsPrimary:         region.regionCode == backupPrimaryRegionCode
//
//    The WAL and clustering endpoints are intentionally the SAME account: the WAL
//    and the membership table share the per-region storage account by design.
// =============================================================================

targetScope = 'resourceGroup'

@description('Lowercase base name shared by every region (for example "lattice"). Combined with a deterministic uniqueString suffix to name globally-unique storage accounts.')
@minLength(3)
@maxLength(16)
param baseName string

@description('Region set, one entry per region. Each item: { regionCode, location, managedIdentityPrincipalId }. managedIdentityPrincipalId is the region compute stack\'s user-assigned identity principal id (from compute[i].outputs.managedIdentityPrincipalId).')
@minLength(1)
param regions array

@description('regionCode of the single backup-PRIMARY region whose silo runs the scheduled-backup writer. That region\'s identity gets Storage Blob Data Contributor on the shared sink; every other region gets Storage Blob Data Reader (restore-only). Must equal one regions[].regionCode.')
param backupPrimaryRegionCode string

@description('Location for the single global Azure Blob backup sink. Defaults to the first region\'s location. The sink is one shared account/container for the whole estate.')
param backupLocation string = regions[0].location

@description('Blob container name for the backup chains (full + incremental). One shared container is the single source of truth for cold-restore.')
param backupContainerName string = 'lattice-backups'

// --- Built-in Azure RBAC role definition ids (data-plane, least privilege) ---
// Storage Table Data Contributor: read/write the WAL + Orleans clustering tables.
var tableDataContributorRoleId = '0a9a7e1f-b9d0-4cc4-a60d-0319b160aaa3'
// Storage Blob Data Contributor: the backup-primary writes the shared chain.
var blobDataContributorRoleId = 'ba92f5b4-2d11-453d-a403-e96b0029c9fe'
// Storage Blob Data Reader: a standby only RESTORE-READS the shared chain.
var blobDataReaderRoleId = '2a2b9908-6ea1-4ae2-8e65-a410df84e7d1'

// Global backup account name: deterministic (pure) so main.bicep can compute the
// matching backup blob endpoint for the silo env WITHOUT a module dependency.
var backupAccountName = toLower('stbk${uniqueString(resourceGroup().id, baseName)}')

// =============================================================================
// Per-region storage account (durable WAL + Orleans clustering table)
// -----------------------------------------------------------------------------
// Keyless, TLS 1.2 minimum, no public blob access, OAuth as the default auth.
// The account name is a pure function of (resourceGroup id, baseName, regionCode)
// so main.bicep can derive the identical *.table endpoint for the silo env with
// no resource reference (breaks the compute<->storage ordering cycle).
// =============================================================================

resource regionStorage 'Microsoft.Storage/storageAccounts@2023-05-01' = [for region in regions: {
  name: toLower('st${uniqueString(resourceGroup().id, baseName, region.regionCode)}')
  location: region.location
  sku: {
    // Zone-redundant durable storage for the region durability boundary (WAL).
    name: 'Standard_ZRS'
  }
  kind: 'StorageV2'
  properties: {
    // Keyless: all access is Entra token + RBAC. No shared key, ever.
    allowSharedKeyAccess: false
    defaultToOAuthAuthentication: true
    minimumTlsVersion: 'TLS1_2'
    supportsHttpsTrafficOnly: true
    allowBlobPublicAccess: false
    // Public endpoint here; the networking sub-issue owns private-endpoint / VNet
    // lockdown. Keyless + RBAC means no anonymous or key-based reach even so.
    publicNetworkAccess: 'Enabled'
  }
}]

// Storage Table Data Contributor for the region identity, scoped to THAT account
// only (least privilege). The name is a guid derived from the scope + principal +
// role so redeploys are idempotent.
resource regionTableRbac 'Microsoft.Authorization/roleAssignments@2022-04-01' = [for (region, i) in regions: {
  name: guid(regionStorage[i].id, region.managedIdentityPrincipalId, tableDataContributorRoleId)
  scope: regionStorage[i]
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', tableDataContributorRoleId)
    principalId: region.managedIdentityPrincipalId
    principalType: 'ServicePrincipal'
  }
}]

// =============================================================================
// Shared global Azure Blob backup sink (single source of truth for restore)
// -----------------------------------------------------------------------------
// ONE account + ONE container for the whole estate. Only the backup-primary
// region writes it; standbys read it to cold-restore. Same keyless, TLS 1.2,
// no-public-access posture as the per-region accounts.
// =============================================================================

resource backupStorage 'Microsoft.Storage/storageAccounts@2023-05-01' = {
  name: backupAccountName
  location: backupLocation
  sku: {
    // Geo-redundant: the sink must survive the loss of its own home region so a
    // surviving region can still cold-restore from it during a region-loss DR.
    name: 'Standard_GZRS'
  }
  kind: 'StorageV2'
  properties: {
    allowSharedKeyAccess: false
    defaultToOAuthAuthentication: true
    minimumTlsVersion: 'TLS1_2'
    supportsHttpsTrafficOnly: true
    allowBlobPublicAccess: false
    publicNetworkAccess: 'Enabled'
  }
}

resource backupBlobService 'Microsoft.Storage/storageAccounts/blobServices@2023-05-01' = {
  parent: backupStorage
  name: 'default'
}

resource backupContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = {
  parent: backupBlobService
  name: backupContainerName
  properties: {
    // No anonymous access: restore reads are RBAC-authenticated managed identity.
    publicAccess: 'None'
  }
}

// Backup-sink RBAC encoding the primary/standby split, scoped to the sink account
// only. Primary -> Blob Data Contributor (writes chains); standby -> Blob Data
// Reader (restore-only). One assignment per region identity.
resource backupBlobRbac 'Microsoft.Authorization/roleAssignments@2022-04-01' = [for region in regions: {
  name: guid(backupStorage.id, region.managedIdentityPrincipalId, region.regionCode == backupPrimaryRegionCode ? blobDataContributorRoleId : blobDataReaderRoleId)
  scope: backupStorage
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', region.regionCode == backupPrimaryRegionCode ? blobDataContributorRoleId : blobDataReaderRoleId)
    principalId: region.managedIdentityPrincipalId
    principalType: 'ServicePrincipal'
  }
}]

// =============================================================================
// Outputs - seams for verification and for the coordinator's wiring.
// Endpoint outputs are keyless URIs only; no keys/SAS are ever emitted.
// =============================================================================

@description('Per-region storage seams in region-list order. Each item: { regionCode, storageAccountName, tableEndpoint }. tableEndpoint backs BOTH the durable WAL and Orleans clustering.')
output perRegionStorage array = [for (region, i) in regions: {
  regionCode: region.regionCode
  storageAccountName: regionStorage[i].name
  tableEndpoint: regionStorage[i].properties.primaryEndpoints.table
}]

@description('Name of the shared global backup blob account (single source of truth for restore).')
output backupAccountNameOut string = backupStorage.name

@description('Blob endpoint of the shared backup sink (keyless URI). Feeds LATTICE_BACKUP_BLOB_ENDPOINT on every silo.')
output backupBlobEndpoint string = backupStorage.properties.primaryEndpoints.blob

@description('Backup container name (single shared source of truth for cold-restore).')
output backupContainerNameOut string = backupContainer.name
