// =============================================================================
// storage.bicepparam - example parameter set for the storage lane (F-189).
// -----------------------------------------------------------------------------
// Demonstrates the N-region shape. In a real deployment the coordinator invokes
// modules/storage.bicep from main.bicep with managedIdentityPrincipalId taken
// from each compute[i].outputs.managedIdentityPrincipalId (see the WIRING RECIPE
// in the module header); the placeholder GUIDs below only let this params file
// stand alone for `az bicep build-params` demonstration.
// =============================================================================

using '../modules/storage.bicep'

param baseName = 'lattice'

// Two regions here; the same file scales to N by editing this array only. The
// single backup-primary owns the scheduler; the other region is DR standby.
param regions = [
  {
    regionCode: 'weu'
    location: 'westeurope'
    // Placeholder - real deploys pass compute[i].outputs.managedIdentityPrincipalId.
    managedIdentityPrincipalId: '00000000-0000-0000-0000-000000000001'
  }
  {
    regionCode: 'eus2'
    location: 'eastus2'
    managedIdentityPrincipalId: '00000000-0000-0000-0000-000000000002'
  }
]

// westeurope runs the scheduled-backup writer; eastus2 is restore-only standby.
param backupPrimaryRegionCode = 'weu'
