// Example parameter file for a single-region deploy of observability.bicep.
//
// In the real estate the orchestrator (main.bicep) loops this module over the
// region list and feeds these values from the compute module's per-region
// outputs (see the WIRING RECIPE in observability.bicep). This standalone
// example is for isolated validation / what-if of the observability lane only.
//
// grafanaAdminPassword is @secure(): never commit a real value. Supply it at
// deploy time from Key Vault or a secure pipeline variable, for example:
//   az deployment group create ... \
//     --parameters observability.example.bicepparam \
//     --parameters grafanaAdminPassword=@Microsoft.KeyVault(SecretUri=...)

using 'observability.bicep'

param location = 'westeurope'
param regionCode = 'weu'
param baseName = 'lattice'

// These four come from compute[i].outputs in the real deploy; placeholders here.
param environmentId = '/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/lattice-rg/providers/Microsoft.App/managedEnvironments/lattice-weu-env'
param managedIdentityId = '/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/lattice-rg/providers/Microsoft.ManagedIdentity/userAssignedIdentities/lattice-weu-id'
param managedIdentityPrincipalId = '00000000-0000-0000-0000-000000000001'
param managedIdentityClientId = '00000000-0000-0000-0000-000000000002'

// Secure: overridden at deploy time from Key Vault / a secure variable.
param grafanaAdminPassword = ''

param provisionDashboards = true
