// =============================================================================
// bootstrap.bicep - pre-image-build registry seam (sub-issue F-192 / #1280)
// -----------------------------------------------------------------------------
// Provisions ONLY the shared, estate-wide Azure Container Registry, ahead of the
// full main.bicep deploy, so the deployer can `az acr build` the three host
// images BEFORE the per-region Container Apps that reference them are created
// (an ACA revision that references an absent image never becomes healthy).
//
// NAME PARITY (load-bearing): the registry name expression below is byte-for-byte
// identical to main.bicep's `registryName` var, and both deploy into the same
// resource group, so `uniqueString(resourceGroup().id)` resolves to the same
// value. main.bicep's own `resource registry` therefore converges onto this exact
// resource on the subsequent full deploy - it is not a second registry. If
// main.bicep's expression ever changes, THIS file must change in lockstep.
//
// Keyless throughout: admin user OFF, anonymous pull OFF. Image pull is by the
// per-region managed identity + AcrPull RBAC that main.bicep assigns. No password
// or key is ever emitted.
// =============================================================================

targetScope = 'resourceGroup'

@description('Lowercase base name shared by every region and the registry (must equal the value passed to main.bicep).')
@minLength(3)
@maxLength(16)
param baseName string

@description('Location for the single global registry. Defaults to the resource group location; pass the first region location to match a non-default main.bicep registryLocation.')
param registryLocation string = resourceGroup().location

// Identical to main.bicep's registryName var - see NAME PARITY note above.
var registryName = toLower('${replace(baseName, '-', '')}acr${uniqueString(resourceGroup().id)}')

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

@description('Registry name (globally unique DNS label).')
output acrName string = registry.name

@description('Login server of the shared registry (the `az acr build --registry` target and the image pull host).')
output acrLoginServer string = registry.properties.loginServer

@description('Resource id of the shared registry.')
output acrId string = registry.id
