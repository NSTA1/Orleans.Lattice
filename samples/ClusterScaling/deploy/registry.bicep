// ClusterScaling - Basic Azure Container Registry for the autoscaling sample.
//
// deploy.ps1 provisions this first (when you do not pass an existing -Registry),
// builds+pushes the silo image into it via `az acr build`, and then passes the
// registry name to main.bicep so the container app pulls from it using the
// user-assigned managed identity (AcrPull). Admin user is disabled: the only
// pull path is the managed identity, the only push path is your `az` login.

@description('Location for the registry.')
param location string = resourceGroup().location

@description('Base name used to derive the registry name. 2-16 lowercase alphanumerics.')
@minLength(2)
@maxLength(16)
param namePrefix string = 'latscale'

// ACR names are globally unique, 5-50 alphanumerics, no hyphens. uniqueString
// makes the name deterministic per resource group so re-running is idempotent.
var registryName = take(toLower('acr${replace(namePrefix, '-', '')}${uniqueString(resourceGroup().id)}'), 50)

resource registry 'Microsoft.ContainerRegistry/registries@2023-11-01-preview' = {
  name: registryName
  location: location
  sku: {
    name: 'Basic'
  }
  properties: {
    adminUserEnabled: false
  }
}

@description('The provisioned registry name (pass to main.bicep as registryName).')
output registryName string = registry.name

@description('The registry login server, e.g. acrxxxx.azurecr.io.')
output loginServer string = registry.properties.loginServer
