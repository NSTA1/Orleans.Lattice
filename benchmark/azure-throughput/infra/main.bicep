// Single Linux VM for the Orleans.Lattice azure-throughput benchmark.
//
// Design constraints (originally from wedge-plan2.md Phase 0, now the
// general-purpose benchmark host):
//   - Deterministic single-tenant host (no ACI noise).
//   - Accelerated networking ON (smallest SKU that supports it).
//   - SSH-only, locked to a single source IP, public-key auth only.
//   - Auto-shutdown at 19:00 UTC daily as a safety net.
//
// SKU note: B-series does NOT support accelerated networking. The smallest
// SKU that does is the 2 vCPU D-family. We default to Standard_D2as_v5
// (AMD, 2 vCPU / 8 GiB, accelerated networking supported, cheapest in its
// class in most regions). Override via the vmSize parameter if needed.

@description('Location for all resources. Pick the same region as the Tables account.')
param location string = resourceGroup().location

@description('Base name used for all resources.')
param namePrefix string = 'lat'

@description('VM size. Must support accelerated networking. Default: smallest D-family. F8as_v6 was tried 2026-06-04 but the silo only used ~5 of 8 cores even at saturation; D2 is enough.')
param vmSize string = 'Standard_D2as_v5'

@description('Linux admin username.')
param adminUsername string

@description('SSH public key (contents of ~/.ssh/id_*.pub).')
param sshPublicKey string

@description('Single public IPv4 (or CIDR) allowed to SSH. Example: 203.0.113.4/32.')
param allowedSshSourceAddress string

@description('Local time zone for the auto-shutdown schedule. UTC by default.')
param autoShutdownTimeZone string = 'UTC'

@description('Daily auto-shutdown time in HHmm (24h). Default 1900 = 19:00 UTC.')
param autoShutdownTime string = '1900'

@description('OS disk size in GiB.')
param osDiskSizeGB int = 64

@description('Optional cloud-init / custom-data payload (base64-encoded). Empty = no custom-data.')
param customDataBase64 string = ''

var vmName = '${namePrefix}-vm'
var nicName = '${namePrefix}-nic'
var pipName = '${namePrefix}-pip'
var nsgName = '${namePrefix}-nsg'
var vnetName = '${namePrefix}-vnet'
var subnetName = 'default'
var dnsLabel = toLower('${namePrefix}-${uniqueString(resourceGroup().id)}')

// Storage account name: 3-24 chars, lowercase + digits only, globally unique.
// uniqueString() always returns 13 chars, so 'st' + first 22 = 24-char safe name.
var storageAccountName = take(toLower('st${replace(namePrefix, '-', '')}${uniqueString(resourceGroup().id)}'), 24)

// Built-in role definition IDs.
// Storage Table Data Contributor: 0a9a7e1f-b9d0-4cc4-a60d-0319b160aaa3
// Storage Blob Data Contributor:  ba92f5b4-2d11-453d-a403-e96b0029c9fe
// Storage Queue Data Contributor: 974c5e8b-45b9-4653-ba55-5f855dd0fb88
var roleDefIdTableContributor = '0a9a7e1f-b9d0-4cc4-a60d-0319b160aaa3'
var roleDefIdBlobContributor  = 'ba92f5b4-2d11-453d-a403-e96b0029c9fe'
var roleDefIdQueueContributor = '974c5e8b-45b9-4653-ba55-5f855dd0fb88'

resource nsg 'Microsoft.Network/networkSecurityGroups@2024-05-01' = {
  name: nsgName
  location: location
  properties: {
	securityRules: [
	  {
		name: 'AllowSshFromOperator'
		properties: {
		  description: 'SSH from the operator IP only.'
		  protocol: 'Tcp'
		  sourcePortRange: '*'
		  destinationPortRange: '22'
		  sourceAddressPrefix: allowedSshSourceAddress
		  destinationAddressPrefix: '*'
		  access: 'Allow'
		  priority: 1000
		  direction: 'Inbound'
		}
	  }
	  {
		name: 'DenyAllOtherInbound'
		properties: {
		  description: 'Deny everything else inbound.'
		  protocol: '*'
		  sourcePortRange: '*'
		  destinationPortRange: '*'
		  sourceAddressPrefix: '*'
		  destinationAddressPrefix: '*'
		  access: 'Deny'
		  priority: 4096
		  direction: 'Inbound'
		}
	  }
	]
  }
}

resource vnet 'Microsoft.Network/virtualNetworks@2024-05-01' = {
  name: vnetName
  location: location
  properties: {
	addressSpace: {
	  addressPrefixes: [ '10.42.0.0/24' ]
	}
	subnets: [
	  {
		name: subnetName
		properties: {
		  addressPrefix: '10.42.0.0/27'
		  networkSecurityGroup: { id: nsg.id }
		}
	  }
	]
  }
}

resource pip 'Microsoft.Network/publicIPAddresses@2024-05-01' = {
  name: pipName
  location: location
  sku: { name: 'Standard' }
  properties: {
	publicIPAllocationMethod: 'Static'
	publicIPAddressVersion: 'IPv4'
	dnsSettings: {
	  domainNameLabel: dnsLabel
	}
  }
}

resource nic 'Microsoft.Network/networkInterfaces@2024-05-01' = {
  name: nicName
  location: location
  properties: {
	enableAcceleratedNetworking: true
	ipConfigurations: [
	  {
		name: 'ipconfig1'
		properties: {
		  subnet: { id: '${vnet.id}/subnets/${subnetName}' }
		  privateIPAllocationMethod: 'Dynamic'
		  publicIPAddress: { id: pip.id }
		}
	  }
	]
  }
}

resource vm 'Microsoft.Compute/virtualMachines@2024-07-01' = {
  name: vmName
  location: location
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
	hardwareProfile: { vmSize: vmSize }
	storageProfile: {
	  imageReference: {
		publisher: 'Canonical'
		offer: 'ubuntu-24_04-lts'
		sku: 'server'
		version: 'latest'
	  }
	  osDisk: {
		createOption: 'FromImage'
		diskSizeGB: osDiskSizeGB
		managedDisk: { storageAccountType: 'Premium_LRS' }
		deleteOption: 'Delete'
	  }
	}
	osProfile: {
	  computerName: vmName
	  adminUsername: adminUsername
	  customData: empty(customDataBase64) ? null : customDataBase64
	  linuxConfiguration: {
		disablePasswordAuthentication: true
		ssh: {
		  publicKeys: [
			{
			  path: '/home/${adminUsername}/.ssh/authorized_keys'
			  keyData: sshPublicKey
			}
		  ]
		}
	  }
	}
	networkProfile: {
	  networkInterfaces: [
		{
		  id: nic.id
		  properties: { deleteOption: 'Delete' }
		}
	  ]
	}
  }
}

// Auto-shutdown via DevTestLab schedule (works on any VM).
resource autoShutdown 'Microsoft.DevTestLab/schedules@2018-09-15' = {
  name: 'shutdown-computevm-${vmName}'
  location: location
  properties: {
	status: 'Enabled'
	taskType: 'ComputeVmShutdownTask'
	dailyRecurrence: { time: autoShutdownTime }
	timeZoneId: autoShutdownTimeZone
	notificationSettings: { status: 'Disabled' }
	targetResourceId: vm.id
  }
}

// Storage account for WAL / bench artefacts. Accessed from the VM via the
// system-assigned managed identity (no keys, no connection strings).
resource storage 'Microsoft.Storage/storageAccounts@2024-01-01' = {
  name: storageAccountName
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

// Grant the VM's system-assigned identity data-plane access to Tables, Blobs,
// and Queues on this storage account. Scoped to this account only.
resource raTable 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(storage.id, vm.id, roleDefIdTableContributor)
  scope: storage
  properties: {
	principalId: vm.identity.principalId
	principalType: 'ServicePrincipal'
	roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', roleDefIdTableContributor)
  }
}

resource raBlob 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(storage.id, vm.id, roleDefIdBlobContributor)
  scope: storage
  properties: {
	principalId: vm.identity.principalId
	principalType: 'ServicePrincipal'
	roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', roleDefIdBlobContributor)
  }
}

resource raQueue 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(storage.id, vm.id, roleDefIdQueueContributor)
  scope: storage
  properties: {
	principalId: vm.identity.principalId
	principalType: 'ServicePrincipal'
	roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', roleDefIdQueueContributor)
  }
}

output vmName string = vm.name
output publicIp string = pip.properties.ipAddress
output fqdn string = pip.properties.dnsSettings.fqdn
output adminUsername string = adminUsername
output sshCommand string = 'ssh ${adminUsername}@${pip.properties.dnsSettings.fqdn}'
output storageAccountName string = storage.name
output storageTableEndpoint string = storage.properties.primaryEndpoints.table
output storageBlobEndpoint string = storage.properties.primaryEndpoints.blob
output vmPrincipalId string = vm.identity.principalId
