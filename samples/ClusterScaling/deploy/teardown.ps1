#Requires -Version 7.0
<#
.SYNOPSIS
    Tears down the ClusterScaling deployment by deleting its resource group.

.DESCRIPTION
    Deletes the entire resource group deploy.ps1 created (container app, managed
    environment, Log Analytics workspace, storage account, managed identity, and
    the role assignment). This is the cheapest and most complete cleanup.

    Cost discipline: an idle ClusterScaling deployment is not free even at
    minReplicas=1. The container app holds at least one always-on replica
    (vCPU + memory billed per second), the Log Analytics workspace bills for
    ingested logs, and the storage account bills for the clustering / reminder /
    grain-state / WAL tables it retains. Delete the resource group as soon as you
    finish a scaling experiment rather than leaving it parked.

    Ingress note: the app was provisioned with EXTERNAL ingress so you can reach
    the data API and drive load from your workstation. If you keep a deployment
    running, scope who can reach it (an IP allow-list on the managed environment,
    or switch the ingress to internal and drive load from inside the environment)
    - the Basic credential rides the managed TLS ingress but an internet-exposed
    write API still benefits from network scoping.

.PARAMETER ResourceGroup
    The resource group to delete.

.PARAMETER Yes
    Skip the confirmation prompt.

.PARAMETER NoWait
    Return immediately instead of waiting for the deletion to complete.

.NOTES
    Requires the Azure CLI (az) and an active `az login`.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string] $ResourceGroup,

    [switch] $Yes,

    [switch] $NoWait
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

if (-not (Get-Command az -ErrorAction SilentlyContinue)) {
    throw 'Azure CLI (az) is not on PATH. Install it and run `az login`.'
}

$exists = az group exists --name $ResourceGroup --output tsv
if ($exists -ne 'true') {
    Write-Host "Resource group '$ResourceGroup' does not exist. Nothing to do." -ForegroundColor Yellow
    return
}

if (-not $Yes) {
    $answer = Read-Host "Delete resource group '$ResourceGroup' and everything in it? (y/N)"
    if ($answer -notin @('y', 'Y')) {
        Write-Host 'Aborted.' -ForegroundColor Yellow
        return
    }
}

Write-Host "==> Deleting resource group '$ResourceGroup'" -ForegroundColor Cyan
$azArgs = @('group', 'delete', '--name', $ResourceGroup, '--yes')
if ($NoWait) { $azArgs += '--no-wait' }
az @azArgs

Write-Host ''
Write-Host "Resource group '$ResourceGroup' deletion requested." -ForegroundColor Green
if ($NoWait) {
    Write-Host "Deletion runs in the background. Confirm with: az group exists --name $ResourceGroup"
}
