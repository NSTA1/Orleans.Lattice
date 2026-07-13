#Requires -Version 7.0
<#
.SYNOPSIS
    Deploys the ClusterScaling Orleans.Lattice.Scaling sample to Azure Container Apps.

.DESCRIPTION
    Provisions (idempotently) a resource group, a user-assigned managed identity,
    a Tables-only storage account (Orleans clustering + reminders + grain state +
    Lattice WAL, shared-key access disabled), the Storage Table Data Contributor
    role for the identity, a Log Analytics workspace, a Container Apps managed
    environment, and the ClusterScaling silo container app with a KEDA
    metrics-api scale rule that reads scaleValue from /lattice/scale.

    The operator supplies a plaintext admin password (as a SecureString). This
    script hashes it with the repository's tools/New-LatticeStateCredential.ps1
    helper (salted PBKDF2-SHA256) and passes only the HASH to the bicep template,
    which injects it as a container-app secret surfaced through the
    LATTICE_DATA_USER_<admin> env var. The plaintext is never stored, never
    baked into the image, and never passed on a command line.

.PARAMETER ResourceGroup
    Target resource group. Created if it does not exist.

.PARAMETER Location
    Azure region. Defaults to eastus.

.PARAMETER ContainerImage
    Fully-qualified silo image, e.g. myregistry.azurecr.io/clusterscaling-silo:latest.
    Build and push it from samples/ClusterScaling/src/ClusterScaling.Silo first
    (see the README). The image's registry must be reachable by the container app.

.PARAMETER AdminPassword
    Plaintext admin password as a SecureString (prompted if omitted). Presented
    later by drive-load.ps1 as an HTTP Basic credential over the managed TLS
    ingress.

.PARAMETER AdminUsername
    Admin username. Must be environment-variable-name-safe. Defaults to admin.

.PARAMETER NamePrefix
    Base name for derived resources (2-16 lowercase alphanumerics). Defaults to latscale.

.PARAMETER MinReplicas
    Scale-in floor (keep >= 1). Defaults to 1.

.PARAMETER MaxReplicas
    Scale-out ceiling. Defaults to 10.

.NOTES
    Requires the Azure CLI (az) with the containerapp extension and an active
    `az login` on a subscription where you can create role assignments.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string] $ResourceGroup,

    [string] $Location = 'eastus',

    [Parameter(Mandatory = $true)]
    [string] $ContainerImage,

    [Parameter(Mandatory = $true)]
    [System.Security.SecureString] $AdminPassword,

    [string] $AdminUsername = 'admin',

    [ValidatePattern('^[a-z0-9]{2,16}$')]
    [string] $NamePrefix = 'latscale',

    [ValidateRange(1, 30)]
    [int] $MinReplicas = 1,

    [ValidateRange(1, 30)]
    [int] $MaxReplicas = 10
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Write-Step([string] $message) {
    Write-Host "==> $message" -ForegroundColor Cyan
}

$scriptRoot = $PSScriptRoot
$templateFile = Join-Path $scriptRoot 'main.bicep'
$toolScript = Join-Path $scriptRoot '..\..\..\tools\New-LatticeStateCredential.ps1'

if (-not (Test-Path $templateFile)) { throw "Template not found: $templateFile" }
if (-not (Test-Path $toolScript)) { throw "Credential helper not found: $toolScript" }

# --- Preflight: az CLI present and logged in ---------------------------------
if (-not (Get-Command az -ErrorAction SilentlyContinue)) {
    throw 'Azure CLI (az) is not on PATH. Install it and run `az login`.'
}

Write-Step 'Verifying Azure CLI login'
az account show --output none 2>$null
if ($LASTEXITCODE -ne 0) {
    throw 'Not logged in. Run `az login` (and `az account set --subscription <id>`) first.'
}

# Ensure the containerapp extension is present (idempotent).
az extension add --name containerapp --upgrade --only-show-errors --output none 2>$null

# --- Hash the admin password (never handle plaintext beyond this block) -------
Write-Step 'Hashing admin password (salted PBKDF2-SHA256)'
$plaintextPtr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($AdminPassword)
try {
    $plaintext = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($plaintextPtr)
    $env:LATTICE_DEPLOY_PW = $plaintext
    $passwordHash = & $toolScript -Username $AdminUsername -PasswordEnv 'LATTICE_DEPLOY_PW' -Format value
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($passwordHash)) {
        throw 'Password hashing failed (see the helper diagnostics above).'
    }
}
finally {
    Remove-Item Env:LATTICE_DEPLOY_PW -ErrorAction SilentlyContinue
    [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($plaintextPtr)
    $plaintext = $null
}

# --- Resource group ----------------------------------------------------------
Write-Step "Ensuring resource group '$ResourceGroup' in $Location"
az group create --name $ResourceGroup --location $Location --output none

# --- Deploy the template -----------------------------------------------------
$deploymentName = "clusterscaling-$([DateTime]::UtcNow.ToString('yyyyMMddHHmmss'))"
Write-Step "Deploying template (deployment: $deploymentName)"
az deployment group create `
    --resource-group $ResourceGroup `
    --name $deploymentName `
    --template-file $templateFile `
    --parameters `
        namePrefix=$NamePrefix `
        containerImage=$ContainerImage `
        adminUsername=$AdminUsername `
        adminPasswordHash=$passwordHash `
        minReplicas=$MinReplicas `
        maxReplicas=$MaxReplicas `
    --output none

# --- Read outputs ------------------------------------------------------------
Write-Step 'Reading deployment outputs'
$outputsJson = az deployment group show `
    --resource-group $ResourceGroup `
    --name $deploymentName `
    --query properties.outputs `
    --output json
$outputs = $outputsJson | ConvertFrom-Json

$fqdn = $outputs.ingressFqdn.value
$dataApiAddress = $outputs.dataApiAddress.value
$appName = $outputs.containerAppName.value

Write-Host ''
Write-Host 'ClusterScaling deployed.' -ForegroundColor Green
Write-Host "  resource group : $ResourceGroup"
Write-Host "  container app  : $appName"
Write-Host "  ingress FQDN   : $fqdn"
Write-Host "  data API       : $dataApiAddress"
Write-Host "  scale rule     : metrics-api reads scaleValue from https://$fqdn/lattice/scale (target 1, replicas $MinReplicas..$MaxReplicas)"
Write-Host ''
Write-Host 'Drive compute-axis load and watch ACA scale out with:' -ForegroundColor Yellow
Write-Host "  ./drive-load.ps1 -ResourceGroup $ResourceGroup -AppName $appName"
Write-Host ''
Write-Host 'Tear everything down when done with:' -ForegroundColor Yellow
Write-Host "  ./teardown.ps1 -ResourceGroup $ResourceGroup"
