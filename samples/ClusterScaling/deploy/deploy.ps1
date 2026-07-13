#Requires -Version 7.0
<#
.SYNOPSIS
    Deploys the ClusterScaling Orleans.Lattice.Scaling sample to Azure Container Apps.

.DESCRIPTION
    Provisions (idempotently) the whole sample stack: a Basic Azure Container
    Registry (unless you point it at an existing one), builds and pushes the silo
    image into that registry via `az acr build` (server-side, no local Docker),
    then a user-assigned managed identity,
    a Tables-only storage account (Orleans clustering + reminders + grain state +
    Lattice WAL, shared-key access disabled), the Storage Table Data Contributor
    role for the identity, an AcrPull grant so the identity can pull the image, a
    Log Analytics workspace, a Container Apps managed environment, and the
    ClusterScaling silo container app with a KEDA metrics-api scale rule that
    reads scaleValue from /lattice/scale.

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

.PARAMETER Registry
    OPTIONAL. Name of an existing Azure Container Registry (not its login server)
    to build and push the silo image into, e.g. myregistry. When omitted, the
    script provisions a Basic ACR as part of the deployment and uses that. Either
    way the container app is wired to pull from the registry via the managed
    identity (AcrPull). Mutually exclusive with -ContainerImage.

.PARAMETER ContainerImage
    Escape hatch: a fully-qualified, already-built silo image (e.g.
    myregistry.azurecr.io/clusterscaling-silo:latest). Supply this INSTEAD of
    -Registry to skip both the registry provisioning and the build and deploy a
    pre-built image. No AcrPull is wired; you own the image's pull access.

.PARAMETER ImageName
    Repository name for the built image. Defaults to clusterscaling-silo. Unused
    with -ContainerImage.

.PARAMETER ImageTag
    Tag for the built image. Defaults to latest. Unused with -ContainerImage.

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

    [string] $Registry,

    [string] $ContainerImage,

    [string] $ImageName = 'clusterscaling-silo',

    [string] $ImageTag = 'latest',

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

# --- Resource group (created first so the registry + app land in it) ---------
Write-Step "Ensuring resource group '$ResourceGroup' in $Location"
az group create --name $ResourceGroup --location $Location --output none
if ($LASTEXITCODE -ne 0) { throw "Resource group creation failed (az exited $LASTEXITCODE)." }

# --- Resolve the silo image and registry -------------------------------------
# Three modes:
#   * -ContainerImage <ref> : deploy a pre-built external image as-is. No ACR is
#     provisioned and no managed pull is wired; you own its pull access.
#   * -Registry <name>      : build+push into an existing ACR you already have,
#     and wire the app to pull from it via the managed identity.
#   * (neither)             : provision a Basic ACR in this resource group, build
#     into it, and wire the app to pull from it via the managed identity.
$repoRoot = (Resolve-Path (Join-Path $scriptRoot '..\..\..')).Path
$dockerfile = Join-Path $repoRoot 'samples/ClusterScaling/src/ClusterScaling.Silo/Dockerfile'
$registryName = ''   # non-empty => main.bicep wires managed-identity pull (AcrPull)

if ($ContainerImage) {
    if ($Registry) {
        throw 'Specify at most one of -ContainerImage (pre-built external image) or -Registry (existing ACR to build into).'
    }
    Write-Step "Using pre-built image '$ContainerImage' (skipping build; you own its pull access)"
}
else {
    if (-not (Test-Path $dockerfile)) { throw "Dockerfile not found: $dockerfile" }

    if ($Registry) {
        $registryName = $Registry
        Write-Step "Using existing container registry '$registryName'"
    }
    else {
        # Provision a Basic ACR as part of the sample's own infrastructure.
        $registryTemplate = Join-Path $scriptRoot 'registry.bicep'
        if (-not (Test-Path $registryTemplate)) { throw "Template not found: $registryTemplate" }
        $registryDeployment = "clusterscaling-acr-$([DateTime]::UtcNow.ToString('yyyyMMddHHmmss'))"
        Write-Step 'Provisioning a Basic Azure Container Registry'
        az deployment group create `
            --resource-group $ResourceGroup `
            --name $registryDeployment `
            --template-file $registryTemplate `
            --parameters namePrefix=$NamePrefix `
            --output none
        if ($LASTEXITCODE -ne 0) { throw "Registry provisioning failed (az exited $LASTEXITCODE)." }
        $registryName = az deployment group show `
            --resource-group $ResourceGroup `
            --name $registryDeployment `
            --query properties.outputs.registryName.value `
            --output tsv
        if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($registryName)) {
            throw 'Could not read the provisioned registry name from the deployment outputs.'
        }
        Write-Host "    registry: $registryName" -ForegroundColor DarkGray
    }

    $imageRef = "${ImageName}:${ImageTag}"

    # az acr build packs the source dir client-side and, unlike docker build,
    # does not reliably honour .dockerignore: it walks into .vs/bin/obj and dies
    # on Visual-Studio-locked files ([Errno 13] Permission denied). Stage only
    # the inputs the image needs into a clean temp dir so the pack is small,
    # deterministic, and free of locked or oversized files.
    $dockerfileRel = 'samples/ClusterScaling/src/ClusterScaling.Silo/Dockerfile'
    $stageRoot = Join-Path ([IO.Path]::GetTempPath()) "clusterscaling-ctx-$([Guid]::NewGuid().ToString('N'))"
    try {
        Write-Step "Staging a clean build context in $stageRoot"
        New-Item -ItemType Directory -Path $stageRoot -Force | Out-Null

        function Copy-BuildTree([string] $relative) {
            $src = Join-Path $repoRoot $relative
            if (-not (Test-Path $src)) { throw "Required build input missing: $src" }
            $dst = Join-Path $stageRoot $relative
            New-Item -ItemType Directory -Path $dst -Force | Out-Null
            # Mirror the tree minus build/IDE dirs. robocopy exit codes < 8 are success.
            robocopy $src $dst /E /XD bin obj .vs /NFL /NDL /NJH /NJS /NP /R:1 /W:1 | Out-Null
            if ($LASTEXITCODE -ge 8) { throw "robocopy failed staging '$relative' (exit $LASTEXITCODE)." }
        }

        Copy-Item (Join-Path $repoRoot 'Directory.Build.targets') (Join-Path $stageRoot 'Directory.Build.targets')
        Copy-BuildTree 'src'
        Copy-BuildTree 'samples/ClusterScaling/src'

        Write-Step "Building and pushing silo image '$imageRef' into registry '$registryName'"
        Write-Host '    (az acr build runs server-side in the registry and streams its logs below)' -ForegroundColor DarkGray
        az acr build `
            --registry $registryName `
            --image $imageRef `
            --file $dockerfileRel `
            $stageRoot
        if ($LASTEXITCODE -ne 0) {
            throw "Container image build/push failed (az acr build exited $LASTEXITCODE)."
        }
    }
    finally {
        Remove-Item -Path $stageRoot -Recurse -Force -ErrorAction SilentlyContinue
    }

    Write-Step "Resolving login server for registry '$registryName'"
    $loginServer = az acr show --name $registryName --query loginServer --output tsv
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($loginServer)) {
        throw "Could not resolve the login server for registry '$registryName'."
    }
    $ContainerImage = "$loginServer/$imageRef"
    Write-Host "    pushed: $ContainerImage" -ForegroundColor DarkGray
}

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

# --- Deploy the template -----------------------------------------------------
$deploymentName = "clusterscaling-$([DateTime]::UtcNow.ToString('yyyyMMddHHmmss'))"
Write-Step "Deploying template (deployment: $deploymentName)"
$deployParams = @(
    "namePrefix=$NamePrefix"
    "containerImage=$ContainerImage"
    "adminUsername=$AdminUsername"
    "adminPasswordHash=$passwordHash"
    "minReplicas=$MinReplicas"
    "maxReplicas=$MaxReplicas"
)
if ($registryName) { $deployParams += "registryName=$registryName" }

az deployment group create `
    --resource-group $ResourceGroup `
    --name $deploymentName `
    --template-file $templateFile `
    --parameters $deployParams `
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
