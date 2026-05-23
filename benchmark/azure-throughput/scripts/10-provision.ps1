#requires -Version 7
<#
.SYNOPSIS
    Provisions the Azure infrastructure for the azure-throughput benchmark.
.DESCRIPTION
    Creates a resource group, an Azure Container Registry, a storage account, and a
    user-assigned managed identity. Grants the identity Storage Table Data Contributor
    on the storage account so the silo can write the WAL with managed identity instead
    of a key. ACR admin is enabled so ACI can pull without an extra identity grant.

    Idempotent: re-running is safe; existing resources are reused.

    Prefix resolution order (first non-empty wins):
      1. -Prefix parameter (or first positional arg).
      2. $env:BENCH_PREFIX.
      3. The persisted ./.prefix file next to this script (gitignored).
      4. Auto-generated: 'lat' + 7 random lowercase hex chars. The generated
         value is written to ./.prefix so every subsequent run of any
         benchmark/azure-throughput script on this machine reuses it. This
         keeps storage-account / ACR names globally unique per operator
         without anyone having to remember to set BENCH_PREFIX.

    Whatever wins is exported to $env:BENCH_PREFIX for the current session so
    subsequent scripts (20-build-and-deploy, 30-tail-logs, 90-teardown) see
    the same value.

    Examples:
      ./10-provision.ps1                                  # uses or creates .prefix
      ./10-provision.ps1 lat9506
      ./10-provision.ps1 -Prefix lat9506 -Location westeurope
      $env:BENCH_PREFIX = 'lat9506'; ./10-provision.ps1

    Parameters:
      -Prefix    short lowercase prefix (3-10 chars, used in resource names).
                 Falls back to $env:BENCH_PREFIX, then to ./.prefix, then to a
                 freshly generated id that is persisted to ./.prefix.
      -Location  azure region. Falls back to $env:BENCH_LOCATION, then 'westeurope'.
#>

[CmdletBinding()]
param(
    [Parameter(Position = 0)]
    [string] $Prefix,

    [string] $Location
)

$ErrorActionPreference = 'Stop'

# Parameter wins over the env var; env var is the fallback so older invocations still work.
if (-not [string]::IsNullOrWhiteSpace($Prefix)) {
    $env:BENCH_PREFIX = $Prefix
}
if (-not [string]::IsNullOrWhiteSpace($Location)) {
    $env:BENCH_LOCATION = $Location
}

# az is a native command; non-zero exits don't throw unless we check $LASTEXITCODE ourselves.
# Without this, an expired refresh token (AADSTS700082) silently produces empty outputs and
# the script writes a half-baked .context.json that breaks 20-build-and-deploy later.
function Invoke-Az {
    param([Parameter(ValueFromRemainingArguments=$true)][string[]] $Args)
    & az @Args
    if ($LASTEXITCODE -ne 0) {
        throw "az $($Args -join ' ') failed with exit code $LASTEXITCODE. If this is an auth error, run: az logout; ./00-login.ps1"
    }
}

# Sanity check: any az call surfaces an auth failure before we create resources.
$whoami = az account show --output json 2>$null
if ($LASTEXITCODE -ne 0 -or -not $whoami) {
    throw "az is not authenticated. Run: az logout; ./00-login.ps1"
}

# `az account show` is a LOCAL cache read - it passes even when the refresh token for
# the active subscription's home tenant has expired (the AADSTS700082 trap). Force a
# real ARM call so we fail loudly here instead of after creating half the resources.
# JMESPath limit, not --max-items: the latter isn't on every az version.
& az group list --query "[0].name" --output tsv 1>$null 2>$null
if ($LASTEXITCODE -ne 0) {
    $acct = $whoami | ConvertFrom-Json
    throw @"
ARM rejected the cached token for subscription '$($acct.id)' (tenant '$($acct.tenantId)').
This is usually AADSTS700082 - the refresh token for this tenant is older than 90 days.

Fix:
  az logout
  ./00-login.ps1 -Tenant $($acct.tenantId) -Subscription $($acct.id)
"@
}

$prefix   = $env:BENCH_PREFIX
$location = if ($env:BENCH_LOCATION) { $env:BENCH_LOCATION } else { 'westeurope' }

# Resolve persisted .prefix next to this script. We deliberately persist *outside*
# .context.json because the prefix has to exist before 10-provision.ps1 writes
# .context.json, and it should survive a teardown so re-provisioning lands on the
# same names (and the same operator stays globally unique on the storage-account
# namespace). The file is gitignored; see ./.gitignore.
$prefixFile = Join-Path $PSScriptRoot '.prefix'

if ([string]::IsNullOrWhiteSpace($prefix) -and (Test-Path $prefixFile)) {
    $persisted = (Get-Content -Raw -Path $prefixFile).Trim()
    if (-not [string]::IsNullOrWhiteSpace($persisted)) {
        $prefix = $persisted
        Write-Host "[provision] using persisted prefix from $prefixFile" -ForegroundColor DarkGray
    }
}

if ([string]::IsNullOrWhiteSpace($prefix)) {
    # 'lat' (Lattice) + 7 random lowercase hex chars => 10 chars total, fits the
    # 3-10 / [a-z0-9] validation rule below and stays well under the 24-char
    # Azure storage-account-name limit when concatenated with 'sa'.
    $rand = [System.Security.Cryptography.RandomNumberGenerator]::GetBytes(4)
    $hex  = ([System.BitConverter]::ToString($rand) -replace '-', '').ToLowerInvariant().Substring(0, 7)
    $prefix = "lat$hex"
    Set-Content -Path $prefixFile -Value $prefix -Encoding ascii -NoNewline
    Write-Host "[provision] generated fresh prefix '$prefix' and persisted to $prefixFile" -ForegroundColor Yellow
}

# Export so the rest of this session (and subsequent script invocations sharing it) see it.
$env:BENCH_PREFIX = $prefix

if ([string]::IsNullOrWhiteSpace($prefix)) {
    throw "Set `$env:BENCH_PREFIX (e.g. 'lat1234') before running."
}
if ($prefix.Length -lt 3 -or $prefix.Length -gt 10 -or $prefix -notmatch '^[a-z0-9]+$') {
    throw "BENCH_PREFIX must be 3-10 lowercase alphanumeric characters."
}

$rg       = "$prefix-rg"
$acr      = "$($prefix)acr"
$storage  = "$($prefix)sa"
$identity = "$prefix-mi"

Write-Host "[provision] prefix=$prefix location=$location" -ForegroundColor Cyan
Write-Host "[provision]   rg=$rg  acr=$acr  storage=$storage  identity=$identity"

# Resource group
Invoke-Az group create --name $rg --location $location --output none
Write-Host "[provision] rg ok"

# ACR (Basic, admin enabled so ACI can pull with username/password)
$acrExists = az acr show --name $acr --resource-group $rg --query name --output tsv 2>$null
if (-not $acrExists) {
    Invoke-Az acr create --resource-group $rg --name $acr --sku Basic --admin-enabled true --output none
}
Write-Host "[provision] acr ok"

# Storage account
$saExists = az storage account show --name $storage --resource-group $rg --query name --output tsv 2>$null
if (-not $saExists) {
    Invoke-Az storage account create --name $storage --resource-group $rg --location $location `
        --sku Standard_LRS --kind StorageV2 --output none
}
Write-Host "[provision] storage ok"

# Managed identity
$miExists = az identity show --name $identity --resource-group $rg --query name --output tsv 2>$null
if (-not $miExists) {
    Invoke-Az identity create --name $identity --resource-group $rg --location $location --output none
}
$miJsonRaw = & az identity show --name $identity --resource-group $rg --output json
if ($LASTEXITCODE -ne 0 -or -not $miJsonRaw) {
    throw "az identity show failed (exit=$LASTEXITCODE). Re-authenticate and retry."
}
$miJson = $miJsonRaw | ConvertFrom-Json
$miPrincipal = $miJson.principalId
$miResourceId = $miJson.id
if ([string]::IsNullOrWhiteSpace($miPrincipal)) {
    throw "Managed identity principalId is empty - az returned a malformed object. Re-authenticate and retry."
}
Write-Host "[provision] identity principalId=$miPrincipal"

# Role assignment: Storage Table Data Contributor on the storage account
$scope = & az storage account show --name $storage --resource-group $rg --query id --output tsv
if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($scope)) {
    throw "az storage account show failed (exit=$LASTEXITCODE)."
}
$existing = az role assignment list --assignee $miPrincipal --scope $scope `
    --query "[?roleDefinitionName=='Storage Table Data Contributor'].id" --output tsv 2>$null
if (-not $existing) {
    # Role assignment propagation can lag the identity create by ~30s; retry briefly.
    for ($i = 1; $i -le 10; $i++) {
        & az role assignment create --assignee-object-id $miPrincipal `
            --assignee-principal-type ServicePrincipal `
            --role "Storage Table Data Contributor" `
            --scope $scope --output none
        if ($LASTEXITCODE -eq 0) { break }
        if ($i -eq 10) { throw "az role assignment create failed after 10 attempts (exit=$LASTEXITCODE)." }
        Start-Sleep -Seconds 3
    }
}
Write-Host "[provision] role assignment ok"

# Save context for subsequent scripts.
$ctx = [pscustomobject]@{
    Prefix       = $prefix
    Location     = $location
    ResourceGroup = $rg
    Acr          = $acr
    AcrLoginServer = "$acr.azurecr.io"
    Storage      = $storage
    StorageUri   = "https://$storage.table.core.windows.net"
    Identity     = $identity
    IdentityResourceId = $miResourceId
    IdentityPrincipalId = $miPrincipal
}
$ctxPath = Join-Path $PSScriptRoot '.context.json'
$ctx | ConvertTo-Json -Depth 4 | Set-Content -Path $ctxPath -Encoding utf8
Write-Host "[provision] wrote $ctxPath"
Write-Host "[provision] done. Next: 20-build-and-deploy.ps1" -ForegroundColor Green
