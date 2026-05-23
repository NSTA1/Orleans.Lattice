#requires -Version 7
<#
.SYNOPSIS
    Azure CLI device-code login for the azure-throughput benchmark.
.DESCRIPTION
    Run interactively. The default flow:
      1. `az account clear` to purge any stale cached refresh tokens (the common cause of
         AADSTS700082 "refresh token expired" failures during 10-provision.ps1).
      2. `az login --use-device-code` (optionally scoped to a single tenant).
      3. If -Subscription is supplied, `az account set --subscription <id>` and then a real
         ARM call to verify the token works against that subscription's home tenant.

    Common failure mode this guards against:
      You have multiple Microsoft accounts in az's cache. `az login` refreshes the token for
      one tenant. You then `az account set --subscription <id>` to a subscription whose home
      tenant has a different, expired refresh token. `az account show` passes (it's a local
      read) but every real ARM call fails with AADSTS700082.

.PARAMETER Tenant
    Optional tenant id. If supplied, az login targets just that tenant.
.PARAMETER Subscription
    Optional subscription id. If supplied, sets it as the active subscription and verifies
    by listing resource groups in it.
.PARAMETER NoClear
    Skip the up-front `az account clear`. Use when you have other tooling that depends on
    cached credentials and you know what you're doing.
#>

[CmdletBinding()]
param(
    [string] $Tenant,
    [string] $Subscription,
    [switch] $NoClear
)

$ErrorActionPreference = 'Stop'

if (-not $NoClear) {
    Write-Host "[login] clearing cached az credentials (az account clear)" -ForegroundColor DarkGray
    az account clear 2>$null | Out-Null
}

$loginArgs = @('login', '--use-device-code')
if ($Tenant) { $loginArgs += @('--tenant', $Tenant) }

Write-Host "[login] launching: az $($loginArgs -join ' ')" -ForegroundColor Cyan
& az @loginArgs | Out-Host
if ($LASTEXITCODE -ne 0) { throw "az login failed (exit=$LASTEXITCODE)." }

if ($Subscription) {
    Write-Host "[login] setting active subscription: $Subscription" -ForegroundColor Cyan
    & az account set --subscription $Subscription
    if ($LASTEXITCODE -ne 0) { throw "az account set failed (exit=$LASTEXITCODE). Is the subscription id correct and visible to the signed-in user?" }
}

$accountJson = & az account show --output json
if ($LASTEXITCODE -ne 0 -or -not $accountJson) { throw "az account show failed after login." }
$account = $accountJson | ConvertFrom-Json

Write-Host ""
Write-Host "[login] active subscription:" -ForegroundColor Green
Write-Host "  name           : $($account.name)"
Write-Host "  subscriptionId : $($account.id)"
Write-Host "  tenantId       : $($account.tenantId)"
Write-Host "  user           : $($account.user.name)"
Write-Host ""

# Real ARM call to prove the refresh token for THIS subscription's tenant is valid.
# `az account show` only reads local cache; the failure mode we're guarding against
# is "cache says you're logged in, ARM says your RT expired 90 days ago".
# Use a JMESPath query that returns at most one row - portable across az versions
# (older az group list doesn't support --max-items).
Write-Host "[login] verifying token works against ARM (az group list) ..." -ForegroundColor DarkGray
& az group list --query "[0].name" --output tsv 1>$null
if ($LASTEXITCODE -ne 0) {
    throw "Auth check failed: az is signed in but ARM rejected the token. Re-run with -Tenant <homeTenantOfSubscription> and/or -Subscription <id>."
}
Write-Host "[login] auth verified. Set `$env:BENCH_PREFIX and run 10-provision.ps1 next." -ForegroundColor Green

