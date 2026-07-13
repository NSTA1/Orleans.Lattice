#Requires -Version 7.0
<#
.SYNOPSIS
    Drives compute-axis load against a deployed ClusterScaling app and reports the
    ACA replica-count timeline.

.DESCRIPTION
    Resolves the deployed app's ingress FQDN, launches the bundled .NET LoadDriver
    console (which speaks gRPC to the write-capable data API over the managed TLS
    ingress, presenting the admin Basic credential), and - while the driver runs -
    polls `az containerapp replica list` once per interval to print a replica-count
    timeline alongside the driver's continuous offered-load throughput.

    The load is COMPUTE-axis by construction: a high op rate spread across many
    distinct trees and keys with a tiny payload. That grows activation + dispatch
    pressure, which is what the scaling signal's scaleValue tracks; it does NOT
    grow retained bytes, which is the storage axis and never inflates replica
    count. Expect scale-out to LAG the load by tens of seconds: KEDA polling
    interval + cooldown + the signal's EWMA smoothing all sit between offered load
    and a replica being added. Sustain the load (default 5 minutes) so the window
    is comfortably crossed, then watch the count settle back to minReplicas after
    the driver stops.

.PARAMETER ResourceGroup
    The resource group deploy.ps1 provisioned into.

.PARAMETER AppName
    The container app name. Resolved from the resource group if omitted (works
    when exactly one ClusterScaling app is present).

.PARAMETER AdminPassword
    Plaintext admin password as a SecureString (prompted if omitted). Must match
    the password deploy.ps1 hashed into the ACA secret.

.PARAMETER AdminUsername
    Admin username. Defaults to admin.

.PARAMETER Rate
    Offered operations per second. Defaults to 2000.

.PARAMETER Duration
    Seconds to sustain the load. Defaults to 300.

.PARAMETER Trees
    Distinct trees to spread load across. Defaults to 64.

.PARAMETER KeySpace
    Distinct keys per tree cycle. Defaults to 100000.

.PARAMETER ReadRatio
    Fraction of ops issued as reads (0..1). Defaults to 0.2.

.PARAMETER PollIntervalSeconds
    Replica-count polling cadence. Defaults to 10.

.NOTES
    Requires the Azure CLI (az) with the containerapp extension, an active
    `az login`, and the .NET SDK (to run the bundled LoadDriver via dotnet run).
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string] $ResourceGroup,

    [string] $AppName,

    [Parameter(Mandatory = $true)]
    [System.Security.SecureString] $AdminPassword,

    [string] $AdminUsername = 'admin',

    [double] $Rate = 2000,

    [double] $Duration = 300,

    [int] $Trees = 64,

    [long] $KeySpace = 100000,

    [double] $ReadRatio = 0.2,

    [int] $PollIntervalSeconds = 10
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Write-Step([string] $message) {
    Write-Host "==> $message" -ForegroundColor Cyan
}

$scriptRoot = $PSScriptRoot
$loadDriverProject = Join-Path $scriptRoot '..\src\ClusterScaling.LoadDriver\ClusterScaling.LoadDriver.csproj'
if (-not (Test-Path $loadDriverProject)) { throw "LoadDriver project not found: $loadDriverProject" }

if (-not (Get-Command az -ErrorAction SilentlyContinue)) {
    throw 'Azure CLI (az) is not on PATH. Install it and run `az login`.'
}
if (-not (Get-Command dotnet -ErrorAction SilentlyContinue)) {
    throw 'The .NET SDK (dotnet) is not on PATH. Install it to run the bundled LoadDriver.'
}

# --- Resolve the app + ingress FQDN ------------------------------------------
if ([string]::IsNullOrWhiteSpace($AppName)) {
    Write-Step 'Resolving container app name'
    $apps = @(az containerapp list --resource-group $ResourceGroup --query "[].name" --output json | ConvertFrom-Json)
    if ($apps.Count -eq 0) { throw "No container apps found in resource group '$ResourceGroup'." }
    if ($apps.Count -gt 1) { throw "Multiple container apps found; pass -AppName explicitly. Found: $($apps -join ', ')" }
    $AppName = $apps[0]
}

Write-Step "Resolving ingress FQDN for '$AppName'"
$fqdn = az containerapp show --resource-group $ResourceGroup --name $AppName `
    --query properties.configuration.ingress.fqdn --output tsv
if ([string]::IsNullOrWhiteSpace($fqdn)) { throw "Could not resolve ingress FQDN for app '$AppName'." }
$target = "https://$fqdn"

Write-Host "  app    : $AppName"
Write-Host "  target : $target"

# --- Pre-flight: confirm the app is actually serving before offering load -----
# Hits the unauthenticated /healthz endpoint (mapped by the silo, served over the
# same ingress). If the container is crash-looping or has no ready replica, the
# ingress returns 5xx / refuses the connection here - so we fail fast with the
# exact diagnostic commands instead of the load driver's cryptic gRPC stream
# reset thirty seconds into the run.
Write-Step "Checking data-API readiness at $target/healthz"
$healthUrl = "$target/healthz"
$ready = $false
for ($attempt = 1; $attempt -le 10; $attempt++) {
    try {
        $resp = Invoke-WebRequest -Uri $healthUrl -Method Get -TimeoutSec 10 -SkipHttpErrorCheck
        if ($resp.StatusCode -eq 200) { $ready = $true; break }
        Write-Host ("  attempt {0,2}: HTTP {1} (not ready yet)" -f $attempt, $resp.StatusCode) -ForegroundColor DarkYellow
    }
    catch {
        Write-Host ("  attempt {0,2}: {1}" -f $attempt, $_.Exception.Message) -ForegroundColor DarkYellow
    }
    Start-Sleep -Seconds 6
}
if (-not $ready) {
    Write-Host ''
    Write-Host 'The container app is not serving: the readiness probe never returned 200.' -ForegroundColor Red
    Write-Host 'The silo container is likely crash-looping or has no ready replica. Diagnose with:' -ForegroundColor Red
    Write-Host "  az containerapp revision list -g $ResourceGroup -n $AppName -o table"
    Write-Host "  az containerapp replica list  -g $ResourceGroup -n $AppName -o table"
    Write-Host "  az containerapp logs show     -g $ResourceGroup -n $AppName --tail 200"
    Write-Host "  az containerapp logs show     -g $ResourceGroup -n $AppName --type system --tail 100"
    throw 'Data API not ready; aborting before generating load.'
}
Write-Host '  ready  : yes (healthz returned 200)' -ForegroundColor Green

# --- Convert the password to plaintext for the driver argument ---------------
$plaintextPtr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($AdminPassword)
$plaintext = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($plaintextPtr)

# Replica-count poller (records a timeline while the driver runs).
function Get-ReplicaCount {
    $replicas = az containerapp replica list --resource-group $ResourceGroup --name $AppName `
        --query "length(@)" --output tsv 2>$null
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($replicas)) { return $null }
    return [int] $replicas
}

$driverJob = $null
try {
    $baseline = Get-ReplicaCount
    Write-Step "Baseline replica count: $(if ($null -eq $baseline) { 'n/a (no ready replica reported)' } else { $baseline })"

    # Launch the LoadDriver in a background job so this script can poll replicas
    # concurrently. Its stdout is drained and echoed each poll.
    Write-Step "Starting LoadDriver (rate=$Rate ops/s, duration=$Duration s)"
    $driverJob = Start-Job -ScriptBlock {
        param($proj, $target, $user, $password, $rate, $duration, $trees, $keyspace, $readRatio)
        & dotnet run --project $proj --configuration Release -- `
            --target $target `
            --user $user `
            --password $password `
            --rate $rate `
            --duration $duration `
            --trees $trees `
            --keyspace $keyspace `
            --read-ratio $readRatio 2>&1
    } -ArgumentList $loadDriverProject, $target, $AdminUsername, $plaintext, $Rate, $Duration, $Trees, $KeySpace, $ReadRatio

    # The plaintext is now captured in the job's argument list; drop our copy.
    $plaintext = $null

    Write-Host ''
    Write-Host 'Replica-count timeline (offered-load lines come from the driver):' -ForegroundColor Yellow
    $startUtc = [DateTime]::UtcNow
    while ($driverJob.State -eq 'Running') {
        Start-Sleep -Seconds $PollIntervalSeconds
        $count = Get-ReplicaCount
        $elapsed = [int]([DateTime]::UtcNow - $startUtc).TotalSeconds
        Write-Host ("  [t={0,5}s] replicas = {1}" -f $elapsed, ($count ?? 'n/a')) -ForegroundColor Green

        # Drain any driver output produced since the last poll.
        Receive-Job -Job $driverJob | ForEach-Object { Write-Host "    $_" }
    }

    # Flush remaining driver output.
    Receive-Job -Job $driverJob | ForEach-Object { Write-Host "    $_" }

    Write-Host ''
    Write-Step 'LoadDriver finished. Watching scale-in for two poll intervals'
    for ($i = 0; $i -lt 2; $i++) {
        Start-Sleep -Seconds $PollIntervalSeconds
        $elapsed = [int]([DateTime]::UtcNow - $startUtc).TotalSeconds
        Write-Host ("  [t={0,5}s] replicas = {1}" -f $elapsed, ((Get-ReplicaCount) ?? 'n/a')) -ForegroundColor Green
    }

    Write-Host ''
    Write-Host 'Scale-in continues after the load stops (KEDA cooldown + stabilization).' -ForegroundColor Yellow
    Write-Host "Watch it settle to minReplicas with:"
    Write-Host "  az containerapp replica list -g $ResourceGroup -n $AppName --query 'length(@)' -o tsv"
}
finally {
    $plaintext = $null
    [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($plaintextPtr)
    if ($null -ne $driverJob) {
        Remove-Job -Job $driverJob -Force -ErrorAction SilentlyContinue
    }
}
