<#
.SYNOPSIS
  Launches the Orleans.Lattice.Explorer pointed at the running MultiSiteManufacturing
  sample cluster (issue #886).

.DESCRIPTION
  Starts the Blazor Server web explorer, seeded to connect to one of the sample's two published Traefik
  endpoints over the read-only state API:

    http://localhost:5001  US cluster
    http://localhost:5002  EU cluster

  The endpoint and transport posture are passed to the explorer through the
  launcher-friendly environment-variable bootstrap, so no per-user app-data
  config is hand-edited. Username and password parameters are accepted for
  compatibility with the old launcher flow, but the rewritten web head ignores
  credential seeding by default; sign in manually in the connection dialog when
  the browser opens. The explorer connects over loopback h2c (HTTP/2 cleartext,
  insecure-loopback-dev mode), which is how the sample exposes the state API
  through Traefik.

  Bring the cluster up first with ./run.ps1 (anonymous) or
  ./run.ps1 -Username <u> -Password <p> (state-API auth enabled), then run this
  script. For an auth-enabled cluster, enter the matching credentials in the
  Explorer sign-in dialog.

.PARAMETER Cluster
  Which cluster to browse: 'us' (default, http://localhost:5001) or 'eu'
  (http://localhost:5002). Ignored when -Endpoint is supplied.

.PARAMETER Endpoint
  Explicit state-API endpoint URL, overriding the -Cluster default.

.PARAMETER Username
  Sign-in username accepted for compatibility with the old launcher flow. The
  rewritten web head keeps credential seeding off, so this value is ignored by
  the web head; use the Explorer sign-in dialog after launch.

.PARAMETER Password
  The plaintext password paired with -Username. It is exported only for
  compatibility and cleared as soon as the explorer exits; the rewritten web head
  ignores it unless a host explicitly enables environment credential seeding.

.EXAMPLE
  ./run-explorer.ps1
    Launch the Blazor web explorer against the US cluster, anonymously.

.EXAMPLE
  ./run-explorer.ps1 -Cluster eu
    Launch the Blazor web explorer against the EU cluster.

.EXAMPLE
  ./run-explorer.ps1 -Username alice -Password 'Sup3rSecret'
    Launch the web explorer against the US cluster; when the sign-in dialog opens,
    enter alice / Sup3rSecret (matching ./run.ps1).
#>
param(
  [ValidateSet('us', 'eu')]
  [string]$Cluster = 'us',

  [string]$Endpoint,

  [string]$Username,

  [string]$Password
)

$ErrorActionPreference = "Stop"

# Validate the -Username / -Password pairing: both or neither.
$authRequested = -not [string]::IsNullOrWhiteSpace($Username) -or -not [string]::IsNullOrWhiteSpace($Password)
$signIn = -not [string]::IsNullOrWhiteSpace($Username) -and -not [string]::IsNullOrWhiteSpace($Password)
if ($authRequested -and -not $signIn) {
    throw "Supply BOTH -Username and -Password to sign in, or neither to connect anonymously."
}

# Resolve the state-API endpoint: explicit -Endpoint wins, else the cluster default.
if ([string]::IsNullOrWhiteSpace($Endpoint)) {
    $Endpoint = if ($Cluster -eq 'eu') { "http://localhost:5002" } else { "http://localhost:5001" }
}

Write-Host "Explorer endpoint: $Endpoint" -ForegroundColor Cyan

# Best-effort reachability probe (non-fatal): the cluster may still be settling.
try {
    $uri = [Uri]$Endpoint
    $probe = [System.Net.Sockets.TcpClient]::new()
    $iar = $probe.BeginConnect($uri.Host, $uri.Port, $null, $null)
    if ($iar.AsyncWaitHandle.WaitOne(2000)) {
        $probe.EndConnect($iar)
        Write-Host "Endpoint is reachable (TCP)." -ForegroundColor Green
    } else {
        Write-Host "Warning: $Endpoint did not accept a TCP connection within 2s." -ForegroundColor Yellow
        Write-Host "  Start the cluster first: ./run.ps1" -ForegroundColor Yellow
    }
    $probe.Close()
} catch {
    Write-Host "Warning: could not probe $Endpoint ($($_.Exception.Message))." -ForegroundColor Yellow
}

# Resolve the standalone web head project relative to this script.
$webProject = Join-Path $PSScriptRoot "..\..\src\lattice.explorer\Web\Orleans.Lattice.Explorer.WebHost.csproj"

# The web head's own listening URL (separate from the state-API endpoint above).
$webUrl = "http://localhost:5290"

# Seed the explorer via the launcher-friendly environment bootstrap. The
# endpoint + insecure-loopback-dev flag are honoured by the config bootstrap.
# Credential env vars are kept only for compatibility with the old launcher flow:
# the rewritten web head ignores them by default, so the operator signs in through
# the browser dialog. The password env var is cleared in the finally block below.
$env:LATTICE_EXPLORER_ENDPOINT = $Endpoint
$env:LATTICE_EXPLORER_INSECURE_DEV = "true"
if ($signIn) {
    $env:LATTICE_EXPLORER_USERNAME = $Username
    $env:LATTICE_EXPLORER_PASSWORD = $Password
    Write-Host "Credential seed for '$Username' was provided, but the web head ignores it by default; sign in through the browser dialog." -ForegroundColor Cyan
} else {
    Remove-Item Env:LATTICE_EXPLORER_USERNAME -ErrorAction SilentlyContinue
    Remove-Item Env:LATTICE_EXPLORER_PASSWORD -ErrorAction SilentlyContinue
}

try {
    if (-not (Test-Path $webProject)) {
        throw "Web explorer project not found at $webProject"
    }
    $env:ASPNETCORE_URLS = $webUrl
    Write-Host "Launching the Blazor web explorer at $webUrl ..." -ForegroundColor Cyan
    Write-Host "  Open $webUrl in a browser once it has started." -ForegroundColor Cyan
    # Pass --urls through to the app (after --) so the bound address is
    # authoritative. dotnet run otherwise applies the Web project's
    # launchSettings.json applicationUrl, which would override
    # ASPNETCORE_URLS and bind a different port than the one printed above.
    & dotnet run --project $webProject -- --urls $webUrl
    if ($LASTEXITCODE -ne 0) { throw "dotnet run (web head) failed (exit $LASTEXITCODE)." }
}
finally {
    # Never leave the plaintext password lingering in this process environment.
    Remove-Item Env:LATTICE_EXPLORER_PASSWORD -ErrorAction SilentlyContinue
    Remove-Item Env:LATTICE_EXPLORER_USERNAME -ErrorAction SilentlyContinue
}
