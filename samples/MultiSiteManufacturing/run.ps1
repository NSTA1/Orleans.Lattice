<#
.SYNOPSIS
  Launches the Multi-Site Manufacturing sample.

.DESCRIPTION
  Starts Azurite (if not already running), builds the host project **once**,
  then launches one or two instances of the MultiSiteManufacturing.Host —
  silo A on http://localhost:5001 and silo B on http://localhost:5002. Both
  silos share the local Azurite account, so they form a two-silo Orleans
  cluster and share grain state + the Lattice fact store.

  We build once and invoke the built DLL directly (instead of `dotnet run`
  per silo) because two concurrent `dotnet run` invocations against the same
  project race on `bin/obj`, which corrupts Blazor's static-asset manifest
  and silently kills the interactive SignalR circuit — the UI renders but
  @onclick handlers (Chaos flyout, Race, Fix) stop firing.

  Requires:
    * .NET 10 SDK on PATH
    * azurite on PATH (npm install -g azurite) for default storage

  Usage:
    ./run.ps1                 # both silos
    ./run.ps1 -SingleSilo     # only silo A
    ./run.ps1 -NoAzurite      # assume Azurite (or real storage) is already running
    ./run.ps1 -NoBuild        # skip the pre-build step (assumes host DLL is current)
#>
param(
  [switch]$SingleSilo,
  [switch]$NoAzurite,
  [switch]$NoBuild
)

$ErrorActionPreference = "Stop"

$hostProj = Join-Path $PSScriptRoot "src\MultiSiteManufacturing.Host\MultiSiteManufacturing.Host.csproj"
if (-not (Test-Path $hostProj)) {
    throw "Host project not found at $hostProj"
}

$hostDll = Join-Path $PSScriptRoot "src\MultiSiteManufacturing.Host\bin\Debug\net10.0\MultiSiteManufacturing.Host.dll"

# Build once so the two silos don't race on bin/obj.
if (-not $NoBuild) {
    Write-Host "Building host project once..."
    & dotnet build $hostProj --configuration Debug --nologo -v minimal
    if ($LASTEXITCODE -ne 0) { throw "Build failed (exit $LASTEXITCODE)." }
}

if (-not (Test-Path $hostDll)) {
    throw "Host DLL not found at $hostDll. Re-run without -NoBuild."
}

# Start Azurite if needed.
$azuriteProc = $null
if (-not $NoAzurite) {
    $existing = Get-Process -Name azurite -ErrorAction SilentlyContinue
    if ($existing) {
        Write-Host "Azurite already running (pid $($existing.Id))."
    } else {
        $azuriteCmd = Get-Command azurite -ErrorAction SilentlyContinue
        if (-not $azuriteCmd) {
            Write-Warning "azurite not found on PATH. Install via 'npm install -g azurite' or start it manually."
            Write-Warning "Continuing without starting Azurite — if it isn't running already, the silos will fail to cluster."
        } else {
            $azuriteDir = Join-Path $env:TEMP "msmfg-azurite"
            New-Item -ItemType Directory -Force -Path $azuriteDir | Out-Null
            Write-Host "Starting Azurite (location: $azuriteDir)..."
            $azuriteExe = $azuriteCmd.Source
            $azuriteProc = Start-Process -FilePath $azuriteExe -ArgumentList @("--silent", "--location", $azuriteDir) -PassThru -WindowStyle Hidden
            Start-Sleep -Seconds 2
        }
    }
}

$procs = @()

Write-Host "Starting silo A on http://localhost:5001 ..."
$siloA = Start-Process -FilePath dotnet `
    -ArgumentList @($hostDll, "--silo-id", "a") `
    -PassThru
$procs += $siloA

if (-not $SingleSilo) {
    Write-Host "Waiting for silo A to become primary before launching silo B..."
    Start-Sleep -Seconds 8

    Write-Host "Starting silo B on http://localhost:5002 ..."
    $siloB = Start-Process -FilePath dotnet `
        -ArgumentList @($hostDll, "--silo-id", "b") `
        -PassThru
    $procs += $siloB
}

Write-Host ""
Write-Host "Silos running. PIDs: $($procs.Id -join ', ')"
Write-Host "Open http://localhost:5001 (silo A) and http://localhost:5002 (silo B)."
Write-Host "Press Ctrl+C to stop."

try {
    while ($procs | Where-Object { -not $_.HasExited }) {
        Start-Sleep -Seconds 1
    }
} finally {
    foreach ($p in $procs) {
        if ($p -and -not $p.HasExited) {
            Write-Host "Stopping pid $($p.Id)..."
            Stop-Process -Id $p.Id -Force -ErrorAction SilentlyContinue
        }
    }
    if ($azuriteProc -and -not $azuriteProc.HasExited) {
        Write-Host "Stopping Azurite..."
        Stop-Process -Id $azuriteProc.Id -Force -ErrorAction SilentlyContinue
    }
}

