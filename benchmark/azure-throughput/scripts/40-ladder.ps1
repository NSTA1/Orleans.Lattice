#requires -Version 7
<#
.SYNOPSIS
    Runs a throughput-ladder sweep: for each (VehicleCount, TickHz) rung, redeploys the
    container group and waits for the producer to exit, then parses the silo log for the
    final "Entries written per second (avg)=..." line.
.DESCRIPTION
    The first rung builds the images (unless -SkipBuild is supplied); subsequent rungs
    re-use them via 20-build-and-deploy.ps1 -SkipBuild, so the ladder spends its wall-clock
    time on actual measurements rather than on `az acr build`.

    Each rung's run time is `DurationSec` (default 60s). A short cooldown between rungs
    lets the previous ACI container group fully shut down before we recreate it.

    Output: a results table printed to stdout and a CSV at .ladder-results.csv.

.PARAMETER Rungs
    Optional list of "vehicles:tickHz" pairs. Defaults to a 5-rung sweep covering
    1k/5k/20k/50k/100k events per second target rates.

.PARAMETER DurationSec
    Seconds per rung (default 60).

.PARAMETER CooldownSec
    Seconds to wait between rungs (default 10).

.PARAMETER SkipBuild
    Skip the initial image build. Use when images are already in ACR from a previous run.
#>

[CmdletBinding()]
param(
    [string[]] $Rungs = @(
        '1000:5',     #   5,000 / s target
        '5000:5',     #  25,000 / s
        '10000:5',    #  50,000 / s
        '20000:5',    # 100,000 / s
        '50000:5'     # 250,000 / s
    ),
    [int] $DurationSec = 60,
    [int] $CooldownSec = 10,
    [switch] $SkipBuild
)

$ErrorActionPreference = 'Stop'

$ctxPath = Join-Path $PSScriptRoot '.context.json'
if (-not (Test-Path $ctxPath)) {
    throw "Run 10-provision.ps1 first; missing $ctxPath."
}
$ctx = Get-Content $ctxPath | ConvertFrom-Json
$containerGroup = "$($ctx.Prefix)-bench"

$results = New-Object System.Collections.Generic.List[object]
$resultsCsv = Join-Path $PSScriptRoot '.ladder-results.csv'

Write-Host "[ladder] rungs=$($Rungs.Count) durationPerRung=${DurationSec}s cooldown=${CooldownSec}s" -ForegroundColor Cyan
Write-Host "[ladder] results -> $resultsCsv" -ForegroundColor DarkGray

$deployScript = Join-Path $PSScriptRoot '20-build-and-deploy.ps1'

for ($i = 0; $i -lt $Rungs.Count; $i++) {
    $rung = $Rungs[$i]
    $parts = $rung.Split(':')
    if ($parts.Count -ne 2) { throw "Bad rung '$rung'. Expected 'vehicles:tickHz'." }
    $vehicles = [int] $parts[0]
    $hz       = [int] $parts[1]
    $target   = $vehicles * $hz

    Write-Host ""
    Write-Host ("=" * 78) -ForegroundColor DarkGray
    Write-Host "[ladder] rung $($i+1)/$($Rungs.Count): vehicles=$vehicles tickHz=$hz target=$target/s" -ForegroundColor Green
    Write-Host ("=" * 78) -ForegroundColor DarkGray

    # Build only on the first rung (unless -SkipBuild for the whole ladder).
    $skipBuildForRung = $SkipBuild -or ($i -gt 0)

    & $deployScript -VehicleCount $vehicles -TickHz $hz -DurationSec $DurationSec -SkipBuild:$skipBuildForRung
    if ($LASTEXITCODE -ne 0) { throw "Deploy failed for rung $rung." }

    # Wait for the producer container to terminate (restartPolicy=Never makes that a
    # terminal state). Poll instanceView every 5s; bail with a useful diagnostic if it
    # takes more than DurationSec + 90s.
    $deadline = (Get-Date).AddSeconds($DurationSec + 90)
    $producerState = ''
    while ((Get-Date) -lt $deadline) {
        $stateJson = & az container show --resource-group $ctx.ResourceGroup --name $containerGroup `
            --query "containers[?name=='producer'].instanceView.currentState.state" --output tsv 2>$null
        if ($LASTEXITCODE -eq 0 -and $stateJson) {
            $producerState = $stateJson.Trim()
            if ($producerState -eq 'Terminated') { break }
        }
        Start-Sleep -Seconds 5
    }

    if ($producerState -ne 'Terminated') {
        Write-Warning "[ladder] producer did not terminate within $($DurationSec + 90)s (last state='$producerState'); reading partial log anyway."
    }

    # Pull the silo log and grep for the FINAL line.
    $siloLog = & az container logs --resource-group $ctx.ResourceGroup --name $containerGroup --container-name silo 2>$null
    if ($LASTEXITCODE -ne 0 -or -not $siloLog) {
        Write-Warning "[ladder] could not read silo log for rung $rung; skipping."
        continue
    }

    # Steady-state: avg of per-second lines from t=10s onward (skip producer warm-up).
    $perSec = ($siloLog -split "`n") |
        Select-String -Pattern 'Entries written per second=\s*([\d,]+)' |
        ForEach-Object {
            $line = $_.Line
            if ($line -match 't=\s*([\d.]+)s.*Entries written per second=\s*([\d,]+)') {
                [pscustomobject]@{
                    T    = [double] $Matches[1]
                    Rate = [long] ($Matches[2] -replace ',', '')
                }
            }
        }

    $steady = $perSec | Where-Object { $_.T -ge 10 }
    if (-not $steady) { $steady = $perSec }    # very short run: fall back to all samples.
    $steadyAvg = if ($steady) { [long] (($steady | Measure-Object Rate -Average).Average) } else { 0 }
    $steadyMax = if ($steady) { ($steady | Measure-Object Rate -Maximum).Maximum } else { 0 }
    $steadyMin = if ($steady) { ($steady | Measure-Object Rate -Minimum).Minimum } else { 0 }

    # Final-line headline (if present).
    $finalMatch = ($siloLog -split "`n") |
        Select-String -Pattern 'FINAL .*written=([\d,]+) failed=([\d,]+).*Entries written per second \(avg\)=\s*([\d,]+)' |
        Select-Object -Last 1
    $finalWritten = 0; $finalFailed = 0; $finalAvg = 0
    if ($finalMatch) {
        $m = $finalMatch.Matches[0]
        $finalWritten = [long] ($m.Groups[1].Value -replace ',', '')
        $finalFailed  = [long] ($m.Groups[2].Value -replace ',', '')
        $finalAvg     = [long] ($m.Groups[3].Value -replace ',', '')
    }

    $row = [pscustomobject]@{
        Rung            = $i + 1
        Vehicles        = $vehicles
        TickHz          = $hz
        TargetRate      = $target
        SteadyMin       = $steadyMin
        SteadyAvg       = $steadyAvg
        SteadyMax       = $steadyMax
        FinalWritten    = $finalWritten
        FinalFailed     = $finalFailed
        FinalAvgRate    = $finalAvg
    }
    $results.Add($row)

    Write-Host ""
    Write-Host "[ladder] rung $($i+1) summary:" -ForegroundColor Cyan
    Write-Host ("  target          : {0,12:N0}/s" -f $target)
    Write-Host ("  steady-state avg: {0,12:N0}/s" -f $steadyAvg)
    Write-Host ("  steady min/max  : {0,12:N0} .. {1,:N0}" -f $steadyMin, $steadyMax)
    Write-Host ("  total written   : {0,12:N0}" -f $finalWritten)
    Write-Host ("  total failed    : {0,12:N0}" -f $finalFailed)

    # Persist incrementally so a crash mid-ladder doesn't lose earlier rungs.
    $results | Export-Csv -Path $resultsCsv -NoTypeInformation -Encoding utf8

    if ($i -lt $Rungs.Count - 1) {
        Write-Host "[ladder] cooldown ${CooldownSec}s ..." -ForegroundColor DarkGray
        Start-Sleep -Seconds $CooldownSec
    }
}

Write-Host ""
Write-Host ("=" * 78) -ForegroundColor Green
Write-Host "[ladder] complete. results table:" -ForegroundColor Green
Write-Host ("=" * 78) -ForegroundColor Green
$results | Format-Table -AutoSize
Write-Host "[ladder] csv: $resultsCsv" -ForegroundColor Cyan
