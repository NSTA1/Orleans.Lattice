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

.PARAMETER LocalBuild
    Build the producer and silo images locally via `docker build` and push them to
    ACR, instead of using the remote `az acr build` path. Forwarded to
    20-build-and-deploy.ps1. Requires Docker Desktop on a linux/amd64 host. Cuts
    the per-code-change build step from ~1m45s (clean ACI build with full source
    upload) to ~15s (local docker layer cache), which is the dominant speedup on
    iterative diagnostic probes.
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
    [switch] $SkipBuild,
    [switch] $LocalBuild
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
$phaseAResults = New-Object System.Collections.Generic.List[object]
$phaseACsv = Join-Path $PSScriptRoot '.ladder-phaseA.csv'

Write-Host "[ladder] rungs=$($Rungs.Count) durationPerRung=${DurationSec}s cooldown=${CooldownSec}s" -ForegroundColor Cyan
Write-Host "[ladder] results -> $resultsCsv" -ForegroundColor DarkGray
Write-Host "[ladder] phaseA  -> $phaseACsv" -ForegroundColor DarkGray

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

    & $deployScript -VehicleCount $vehicles -TickHz $hz -DurationSec $DurationSec -SkipBuild:$skipBuildForRung -LocalBuild:$LocalBuild
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

    # Prefer the local streamed silo log written by 20-build-and-deploy.ps1 (full transcript;
    # `az container logs` truncates and may race the force-stop). Find the most-recent
    # silo-*.log under benchmark/azure-throughput/.run/.
    $runDir   = Join-Path $PSScriptRoot '..' '.run'
    $localLog = Get-ChildItem -Path $runDir -Filter 'silo-*.log' -File -ErrorAction SilentlyContinue |
                Where-Object { $_.Name -notlike '*.err.log' } |
                Sort-Object LastWriteTime -Descending |
                Select-Object -First 1
    if ($localLog) {
        $siloLog = Get-Content -LiteralPath $localLog.FullName -Raw
        Write-Host "[ladder] silo log: $($localLog.Name) ($([math]::Round($localLog.Length/1MB,2)) MiB)" -ForegroundColor DarkGray
    } else {
        # Fallback: live container logs (truncated, but better than nothing).
        $siloLog = & az container logs --resource-group $ctx.ResourceGroup --name $containerGroup --container-name silo 2>$null
        if ($LASTEXITCODE -ne 0 -or -not $siloLog) {
            Write-Warning "[ladder] could not read silo log for rung $rung; skipping."
            continue
        }
    }

    # Silo-side hard-fail gate. The silo emits a loud, greppable line of
    # the shape `[silo] ERROR <name> ABORTED ...` whenever a startup-
    # critical hook fails (reshard, warm-up). The silo's own throw exits
    # the process, but ACI's default per-container exit-code propagation
    # is not enough for the harness to distinguish "silo healthy, just
    # got 0 offered load" from "silo failed to start"; the steady-state
    # parser below would happily report SteadyAvg=0 as a real
    # measurement. Scan for any `[silo] ERROR` line and abort the entire
    # ladder so an Azure-side regression cannot silently land a 0 keys/s
    # cell in the results CSV.
    $siloErrors = ($siloLog -split "`n") |
        Select-String -Pattern '^\[silo\] ERROR\b' |
        ForEach-Object { $_.Line.Trim() }
    if ($siloErrors -and $siloErrors.Count -gt 0) {
        $logName = if ($localLog) { $localLog.Name } else { '(streamed container logs)' }
        $errSummary = ($siloErrors | Select-Object -First 5) -join "`n  "
        throw "[ladder] silo reported $($siloErrors.Count) ERROR line(s) for rung '$rung'; aborting ladder run. Log: $logName. First $([math]::Min(5,$siloErrors.Count)):`n  $errSummary"
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

    # Steady-state: avg of per-second lines over the productive window.
    # The productive window is from t >= 10s (skip producer warm-up) to
    # the LAST second with a non-zero rate (skip trailing zero windows
    # while the silo drains and the watchdog ticks down). The prior
    # logic averaged over the full t >= 10s span including ~50s of
    # post-producer zero windows from the BENCH_TOTAL_DURATION_SEC=120s
    # watchdog tail, which dragged every "SteadyAvg" number across the
    # campaign roughly 2x low against the actual sustained throughput
    # during the producer's active window. See scaling.md U9p step
    # 8c-c-iv-c2-vi for the audit.
    $allSinceWarmup = $perSec | Where-Object { $_.T -ge 10 }
    if (-not $allSinceWarmup) { $allSinceWarmup = $perSec }  # very short run: fall back.

    # Find the last second with rate > 0; everything beyond is drain-tail.
    $lastProductive = ($allSinceWarmup | Where-Object { $_.Rate -gt 0 } | Measure-Object T -Maximum).Maximum
    $steady = if ($null -ne $lastProductive) {
        $allSinceWarmup | Where-Object { $_.T -le $lastProductive }
    } else {
        $allSinceWarmup  # no non-zero samples; fall back to everything past warmup.
    }
    $steadyAvg = if ($steady) { [long] (($steady | Measure-Object Rate -Average).Average) } else { 0 }
    $steadyMax = if ($steady) { ($steady | Measure-Object Rate -Maximum).Maximum } else { 0 }
    $steadyMin = if ($steady) { ($steady | Measure-Object Rate -Minimum).Minimum } else { 0 }
    $steadyDurationSec = if ($lastProductive) { [int] ($lastProductive - 10) } else { 0 }
    $drainTailSec      = if ($lastProductive) { [int] (($allSinceWarmup | Measure-Object T -Maximum).Maximum - $lastProductive) } else { 0 }

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
        Rung               = $i + 1
        Vehicles           = $vehicles
        TickHz             = $hz
        TargetRate         = $target
        SteadyMin          = $steadyMin
        SteadyAvg          = $steadyAvg
        SteadyMax          = $steadyMax
        SteadyDurationSec  = $steadyDurationSec
        DrainTailSec       = $drainTailSec
        FinalWritten       = $finalWritten
        FinalFailed        = $finalFailed
        FinalAvgRate       = $finalAvg
    }
    $results.Add($row)

    Write-Host ""
    Write-Host "[ladder] rung $($i+1) summary:" -ForegroundColor Cyan
    Write-Host ("  target          : {0,12:N0}/s" -f $target)
    Write-Host ("  steady-state avg: {0,12:N0}/s  (over {1}s productive window; {2}s drain tail excluded)" -f $steadyAvg, $steadyDurationSec, $drainTailSec)
    Write-Host ("  steady min/max  : {0,12:N0} .. {1,12:N0}" -f $steadyMin, $steadyMax)
    Write-Host ("  total written   : {0,12:N0}" -f $finalWritten)
    Write-Host ("  total failed    : {0,12:N0}" -f $finalFailed)

    # Persist incrementally so a crash mid-ladder doesn't lose earlier rungs.
    $results | Export-Csv -Path $resultsCsv -NoTypeInformation -Encoding utf8

    # Phase A diagnostic scrape. The silo emits one [phaseA] line per
    # (instrument, tree, shard, phase, status) tuple per cadence tick
    # (BENCH_PHASEA_REPORT_SEC, default 10). We keep only the LAST line
    # per tuple in the rung as the steady-state representative; that
    # last line covers the final cadence window before the producer
    # exited, which is the closest to steady-state the run produces.
    # Both histogram-shape and counter-shape lines are parsed; counter
    # lines have no p50/p90/p99/min/max/sum fields, which the regex
    # makes optional.
    $phaseALines = ($siloLog -split "`n") |
        Select-String -Pattern '\[phaseA\] t=\s*([\d.]+)s\s+instrument=(\S+)\s+tree=(\S+)\s+shard=(\S+)\s+phase=(\S+)\s+status=(\S+)\s+count=(\d+)(?:\s+sum=([\d.\-]+)\s+min=([\d.\-]+)\s+p50=([\d.\-]+)\s+p90=([\d.\-]+)\s+p99=([\d.\-]+)\s+max=([\d.\-]+))?' |
        ForEach-Object {
            $m = $_.Matches[0]
            [pscustomobject]@{
                T          = [double] $m.Groups[1].Value
                Instrument = $m.Groups[2].Value
                Tree       = $m.Groups[3].Value
                Shard      = $m.Groups[4].Value
                Phase      = $m.Groups[5].Value
                Status     = $m.Groups[6].Value
                Count      = [long]   $m.Groups[7].Value
                Sum        = if ($m.Groups[8].Success) { [double] $m.Groups[8].Value } else { 0.0 }
                Min        = if ($m.Groups[9].Success) { [double] $m.Groups[9].Value } else { 0.0 }
                P50        = if ($m.Groups[10].Success) { [double] $m.Groups[10].Value } else { 0.0 }
                P90        = if ($m.Groups[11].Success) { [double] $m.Groups[11].Value } else { 0.0 }
                P99        = if ($m.Groups[12].Success) { [double] $m.Groups[12].Value } else { 0.0 }
                Max        = if ($m.Groups[13].Success) { [double] $m.Groups[13].Value } else { 0.0 }
            }
        }

    if ($phaseALines) {
        # Group by tuple, keep the row with the highest T (latest cadence
        # window observed during the rung). Hashtable keyed by the tuple
        # string keeps this O(N) without LINQ overhead.
        $latestByTuple = @{}
        foreach ($row in $phaseALines) {
            $tupleKey = '{0}|{1}|{2}|{3}|{4}' -f $row.Instrument, $row.Tree, $row.Shard, $row.Phase, $row.Status
            if (-not $latestByTuple.ContainsKey($tupleKey) -or $latestByTuple[$tupleKey].T -lt $row.T) {
                $latestByTuple[$tupleKey] = $row
            }
        }
        foreach ($row in $latestByTuple.Values) {
            $phaseAResults.Add([pscustomobject]@{
                Rung       = $i + 1
                Vehicles   = $vehicles
                TickHz     = $hz
                TargetRate = $target
                T          = $row.T
                Instrument = $row.Instrument
                Tree       = $row.Tree
                Shard      = $row.Shard
                Phase      = $row.Phase
                Status     = $row.Status
                Count      = $row.Count
                Sum        = $row.Sum
                Min        = $row.Min
                P50        = $row.P50
                P90        = $row.P90
                P99        = $row.P99
                Max        = $row.Max
            })
        }
        $phaseAResults | Export-Csv -Path $phaseACsv -NoTypeInformation -Encoding utf8
        Write-Host ("[ladder] phaseA  : {0} tuples observed (latest window per tuple)" -f $latestByTuple.Count) -ForegroundColor DarkGray
    } else {
        Write-Host "[ladder] phaseA  : no [phaseA] lines in silo log (reporter disabled or BENCH_PHASEA_REPORT_SEC > rung duration?)" -ForegroundColor DarkYellow
    }

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
if ($phaseAResults.Count -gt 0) {
    Write-Host "[ladder] phaseA csv: $phaseACsv ($($phaseAResults.Count) rows across $($Rungs.Count) rungs)" -ForegroundColor Cyan
} else {
    Write-Host "[ladder] phaseA csv: no rows captured (silo did not emit [phaseA] lines)" -ForegroundColor DarkYellow
}
