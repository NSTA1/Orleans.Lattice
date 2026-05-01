<#
.SYNOPSIS
    One-time per-host calibration: finds the fleet size that stresses this
    host without saturating it, and writes it to benchmark/.fleet-size.config
    so every subsequent benchmark run uses the same load.

.DESCRIPTION
    The benchmark suite drives load through the Vehicle Fleet Simulator. At
    too-low fleet sizes the host has spare capacity and a real performance
    regression hides in the noise; at too-high fleet sizes the simulator's
    bounded sink channel overflows and benchmark numbers reflect the channel
    backpressure, not the lattice. The "right" fleet size depends on the
    host's CPU, memory, and storage and has to be measured.

    initialise.ps1 sweeps a geometric ladder of fleet sizes (default
    500, 1000, 2000, 4000, 8000, 16000) against the cheapest scenario
    (current-state-no-replication) and watches three signals after each rung:

      1. Sink drops    -- if the LatticeSink dropped events, the rung is past
                          the producer-side knee. (hard signal)
      2. Throughput plateau -- if commits/s grew less than 10% from the
                          previous rung despite the fleet roughly doubling,
                          the silo is past the commit-path knee (adding
                          load no longer adds work).
      3. Commit p99    -- if the leaf commit tail jumped > 2x relative to the
                          baseline rung, the silo is past the latency knee.

    Once the geometric ladder finds a knee, the script enters a bisection
    phase: it picks fleet sizes between the highest healthy rung and the
    lowest knee rung and runs them, narrowing the saturation window from
    the ladder's native 2x resolution down to BisectMinWindow vehicles
    (default 250). Without bisection, a knee at fleet=2000 with the previous
    healthy rung at fleet=1000 leaves the actual saturation point anywhere
    in (1000, 2000); the safety-margin pick has to assume worst case. With
    bisection the window collapses to <= 250 vehicles, so the operating
    fleet size has one bucket of meaningful resolution.

    The largest "healthy" rung is the saturation point. The operating fleet
    size written to .fleet-size.config is 65% of that -- large enough to make
    a regression visible above noise, small enough to leave headroom so a
    regression manifests as a measurable shift in p99 rather than disappearing
    into the saturation cliff.

    Calibration runs are passed -NoHistoryPush so they don't pollute the
    history VictoriaMetrics that the long-lived regression dashboards bind to.

.PARAMETER Ladder
    Geometric fleet-size ladder to sweep. Default is 500..16000 in 6 rungs;
    bump or shrink as the host warrants.

.PARAMETER Scenario
    Scenario id to drive the ladder. Defaults to current-state-no-replication
    (the cheapest write-heavy scenario; replication and chaos add noise that
    would distort the saturation signal).

.PARAMETER Force
    Skip the "history VM is empty" precondition and the "config file already
    exists" warning. Use this if you understand that calibration data and
    benchmark data already coexist in VM and you're re-calibrating intentionally.

.PARAMETER WarmupSeconds
    Per-rung warmup. Defaults to 15 (matches the steady-state scenarios).
    Note: the actual warmup/duration are read from the scenario .env by
    benchmark.ps1; this parameter is currently informational and used only
    in the time estimate.

.PARAMETER DurationSeconds
    Per-rung measurement window. Defaults to 30. As above, the actual value
    comes from the scenario .env; this parameter is informational.

.PARAMETER SafetyMargin
    Fraction of the saturation knee to use as the operating point. Default
    0.65 -- at 65% of saturation, a 10% regression still shifts p99 visibly
    instead of vanishing into the knee. Range 0.4..0.95.

.PARAMETER BisectIterations
    Maximum number of bisection rungs to run after the geometric ladder
    locates a knee. Default 4. Set to 0 to disable bisection (the script
    then behaves as before, with knee resolution limited to the ladder's
    2x step).

.PARAMETER BisectMinWindow
    Stop bisecting once the gap between the highest healthy rung and the
    lowest knee rung is <= this many vehicles. Default 250 -- matches the
    operating-fleet rounding bucket, so further refinement would be
    rounded away anyway.

.EXAMPLE
    ./initialise.ps1

.EXAMPLE
    ./initialise.ps1 -Ladder 1000,4000,16000,64000

.EXAMPLE
    ./initialise.ps1 -BisectIterations 0   # skip bisection refinement

.NOTES
    The output file (.fleet-size.config) is gitignored, so each host has its
    own calibrated value. Re-run the script if you change hardware, change
    the OS, or upgrade Docker -- anything that materially changes available
    CPU / memory / I/O.
#>
[CmdletBinding()]
param(
    [int[]]  $Ladder           = @(500, 1000, 2000, 4000, 8000, 16000),
    [string] $Scenario         = 'current-state-no-replication',
    [switch] $Force,
    [int]    $WarmupSeconds    = 15,
    [int]    $DurationSeconds  = 30,
    [double] $SafetyMargin     = 0.65,
    [int]    $BisectIterations = 4,
    [int]    $BisectMinWindow  = 250
)

$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true

# -- Paths and endpoints -------------------------------------------------------
$benchmarkRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$benchmarkPs1  = Join-Path $benchmarkRoot 'benchmark.ps1'
$runDir        = Join-Path $benchmarkRoot '.run'
$configPath    = Join-Path $benchmarkRoot '.fleet-size.config'
$historyVmUrl  = $env:BENCH_HISTORY_VM_URL ?? 'http://localhost:8428'

if (-not (Test-Path $benchmarkPs1)) {
    throw "benchmark.ps1 not found at $benchmarkPs1 (initialise.ps1 must live alongside it)."
}

# -- Validation ----------------------------------------------------------------
if ($Ladder.Count -lt 2) {
    throw "Ladder must have at least 2 rungs so saturation can be detected. Got $($Ladder.Count)."
}
if ($SafetyMargin -lt 0.4 -or $SafetyMargin -gt 0.95) {
    throw "SafetyMargin must be in [0.4, 0.95]. Got $SafetyMargin."
}
$Ladder = @($Ladder | Sort-Object -Unique)

# -- Banner --------------------------------------------------------------------
Write-Host ""
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host " Orleans.Lattice benchmark -- host calibration" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "This script measures the largest fleet size your host can absorb" -ForegroundColor White
Write-Host "without the simulator's sink channel overflowing or the lattice" -ForegroundColor White
Write-Host "commit path saturating. It then writes a fraction of that as the" -ForegroundColor White
Write-Host "operating fleet size, so every benchmark scenario runs at a" -ForegroundColor White
Write-Host "consistent, host-tuned load." -ForegroundColor White
Write-Host ""
Write-Host "Plan:" -ForegroundColor Yellow
Write-Host  "  1. Verify the history VictoriaMetrics is empty (or -Force)." -ForegroundColor Gray
Write-Host ("  2. Run scenario '{0}' at {1} fleet sizes:" -f $Scenario, $Ladder.Count) -ForegroundColor Gray
Write-Host ("       {0}" -f ($Ladder -join ', ')) -ForegroundColor Gray
Write-Host  "  3. After each rung, check three saturation signals:" -ForegroundColor Gray
Write-Host  "       sink drops, throughput linearity, commit p99 growth." -ForegroundColor Gray
Write-Host ("  4. Pick {0:P0} of the saturation knee as the operating size." -f $SafetyMargin) -ForegroundColor Gray
Write-Host ("  5. Write it to {0}." -f $configPath) -ForegroundColor Gray
Write-Host ""
$totalSeconds = $Ladder.Count * ($WarmupSeconds + $DurationSeconds + 60)
Write-Host ("Estimated wall-clock time: ~{0} minutes ({1} rungs)." -f [Math]::Ceiling($totalSeconds / 60.0), $Ladder.Count) -ForegroundColor DarkGray
Write-Host ""

# -- Step 1: VM emptiness check ------------------------------------------------
function Test-HistoryVmReachable {
    try {
        $r = Invoke-WebRequest -Uri "$historyVmUrl/health" -UseBasicParsing -TimeoutSec 3 -ErrorAction Stop
        return $r.StatusCode -eq 200
    } catch {
        return $false
    }
}

function Get-HistoryVmSeriesCount {
    # /api/v1/series?match[]={__name__=~"bench_.*"} -- url-encoded.
    try {
        $uri = "$historyVmUrl/api/v1/series?match%5B%5D=%7B__name__%3D~%22bench_.%2A%22%7D"
        $resp = Invoke-RestMethod -Uri $uri -TimeoutSec 10 -ErrorAction Stop
        if ($resp.status -ne 'success') { return -1 }
        return @($resp.data).Count
    } catch {
        return -1
    }
}

Write-Host "[1/5] Checking history VictoriaMetrics state ..." -ForegroundColor Cyan
if (Test-HistoryVmReachable) {
    $seriesCount = Get-HistoryVmSeriesCount
    if ($seriesCount -gt 0) {
        Write-Host ""
        Write-Host "    History VM at $historyVmUrl already holds $seriesCount bench_* series." -ForegroundColor Yellow
        if (-not $Force.IsPresent) {
            Write-Host ""
            Write-Host "    Calibration runs are passed -NoHistoryPush so they will NOT add to" -ForegroundColor White
            Write-Host "    that dataset, but the script aborts by default to make sure you are" -ForegroundColor White
            Write-Host "    not silently re-calibrating on top of a populated history dataset." -ForegroundColor White
            Write-Host ""
            Write-Host "    Options:" -ForegroundColor Yellow
            Write-Host "      - To wipe the history dataset and start fresh:" -ForegroundColor Gray
            Write-Host "          ./benchmark.ps1 -CloseHistory" -ForegroundColor White
            Write-Host "          docker volume rm benchmark_victoriametrics-data" -ForegroundColor White
            Write-Host "      - To re-calibrate in place anyway:" -ForegroundColor Gray
            Write-Host "          ./initialise.ps1 -Force" -ForegroundColor White
            Write-Host ""
            exit 1
        }
        Write-Host "    -Force set; proceeding without wiping." -ForegroundColor DarkYellow
    } else {
        Write-Host "    VM is reachable and contains no bench_* series. Good." -ForegroundColor Green
    }
} else {
    Write-Host "    History VM not reachable at $historyVmUrl -- fine, calibration does not need it." -ForegroundColor Green
}
Write-Host ""

# -- Step 2: confirm overwrite of an existing config ---------------------------
if ((Test-Path $configPath) -and -not $Force.IsPresent) {
    Write-Host "[note] An existing $configPath was found." -ForegroundColor DarkYellow
    Write-Host "       It will be overwritten when calibration completes." -ForegroundColor DarkYellow
    Write-Host ""
}

# -- Step 3: run the ladder ----------------------------------------------------
Write-Host "[2/5] Running fleet-size ladder ..." -ForegroundColor Cyan
Write-Host ""

# Per-rung result accumulator. Each entry is a pscustomobject with metrics + verdict.
$rungs = New-Object System.Collections.Generic.List[object]

# benchmark.ps1 writes results into .run/<scenario>/<run_id>/. We snapshot the
# pre-existing run_ids and pick the new one after each rung.
$scenarioRunDir = Join-Path $runDir $Scenario
$preExisting = @{}
if (Test-Path $scenarioRunDir) {
    foreach ($d in Get-ChildItem -Path $scenarioRunDir -Directory) { $preExisting[$d.Name] = $true }
}

function Find-LatestResultsJson {
    param([hashtable] $Existing, [string] $ScenarioRunDir)
    if (-not (Test-Path $ScenarioRunDir)) { return $null }
    $candidates = Get-ChildItem -Path $ScenarioRunDir -Directory `
        | Where-Object { -not $Existing.Contains($_.Name) } `
        | Sort-Object -Property Name -Descending
    foreach ($d in $candidates) {
        $p = Join-Path $d.FullName 'results.json'
        if (Test-Path $p) { return $p }
    }
    return $null
}

function Format-Verdict {
    param([string] $Verdict)
    switch ($Verdict) {
        'healthy' { return @{ Text = 'OK';     Color = 'Green' } }
        'knee'    { return @{ Text = 'KNEE';   Color = 'Yellow' } }
        'failed'  { return @{ Text = 'FAILED'; Color = 'Red' } }
        default   { return @{ Text = $Verdict; Color = 'Gray' } }
    }
}

$rungIndex = 0
foreach ($fleetSize in $Ladder) {
    $rungIndex++
    Write-Host ""
    Write-Host ("----- Rung {0}/{1}: fleet={2} -----" -f $rungIndex, $Ladder.Count, $fleetSize) -ForegroundColor Cyan

    # Invoke benchmark.ps1 with explicit named parameters. We deliberately do NOT
    # use array splatting (`& $benchmarkPs1 @argList`) here — when the splat array
    # mixes named-string args, values, and switches, PowerShell's parameter binder
    # has been observed to surface a misleading "A positional parameter cannot be
    # found ..." error against the calling script. Explicit named-parameter
    # invocation is reliable across PS7 minor versions.
    if ($rungIndex -gt 1) {
        & $benchmarkPs1 -Scenario $Scenario -FleetSizeOverride $fleetSize `
                        -SkipFleetSizeCheck -NoHistoryPush -NoBuild
    } else {
        & $benchmarkPs1 -Scenario $Scenario -FleetSizeOverride $fleetSize `
                        -SkipFleetSizeCheck -NoHistoryPush
    }
    if ($LASTEXITCODE -ne 0) {
        throw "benchmark.ps1 exited with code $LASTEXITCODE for fleet size $fleetSize."
    }

    $resultsPath = Find-LatestResultsJson -Existing $preExisting -ScenarioRunDir $scenarioRunDir
    if (-not $resultsPath) {
        throw "No new results.json appeared under $scenarioRunDir after rung $rungIndex."
    }
    # Mark this run id as seen so subsequent rungs find their own.
    $preExisting[(Split-Path -Leaf (Split-Path -Parent $resultsPath))] = $true

    $payload = Get-Content -Path $resultsPath -Raw | ConvertFrom-Json
    $m = $payload.metrics

    $tps   = [double] (($m.lattice_commits_per_second)        ?? 0)
    $p99   = [double] (($m.lattice_commit_p99_ms)             ?? 0)
    $drops = [double] (($m.sink_dropped_combined_increase)    ?? 0)
    $cache = [double] (($m.lattice_cache_hit_ratio)           ?? 0)
    $gen2  = [double] (($m.dotnet_gc_gen2_collections_increase) ?? 0)
    $publ  = [double] (($m.sink_published_per_second)         ?? 0)

    $linearity = if ($fleetSize -gt 0) { $tps / $fleetSize } else { 0.0 }

    $rung = [pscustomobject]@{
        Rung         = $rungIndex
        FleetSize    = $fleetSize
        Tps          = $tps
        P99Ms        = $p99
        Drops        = $drops
        CacheHit     = $cache
        Gen2Increase = $gen2
        Publish      = $publ
        Linearity    = $linearity
        ResultsPath  = $resultsPath
        Verdict      = $null
    }
    $rungs.Add($rung)

    # First rung is the baseline; subsequent rungs are scored against it.
    $baseline = $rungs[0]
    $linearityRatio = if ($baseline.Linearity -gt 0) { $rung.Linearity / $baseline.Linearity } else { 1.0 }
    $p99Ratio       = if ($baseline.P99Ms     -gt 0) { $rung.P99Ms     / $baseline.P99Ms     } else { 1.0 }

    # Throughput-growth ratio against the *previous* rung. The ladder is
    # geometric with ratio ~2, so a healthy rung roughly doubles tps; a
    # plateau (tps_growth < 1.10x) means adding load no longer adds work,
    # which is the actual saturation signal we want.
    $prev      = if ($rungIndex -gt 1) { $rungs[$rungIndex - 2] } else { $null }
    $tpsGrowth = if ($prev -and $prev.Tps -gt 0) { $rung.Tps / $prev.Tps } else { $null }

    # Saturation predicates (in order of strength):
    #   1. drops > 0       — hard producer-side signal: the LatticeSink channel
    #                        overflowed, so we're past the producer-side knee.
    #   2. tps plateau     — throughput grew < 10% despite the fleet roughly
    #                        doubling (geometric ladder), meaning adding load
    #                        no longer adds work done. This is the headline
    #                        commit-path saturation signal.
    #   3. p99 explosion   — leaf commit tail jumped > 2x relative to the
    #                        baseline rung, meaning we're past the latency
    #                        cliff even if throughput is still climbing.
    #
    # An earlier version checked linearity (tps/fleet) < 0.85 vs. baseline,
    # but that trips on perfectly normal sub-linear scaling (a 50% growth
    # at 2x fleet is healthy, not saturated) and aborted the ladder long
    # before the system actually saturated. Drops + tps-plateau + p99 are
    # the load-bearing signals; linearity is recorded for diagnostics only.
    $verdict = 'healthy'
    if ($tps -le 0 -or $p99 -le 0) {
        $verdict = 'failed'
    } elseif ($drops -gt 0) {
        $verdict = 'knee'
    } elseif ($null -ne $tpsGrowth -and $tpsGrowth -lt 1.10) {
        $verdict = 'knee'
    } elseif ($p99Ratio -gt 2.0) {
        $verdict = 'knee'
    }
    $rung.Verdict = $verdict

    $v = Format-Verdict $verdict
    Write-Host ""
    Write-Host ("  fleet={0,6}  tps={1,8:N0}/s  p99={2,7:N2}ms  drops={3,3:N0}  cache={4:P0}  gen2d={5,3:N0}  -> " -f `
        $fleetSize, $tps, $p99, $drops, $cache, $gen2) -NoNewline -ForegroundColor Gray
    Write-Host $v.Text -ForegroundColor $v.Color

    if ($rungIndex -gt 1) {
        Write-Host ("    (vs. F={0}: tps_growth={1:N2}x; vs. baseline F={2}: linearity={3:P0}, p99={4:N2}x)" -f `
            $prev.FleetSize, $tpsGrowth, $baseline.FleetSize, $linearityRatio, $p99Ratio) -ForegroundColor DarkGray
    }

    # Hard stop: a failed run means something is wrong (silo crash, network, etc.).
    if ($verdict -eq 'failed') {
        Write-Host ""
        Write-Host "    Rung produced no usable metrics. Aborting calibration." -ForegroundColor Red
        Write-Host "    Check $resultsPath and the docker logs." -ForegroundColor DarkGray
        exit 2
    }

    # Early stop: once we've seen the knee AND have at least one healthy rung,
    # we don't need to keep climbing -- one knee rung localises saturation to
    # within a 2x window, which is good enough for the safety-margin pick.
    if ($verdict -eq 'knee') {
        $healthyCount = @($rungs | Where-Object { $_.Verdict -eq 'healthy' }).Count
        if ($healthyCount -ge 1 -and $rungIndex -lt $Ladder.Count) {
            Write-Host ""
            Write-Host ("    Saturation knee detected at fleet={0}. Skipping remaining {1} rung(s)." -f `
                $fleetSize, ($Ladder.Count - $rungIndex)) -ForegroundColor Yellow
            break
        }
    }
}

# -- Step 3b: bisection refinement --------------------------------------------
# The geometric ladder localises saturation to a 2x window (the gap between
# the highest healthy rung and the first knee rung). Bisect inside that window
# until it shrinks to <= BisectMinWindow vehicles, so the safety-margin pick
# below has at least one bucket of meaningful resolution.
#
# Bisection rungs use a *different* verdict criterion than the main ladder:
# at a 1.1x..1.5x step (typical mid-bracket) the main loop's
# "tps_growth < 1.10x vs previous rung" threshold false-positives on healthy
# sub-linear scaling. Instead we compare a candidate rung's tps directly
# against the lower-bracket healthy rung -- if throughput is essentially flat
# (< 1.05x) despite a meaningful fleet increase, that's the actual saturation
# signal.
$initialKnee = @($rungs | Where-Object { $_.Verdict -eq 'knee' } | Sort-Object FleetSize | Select-Object -First 1)
if ($initialKnee -and $BisectIterations -gt 0) {
    Write-Host ""
    Write-Host "[2b/5] Bisecting between last-healthy and knee ..." -ForegroundColor Cyan

    for ($b = 1; $b -le $BisectIterations; $b++) {
        $loRung = @($rungs | Where-Object { $_.Verdict -eq 'healthy' } | Sort-Object FleetSize -Descending | Select-Object -First 1)
        $hiRung = @($rungs | Where-Object { $_.Verdict -eq 'knee' }    | Sort-Object FleetSize             | Select-Object -First 1)
        if (-not $loRung -or -not $hiRung) { break }

        $window = $hiRung.FleetSize - $loRung.FleetSize
        if ($window -le $BisectMinWindow) {
            Write-Host ""
            Write-Host ("    Window narrowed to {0} <= {1}; bisection complete." -f $window, $BisectMinWindow) -ForegroundColor Green
            break
        }

        # Pick the midpoint, rounded to the nearest 50 to keep fleet sizes readable.
        $mid = [int]([Math]::Round((($loRung.FleetSize + $hiRung.FleetSize) / 2.0) / 50.0) * 50)
        if ($mid -le $loRung.FleetSize -or $mid -ge $hiRung.FleetSize) { break }

        $rungIndex++
        Write-Host ""
        Write-Host ("----- Bisect {0}/{1}: fleet={2}  (window {3}..{4}) -----" -f `
            $b, $BisectIterations, $mid, $loRung.FleetSize, $hiRung.FleetSize) -ForegroundColor Cyan

        & $benchmarkPs1 -Scenario $Scenario -FleetSizeOverride $mid `
                        -SkipFleetSizeCheck -NoHistoryPush -NoBuild
        if ($LASTEXITCODE -ne 0) {
            throw "benchmark.ps1 exited with code $LASTEXITCODE for bisection fleet $mid."
        }

        $resultsPath = Find-LatestResultsJson -Existing $preExisting -ScenarioRunDir $scenarioRunDir
        if (-not $resultsPath) {
            throw "No new results.json appeared under $scenarioRunDir after bisection rung at fleet=$mid."
        }
        $preExisting[(Split-Path -Leaf (Split-Path -Parent $resultsPath))] = $true

        $payload = Get-Content -Path $resultsPath -Raw | ConvertFrom-Json
        $m = $payload.metrics

        $tps   = [double] (($m.lattice_commits_per_second)        ?? 0)
        $p99   = [double] (($m.lattice_commit_p99_ms)             ?? 0)
        $drops = [double] (($m.sink_dropped_combined_increase)    ?? 0)
        $cache = [double] (($m.lattice_cache_hit_ratio)           ?? 0)
        $gen2  = [double] (($m.dotnet_gc_gen2_collections_increase) ?? 0)
        $publ  = [double] (($m.sink_published_per_second)         ?? 0)

        $linearity = if ($mid -gt 0) { $tps / $mid } else { 0.0 }

        $baseline = $rungs[0]
        $p99Ratio = if ($baseline.P99Ms -gt 0) { $p99 / $baseline.P99Ms } else { 1.0 }

        # Bisection-specific saturation predicates:
        #   1. drops > 0  -- still the hard producer-side signal.
        #   2. p99 > 2x baseline -- still the latency cliff signal.
        #   3. tps < lo.Tps * 1.05 -- "essentially flat vs the lower-bracket
        #      healthy rung". This replaces the main loop's
        #      "tps_growth < 1.10x vs previous chronological rung", which
        #      doesn't apply here: bisection rungs aren't visited in fleet
        #      order, and the step is much smaller than 2x.
        $verdict = 'healthy'
        if     ($tps -le 0 -or $p99 -le 0)      { $verdict = 'failed' }
        elseif ($drops -gt 0)                   { $verdict = 'knee' }
        elseif ($p99Ratio -gt 2.0)              { $verdict = 'knee' }
        elseif ($tps -lt ($loRung.Tps * 1.05))  { $verdict = 'knee' }

        $rung = [pscustomobject]@{
            Rung         = $rungIndex
            FleetSize    = $mid
            Tps          = $tps
            P99Ms        = $p99
            Drops        = $drops
            CacheHit     = $cache
            Gen2Increase = $gen2
            Publish      = $publ
            Linearity    = $linearity
            ResultsPath  = $resultsPath
            Verdict      = $verdict
        }
        $rungs.Add($rung)

        $v = Format-Verdict $verdict
        Write-Host ""
        Write-Host ("  fleet={0,6}  tps={1,8:N0}/s  p99={2,7:N2}ms  drops={3,3:N0}  cache={4:P0}  gen2d={5,3:N0}  -> " -f `
            $mid, $tps, $p99, $drops, $cache, $gen2) -NoNewline -ForegroundColor Gray
        Write-Host $v.Text -ForegroundColor $v.Color

        $tpsRatio = if ($loRung.Tps -gt 0) { $tps / $loRung.Tps } else { 0.0 }
        Write-Host ("    (vs. lo F={0} tps={1:N0}: tps_ratio={2:N2}x; vs. baseline p99={3:N2}x)" -f `
            $loRung.FleetSize, $loRung.Tps, $tpsRatio, $p99Ratio) -ForegroundColor DarkGray

        if ($verdict -eq 'failed') {
            Write-Host ""
            Write-Host "    Bisection rung produced no usable metrics. Stopping bisection (analysis will use ladder rungs)." -ForegroundColor Yellow
            break
        }
    }
}

# -- Step 4: pick the operating fleet size -------------------------------------
Write-Host ""
Write-Host "[3/5] Analysing ladder ..." -ForegroundColor Cyan
Write-Host ""

$healthy = @($rungs | Where-Object { $_.Verdict -eq 'healthy' })
if ($healthy.Count -eq 0) {
    Write-Host "    No healthy rungs were observed -- even fleet=$($Ladder[0]) saturated this host." -ForegroundColor Red
    Write-Host "    Re-run with a smaller starting rung, e.g.:" -ForegroundColor Yellow
    Write-Host "        ./initialise.ps1 -Ladder 100,250,500,1000" -ForegroundColor White
    exit 3
}

# Sort by FleetSize rather than by rung order so bisection rungs (which are
# visited out of fleet order) are picked correctly. The lowest-fleet knee is
# the tightest upper bound on saturation; the highest-fleet healthy is the
# tightest lower bound.
$kneeRung    = @($rungs | Where-Object { $_.Verdict -eq 'knee' }    | Sort-Object FleetSize             | Select-Object -First 1)
$lastHealthy = @($rungs | Where-Object { $_.Verdict -eq 'healthy' } | Sort-Object FleetSize -Descending | Select-Object -First 1)

if ($kneeRung) {
    $saturationFleet = $kneeRung.FleetSize
    Write-Host ("    Saturation knee     : fleet={0,6}  ({1})" -f $kneeRung.FleetSize, ($kneeRung.Verdict.ToUpperInvariant())) -ForegroundColor Yellow
    Write-Host ("    Last healthy rung   : fleet={0,6}" -f $lastHealthy.FleetSize) -ForegroundColor Green
} else {
    # All rungs healthy -- host has more headroom than the ladder probed.
    $saturationFleet = $lastHealthy.FleetSize
    Write-Host "    All rungs were healthy -- your host has more headroom than this ladder tested." -ForegroundColor Yellow
    Write-Host "    The recommended operating size will be a fraction of the highest tested" -ForegroundColor Yellow
    Write-Host "    rung; consider re-running with a wider ladder if you want a tighter calibration." -ForegroundColor Yellow
    Write-Host ""
    Write-Host ("    Highest tested rung : fleet={0,6}  (no knee observed)" -f $lastHealthy.FleetSize) -ForegroundColor Green
}

# Apply safety margin and round down to a "nice" multiple of 250 to keep .env
# values readable. Floor so we never round up past the saturation point.
$rawTarget = $saturationFleet * $SafetyMargin
$bucket = 250
$operatingFleet = [int]([Math]::Floor($rawTarget / $bucket) * $bucket)
if ($operatingFleet -lt 250) { $operatingFleet = 250 }   # don't go absurdly small

Write-Host ""
Write-Host ("    Operating fleet size: {0} ({1:P0} of saturation, rounded down to nearest {2})" -f `
    $operatingFleet, $SafetyMargin, $bucket) -ForegroundColor Green
Write-Host ""

# -- Step 5: write the config --------------------------------------------------
Write-Host "[4/5] Writing $configPath ..." -ForegroundColor Cyan

$now = (Get-Date).ToUniversalTime().ToString('o')
$gitSha = try {
    (& git -C (Split-Path -Parent $benchmarkRoot) rev-parse --short HEAD 2>$null).Trim()
} catch { 'unknown' }

$configLines = @(
    '# Generated by benchmark/initialise.ps1 -- DO NOT EDIT BY HAND.',
    '# Re-run ./initialise.ps1 to regenerate after a host or hardware change.',
    "# Calibrated:        $now",
    "# Git sha:           $gitSha",
    "# Scenario:          $Scenario",
    "# Ladder probed:     $($Ladder -join ', ')",
    "# Saturation knee:   $saturationFleet",
    ('# Safety margin:     {0:P0}' -f $SafetyMargin),
    "BENCH_FLEET_SIZE=$operatingFleet"
)
Set-Content -Path $configPath -Value $configLines -Encoding utf8
Write-Host "    Wrote $configPath" -ForegroundColor Green
Write-Host ""

# -- Step 6: summary -----------------------------------------------------------
Write-Host "[5/5] Summary" -ForegroundColor Cyan
Write-Host ""
Write-Host "  Per-rung results:" -ForegroundColor White
Write-Host ""

# Sort by FleetSize so bisection rungs interleave with their ladder neighbours
# instead of appearing in chronological order at the end of the table.
$rows = $rungs | Sort-Object FleetSize | ForEach-Object {
    [pscustomobject]@{
        '#'         = $_.Rung
        'fleet'     = $_.FleetSize
        'tps/s'     = ('{0:N0}' -f $_.Tps)
        'p99 ms'    = ('{0:N2}' -f $_.P99Ms)
        'drops'     = ('{0:N0}' -f $_.Drops)
        'cache hit' = ('{0:P0}' -f $_.CacheHit)
        'gen2 d'    = ('{0:N0}' -f $_.Gen2Increase)
        'verdict'   = $_.Verdict
    }
}
$rows | Format-Table -AutoSize | Out-Host

Write-Host "  Saturation analysis:" -ForegroundColor White
if ($kneeRung) {
    Write-Host ("    Knee detected at fleet={0}. Last healthy rung was fleet={1}." -f `
        $kneeRung.FleetSize, $lastHealthy.FleetSize) -ForegroundColor Gray
    Write-Host ("    Operating fleet = {0:P0} of {1} = {2}." -f `
        $SafetyMargin, $kneeRung.FleetSize, $operatingFleet) -ForegroundColor Gray
} else {
    Write-Host ("    No saturation observed across the {0}-rung ladder." -f $rungs.Count) -ForegroundColor Gray
    Write-Host ("    Operating fleet = {0:P0} of {1} (highest tested) = {2}." -f `
        $SafetyMargin, $lastHealthy.FleetSize, $operatingFleet) -ForegroundColor Gray
}
Write-Host ""
Write-Host "  Calibrated fleet size:  $operatingFleet" -ForegroundColor Green
Write-Host "  Config file:            $configPath" -ForegroundColor Green
Write-Host ""
Write-Host "  You can now run any benchmark scenario:" -ForegroundColor White
Write-Host "      ./benchmark.ps1 current-state-no-replication" -ForegroundColor White
Write-Host ""
