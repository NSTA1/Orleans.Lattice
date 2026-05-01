<#
.SYNOPSIS
    Regenerates the persona-grouped benchmark-history dashboards under
    benchmark/history/grafana/dashboards/.

.DESCRIPTION
    The benchmark-history Grafana stack hosts one dashboard per *lattice-usage
    persona* (replication, write-heavy random/ordered, read-heavy random/ordered,
    read-write-mix random/ordered, microbench). Each persona aggregates one or
    more concrete benchmark scenarios (`scenarios/*.env`) so the cross-run trend
    view answers a single question:

        "Did the latest commit make this kind of workload slower?"

    Per-dashboard layout - 3 bands, top-to-bottom:

      Band 0 (KPI strip): stat panels with big number = lastNotNull, sparkline
              underneath, threshold-coloured background based on absolute value
              against per-metric thresholds. One stat per headline metric.

      Band 1 (trend strip): time-series panels with x-axis = run end-time and
              y-axis = metric value. One panel per metric family relevant to
              the persona; multiple lines coloured by `scenario` so dashboards
              that aggregate >1 scenario plot them side-by-side.

      Band 2 (commit comparator): barchart panels with one bar per run, hover
              showing `{{scenario}} {{run_id}} @ {{git_sha}}`. One barchart per
              headline KPI. Whatever the dashboard time-picker covers.

    Output: one JSON per persona at
        benchmark/history/grafana/dashboards/BenchmarkHistory.<persona>.json
    UID format: `lat-hist-<persona>` (Grafana''s 40-char UID limit gives this
    exactly enough room for the longest persona id).

.NOTES
    Re-run is idempotent. Stale BenchmarkHistory*.json files are wiped first
    so deleted personas don''t leak. Drift between this script''s persona table
    and the on-disk scenarios/*.env list emits a non-fatal warning.
#>

[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'

# ---------------------------------------------------------------------------
# Persona table - what every dashboard is and what it asks.
# ---------------------------------------------------------------------------

$Personas = [ordered]@{
    'replication' = [ordered]@{
        Title       = 'Lattice History - Replication'
        Subtitle    = 'Replication ship/apply latency and underlying commit path under replication load'
        Scenarios   = @('current-state-single-peer','bidirectional-replication','observer-no-peer','replication-key-filter','replication-backpressure','receiver-crash')
        Kpis        = @(
            @{ Metric='bench_orleans_lattice_replication_ship_p95_ms';      Title='Replication ship p95 (ms)';      Unit='ms';   LowerIsBetter=$true;  WarnAt=50;  CritAt=200 }
            @{ Metric='bench_orleans_lattice_replication_apply_lag_p95_ms'; Title='Replication apply lag p95 (ms)'; Unit='ms';   LowerIsBetter=$true;  WarnAt=100; CritAt=500 }
            @{ Metric='bench_lattice_commit_p99_ms';                        Title='Commit p99 (ms)';                Unit='ms';   LowerIsBetter=$true;  WarnAt=10;  CritAt=50  }
            @{ Metric='bench_lattice_commits_per_second';                   Title='Commits/sec';                    Unit='ops';  LowerIsBetter=$false; WarnAt=500; CritAt=100 }
        )
        Families    = @('replication','commit','cache','process')
    }
    'write-heavy-random' = [ordered]@{
        Title       = 'Lattice History - Write-heavy (random keys)'
        Subtitle    = 'Per-vehicle current-state overwrites: steady-state and hot-key variants'
        Scenarios   = @('current-state-no-replication','skewed-key-shard-splits')
        Kpis        = @(
            @{ Metric='bench_lattice_commit_p99_ms';        Title='Commit p99 (ms)';     Unit='ms';   LowerIsBetter=$true;  WarnAt=10;  CritAt=50  }
            @{ Metric='bench_lattice_commits_per_second';   Title='Commits/sec';         Unit='ops';  LowerIsBetter=$false; WarnAt=500; CritAt=100 }
            @{ Metric='bench_orleans_lattice_leaf_write_duration_milliseconds_p99'; Title='Leaf write p99 (ms)'; Unit='ms'; LowerIsBetter=$true; WarnAt=10; CritAt=50 }
            @{ Metric='bench_lattice_cache_hit_ratio';      Title='Cache hit ratio';     Unit='percentunit'; LowerIsBetter=$false; WarnAt=0.7; CritAt=0.4 }
        )
        Families    = @('commit','cache','sink','process')
    }
    'write-heavy-ordered' = [ordered]@{
        Title       = 'Lattice History - Write-heavy (ordered / append-only)'
        Subtitle    = 'Event-log keyspace with TTL: each tick produces a new key; TTL drives compaction'
        Scenarios   = @('event-log-with-ttl')
        Kpis        = @(
            @{ Metric='bench_lattice_commit_p99_ms';        Title='Commit p99 (ms)';     Unit='ms';   LowerIsBetter=$true;  WarnAt=10;  CritAt=50  }
            @{ Metric='bench_lattice_commits_per_second';   Title='Commits/sec';         Unit='ops';  LowerIsBetter=$false; WarnAt=500; CritAt=100 }
            @{ Metric='bench_sink_published_per_second';    Title='Sink publishes/sec';  Unit='ops';  LowerIsBetter=$false; WarnAt=500; CritAt=100 }
        )
        Families    = @('commit','sink','process')
    }
    'read-heavy-random' = [ordered]@{
        Title       = 'Lattice History - Read-heavy (random keys)'
        Subtitle    = 'GetAsync-dominant load against random keys; 95:5 read:write ratio'
        Scenarios   = @('read-heavy-random')
        Kpis        = @(
            @{ Metric='bench_vehicle_fleet_simulator_read_driver_duration_ms_p99'; Title='Read p99 (ms)';        Unit='ms';   LowerIsBetter=$true;  WarnAt=5;    CritAt=25  }
            @{ Metric='bench_vehicle_fleet_simulator_read_driver_reads_per_second'; Title='Reads/sec';            Unit='ops';  LowerIsBetter=$false; WarnAt=20000; CritAt=5000 }
            @{ Metric='bench_lattice_cache_hit_ratio';      Title='Cache hit ratio';     Unit='percentunit'; LowerIsBetter=$false; WarnAt=0.7; CritAt=0.4 }
            @{ Metric='bench_lattice_commit_p99_ms';        Title='Commit p99 (ms)';     Unit='ms';   LowerIsBetter=$true;  WarnAt=10;  CritAt=50  }
        )
        Families    = @('read','cache','commit','process')
    }
    'read-heavy-ordered' = [ordered]@{
        Title       = 'Lattice History - Read-heavy (ordered keys)'
        Subtitle    = 'GetAsync-dominant load with sequential keyspace walk; tests prefetch / locality'
        Scenarios   = @('read-heavy-ordered')
        Kpis        = @(
            @{ Metric='bench_vehicle_fleet_simulator_read_driver_duration_ms_p99'; Title='Read p99 (ms)';        Unit='ms';   LowerIsBetter=$true;  WarnAt=5;    CritAt=25  }
            @{ Metric='bench_vehicle_fleet_simulator_read_driver_reads_per_second'; Title='Reads/sec';            Unit='ops';  LowerIsBetter=$false; WarnAt=20000; CritAt=5000 }
            @{ Metric='bench_lattice_cache_hit_ratio';      Title='Cache hit ratio';     Unit='percentunit'; LowerIsBetter=$false; WarnAt=0.7; CritAt=0.4 }
            @{ Metric='bench_lattice_commit_p99_ms';        Title='Commit p99 (ms)';     Unit='ms';   LowerIsBetter=$true;  WarnAt=10;  CritAt=50  }
        )
        Families    = @('read','cache','commit','process')
    }
    'read-write-mix-random' = [ordered]@{
        Title       = 'Lattice History - Read/write mix (random keys)'
        Subtitle    = 'Balanced 50:50 read/write load against random keys'
        Scenarios   = @('read-write-mix-random')
        Kpis        = @(
            @{ Metric='bench_vehicle_fleet_simulator_read_driver_duration_ms_p99'; Title='Read p99 (ms)';        Unit='ms';   LowerIsBetter=$true;  WarnAt=5;    CritAt=25  }
            @{ Metric='bench_lattice_commit_p99_ms';        Title='Commit p99 (ms)';     Unit='ms';   LowerIsBetter=$true;  WarnAt=10;  CritAt=50  }
            @{ Metric='bench_vehicle_fleet_simulator_read_driver_reads_per_second'; Title='Reads/sec';            Unit='ops';  LowerIsBetter=$false; WarnAt=1500; CritAt=500 }
            @{ Metric='bench_lattice_commits_per_second';   Title='Commits/sec';         Unit='ops';  LowerIsBetter=$false; WarnAt=1500; CritAt=500 }
        )
        Families    = @('read','commit','cache','sink','process')
    }
    'read-write-mix-ordered' = [ordered]@{
        Title       = 'Lattice History - Read/write mix (ordered keys)'
        Subtitle    = 'Balanced 50:50 read/write load with sequential keyspace walk'
        Scenarios   = @('read-write-mix-ordered')
        Kpis        = @(
            @{ Metric='bench_vehicle_fleet_simulator_read_driver_duration_ms_p99'; Title='Read p99 (ms)';        Unit='ms';   LowerIsBetter=$true;  WarnAt=5;    CritAt=25  }
            @{ Metric='bench_lattice_commit_p99_ms';        Title='Commit p99 (ms)';     Unit='ms';   LowerIsBetter=$true;  WarnAt=10;  CritAt=50  }
            @{ Metric='bench_vehicle_fleet_simulator_read_driver_reads_per_second'; Title='Reads/sec';            Unit='ops';  LowerIsBetter=$false; WarnAt=1500; CritAt=500 }
            @{ Metric='bench_lattice_commits_per_second';   Title='Commits/sec';         Unit='ops';  LowerIsBetter=$false; WarnAt=1500; CritAt=500 }
        )
        Families    = @('read','commit','cache','sink','process')
    }
    'microbench' = [ordered]@{
        Title       = 'Lattice History - Microbench'
        Subtitle    = 'BenchmarkDotNet ILattice micro-suite (in-process, no Orleans cluster)'
        Scenarios   = @('microbench')
        Kpis        = @()
        Families    = @('microbench')
    }
}

# ---------------------------------------------------------------------------
# Family definitions: regex used to populate templating dropdowns + trend rows.
# ---------------------------------------------------------------------------

$Families = [ordered]@{
    'commit'      = @{ Title='Commit path';       Regex='bench_(orleans_lattice_(leaf|shard|atomic_write|coordinator|tree)_.*|lattice_commit_.*|lattice_commits_.*)' }
    'cache'       = @{ Title='Cache & metadata';  Regex='bench_(orleans_lattice_(cache|events|config)_.*|lattice_cache_.*)' }
    'sink'        = @{ Title='Telemetry sink';    Regex='bench_(vehicle_fleet_simulator_sink_.*|sink_.*)' }
    'replication' = @{ Title='Replication';       Regex='bench_orleans_lattice_replication_.*' }
    'read'        = @{ Title='Read driver';       Regex='bench_vehicle_fleet_simulator_read_driver_.*' }
    'process'     = @{ Title='Process / runtime'; Regex='bench_dotnet_.*' }
    'microbench'  = @{ Title='BenchmarkDotNet';   Regex='bench_microbench_.*' }
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

$DataSource = [ordered]@{ type = 'prometheus'; uid = 'victoriametrics' }
$panelIdSeq = 0
function Next-PanelId { $script:panelIdSeq++; return $script:panelIdSeq }

function New-RowPanel {
    param([string]$Title, [int]$Y)
    return [ordered]@{
        type     = 'row'
        title    = $Title
        collapsed= $false
        gridPos  = @{ h = 1; w = 24; x = 0; y = $Y }
        id       = Next-PanelId
        panels   = @()
    }
}

function ConvertTo-PromScenarioSelector {
    param([string[]]$Scenarios)
    if ($Scenarios.Count -eq 1) { return ('scenario="{0}"' -f $Scenarios[0]) }
    return ('scenario=~"({0})"' -f ($Scenarios -join '|'))
}

function New-StatPanel {
    param(
        [hashtable]$Kpi,
        [string[]]$Scenarios,
        [int]$X, [int]$Y, [int]$W = 6, [int]$H = 5
    )
    $sel = ConvertTo-PromScenarioSelector $Scenarios
    # Threshold direction: lower-is-better metrics turn red when high; higher-is-better turn red when low.
    if ($Kpi.LowerIsBetter) {
        $thresholds = @(
            @{ color = 'green';  value = $null },
            @{ color = 'yellow'; value = $Kpi.WarnAt },
            @{ color = 'red';    value = $Kpi.CritAt }
        )
    } else {
        # Inverted: green-yellow-red descending. Grafana evaluates step-by-step ascending,
        # so we encode the ranges as ascending values, with crit being lowest.
        $thresholds = @(
            @{ color = 'red';    value = $null },
            @{ color = 'yellow'; value = $Kpi.CritAt },
            @{ color = 'green';  value = $Kpi.WarnAt }
        )
    }
    return [ordered]@{
        id          = Next-PanelId
        type        = 'stat'
        title       = $Kpi.Title
        datasource  = $DataSource
        gridPos     = @{ h = $H; w = $W; x = $X; y = $Y }
        fieldConfig = @{
            defaults  = [ordered]@{
                unit       = $Kpi.Unit
                decimals   = 2
                color      = @{ mode = 'thresholds' }
                thresholds = @{ mode = 'absolute'; steps = $thresholds }
            }
            overrides = @()
        }
        options     = [ordered]@{
            graphMode     = 'area'
            colorMode     = 'background'
            justifyMode   = 'auto'
            textMode      = 'value_and_name'
            orientation   = 'auto'
            reduceOptions = [ordered]@{ calcs = @('lastNotNull'); fields = ''; values = $false }
        }
        targets     = @(
            [ordered]@{
                datasource   = $DataSource
                expr         = ('{0}{{{1}}}' -f $Kpi.Metric, $sel)
                legendFormat = '{{scenario}} @ {{git_sha}}'
                refId        = 'A'
            }
        )
    }
}

function New-TrendPanel {
    param(
        [string]$FamilyId,
        [string[]]$Scenarios,
        [int]$X, [int]$Y, [int]$W = 12, [int]$H = 8
    )
    $fam = $Families[$FamilyId]
    if (-not $fam) { throw "Unknown family $FamilyId" }
    $sel = ConvertTo-PromScenarioSelector $Scenarios
    return [ordered]@{
        id          = Next-PanelId
        type        = 'timeseries'
        title       = $fam.Title
        datasource  = $DataSource
        gridPos     = @{ h = $H; w = $W; x = $X; y = $Y }
        fieldConfig = @{
            defaults  = [ordered]@{
                custom = [ordered]@{
                    drawStyle       = 'points'
                    pointSize       = 7
                    showPoints      = 'always'
                    lineInterpolation = 'linear'
                    spanNulls       = $true
                }
            }
            overrides = @()
        }
        options     = [ordered]@{
            legend  = [ordered]@{ displayMode = 'list'; placement = 'bottom'; showLegend = $true }
            tooltip = [ordered]@{ mode = 'multi'; sort = 'desc' }
        }
        targets     = @(
            [ordered]@{
                datasource   = $DataSource
                expr         = ('{{__name__=~"{0}",{1}}}' -f $fam.Regex, $sel)
                legendFormat = '{{__name__}} | {{scenario}} | {{git_sha}}'
                refId        = 'A'
            }
        )
    }
}

function New-BarchartPanel {
    param(
        [hashtable]$Kpi,
        [string[]]$Scenarios,
        [int]$X, [int]$Y, [int]$W = 12, [int]$H = 7
    )
    $sel = ConvertTo-PromScenarioSelector $Scenarios
    return [ordered]@{
        id          = Next-PanelId
        type        = 'barchart'
        title       = $Kpi.Title + ' - per-run history'
        datasource  = $DataSource
        gridPos     = @{ h = $H; w = $W; x = $X; y = $Y }
        fieldConfig = @{
            defaults  = [ordered]@{
                unit     = $Kpi.Unit
                decimals = 2
                color    = @{ mode = 'palette-classic' }
            }
            overrides = @()
        }
        options     = [ordered]@{
            orientation       = 'vertical'
            xTickLabelRotation= -45
            xTickLabelSpacing = 100
            showValue         = 'never'
            stacking          = 'none'
            tooltip           = [ordered]@{ mode = 'single'; sort = 'none' }
            legend            = [ordered]@{ displayMode = 'hidden'; placement = 'bottom'; showLegend = $false }
        }
        targets     = @(
            [ordered]@{
                datasource   = $DataSource
                expr         = ('{0}{{{1}}}' -f $Kpi.Metric, $sel)
                legendFormat = '{{scenario}} {{run_id}} @ {{git_sha}}'
                format       = 'time_series'
                refId        = 'A'
            }
        )
    }
}

function New-PersonaDashboard {
    param([string]$Id, [hashtable]$Persona)
    $script:panelIdSeq = 0
    $panels = @()

    $y = 0

    # ── Band 0: persona KPIs ──
    if ($Persona.Kpis.Count -gt 0) {
        $panels += New-RowPanel -Title 'Headline KPIs' -Y $y
        $y += 1
        $x = 0
        $kpiW = [int]([math]::Floor(24 / [math]::Min($Persona.Kpis.Count, 4)))
        foreach ($kpi in $Persona.Kpis) {
            $panels += New-StatPanel -Kpi $kpi -Scenarios $Persona.Scenarios -X $x -Y $y -W $kpiW -H 5
            $x += $kpiW
            if ($x -ge 24) { $x = 0; $y += 5 }
        }
        if ($x -ne 0) { $y += 5 }
    }

    # ── Band 1: trend strip per family ──
    $panels += New-RowPanel -Title 'Trends across runs' -Y $y
    $y += 1
    $x = 0
    foreach ($fam in $Persona.Families) {
        $panels += New-TrendPanel -FamilyId $fam -Scenarios $Persona.Scenarios -X $x -Y $y -W 12 -H 8
        $x += 12
        if ($x -ge 24) { $x = 0; $y += 8 }
    }
    if ($x -ne 0) { $y += 8 }

    # ── Band 2: commit comparator (one barchart per KPI) ──
    if ($Persona.Kpis.Count -gt 0) {
        $panels += New-RowPanel -Title 'Per-run history (hover bars for run_id and git_sha)' -Y $y
        $y += 1
        $x = 0
        foreach ($kpi in $Persona.Kpis) {
            $panels += New-BarchartPanel -Kpi $kpi -Scenarios $Persona.Scenarios -X $x -Y $y -W 12 -H 7
            $x += 12
            if ($x -ge 24) { $x = 0; $y += 7 }
        }
    }

    return [ordered]@{
        annotations    = @{ list = @() }
        editable       = $true
        fiscalYearStartMonth = 0
        graphTooltip   = 1
        liveNow        = $false
        panels         = $panels
        refresh        = ''
        schemaVersion  = 39
        style          = 'dark'
        tags           = @('orleans-lattice','benchmark-history',$Id)
        templating     = @{ list = @() }
        time           = [ordered]@{ from = 'now-30d'; to = 'now' }
        timepicker     = @{}
        timezone       = ''
        title          = $Persona.Title
        uid            = ('lat-hist-{0}' -f $Id)
        version        = 1
        weekStart      = ''
        description    = $Persona.Subtitle
    }
}

# ---------------------------------------------------------------------------
# Drift check vs scenarios/*.env
# ---------------------------------------------------------------------------

$repoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$scenarioRoot = Join-Path $repoRoot 'benchmark/scenarios'
$envFiles = @(Get-ChildItem $scenarioRoot -Filter '*.env' -ErrorAction SilentlyContinue | ForEach-Object { $_.BaseName })
$personaScenarios = @($Personas.Values | ForEach-Object { $_.Scenarios } | Sort-Object -Unique)
$missingFromPersonas = @($envFiles | Where-Object { $_ -notin $personaScenarios -and $_ -ne 'simulator-baseline' })
$missingFromDisk     = @($personaScenarios | Where-Object { $_ -notin $envFiles })
if ($missingFromPersonas) {
    Write-Warning "scenarios with no persona mapping: $($missingFromPersonas -join ', ')"
}
if ($missingFromDisk) {
    Write-Warning "personas reference scenarios with no .env on disk: $($missingFromDisk -join ', ')"
}

# ---------------------------------------------------------------------------
# Wipe stale BenchmarkHistory*.json and emit fresh ones.
# ---------------------------------------------------------------------------

$outDir = Join-Path $PSScriptRoot 'grafana/dashboards'
if (-not (Test-Path $outDir)) { New-Item -ItemType Directory -Path $outDir -Force | Out-Null }
Get-ChildItem $outDir -Filter 'BenchmarkHistory*.json' -ErrorAction SilentlyContinue | Remove-Item -Force

$utf8NoBom = New-Object System.Text.UTF8Encoding $false
foreach ($id in $Personas.Keys) {
    $persona = $Personas[$id]
    $dash = New-PersonaDashboard -Id $id -Persona $persona
    $json = $dash | ConvertTo-Json -Depth 100
    $path = Join-Path $outDir ('BenchmarkHistory.{0}.json' -f $id)
    [System.IO.File]::WriteAllText($path, $json, $utf8NoBom)
    Write-Host ("wrote {0,-30} ({1} panels, {2} bytes)" -f $id, $dash.panels.Count, (Get-Item $path).Length)
}

Write-Host ""
Write-Host ("Done: {0} persona dashboards regenerated under {1}." -f $Personas.Count, $outDir) -ForegroundColor Green