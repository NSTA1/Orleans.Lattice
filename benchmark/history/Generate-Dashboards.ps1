<#
.SYNOPSIS
    Regenerates the persona-grouped benchmark-history dashboards under
    benchmark/history/grafana/dashboards/.

.DESCRIPTION
    The benchmark-history Grafana stack hosts one dashboard per *lattice-usage
    persona* (replication, write-heavy random/ordered, read-heavy, read-write-mix,
    microbench). Each persona aggregates one or more concrete benchmark scenarios
    (`scenarios/*.env`) so the cross-run trend view answers a single question:

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

    KPI definitions live in `$KpiCatalog`; personas reference catalog ids so a
    threshold change is one edit. KPI metric names are validated at generation
    time against benchmark.ps1's `$ScalarPanelExtra` (parsed from disk) plus the
    auto-discovery shape regex - a "ghost-KPI" that doesn't resolve to either is
    a fatal error, not a silent empty panel.

    Output: one JSON per persona at
        benchmark/history/grafana/dashboards/BenchmarkHistory.<persona>.json
    UID format: `lat-hist-<persona>` (Grafana's 40-char UID limit gives this
    exactly enough room for the longest persona id).

.NOTES
    Re-run is idempotent. Stale BenchmarkHistory*.json files are wiped first
    so deleted personas don't leak. Drift between this script's persona table
    and the on-disk scenarios/*.env list emits a non-fatal warning.
#>

[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'

# ---------------------------------------------------------------------------
# KPI catalog - one entry per headline metric. Personas reference by id.
# Adding a new KPI metric to this catalog requires either:
#   (a) the bare name (without the "bench_" prefix) appears as a key in
#       benchmark.ps1's $ScalarPanelExtra, or
#   (b) the name matches one of the auto-discovery synthesis shapes
#       (e.g. <prefix>_p99, <prefix>_per_second).
# Test-KpiMetricResolvable enforces this at generation time.
# ---------------------------------------------------------------------------

$KpiCatalog = [ordered]@{
    commit_p99 = @{
        Metric='bench_lattice_commit_p99_ms'; Title='Commit p99 (ms)';
        Unit='ms'; LowerIsBetter=$true;  WarnAt=10;  CritAt=50
    }
    commits_per_sec = @{
        Metric='bench_lattice_commits_per_second'; Title='Commits/sec';
        Unit='ops'; LowerIsBetter=$false; WarnAt=500; CritAt=100
    }
    leaf_write_p99 = @{
        Metric='bench_orleans_lattice_leaf_write_duration_milliseconds_p99'; Title='Leaf write p99 (ms)';
        Unit='ms'; LowerIsBetter=$true;  WarnAt=10;  CritAt=50
    }
    cache_hit_ratio = @{
        Metric='bench_lattice_cache_hit_ratio'; Title='Cache hit ratio';
        Unit='percentunit'; LowerIsBetter=$false; WarnAt=0.7; CritAt=0.4
    }
    sink_published_per_sec = @{
        Metric='bench_sink_published_per_second'; Title='Sink publishes/sec';
        Unit='ops'; LowerIsBetter=$false; WarnAt=500; CritAt=100
    }
    replication_ship_p95 = @{
        Metric='bench_replication_ship_p95_ms'; Title='Replication ship p95 (ms)';
        Unit='ms'; LowerIsBetter=$true; WarnAt=50; CritAt=200
    }
    replication_apply_lag_p95 = @{
        Metric='bench_replication_apply_lag_p95_ms'; Title='Replication apply lag p95 (ms)';
        Unit='ms'; LowerIsBetter=$true; WarnAt=100; CritAt=500
    }
    read_p99 = @{
        Metric='bench_vehicle_fleet_simulator_read_driver_duration_ms_p99'; Title='Read p99 (ms)';
        Unit='ms'; LowerIsBetter=$true; WarnAt=5; CritAt=25
    }
    reads_per_sec = @{
        Metric='bench_vehicle_fleet_simulator_read_driver_reads_per_second'; Title='Reads/sec';
        Unit='ops'; LowerIsBetter=$false; WarnAt=20000; CritAt=5000
    }
    reads_per_sec_mixed = @{
        Metric='bench_vehicle_fleet_simulator_read_driver_reads_per_second'; Title='Reads/sec';
        Unit='ops'; LowerIsBetter=$false; WarnAt=1500; CritAt=500
    }
    commits_per_sec_mixed = @{
        Metric='bench_lattice_commits_per_second'; Title='Commits/sec';
        Unit='ops'; LowerIsBetter=$false; WarnAt=1500; CritAt=500
    }
    microbench_point_write_p99 = @{
        Metric='bench_microbench_point_write_p99_ns'; Title='Point-write p99 (ns)';
        Unit='ns'; LowerIsBetter=$true; WarnAt=50000; CritAt=200000
    }
    microbench_point_read_p99 = @{
        Metric='bench_microbench_point_read_p99_ns'; Title='Point-read p99 (ns)';
        Unit='ns'; LowerIsBetter=$true; WarnAt=20000; CritAt=100000
    }
    microbench_bulk_load_per_sec = @{
        Metric='bench_microbench_bulk_load_per_second'; Title='Bulk-load ops/sec';
        Unit='ops'; LowerIsBetter=$false; WarnAt=50000; CritAt=10000
    }
    microbench_mixed_p99 = @{
        Metric='bench_microbench_mixed_70r_30w_p99_ns'; Title='Mixed 70r/30w p99 (ns)';
        Unit='ns'; LowerIsBetter=$true; WarnAt=30000; CritAt=150000
    }
    # ── WAL Performance KPIs ──
    # The leaf.commit.duration histogram is tagged step={wal|apply|shadow|observer}
    # and the four aliases below (defined in benchmark.ps1's $ScalarPanelExtra)
    # are label-filtered cuts of it. Pre-flip (LeafShadowWrites=true) the four
    # steps add up to the full commit tail; post-flip the shadow tile should
    # collapse to zero while WAL-append remains the durable boundary and Apply
    # becomes async background work. Watching all three p99s side-by-side is
    # the visual evidence harness for the LeafShadowWrites default flip.
    wal_append_p99 = @{
        Metric='bench_lattice_wal_append_p99_ms'; Title='WAL append p99 (ms)';
        Unit='ms'; LowerIsBetter=$true;  WarnAt=10;  CritAt=50
    }
    wal_apply_p99 = @{
        Metric='bench_lattice_apply_p99_ms'; Title='Apply step p99 (ms)';
        Unit='ms'; LowerIsBetter=$true;  WarnAt=10;  CritAt=50
    }
    wal_shadow_write_p99 = @{
        Metric='bench_lattice_shadow_write_p99_ms'; Title='Shadow-write p99 (ms)';
        Unit='ms'; LowerIsBetter=$true;  WarnAt=10;  CritAt=50
    }
    wal_appends_per_sec = @{
        Metric='bench_lattice_wal_appends_per_second'; Title='WAL appends/sec';
        Unit='ops'; LowerIsBetter=$false; WarnAt=500; CritAt=100
    }
}

# ---------------------------------------------------------------------------
# Persona table - what every dashboard is and what it asks. Kpis are catalog
# ids; Families are family-table ids.
# ---------------------------------------------------------------------------

$Personas = [ordered]@{
    'replication' = [ordered]@{
        Title     = 'Replication'
        Subtitle  = 'Replication ship/apply latency and underlying commit path under replication load'
        Scenarios = @('current-state-single-peer','bidirectional-replication','observer-no-peer','replication-key-filter','replication-backpressure','receiver-crash')
        Kpis      = @('replication_ship_p95','replication_apply_lag_p95','commit_p99','commits_per_sec')
        Families  = @('replication','commit','cache','process')
    }
    'write-heavy-random' = [ordered]@{
        Title     = 'Write-heavy (random keys)'
        Subtitle  = 'Per-vehicle current-state overwrites: steady-state and hot-key variants'
        Scenarios = @('current-state-no-replication','skewed-key-shard-splits')
        Kpis      = @('commit_p99','commits_per_sec','leaf_write_p99','cache_hit_ratio')
        Families  = @('commit','cache','sink','process')
    }
    'write-heavy-ordered' = [ordered]@{
        Title     = 'Write-heavy (ordered / append-only)'
        Subtitle  = 'Event-log keyspace with TTL: each tick produces a new key; TTL drives compaction'
        Scenarios = @('event-log-with-ttl')
        Kpis      = @('commit_p99','commits_per_sec','sink_published_per_sec')
        Families  = @('commit','sink','process')
    }
    'read-heavy' = [ordered]@{
        Title     = 'Read-heavy'
        Subtitle  = 'GetAsync-dominant load (95:5 read:write) - random and ordered keyspace variants overlaid'
        Scenarios = @('read-heavy-random','read-heavy-ordered')
        Kpis      = @('read_p99','reads_per_sec','cache_hit_ratio','commit_p99')
        Families  = @('read','cache','commit','process')
    }
    'read-write-mix' = [ordered]@{
        Title     = 'Read/write mix'
        Subtitle  = 'Balanced 50:50 read/write - random and ordered keyspace variants overlaid'
        Scenarios = @('read-write-mix-random','read-write-mix-ordered')
        Kpis      = @('read_p99','commit_p99','reads_per_sec_mixed','commits_per_sec_mixed')
        Families  = @('read','commit','cache','sink','process')
    }
    'microbench' = [ordered]@{
        Title     = 'Performance: Microbench'
        Subtitle  = 'BenchmarkDotNet ILattice micro-suite (in-process, no Orleans cluster)'
        Scenarios = @('microbench')
        Kpis      = @('microbench_point_write_p99','microbench_point_read_p99','microbench_bulk_load_per_sec','microbench_mixed_p99')
        Families  = @('microbench')
    }
    'wal-performance' = [ordered]@{
        Title     = 'WAL Performance'
        Subtitle  = 'Dual-durability commit path: WAL append, in-memory Apply, legacy shadow-write tail. Shadow tile is the dashboard the shadow-write removal flip is read against - it must collapse to zero post-flip while WAL append remains the load-bearing step.'
        Scenarios = @('current-state-single-peer','replication-backpressure','receiver-crash','bidirectional-replication','replication-key-filter')
        Kpis      = @('wal_append_p99','wal_apply_p99','wal_shadow_write_p99','wal_appends_per_sec')
        Families  = @('wal','commit','replication','process')
    }
}

# ---------------------------------------------------------------------------
# Family definitions: regex used to populate trend-strip timeseries panels.
# The replication family also matches the curated `bench_replication_*`
# aliases so they appear alongside the auto-discovered raw names.
# ---------------------------------------------------------------------------

$Families = [ordered]@{
    'commit'      = @{ Title='Commit path';       Regex='bench_(orleans_lattice_(leaf|shard|atomic_write|coordinator|tree)_.*|lattice_commit_.*|lattice_commits_.*)' }
    'cache'       = @{ Title='Cache & metadata';  Regex='bench_(orleans_lattice_(cache|events|config)_.*|lattice_cache_.*)' }
    'sink'        = @{ Title='Telemetry sink';    Regex='bench_(vehicle_fleet_simulator_sink_.*|sink_.*)' }
    'replication' = @{ Title='Replication';       Regex='bench_(orleans_lattice_replication_.*|replication_.*)' }
    'read'        = @{ Title='Read driver';       Regex='bench_vehicle_fleet_simulator_read_driver_.*' }
    'process'     = @{ Title='Process / runtime'; Regex='bench_dotnet_.*' }
    'microbench'  = @{ Title='BenchmarkDotNet';   Regex='bench_microbench_.*' }
    'wal'         = @{ Title='WAL & commit steps'; Regex='bench_(orleans_lattice_(leaf_(commit|shadow_write|replay)_.*|replication_wal_.*)|lattice_(wal|apply|shadow_write)_.*)' }
}

# ---------------------------------------------------------------------------
# KPI-name validation: parse $ScalarPanelExtra from benchmark.ps1, then check
# every persona-referenced KPI metric resolves to either an alias or an
# auto-discovery shape. Fatal on miss - prevents the "ghost-KPI panel" bug
# where the dashboard renders an empty stat tile because no series exists.
# ---------------------------------------------------------------------------

function Get-BenchmarkAliases {
    param([string]$BenchmarkScriptPath)
    if (-not (Test-Path $BenchmarkScriptPath)) {
        throw "benchmark.ps1 not found at $BenchmarkScriptPath; cannot validate KPI metric names"
    }
    $content = [System.IO.File]::ReadAllText($BenchmarkScriptPath)
    # Match the [ordered]@{ ... } block top-to-bottom, non-greedy, multiline.
    $blockRx = [regex]::new('\$ScalarPanelExtra\s*=\s*\[ordered\]@\{(?<body>.*?)^\}', 'Singleline,Multiline')
    $m = $blockRx.Match($content)
    if (-not $m.Success) {
        throw "could not locate `$ScalarPanelExtra block in $BenchmarkScriptPath"
    }
    $body = $m.Groups['body'].Value
    # Each alias key is a single-quoted bareword on its own line: ' <name>' =
    $keyRx = [regex]::new("(?m)^\s*'(?<name>[a-zA-Z_][a-zA-Z_0-9]*)'\s*=")
    return @($keyRx.Matches($body) | ForEach-Object { $_.Groups['name'].Value })
}

function Test-KpiMetricResolvable {
    param(
        [string]   $Metric,
        [string[]] $KnownAliases
    )
    if (-not $Metric.StartsWith('bench_', [StringComparison]::Ordinal)) { return $false }
    $bare = $Metric.Substring('bench_'.Length)
    if ($KnownAliases -contains $bare) { return $true }
    # Auto-discovery shapes (see benchmark.ps1::Get-AutoScalarPanel).
    if ($bare -match '^(orleans_lattice|vehicle_fleet_simulator|dotnet)_[a-z0-9_]+_(p\d{1,3}|per_second|increase|max|avg)$') { return $true }
    # Microbench shapes - HarnessJsonExporter writes these keys directly.
    if ($bare -match '^microbench_[a-z0-9_]+_(p\d{1,3}_ns|mean_ns|per_second|alloc_b)$') { return $true }
    return $false
}

function Resolve-Kpi {
    param([string]$KpiId)
    if (-not $KpiCatalog.Contains($KpiId)) {
        throw "persona references unknown KPI id '$KpiId'; not in `$KpiCatalog"
    }
    return $KpiCatalog[$KpiId]
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
        gridPos  = [ordered]@{ x = 0; y = $Y; w = 24; h = 1 }
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
            [ordered]@{ color = 'green';  value = $null },
            [ordered]@{ color = 'yellow'; value = $Kpi.WarnAt },
            [ordered]@{ color = 'red';    value = $Kpi.CritAt }
        )
    } else {
        # Inverted: green-yellow-red descending. Grafana evaluates step-by-step ascending,
        # so we encode the ranges as ascending values, with crit being lowest.
        $thresholds = @(
            [ordered]@{ color = 'red';    value = $null },
            [ordered]@{ color = 'yellow'; value = $Kpi.CritAt },
            [ordered]@{ color = 'green';  value = $Kpi.WarnAt }
        )
    }
    return [ordered]@{
        id          = Next-PanelId
        type        = 'stat'
        title       = $Kpi.Title
        datasource  = $DataSource
        gridPos     = [ordered]@{ x = $X; y = $Y; w = $W; h = $H }
        fieldConfig = [ordered]@{
            overrides = @()
            defaults  = [ordered]@{
                unit       = $Kpi.Unit
                decimals   = 2
                color      = [ordered]@{ mode = 'thresholds' }
                thresholds = [ordered]@{ steps = $thresholds; mode = 'absolute' }
            }
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
        gridPos     = [ordered]@{ x = $X; y = $Y; w = $W; h = $H }
        fieldConfig = [ordered]@{
            overrides = @()
            defaults  = [ordered]@{
                custom = [ordered]@{
                    drawStyle       = 'points'
                    pointSize       = 7
                    showPoints      = 'always'
                    lineInterpolation = 'linear'
                    spanNulls       = $true
                }
            }
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
        gridPos     = [ordered]@{ x = $X; y = $Y; w = $W; h = $H }
        fieldConfig = [ordered]@{
            overrides = @()
            defaults  = [ordered]@{
                unit     = $Kpi.Unit
                decimals = 2
                color    = [ordered]@{ mode = 'palette-classic' }
            }
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
    $resolvedKpis = @($Persona.Kpis | ForEach-Object { Resolve-Kpi -KpiId $_ })

    $y = 0

    # ── Band 0: persona KPIs ──
    if ($resolvedKpis.Count -gt 0) {
        $panels += New-RowPanel -Title 'Headline KPIs' -Y $y
        $y += 1
        $x = 0
        $kpiW = [int]([math]::Floor(24 / [math]::Min($resolvedKpis.Count, 4)))
        foreach ($kpi in $resolvedKpis) {
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
    if ($resolvedKpis.Count -gt 0) {
        $panels += New-RowPanel -Title 'Per-run history (hover bars for run_id and git_sha)' -Y $y
        $y += 1
        $x = 0
        foreach ($kpi in $resolvedKpis) {
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
        time           = [ordered]@{ from = 'now-90d'; to = 'now' }
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
# Overview dashboard: a single-page roll-up that shows every persona's
# headline KPIs in one view. One row per persona, banner labelled with the
# persona title, KPIs as stat panels scoped to the persona's scenarios so the
# values match what the per-persona dashboard renders. No trend strip, no
# barchart - the Overview's job is "is anything red right now?", and the
# detail view lives one click away on the persona dashboard.
# ---------------------------------------------------------------------------

function New-OverviewDashboard {
    param([System.Collections.IDictionary]$Personas)
    $script:panelIdSeq = 0
    $panels = @()
    $y = 0

    foreach ($personaId in $Personas.Keys) {
        $persona = $Personas[$personaId]
        $resolvedKpis = @($persona.Kpis | ForEach-Object { Resolve-Kpi -KpiId $_ })
        if ($resolvedKpis.Count -eq 0) { continue }

        $rowTitle = '{0}  -  {1}' -f $persona.Title, ($persona.Scenarios -join ', ')
        $panels += New-RowPanel -Title $rowTitle -Y $y
        $y += 1
        $x = 0
        $kpiW = [int]([math]::Floor(24 / [math]::Min($resolvedKpis.Count, 4)))
        foreach ($kpi in $resolvedKpis) {
            $panels += New-StatPanel -Kpi $kpi -Scenarios $persona.Scenarios -X $x -Y $y -W $kpiW -H 4
            $x += $kpiW
            if ($x -ge 24) { $x = 0; $y += 4 }
        }
        if ($x -ne 0) { $y += 4 }
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
        tags           = @('orleans-lattice','benchmark-history','overview')
        templating     = @{ list = @() }
        time           = [ordered]@{ from = 'now-90d'; to = 'now' }
        timepicker     = @{}
        timezone       = ''
        title          = 'Overview'
        uid            = 'lat-hist-overview'
        version        = 1
        weekStart      = ''
        description    = 'Roll-up of every persona dashboard''s headline KPIs in a single view. One row per persona; values are scoped to that persona''s scenarios so they match the per-persona dashboard.'
    }
}

# ---------------------------------------------------------------------------
# Validate KPI metric names BEFORE emitting any dashboards.
# ---------------------------------------------------------------------------

$repoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$benchmarkScript = Join-Path $repoRoot 'benchmark/benchmark.ps1'
$knownAliases = Get-BenchmarkAliases -BenchmarkScriptPath $benchmarkScript
Write-Host ("[validate] {0} aliases parsed from benchmark.ps1's `$ScalarPanelExtra: {1}" -f $knownAliases.Count, ($knownAliases -join ', ')) -ForegroundColor DarkGray

$invalidKpis = @()
foreach ($personaId in $Personas.Keys) {
    foreach ($kpiId in $Personas[$personaId].Kpis) {
        $kpi = Resolve-Kpi -KpiId $kpiId
        if (-not (Test-KpiMetricResolvable -Metric $kpi.Metric -KnownAliases $knownAliases)) {
            $invalidKpis += [pscustomobject]@{ Persona=$personaId; KpiId=$kpiId; Metric=$kpi.Metric }
        }
    }
}
if ($invalidKpis.Count -gt 0) {
    $invalidKpis | ForEach-Object {
        Write-Error ("[{0}] KPI '{1}' references metric '{2}' which is neither a benchmark.ps1 alias nor an auto-discovery shape" -f $_.Persona, $_.KpiId, $_.Metric)
    }
    throw "$($invalidKpis.Count) KPI(s) have unresolvable metric names; aborting before emitting incoherent dashboards"
}
Write-Host ("[validate] all {0} persona KPI(s) resolve to known aliases or auto-discovery shapes" -f ($Personas.Values | ForEach-Object { $_.Kpis.Count } | Measure-Object -Sum).Sum) -ForegroundColor Green

# ---------------------------------------------------------------------------
# Drift check vs scenarios/*.env. Fatal both ways: a persona referencing a
# scenario that has no .env on disk would render a dashboard with no data,
# and a scenario file with no persona means runs of it never appear on any
# dashboard (silent data loss for the operator).
# ---------------------------------------------------------------------------

$scenarioRoot = Join-Path $repoRoot 'benchmark/scenarios'
$envFiles = @(Get-ChildItem $scenarioRoot -Filter '*.env' -ErrorAction SilentlyContinue | ForEach-Object { $_.BaseName })
$personaScenarios = @($Personas.Values | ForEach-Object { $_.Scenarios } | Sort-Object -Unique)
$missingFromPersonas = @($envFiles | Where-Object { $_ -notin $personaScenarios })
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

# ── Overview roll-up: emitted last so the persona-validation pass is
#    guaranteed to have run before it starts pulling KPIs out of the catalog. ──
$overviewDash = New-OverviewDashboard -Personas $Personas
$overviewJson = $overviewDash | ConvertTo-Json -Depth 100
$overviewPath = Join-Path $outDir 'BenchmarkHistory.overview.json'
[System.IO.File]::WriteAllText($overviewPath, $overviewJson, $utf8NoBom)
Write-Host ("wrote {0,-30} ({1} panels, {2} bytes)" -f 'overview', $overviewDash.panels.Count, (Get-Item $overviewPath).Length)

Write-Host ""
Write-Host ("Done: {0} persona dashboards + 1 overview regenerated under {1}." -f $Personas.Count, $outDir) -ForegroundColor Green