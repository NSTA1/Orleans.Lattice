#requires -Version 7
<#
.SYNOPSIS
    Reconfigures the azure-throughput bench container group without rebuilding the
    silo/producer images. Drives a one-shot run or a multi-cell parameter sweep and
    collects steady-state throughput stats into a CSV.

.DESCRIPTION
    ACI does not allow mutating env vars on a running container group, so every
    "reconfigure" is delete + recreate of the container group. This script keeps the
    images that 20-build-and-deploy.ps1 already pushed and only regenerates the YAML
    with the new env vars, which makes a redeploy 5-10 seconds instead of the 2-3
    minutes of an `az acr build` cycle.

    Two modes:

      Single-cell (default): pass silo knobs (BatchSize, FlushConcurrency, ...) and
      producer knobs (VehicleCount, TickHz, DurationSec) as named params. The script
      deploys, waits for the producer to terminate (or DurationSec+90s, whichever
      hits first), parses the silo log, and appends one row to the results CSV.

      Matrix (`-MatrixFile <path>`): read a JSON array of cell hashtables and iterate
      every cell. Each cell may override any of the same knobs; missing knobs fall
      back to the named-param defaults. Use this to sweep batch sizes, flush
      concurrencies, pipeline modes, etc. against the same image build.

    The agent can drive this end-to-end: build the matrix JSON in .scratch/, invoke
    this script with -MatrixFile, parse the printed CSV path / final table.

.PARAMETER BatchSize
    Silo BENCH_BATCH_SIZE (default 4096).

.PARAMETER FlushMs
    Silo BENCH_FLUSH_MS (default 50).

.PARAMETER FlushConcurrency
    Silo BENCH_FLUSH_CONCURRENCY (default 8).

.PARAMETER WalPartitions
    Silo BENCH_WAL_PARTITIONS (default 8).

.PARAMETER WalMaxPendingBatches
    Silo BENCH_WAL_MAX_PENDING_BATCHES (default 8).

.PARAMETER PipelinePhase2
    Silo BENCH_PIPELINE_PHASE2 (default $true).

.PARAMETER ShardCountOverride
    Silo BENCH_SHARD_COUNT (default 0 = no reshard).

.PARAMETER TreeId
    Silo BENCH_TREE_ID. Default empty string = let the silo auto-rotate to
    azure-throughput-{utcTimestamp}, which avoids WAL-table cross-run history.
    Pin to a string when you want to reuse the same WAL namespace across cells.

.PARAMETER VehicleCount
    Producer BENCH_VEHICLE_COUNT (default 1000).

.PARAMETER TickHz
    Producer BENCH_TICK_HZ (default 5).

.PARAMETER DurationSec
    Producer BENCH_DURATION_SEC (default 60).

.PARAMETER Tag
    Image tag to redeploy (default 'latest'). Must already exist in ACR.

.PARAMETER Label
    Optional cell label that gets recorded in the results CSV (default = derived
    from the most distinctive overridden knob).

.PARAMETER ResultsCsv
    Results CSV path (default .matrix-results.csv next to the script).

.PARAMETER MatrixFile
    Path to a JSON file containing an array of cell hashtables. When supplied,
    every cell is run in sequence and the named per-cell knobs are ignored.

.PARAMETER CooldownSec
    Seconds to wait between cells in a matrix run (default 10) so the previous
    ACI delete settles before the next create. Ignored in single-cell mode.

.PARAMETER NoWait
    Skip the producer-terminate wait and log parse. Use when you just want to
    redeploy with new knobs and watch the silo log via 30-tail-logs.ps1.

.EXAMPLE
    # Single cell, current deploy-script defaults, 60s run:
    ./21-reconfigure.ps1

.EXAMPLE
    # Single cell with smaller batch + pipeline off, for an A/B baseline:
    ./21-reconfigure.ps1 -BatchSize 256 -PipelinePhase2:$false -Label 'baseline-256-nopipe'

.EXAMPLE
    # Drive a matrix from a JSON file:
    # matrix.json:
    #   [
    #     { "Label": "baseline-256-nopipe", "BatchSize": 256, "PipelinePhase2": false },
    #     { "Label": "pipe-256",            "BatchSize": 256, "PipelinePhase2": true  },
    #     { "Label": "pipe-4096",           "BatchSize": 4096 },
    #     { "Label": "pipe-4096-conc16",    "BatchSize": 4096, "FlushConcurrency": 16, "WalPartitions": 16 }
    #   ]
    ./21-reconfigure.ps1 -MatrixFile .scratch/matrix.json -DurationSec 90
#>

[CmdletBinding()]
param(
    [int]    $BatchSize            = 4096,
    [int]    $FlushMs              = 50,
    [int]    $FlushConcurrency     = 8,
    [int]    $WalPartitions        = 8,
    [int]    $WalMaxPendingBatches = 8,
    [bool]   $PipelinePhase2       = $true,
    [int]    $ShardCountOverride   = 0,
    [string] $TreeId               = '',
    [int]    $VehicleCount         = 1000,
    [int]    $TickHz               = 5,
    [int]    $DurationSec          = 60,
    [string] $Tag                  = 'latest',
    [string] $Label                = '',
    [string] $ResultsCsv,
    [string] $MatrixFile,
    [int]    $CooldownSec          = 10,
    [switch] $NoWait
)

$ErrorActionPreference = 'Stop'

$ctxPath = Join-Path $PSScriptRoot '.context.json'
if (-not (Test-Path $ctxPath)) {
    throw "Run 10-provision.ps1 first; missing $ctxPath."
}
$ctx = Get-Content $ctxPath | ConvertFrom-Json
$containerGroup = "$($ctx.Prefix)-bench"

if (-not $ResultsCsv) {
    $ResultsCsv = Join-Path $PSScriptRoot '.matrix-results.csv'
}

# ACR admin creds for image pull. (Storage still uses managed identity.)
$acrCreds = az acr credential show --name $ctx.Acr --output json | ConvertFrom-Json
$acrUser  = $acrCreds.username
$acrPass  = $acrCreds.passwords[0].value
$identityClientId = az identity show --name $ctx.Identity --resource-group $ctx.ResourceGroup --query clientId --output tsv

$producerImage = "$($ctx.AcrLoginServer)/azure-throughput-producer:$Tag"
$siloImage     = "$($ctx.AcrLoginServer)/azure-throughput-silo:$Tag"

# ---------- helpers -----------------------------------------------------------

function Resolve-CellSettings {
    <#
        Merges per-cell overrides on top of the script-level defaults. Returns a
        hashtable with every silo + producer knob filled in, plus a derived Label
        if the caller didn't supply one.
    #>
    param([hashtable] $Overrides = @{})

    $defaults = @{
        BatchSize            = $BatchSize
        FlushMs              = $FlushMs
        FlushConcurrency     = $FlushConcurrency
        WalPartitions        = $WalPartitions
        WalMaxPendingBatches = $WalMaxPendingBatches
        PipelinePhase2       = $PipelinePhase2
        ShardCountOverride   = $ShardCountOverride
        TreeId               = $TreeId
        VehicleCount         = $VehicleCount
        TickHz               = $TickHz
        DurationSec          = $DurationSec
        Label                = $Label
    }

    $merged = @{}
    foreach ($k in $defaults.Keys) { $merged[$k] = $defaults[$k] }
    foreach ($k in $Overrides.Keys) { $merged[$k] = $Overrides[$k] }

    if ([string]::IsNullOrWhiteSpace($merged.Label)) {
        $merged.Label = ('b{0}_c{1}_p{2}_wp{3}_wmp{4}_pipe{5}' -f `
            $merged.BatchSize, $merged.FlushConcurrency, $merged.PipelinePhase2,
            $merged.WalPartitions, $merged.WalMaxPendingBatches,
            $(if ($merged.PipelinePhase2) { '1' } else { '0' }))
    }

    return $merged
}

function Build-Yaml {
    <#
        Renders the ACI container-group YAML for a cell. Mirrors the heredoc in
        20-build-and-deploy.ps1 but parameterizes every silo env var so a matrix
        run can vary them per cell without rebuilding images.
    #>
    param([hashtable] $Cell)

    # Empty TreeId => omit the env var entirely so the silo auto-rotates.
    $treeIdBlock = ''
    if (-not [string]::IsNullOrWhiteSpace($Cell.TreeId)) {
        $treeIdBlock = @"
          - name: BENCH_TREE_ID
            value: $($Cell.TreeId)
"@
    }

    $pipeFlag = if ($Cell.PipelinePhase2) { '1' } else { '0' }

    return @"
apiVersion: '2021-10-01'
location: $($ctx.Location)
name: $containerGroup
identity:
  type: UserAssigned
  userAssignedIdentities:
    $($ctx.IdentityResourceId): {}
properties:
  osType: Linux
  restartPolicy: Never
  imageRegistryCredentials:
    - server: $($ctx.AcrLoginServer)
      username: $acrUser
      password: $acrPass
  containers:
    - name: silo
      properties:
        image: $siloImage
        resources:
          requests:
            cpu: 2.0
            memoryInGB: 4.0
        ports:
          - port: 7000
            protocol: TCP
        environmentVariables:
          - name: BENCH_STORAGE_URI
            value: $($ctx.StorageUri)
          - name: BENCH_WAL_TABLE
            value: OrleansLatticeWal
$treeIdBlock
          - name: BENCH_TCP_PORT
            value: '7000'
          - name: BENCH_BATCH_SIZE
            value: '$($Cell.BatchSize)'
          - name: BENCH_FLUSH_MS
            value: '$($Cell.FlushMs)'
          - name: BENCH_FLUSH_CONCURRENCY
            value: '$($Cell.FlushConcurrency)'
          - name: BENCH_WAL_PARTITIONS
            value: '$($Cell.WalPartitions)'
          - name: BENCH_WAL_MAX_PENDING_BATCHES
            value: '$($Cell.WalMaxPendingBatches)'
          - name: BENCH_PIPELINE_PHASE2
            value: '$pipeFlag'
          - name: BENCH_SHARD_COUNT
            value: '$($Cell.ShardCountOverride)'
          - name: BENCH_REPORT_SEC
            value: '1'
          - name: AZURE_CLIENT_ID
            value: $identityClientId
    - name: producer
      properties:
        image: $producerImage
        resources:
          requests:
            cpu: 1.0
            memoryInGB: 1.5
        environmentVariables:
          - name: BENCH_VEHICLE_COUNT
            value: '$($Cell.VehicleCount)'
          - name: BENCH_TICK_HZ
            value: '$($Cell.TickHz)'
          - name: BENCH_SILO_HOST
            value: 127.0.0.1
          - name: BENCH_SILO_PORT
            value: '7000'
          - name: BENCH_DURATION_SEC
            value: '$($Cell.DurationSec)'
"@
}

function Invoke-Cell {
    <#
        Delete + recreate the container group for a single cell, optionally wait
        for the producer to terminate, then parse the silo log and return a
        result row.
    #>
    param([hashtable] $Cell)

    Write-Host ""
    Write-Host ("=" * 78) -ForegroundColor DarkGray
    Write-Host "[cell] $($Cell.Label)" -ForegroundColor Green
    Write-Host ("  silo     : batch={0} flushMs={1} flushConc={2} walPart={3} walMaxPend={4} pipe={5} shardOverride={6} treeId={7}" -f `
        $Cell.BatchSize, $Cell.FlushMs, $Cell.FlushConcurrency,
        $Cell.WalPartitions, $Cell.WalMaxPendingBatches,
        $Cell.PipelinePhase2, $Cell.ShardCountOverride,
        $(if ([string]::IsNullOrWhiteSpace($Cell.TreeId)) { '<auto>' } else { $Cell.TreeId }))
    Write-Host ("  producer : vehicles={0} tickHz={1} duration={2}s targetRate={3}/s" -f `
        $Cell.VehicleCount, $Cell.TickHz, $Cell.DurationSec, ($Cell.VehicleCount * $Cell.TickHz))
    Write-Host ("=" * 78) -ForegroundColor DarkGray

    # 1. Delete any existing container group so the create gets fresh env vars.
    $existing = az container show --resource-group $ctx.ResourceGroup --name $containerGroup --query name --output tsv 2>$null
    if ($existing) {
        Write-Host "[cell] deleting existing container group ..." -ForegroundColor DarkGray
        az container delete --resource-group $ctx.ResourceGroup --name $containerGroup --yes --output none
    }

    # 2. Render YAML and create.
    $yaml     = Build-Yaml -Cell $Cell
    $yamlPath = Join-Path $PSScriptRoot '.aci-deploy.yaml'
    Set-Content -Path $yamlPath -Value $yaml -Encoding utf8

    Write-Host "[cell] creating container group ..." -ForegroundColor DarkGray
    az container create --resource-group $ctx.ResourceGroup --file $yamlPath --output none
    if ($LASTEXITCODE -ne 0) { throw "az container create failed for cell '$($Cell.Label)'." }

    if ($NoWait) {
        Write-Host "[cell] -NoWait: created, not measuring. Tail logs via 30-tail-logs.ps1." -ForegroundColor Yellow
        return $null
    }

    # 3. Wait for the producer to terminate (restartPolicy=Never).
    $deadline = (Get-Date).AddSeconds($Cell.DurationSec + 90)
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
        Write-Warning "[cell] producer did not terminate within $($Cell.DurationSec + 90)s (last state='$producerState'); reading partial log anyway."
    }

    # 4. Pull silo log and parse.
    $siloLog = & az container logs --resource-group $ctx.ResourceGroup --name $containerGroup --container-name silo 2>$null
    if ($LASTEXITCODE -ne 0 -or -not $siloLog) {
        Write-Warning "[cell] could not read silo log for cell '$($Cell.Label)'; skipping."
        return $null
    }

    # The per-second progress lines look like:
    #   [silo] t=   12.0s written=         3,840 Entries written per second=       512 inFlight=  8
    $perSec = ($siloLog -split "`n") | ForEach-Object {
        if ($_ -match 't=\s*([\d.]+)s.*written=\s*([\d,]+).*Entries written per second=\s*([\d,]+).*inFlight=\s*(\d+)') {
            [pscustomobject]@{
                T        = [double] $Matches[1]
                Written  = [long]   ($Matches[2] -replace ',', '')
                Rate     = [long]   ($Matches[3] -replace ',', '')
                InFlight = [int]    $Matches[4]
            }
        }
    }

    # Steady state: from t=15s onward (skip warm-up).
    $steady = $perSec | Where-Object { $_.T -ge 15 }
    if (-not $steady -or $steady.Count -lt 3) { $steady = $perSec }

    $steadyAvg = 0; $steadyMin = 0; $steadyMax = 0; $steadyP50 = 0; $steadyP95 = 0
    if ($steady) {
        $rates = @($steady | ForEach-Object { $_.Rate } | Sort-Object)
        $steadyAvg = [long] (($steady | Measure-Object Rate -Average).Average)
        $steadyMin = ($steady | Measure-Object Rate -Minimum).Minimum
        $steadyMax = ($steady | Measure-Object Rate -Maximum).Maximum
        $steadyP50 = $rates[[int] ($rates.Count * 0.50)]
        $steadyP95 = $rates[[int] [Math]::Min($rates.Count - 1, $rates.Count * 0.95)]
    }

    $totalWritten = if ($perSec) { ($perSec | Select-Object -Last 1).Written } else { 0 }
    $maxInFlight  = if ($perSec) { ($perSec | Measure-Object InFlight -Maximum).Maximum } else { 0 }

    # Pull the [silo:ingest] diagnostic lines so the CSV records what the
    # binary ACTUALLY saw (catches stale-image / env-var regressions).
    $ingestSettings = ($siloLog -split "`n") | Where-Object { $_ -match '\[silo:ingest\] settings\.' } | Select-Object -First 1
    $firstDispatch  = ($siloLog -split "`n") | Where-Object { $_ -match '\[silo:ingest\] first dispatch' } | Select-Object -First 1
    $startupLine    = ($siloLog -split "`n") | Where-Object { $_ -match '^\[silo\] treeId=' } | Select-Object -First 1

    $row = [pscustomobject]@{
        Label                = $Cell.Label
        BatchSize            = $Cell.BatchSize
        FlushMs              = $Cell.FlushMs
        FlushConcurrency     = $Cell.FlushConcurrency
        WalPartitions        = $Cell.WalPartitions
        WalMaxPendingBatches = $Cell.WalMaxPendingBatches
        PipelinePhase2       = $Cell.PipelinePhase2
        ShardCountOverride   = $Cell.ShardCountOverride
        VehicleCount         = $Cell.VehicleCount
        TickHz               = $Cell.TickHz
        TargetRate           = $Cell.VehicleCount * $Cell.TickHz
        DurationSec          = $Cell.DurationSec
        SteadyAvg            = $steadyAvg
        SteadyP50            = $steadyP50
        SteadyP95            = $steadyP95
        SteadyMin            = $steadyMin
        SteadyMax            = $steadyMax
        TotalWritten         = $totalWritten
        MaxInFlight          = $maxInFlight
        ProducerState        = $producerState
        StartupLine          = $startupLine
        IngestSettings       = $ingestSettings
        FirstDispatch        = $firstDispatch
        Timestamp            = (Get-Date).ToString('s')
    }

    Write-Host ("[cell] steady avg/p50/p95/min/max = {0:N0} / {1:N0} / {2:N0} / {3:N0} / {4:N0} entries per second" -f `
        $steadyAvg, $steadyP50, $steadyP95, $steadyMin, $steadyMax) -ForegroundColor Cyan
    Write-Host ("[cell] totalWritten={0:N0} maxInFlight={1} producerState={2}" -f $totalWritten, $maxInFlight, $producerState)
    if ($startupLine)    { Write-Host "[cell] $startupLine" -ForegroundColor DarkGray }
    if ($ingestSettings) { Write-Host "[cell] $ingestSettings" -ForegroundColor DarkGray }
    if ($firstDispatch)  { Write-Host "[cell] $firstDispatch" -ForegroundColor DarkGray }

    return $row
}

# ---------- main --------------------------------------------------------------

# Build the list of cells. Single-cell mode just wraps the named params.
$cells = @()
if ($MatrixFile) {
    if (-not (Test-Path $MatrixFile)) { throw "MatrixFile not found: $MatrixFile" }
    Write-Host "[matrix] loading $MatrixFile" -ForegroundColor Cyan
    $rawCells = Get-Content $MatrixFile -Raw | ConvertFrom-Json
    foreach ($raw in $rawCells) {
        # ConvertFrom-Json returns PSCustomObject; convert to hashtable for Resolve-CellSettings.
        $ht = @{}
        foreach ($p in $raw.PSObject.Properties) { $ht[$p.Name] = $p.Value }
        $cells += , (Resolve-CellSettings -Overrides $ht)
    }
    Write-Host "[matrix] $($cells.Count) cells loaded, results -> $ResultsCsv" -ForegroundColor Cyan
} else {
    $cells = @(Resolve-CellSettings)
}

$results = New-Object System.Collections.Generic.List[object]

for ($i = 0; $i -lt $cells.Count; $i++) {
    $cell = $cells[$i]
    $row = Invoke-Cell -Cell $cell
    if ($null -ne $row) {
        $results.Add($row)
        # Persist incrementally - a crash mid-matrix doesn't lose prior cells.
        $results | Export-Csv -Path $ResultsCsv -NoTypeInformation -Encoding utf8
    }

    if ($i -lt $cells.Count - 1) {
        Write-Host "[matrix] cooldown ${CooldownSec}s ..." -ForegroundColor DarkGray
        Start-Sleep -Seconds $CooldownSec
    }
}

if ($results.Count -gt 0) {
    Write-Host ""
    Write-Host ("=" * 78) -ForegroundColor Green
    Write-Host "[summary] $($results.Count) cell(s)" -ForegroundColor Green
    Write-Host ("=" * 78) -ForegroundColor Green
    $results | Select-Object Label, BatchSize, FlushConcurrency, WalPartitions, WalMaxPendingBatches, PipelinePhase2, SteadyAvg, SteadyP50, SteadyP95, MaxInFlight, TotalWritten |
        Format-Table -AutoSize
    Write-Host "[summary] csv: $ResultsCsv" -ForegroundColor Cyan
}
