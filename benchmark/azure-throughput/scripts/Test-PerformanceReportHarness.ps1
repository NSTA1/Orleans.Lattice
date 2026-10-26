#!/usr/bin/env pwsh
#requires -Version 7.0
[CmdletBinding()] param()
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$passed = 0
function Assert-Case([string] $Name, [bool] $Condition) {
    if (-not $Condition) { throw "FAIL: $Name" }
    $script:passed++
    Write-Host "PASS: $Name"
}
function Read-Ast([string] $Path) {
    $tokens = $null; $errors = $null
    $ast = [System.Management.Automation.Language.Parser]::ParseFile($Path, [ref]$tokens, [ref]$errors)
    if ($errors.Count -gt 0) { throw ($errors -join "`n") }
    return $ast
}
$report = Read-Ast (Join-Path $PSScriptRoot '..\..\performance-report.ps1')
$common = Read-Ast (Join-Path $PSScriptRoot 'aca-common.ps1')
foreach ($ast in @($report, $common)) {
    foreach ($fn in $ast.FindAll({ param($n) $n -is [System.Management.Automation.Language.FunctionDefinitionAst] }, $false)) {
        . ([scriptblock]::Create($fn.Extent.Text))
    }
}
# The report dot-sources these at script scope, which a function-only load skips.
. (Join-Path $PSScriptRoot '_run-cohort-helpers.ps1')
. (Join-Path $PSScriptRoot '_storage-metrics.ps1')
$Layer2Rows = @(
    @{ Label = 'Reads'; WorkloadMode = 'get-point'; ThroughputUnit = 'keys/s' },
    @{ Label = 'Writes'; WorkloadMode = 'set-point'; ThroughputUnit = 'keys/s' },
    @{ Label = 'Not rerun'; WorkloadMode = 'set-many'; ThroughputUnit = 'keys/s' }
)
function New-Cohort([string] $Log, [double] $Rate = 1000) {
    return @{ siloLog = $Log; rungVehicles = 10; verdict = 'HEALTHY'; steadyMean = $Rate;
        perCallP50Ms = 1; perCallP75Ms = 2; perCallP90Ms = 3; perCallP99Ms = 4 }
}
$log = [IO.Path]::GetTempFileName()
$doc = [IO.Path]::GetTempFileName()
$stateDir = Join-Path ([IO.Path]::GetTempPath()) ("perf-harness-" + [guid]::NewGuid().ToString('N'))
[IO.Directory]::CreateDirectory($stateDir) | Out-Null
try {
    $runRoot = $stateDir
    foreach ($spec in @(@{ Layer = 'layer1'; Age = 3 }, @{ Layer = 'layer2'; Age = 2 }, @{ Layer = 'layer3'; Age = 1 })) {
        $target = Join-Path $stateDir $spec.Layer
        [IO.Directory]::CreateDirectory($target) | Out-Null
        $target = Join-Path $target 'state.json'
        [IO.File]::WriteAllText($target, (@{ $spec.Layer = @{ rows = @{ measured = @{} } } } | ConvertTo-Json -Depth 5))
        [IO.File]::SetLastWriteTimeUtc($target, [datetime]::UtcNow.AddMinutes(-$spec.Age))
    }
    Assert-Case 'newer Layer3 state cannot displace Layer2 replay' ((Get-LatestStateFile -Layer '2') -eq (Join-Path $stateDir 'layer2\state.json'))
    Assert-Case 'Layer1 replay selects its own newest state' ((Get-LatestStateFile -Layer '1') -eq (Join-Path $stateDir 'layer1\state.json'))
    Assert-Case 'Layer3 replay selects multi-silo state' ((Get-LatestStateFile -Layer '3') -eq (Join-Path $stateDir 'layer3\state.json'))
    Assert-Case 'default single-silo replay ignores newer multi-silo state' ((Get-LatestStateFile) -eq (Join-Path $stateDir 'layer2\state.json'))
    foreach ($case in @(
        @{ Text = ''; Reject = $true; Name = 'missing preseed' },
        @{ Text = '[silo] preseed treeId=t entries=0'; Reject = $true; Name = 'zero preseed' },
        @{ Text = '[silo] preseed treeId=t entries=9'; Reject = $true; Name = 'partial preseed' },
        @{ Text = '[producer] preseed treeId=t entries=10'; Reject = $true; Name = 'wrong seed source' },
        @{ Text = 'Oct 13 12:00:00 host dotnet[1]: [silo] preseed treeId=t entries=10'; Reject = $false; Name = 'complete journal preseed' }
    )) {
        [IO.File]::WriteAllText($log, $case.Text)
        $c = New-Cohort $log
        Set-Layer2PreseedEvidence $c 'get-point' -WarningAction SilentlyContinue
        Assert-Case $case.Name ($c.unseeded -eq $case.Reject)
    }
    $c = New-Cohort "$log.missing"
    Set-Layer2PreseedEvidence $c 'get-many' -WarningAction SilentlyContinue
    Assert-Case 'missing retained log fails closed' $c.unseeded
    [IO.File]::WriteAllText($log, '')
    $rows = Aggregate-Layer2Cells @{ 'get-point' = @((New-Cohort $log)); 'set-point' = @((New-Cohort $log), (New-Cohort $log 0)) } -WarningAction SilentlyContinue
    Assert-Case 'legacy HEALTHY miss-path reads excluded' (-not $rows.ContainsKey('Reads'))
    Assert-Case 'n counts actual throughput samples' ($rows.Writes.cohortN -eq 1 -and $rows.Writes.sustainedThroughput -eq 1000)
    Assert-Case 'per-row cohort count rendered' ((Render-Layer2Table $rows @{}) -match 'Writes: n=1')

    # Per-call quantiles merge the [phaseA] windows of the measurement rather
    # than publishing the last one. Layer 2: progress and phaseA share the
    # silo's clock, so only windows inside the steady window (t=15..54) count.
    $progress = @(15..54 | ForEach-Object { "[silo] t= $_.0s ops= $($_ * 1000) ops/sec= 1,000 inFlight= 1" }) + @(55..60 | ForEach-Object { "[silo] t= $_.0s ops= 54,000 ops/sec= 0 inFlight= 0" })
    $phaseA = @(
        '[phaseA] t=20.3s instrument=get_many.duration tree=t shard=- phase=- status=- count=40 sum=1 min=9 p50=10 p75=11 p90=12 p99=13 max=13',
        '[phaseA] t=30.3s instrument=get_many.duration tree=t shard=- phase=- status=- count=50 sum=1 min=3 p50=4 p75=5 p90=6 p99=7 max=7',
        '[phaseA] t=40.3s instrument=get_many.duration tree=t shard=- phase=- status=- count=50 sum=1 min=3 p50=4 p75=5 p90=6 p99=7 max=7',
        '[phaseA] t=40.3s instrument=get_many.duration tree=_lattice_trees shard=- phase=- status=- count=500 sum=1 min=0.1 p50=0.1 p75=0.1 p90=0.1 p99=0.1 max=0.1',
        '[phaseA] t=60.3s instrument=get_many.duration tree=t shard=- phase=- status=- count=900 sum=1 min=0.1 p50=0.5 p75=0.6 p90=0.7 p99=0.8 max=0.8'
    )
    [IO.File]::WriteAllLines($log, @($progress + $phaseA + 'Verdict      : HEALTHY'))
    $l2 = Read-SiloLogStats -SiloLogPath $log -WorkloadMode 'get-many'
    Assert-Case 'Layer 2 per-call quantiles come from windows inside the steady window' ($l2.PerCallP50Ms -eq 4 -and $l2.PerCallP99Ms -eq 7 -and $l2.PerCallWindows -eq 2 -and $l2.PerCallCalls -eq 100)
    Assert-Case 'Layer 2 per-call basis is stated' ($l2.PerCallBasis -eq 'windows inside the steady-state window')
    # Layer 3: separate clocks, so every window at t>=15 of every silo merges.
    [IO.File]::WriteAllLines($log, @('[producer] mode=orleans-client vehicles=1 tickHz=1 duration=1s clients=1') + $progress + $phaseA)
    $l3 = Read-SiloLogStats -SiloLogPath $log -WorkloadMode 'get-many'
    Assert-Case 'Layer 3 per-call quantiles merge every window of every silo' ($l3.PerCallWindows -eq 4 -and $l3.PerCallCalls -eq 1040 -and $l3.PerCallBasis -eq 'all windows t>=15s' -and $l3.PerCallP50Ms -lt 4)

    $vm = Read-Ast (Join-Path $PSScriptRoot 'run-cohort.ps1')
    $Vehicles = 10; $treeId = 't'; $ExtraSiloEnv = @{ BENCH_VEHICLE_COUNT = '999'; BENCH_WORKLOAD_MODE = 'get-point' }
    $envAssignment = $vm.Find({ param($n) $n -is [System.Management.Automation.Language.AssignmentStatementAst] -and $n.Left.Extent.Text -eq '$siloEnvLines' }, $true)
    $envLoop = $vm.Find({ param($n) $n -is [System.Management.Automation.Language.ForEachStatementAst] -and $n.Condition.Extent.Text -eq '$ExtraSiloEnv.Keys' }, $true)
    . ([scriptblock]::Create($envAssignment.Extent.Text))
    . ([scriptblock]::Create($envLoop.Extent.Text))
    Assert-Case 'VM seeds exactly producer fleet even with conflicting override' (@($siloEnvLines | Where-Object { $_ -like 'Environment=BENCH_VEHICLE_COUNT=*' }).Count -eq 1 -and $siloEnvLines -contains 'Environment=BENCH_VEHICLE_COUNT=10')
    $siloSource = [IO.File]::ReadAllText((Join-Path $PSScriptRoot '..\Silo\Program.cs'))
    $engineSource = [IO.File]::ReadAllText((Join-Path $PSScriptRoot '..\Engine\BenchIngestEngine.cs'))
    Assert-Case 'view registration uses source-scoped identity' ($siloSource.Contains('views.AddView(BenchWorkloadMetadata.ViewNameFor(treeId), treeId,'))
    Assert-Case 'lag reporter uses same identity as registration' ($engineSource.Contains('BenchWorkloadMetadata.ViewNameFor(settings.TreeId)') -and $engineSource.Contains('new ViewLagMeterProbe(viewName)'))

    $script:query = ''; $script:response = @(
        @{ TimeGenerated = '2026-10-13T12:00:00Z'; ContainerGroupName_s = 'replica-a'; Log_s = '[phaseA] t=1s instrument=test' },
        @{ TimeGenerated = '2026-10-13T12:00:01Z'; ContainerGroupName_s = 'replica-b'; Log_s = 'fail: root-cause' },
        @{ TimeGenerated = '2026-10-13T12:00:02Z'; ContainerGroupName_s = 'replica-b'; Log_s = '[silo] t=2s ops=999 ops/sec=999 inFlight=1' }
    ) | ConvertTo-Json
    function Invoke-Az {
        param([string[]] $Arguments, [switch] $AllowFailure)
        $script:query = $Arguments[$Arguments.IndexOf('--analytics-query') + 1]
        $global:LASTEXITCODE = 0
        return $script:response
    }
    $ctx = @{ siloApp = 'silo'; workspaceId = 'workspace' }
    $lines = Get-AcaSiloLog $ctx ([datetime]'2026-10-13T12:00:00Z') ([datetime]'2026-10-13T13:00:00Z') -RevisionName 'current' -IncludeDiagnostics
    Assert-Case 'diagnostic harvest pins revision' ($script:query.Contains("RevisionName_s == 'current'") -and -not $script:query.Contains("where Log_s startswith"))
    Assert-Case 'phaseA parser surface unchanged' ($lines[0] -eq '[phaseA] t=1s instrument=test')
    Assert-Case 'root cause includes replica and timestamp' ($lines[1] -match 'replica=replica-b.*root-cause')
    Assert-Case 'silo progress cannot contaminate producer throughput' ($lines[2] -notmatch '^\[silo\]')
    $threw = $false
    try { Get-AcaSiloLog $ctx ([datetime]::UtcNow) ([datetime]::UtcNow) -IncludeDiagnostics | Out-Null } catch { $threw = $true }
    Assert-Case 'full diagnostics refuse unscoped revision' $threw
    $script:response = 'not JSON'; $threw = $false
    try { Get-AcaSiloLog $ctx ([datetime]::UtcNow) ([datetime]::UtcNow) -RevisionName 'current' | Out-Null } catch { $threw = $true }
    Assert-Case 'malformed diagnostics never silently disappear' $threw
    $script:pages = [System.Collections.Generic.List[string]]::new()
    function Invoke-Az {
        param([string[]] $Arguments, [switch] $AllowFailure)
        $kql = $Arguments[$Arguments.IndexOf('--analytics-query') + 1]
        $script:pages.Add($kql)
        $global:LASTEXITCODE = 0
        $n = if ($script:pages.Count -eq 1) { 200 } else { 1 }
        return (@(1..$n | ForEach-Object {
            @{ TimeGenerated = '2026-10-13T12:00:00Z'; ContainerGroupName_s = 'replica'; Log_s = "error $_" }
        }) | ConvertTo-Json -Compress)
    }
    $lines = Get-AcaSiloLog $ctx ([datetime]::UtcNow.AddHours(-1)) ([datetime]::UtcNow) -RevisionName 'current' -IncludeDiagnostics
    Assert-Case 'diagnostics page beyond first bounded response' ($lines.Count -eq 201 -and $script:pages.Count -eq 2 -and $script:pages[1].Contains('row > 200 and row <= 400'))
    $snapshots = @($script:pages | ForEach-Object { [regex]::Match($_, 'ingestion_time\(\) <= datetime\(([^)]+)\)').Groups[1].Value } | Sort-Object -Unique)
    Assert-Case 'diagnostic pagination uses one ingestion snapshot' ($snapshots.Count -eq 1 -and $snapshots[0].Length -gt 0)

    function New-MetaHeaderForLayer2 { param($State, $RowsAgg, $Existing) return @{ cohortN = '1'; date = 'today' } }
    function Render-ProvenanceNote { param($Layer, $Meta) return '> measured today' }
    $Layer1Rows = @()
    $state = @{ layer1 = @{ rows = @{} }; layer2 = @{ rows = $rows; cohorts = @{ 'get-point' = @((New-Cohort $log)); 'set-point' = @((New-Cohort $log)) } } }
    [IO.File]::WriteAllText($doc, "<!-- perf-table:layer1:start -->`n<!-- perf-table:layer1:end -->`n<!-- perf-table:layer2:start -->`n| Reads | stale-read |`n| Writes | stale-write |`n| Not rerun | previous-valid |`n<!-- perf-table:layer2:end -->")
    Update-DocMarkers $doc $state
    $text = [IO.File]::ReadAllText($doc)
    Assert-Case 'attempted invalid row cannot retain stale measurement' ($text -notmatch 'stale-read' -and $text -match '\| Reads\s+\| _pending_')
    Assert-Case 'unattempted prior row remains intact' ($text -match 'previous-valid')
    $state.layer2.rows = @{}
    Update-DocMarkers $doc $state
    $text = [IO.File]::ReadAllText($doc)
    Assert-Case 'all-failed attempted layer removes old results' ($text -notmatch 'stale-read|stale-write|Writes: n=1' -and $text -match 'cohortN=0' -and $text -match 'previous-valid')

    function Get-LatestStateFile { param($Layer) $script:selectedLayer = $Layer; return 'state.json' }
    function Read-StateFile { param($Path) return @{ layer3 = @{ rows = @{} } } }
    function Update-MultiSiloDocMarkers { param($DocPath, $State, [switch]$WhatIf) $script:multiRendered = $true; return $true }
    $script:multiRendered = $false
    $ParametersFile = ''; $azScriptsDir = $PSScriptRoot; $DryRun = $true; $Layer = '3'; $Layer3 = $false
    $Diff = $true; $SkipDocUpdate = $false; $multiSiloDocPath = $doc
    Main
    Assert-Case 'numeric Layer3 DryRun dispatches multi-silo renderer' $script:multiRendered
    function Read-StateFile { param($Path) return @{ layer1 = @{ rows = @{ older = @{} } }; layer2 = @{ rows = @{ measured = @{} } } } }
    function Update-DocMarkers { param($DocPath, $State, [switch]$WhatIf) $script:replayed = $State }
    $Layer = '2'; $docPath = $doc
    Main
    Assert-Case 'single-layer replay selects matching state' ($script:selectedLayer -eq '2')
    Assert-Case 'Layer2 replay does not overwrite Layer1 measurements' ($script:replayed.layer1.rows.Count -eq 0 -and $script:replayed.layer2.rows.Count -eq 1)
    Write-Host "$passed assertions passed."
} finally {
    Remove-Item -LiteralPath $log, $doc -Force
    Remove-Item -LiteralPath $stateDir -Recurse -Force
}
