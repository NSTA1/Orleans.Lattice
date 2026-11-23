#!/usr/bin/env pwsh
#requires -Version 7.0
<#
.SYNOPSIS
    Local regression tests for benchmark/tree-size-report.ps1: log parsing,
    knee detection and block rendering. Loads only function definitions,
    never the report's Azure-running Main.
#>
[CmdletBinding()] param()
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$path = Join-Path $PSScriptRoot '..\..\tree-size-report.ps1'
$tokens = $null
$errors = $null
$ast = [System.Management.Automation.Language.Parser]::ParseFile(
    (Resolve-Path $path), [ref]$tokens, [ref]$errors)
if ($errors.Count -gt 0) { throw ($errors -join "`n") }
foreach ($function in $ast.FindAll({
    param($node)
    $node -is [System.Management.Automation.Language.FunctionDefinitionAst]
}, $false)) {
    . ([scriptblock]::Create($function.Extent.Text))
}

$passed = 0
function Assert-Case([string] $Name, [bool] $Condition) {
    if (-not $Condition) { throw "FAIL: $Name" }
    $script:passed++
}

# A stray "``" continuation passes a literal backtick as the next parameter's value.
$cohortCall = @($ast.FindAll({
    param($node)
    $node -is [System.Management.Automation.Language.CommandAst] -and
        $node.CommandElements[0].Extent.Text -eq '$runCohort'
}, $true))
Assert-Case 'driver invokes run-cohort-aca once' ($cohortCall.Count -eq 1)
Assert-Case 'run-cohort-aca call has no literal backtick argument' (-not @($cohortCall[0].CommandElements |
    Where-Object { $_.Extent.Text -match '^`+$' }))
Assert-Case 'run-cohort-aca call passes -ExtraSiloEnv' (@($cohortCall[0].CommandElements |
    Where-Object { $_.Extent.Text -eq '-ExtraSiloEnv' }).Count -eq 1)

$tmp = Join-Path ([IO.Path]::GetTempPath()) "tree-size-selftest-$PID"
New-Item -ItemType Directory -Path $tmp -Force | Out-Null
try {
    # --- Ladder log splitting and segment parsing -----------------------------
    $log = Join-Path $tmp 'ladder.log'
    @(
        '[producer] warm-up complete'
        '[producer] tree-size-rung index=0 rung=1000000 phase=seed from=0'
        '[producer] preseed-progress treeId=t keys=524288'
        '[producer] preseed treeId=ts-1 entries=1000000 payloadBytes=245 method=bulk-append chunkSize=16384 attempts=62 elapsedMs=40000 segmentStart=0 segmentEntries=1000000'
        '[producer] tree-size-rung index=0 rung=1000000 phase=measure'
        '[producer] read-latency t=12.0s count=100 p50Ms=9.0 p95Ms=40.0 p99Ms=80.0 maxMs=120.0'
        '[producer] read-latency-final window=total unit=key count=300000 p50Ms=1.200 p95Ms=9.500 p99Ms=30.000 maxMs=120.000'
        '[producer] read-latency-final window=steady unit=key count=225000 p50Ms=1.100 p95Ms=2.000 p99Ms=4.000 maxMs=60.000'
        '[silo] FINAL ops=300,000 failed=0 discarded=0 elapsed=61.0s unresolved=0 unresolvedFlushes=0'
        '[producer] DONE total=300000'
        '[producer] tree-size-rung index=0 rung=1000000 phase=end'
        '[producer] tree-size-rung index=1 rung=3000000 phase=seed from=1000000'
        '[producer] preseed treeId=ts-1 entries=3000000 payloadBytes=245 method=bulk-append chunkSize=16384 attempts=123 elapsedMs=80000 segmentStart=1000000 segmentEntries=2000000'
        '[producer] tree-size-rung index=1 rung=3000000 phase=measure'
        '[producer] read-latency-final window=steady unit=key count=1,000 p50Ms=1.000 p95Ms=9.000 p99Ms=18.000 maxMs=20.000'
        '[silo] FINAL ops=1,234,567 failed=1,002 discarded=0 elapsed=61.0s unresolved=3 unresolvedFlushes=1'
        '[producer] DONE total=1234567'
        '[producer] tree-size-rung index=1 rung=3000000 phase=end'
        '[producer] tree-size-rung index=2 rung=10000000 phase=seed from=3000000'
        '[producer] preseed-progress treeId=t keys=4194304'
        'Verdict     : WEDGE'
    ) | Set-Content $log
    $l = Read-TreeSizeLadderLog -Path $log
    Assert-Case 'cohort verdict parsed' ($l.verdict -eq 'WEDGE')
    Assert-Case 'ladder incomplete' (-not $l.ladderComplete)
    Assert-Case 'one segment per rung' (@($l.rungs.Keys) -join ',' -eq '1m,3m,10m')
    $r1 = $l.rungs['1m']; $r3 = $l.rungs['3m']; $r10 = $l.rungs['10m']
    Assert-Case 'rung 1 seed segment' ($r1.seedSegmentStart -eq 0 -and $r1.seedSegmentEntries -eq 1000000 -and $r1.seedElapsedMs -eq 40000)
    Assert-Case 'rung 1 steady window chosen' ($r1.p95Ms -eq 2.0 -and $r1.readCount -eq 225000)
    Assert-Case 'rung 1 usable' ((Test-TreeSizeMeasureUsable $r1) -and (Test-TreeSizeSeedUsable $r1))
    Assert-Case 'rung 2 seed segment' ($r3.seedSegmentStart -eq 1000000 -and $r3.seedSegmentEntries -eq 2000000)
    Assert-Case 'rung 2 does not see rung 1 latency' ($r3.p95Ms -eq 9.0)
    Assert-Case 'grouped FINAL failed parsed' ($r3.failed -eq 1002)
    Assert-Case 'grouped FINAL unresolved parsed' ($r3.unresolved -eq 3)
    Assert-Case 'unresolved rung unusable' (-not (Test-TreeSizeMeasureUsable $r3))
    Assert-Case 'truncated rung has no end' (-not $r10.hasEnd -and $null -eq $r10.p95Ms)
    Assert-Case 'truncated rung unusable' (-not (Test-TreeSizeMeasureUsable $r10) -and -not (Test-TreeSizeSeedUsable $r10))
    Assert-Case 'usable rung count' ((Get-TreeSizeUsableRungCount -Cohort $l -Ladder @(1000000, 3000000, 10000000)) -eq 1)

    $missing = Read-TreeSizeLadderLog -Path (Join-Path $tmp 'absent.log')
    Assert-Case 'missing log yields no rungs' ($missing.rungs.Count -eq 0 -and $null -eq $missing.verdict)

    # Log Analytics reorders lines within one second: the closing lines of a
    # rung can follow its phase=end marker (seen on the first live cohort).
    $shuffled = Join-Path $tmp 'shuffled.log'
    @(
        '[producer] tree-size-rung index=0 rung=1000000 phase=seed from=0'
        '[producer] preseed treeId=ts-1 entries=1000000 payloadBytes=245 method=bulk-append chunkSize=16384 attempts=62 elapsedMs=537254 segmentStart=0 segmentEntries=1000000'
        '[producer] tree-size-rung index=0 rung=1000000 phase=measure'
        '[producer] read-latency-final window=total unit=key count=298000 p50Ms=1.797 p95Ms=6.048 p99Ms=17.108 maxMs=142.934'
        '[silo] FINAL ops=298,000 failed=0 discarded=0 elapsed=60.3s unresolved=0 unresolvedFlushes=0'
        '[producer] tree-size-rung index=0 rung=1000000 phase=end'
        '[producer] tree-size-ladder complete rungs=1 seeded=1000000'
        '[producer] read-latency-final window=steady unit=key count=225102 p50Ms=1.721 p95Ms=5.311 p99Ms=11.584 maxMs=142.934'
        '[producer] DONE total=298,000 elapsed=60.162s avg=4,953 msg/s'
        'Verdict     : HEALTHY'
    ) | Set-Content $shuffled
    $s = Read-TreeSizeLadderLog -Path $shuffled
    $s1 = $s.rungs['1m']
    Assert-Case 'reordered steady line still attributed' ($s1.p95Ms -eq 5.311 -and $s1.readCount -eq 225102 -and $s1.done)
    Assert-Case 'reordered rung usable' ((Test-TreeSizeMeasureUsable $s1) -and (Test-TreeSizeSeedUsable $s1) -and $s1.seedElapsedMs -eq 537254)

    # --- Formatting ---------------------------------------------------------
    Assert-Case 'size format M' ((Format-TreeSize 30000000) -eq '30 M')
    Assert-Case 'size tag' ((Format-SizeTag 1000000) -eq '1m')
    Assert-Case 'duration minutes' ((Format-Duration 125000) -eq '2 min 05 s')
    Assert-Case 'duration seconds' ((Format-Duration 9000) -eq '9 s')
    Assert-Case 'duration hours' ((Format-Duration 18000000) -eq '5 h 00 min')
    Assert-Case 'rate' ((Format-Rate 2000000 100000) -eq '20,000')

    # --- Knee and rendering -------------------------------------------------
    function New-Rung([long] $Start, [long] $Entries, [long] $Ms, [double] $P95) {
        [ordered]@{ hasEnd = $true; seedMethod = 'bulk-append'; seedSegmentStart = $Start; seedSegmentEntries = $Entries; seedElapsedMs = $Ms
            p50Ms = 1.0; p95Ms = $P95; p99Ms = $P95 * 2; readCount = 1000; failed = 0; unresolved = 0 }
    }
    $state = [ordered]@{
        region = 'westus3'; dotnetVersion = '10.0'; gitSha = 'abc1234'; measuredDate = '2026-11-22'
        workloadMode = 'get-point'; vehiclesPerSilo = 1000; tickHz = 5; durationSec = 60; degradeP95Ms = 5.0
        sizes = @(1000000, 3000000, 10000000); siloAxisSizes = @(1000000, 10000000); siloCounts = @(1, 2, 4)
        cohorts = [ordered]@{
            'ladder-n1' = [ordered]@{ verdict = 'HEALTHY'; rungs = [ordered]@{
                    '1m' = New-Rung 0 1000000 40000 2.0; '3m' = New-Rung 1000000 2000000 80000 4.0; '10m' = New-Rung 3000000 7000000 300000 9.0 } }
            'ladder-n2' = [ordered]@{ verdict = 'HEALTHY'; rungs = [ordered]@{
                    '1m' = New-Rung 0 1000000 25000 1.5; '10m' = New-Rung 1000000 9000000 200000 2.5 } }
            'ladder-n4' = [ordered]@{ verdict = 'WEDGE'; rungs = [ordered]@{
                    '1m' = New-Rung 0 1000000 20000 1.2 } }
        }
    }
    $knee = Get-TreeSizeKnee -State $state
    Assert-Case 'knee within' ($knee.Within -eq 3000000)
    Assert-Case 'knee exceeded' ($knee.Exceeded -eq 10000000)
    Assert-Case 'silo ladder uses axis sizes' ((Get-TreeSizeLadderFor -State $state -Silos 2) -join ',' -eq '1000000,10000000')

    $ladder = Render-TreeSizeLadder -State $state
    Assert-Case 'ladder cohortN' ($ladder.CohortN -eq 3)
    Assert-Case 'ladder summary' ($ladder.Block -match 'up to 3 M keys and exceeded it at 10 M')
    Assert-Case 'ladder row' ($ladder.Block -match '\| 10 M \| 7 M \| 5 min 00 s \| 23,333 \| 1.00 ms \| 9.00 ms \| 18.0 ms \| 1,000 \| no \|')
    Assert-Case 'ladder CRLF header' ($ladder.Block.StartsWith("<!-- perf-table:treesize:start`r`n  schema=v1`r`n"))

    $silos = Render-TreeSizeSilos -State $state
    Assert-Case 'silo cohortN' ($silos.CohortN -eq 5)
    Assert-Case 'silo row' ($silos.Block -match '\| 1 M \| 2.00 ms \(1.00 ms\) \| 1.50 ms \(1.00 ms\) \| 1.20 ms \(1.00 ms\) \|')
    Assert-Case 'silo unmeasured cell' ($silos.Block -match '\| 10 M \| 9.00 ms \(1.00 ms\) \| 2.50 ms \(1.00 ms\) \| _not measured_ \|')

    Assert-Case 'cumulative seed n1 to 10M' ((Get-TreeSizeCumulativeSeedMs -State $state -Silos 1 -Size 10000000) -eq 420000)
    Assert-Case 'cumulative seed gap is null' ($null -eq (Get-TreeSizeCumulativeSeedMs -State $state -Silos 4 -Size 10000000))
    $seed = Render-TreeSizeSeed -State $state
    Assert-Case 'seed cohortN' ($seed.CohortN -eq 5)
    Assert-Case 'seed row n1' ($seed.Block -match '\| 1 \| 40 s \(25,000\) \| 7 min 00 s \(23,810\) \|')
    Assert-Case 'seed row n4' ($seed.Block -match '\| 4 \| 20 s \(50,000\) \| _not measured_ \|')
    Assert-Case 'seed meta drops load keys' ($seed.Block -notmatch 'vehiclesPerSilo=')

    # --- Document update is idempotent and keeps notes single ----------------
    $doc = "intro`r`n`r`n<!-- perf-table:treesize:start -->`r`n<!-- perf-table:treesize:end -->`r`n`r`nmid`r`n`r`n<!-- perf-table:treesize-silos:start -->`r`n<!-- perf-table:treesize-silos:end -->`r`n`r`n<!-- perf-table:treesize-seed:start -->`r`n<!-- perf-table:treesize-seed:end -->`r`n`r`ntail`r`n"
    $once = Update-TreeSizeDoc -Content $doc -State $state
    $twice = Update-TreeSizeDoc -Content $once -State $state
    Assert-Case 'doc update idempotent' ($once -eq $twice)
    Assert-Case 'one note per block' (([regex]::Matches($twice, '> Measured ')).Count -eq 3)
    Assert-Case 'surrounding prose kept' ($twice.Contains("mid`r`n") -and $twice.EndsWith("tail`r`n"))
    Assert-Case 'doc is ASCII' (-not ($twice -match '[^\x00-\x7F]'))

    # --- State round trip through JSON (the -Resume / -DryRun path) ----------
    $round = $state | ConvertTo-Json -Depth 10 | ConvertFrom-Json -AsHashtable
    Assert-Case 'state round trip renders identically' ((Update-TreeSizeDoc -Content $doc -State $round) -eq $once)
} finally {
    Remove-Item $tmp -Recurse -Force -ErrorAction SilentlyContinue
}

Write-Host "Test-TreeSizeReport: $passed checks passed" -ForegroundColor Green
