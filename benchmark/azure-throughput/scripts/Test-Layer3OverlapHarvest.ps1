#!/usr/bin/env pwsh
#requires -Version 7.0
<#
.SYNOPSIS
    Local regression tests for -Layer3OverlapHarvest: which cohorts the
    Layer 3 sweep defers, and how a deferred harvest is joined. Loads only
    the report's function definitions and drives Invoke-Layer3Cohorts against
    fake run-cohort-aca.ps1 / complete-cohort-aca.ps1 scripts.
.DESCRIPTION
    A deferred harvest runs in a background job while the next cohort runs,
    so it is only safe for a cohort whose result cannot change what the
    sweep does next: never a cell's escalating first cohort, never a read
    cohort (an UNSEEDED read is re-run at once). Deferred cohorts are joined
    before the cell is checkpointed and graded exactly as an inline
    non-first cohort is.
#>
[CmdletBinding()] param()
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$path = Join-Path $PSScriptRoot '..\..\performance-report.ps1'
$tokens = $null
$errors = $null
$ast = [System.Management.Automation.Language.Parser]::ParseFile((Resolve-Path $path), [ref]$tokens, [ref]$errors)
if ($errors.Count -gt 0) { throw ($errors -join "`n") }
foreach ($function in $ast.FindAll({ param($node) $node -is [System.Management.Automation.Language.FunctionDefinitionAst] }, $false)) {
    . ([scriptblock]::Create($function.Extent.Text))
}

# A fake cohort log is '<seeded|unseeded> <rate>'; these stand in for the log
# parsers, which have their own tests.
function Read-SiloLogStats([string] $SiloLogPath, [string] $WorkloadMode, [int] $BatchSize) {
    $rate = [double]((Get-Content -Raw $SiloLogPath).Trim().Split(' ')[1])
    return [pscustomobject]@{
        SteadyMean = $rate; FinalOps = 1; FinalActiveSec = 1; FinalThroughput = $rate
        PerCallP50Ms = 1; PerCallP75Ms = 1; PerCallP90Ms = 1; PerCallP99Ms = 1
        InFlightMax = 1; Failed = 0; Verdict = 'HEALTHY'
    }
}
function Set-Layer3ProducerEvidence($Cohort, [string] $LogPath) { $Cohort['producerBound'] = $false }
function Set-Layer3StorageEvidence($Cohort, [string] $LogPath) { $Cohort['storage'] = $null }
function Set-Layer3PreseedEvidence($Cohort, [string] $LogPath, [string] $WorkloadMode) {
    $Cohort['unseeded'] = (Get-Content -Raw $LogPath).StartsWith('unseeded')
}

$passed = 0
function Assert-Case([string] $Name, [bool] $Condition) {
    if (-not $Condition) { throw "FAIL: $Name" }
    $script:passed++
    Write-Host "PASS: $Name"
}

$root = Join-Path ([IO.Path]::GetTempPath()) ("l3-overlap-" + [guid]::NewGuid().ToString('N'))
$azScriptsDir = Join-Path $root 'scripts'
$runRoot = Join-Path $root 'run'
New-Item -ItemType Directory -Force $azScriptsDir, $runRoot | Out-Null
$queueFile = Join-Path $root 'queue.txt'
$callsFile = Join-Path $root 'calls.txt'
Set-Content (Join-Path $azScriptsDir 'aca-common.ps1') "function Get-AcaRunRoot { '$runRoot' }"
# Pops the next scripted outcome. Inline: writes the cohort log. Deferred:
# writes only the pending descriptor, as the real script does.
Set-Content (Join-Path $azScriptsDir 'run-cohort-aca.ps1') @"
param([string]`$NamePrefix, [int]`$SiloCount, [string]`$WorkloadMode, [int]`$DurationSec,
    [int]`$VehiclesPerSilo, [int]`$TickHz, [int]`$BatchSize, [int]`$FlushConcurrencyPerSilo,
    [int]`$ShardCount, [int]`$WalPartitions, [int]`$SetManyFanOutBudgetSec,
    [int]`$WalAdmissionCallBudgetSec, [int]`$WalAppendCoalescingInFlightThreshold,
    [int]`$WalSaturationRecoveryReleaseBatch, [int]`$ClientsPerSilo, [string]`$CohortTag,
    [int]`$WalAccounts, [switch]`$DeferHarvest)
`$queue = @(Get-Content '$queueFile')
Set-Content '$queueFile' @(`$queue | Select-Object -Skip 1)
`$log = Join-Path '$runRoot' "`$NamePrefix.n`$SiloCount.`$WorkloadMode.`$CohortTag.log"
Add-Content '$callsFile' "`${CohortTag}:`${VehiclesPerSilo}:`$(if (`$DeferHarvest) { 'deferred' } else { 'inline' })"
if (`$DeferHarvest) { Set-Content "`$log.pending.json" `$queue[0] } else { Set-Content `$log `$queue[0] }
"@
# The deferred half: turns the descriptor into the cohort log, as the real
# complete-cohort-aca.ps1 does after its harvest.
Set-Content (Join-Path $azScriptsDir 'complete-cohort-aca.ps1') @'
param([string] $PendingHarvestPath)
Start-Sleep -Milliseconds 200
Set-Content ($PendingHarvestPath -replace '\.pending\.json$', '') (Get-Content -Raw $PendingHarvestPath).Trim()
Remove-Item $PendingHarvestPath
Write-Host "harvested $PendingHarvestPath"
'@

# Starting rung 1000 veh/silo x 5 Hz = 5000 keys/s offered at one silo.
$Layer3Rows = @(
    @{ Label = 'SetManyAsync'; WorkloadId = 'set-many'; WorkloadMode = 'set-many'; RungPerSilo = '1000:5:10' },
    @{ Label = 'GetManyAsync'; WorkloadId = 'get-many'; WorkloadMode = 'get-many'; RungPerSilo = '1000:5:10' }
)

function Invoke-Case([string] $Workload, [string[]] $Outcomes, [int] $N = 3, [switch] $Overlap, [double] $Ratio = 0.9) {
    Set-Content $queueFile $Outcomes
    Set-Content $callsFile @()
    Get-ChildItem $runRoot | Remove-Item -Force
    $mode = @($Layer3Rows | Where-Object { $_.WorkloadId -eq $Workload })[0].WorkloadMode
    $cells = Invoke-Layer3Cohorts -AcaPrefix t -WorkloadIds @($Workload) -SiloCounts @(1) -N $N `
        -SaturationRatio $Ratio -OverlapHarvest:$Overlap 3>$null 6>$null
    return @{ Cells = @($cells[$mode]['1']); Calls = @(Get-Content $callsFile) }
}

try {
    # 1. The first cohort decides the rung, so it is harvested inline; the
    #    rest run at the settled rung and are deferred.
    $r = Invoke-Case 'set-many' @('seeded 1000', 'seeded 1100', 'seeded 1200') -Overlap
    Assert-Case 'escalating first cohort is harvested inline' ($r.Calls[0] -eq 'c1:1000:inline')
    Assert-Case 'later cohorts of the cell are deferred' ($r.Calls[1] -eq 'c2:1000:deferred' -and $r.Calls[2] -eq 'c3:1000:deferred')
    Assert-Case 'deferred cohorts are joined into the cell before it returns' ($r.Cells.Count -eq 3)
    Assert-Case 'joined cohorts keep run order and carry their own results' (($r.Cells | ForEach-Object { $_.finalThroughput }) -join ',' -eq '1000,1100,1200')
    Assert-Case 'deferred cohorts are marked' (-not $r.Cells[0].ContainsKey('harvestDeferred') -and $r.Cells[1].harvestDeferred -and $r.Cells[2].harvestDeferred)
    Assert-Case 'no pending descriptor is left behind' (@(Get-ChildItem $runRoot -Filter '*.pending.json').Count -eq 0)

    # 2. Without the switch nothing is deferred.
    $r = Invoke-Case 'set-many' @('seeded 1000', 'seeded 1100', 'seeded 1200')
    Assert-Case 'harvests stay inline without -OverlapHarvest' (@($r.Calls | Where-Object { $_ -like '*:deferred' }).Count -eq 0 -and $r.Cells.Count -eq 3)

    # 3. A deferred cohort that still reached its offered load is graded
    #    offer-bound, exactly as an inline non-first cohort would be.
    $r = Invoke-Case 'set-many' @('seeded 1000', 'seeded 4800') -N 2 -Overlap
    Assert-Case 'a deferred cohort that reached its offer is offer-bound' ($r.Cells[1].offerBound -and -not $r.Cells[0].offerBound)

    # 4. With escalation off nothing depends on any cohort, so even the first defers.
    $r = Invoke-Case 'set-many' @('seeded 1000', 'seeded 1000') -N 2 -Overlap -Ratio 0
    Assert-Case 'with escalation off every cohort defers' (@($r.Calls | Where-Object { $_ -like '*:deferred' }).Count -eq 2 -and $r.Cells.Count -eq 2)

    # 5. A read cohort's UNSEEDED result triggers an immediate re-run, so read
    #    cohorts never defer.
    $r = Invoke-Case 'get-many' @('seeded 1000', 'seeded 1000', 'seeded 1000') -Overlap
    Assert-Case 'read cohorts are never deferred' (@($r.Calls | Where-Object { $_ -like '*:deferred' }).Count -eq 0 -and $r.Cells.Count -eq 3)

    # 6. Escalation still happens inline before anything is deferred: the
    #    first cohort reaches its offer, is re-run at double the rung, and the
    #    deferred cohorts run at the escalated rung.
    $r = Invoke-Case 'set-many' @('seeded 4900', 'seeded 6000', 'seeded 6100') -N 2 -Overlap
    Assert-Case 'escalation runs inline and later cohorts defer at the escalated rung' (($r.Calls -join ',') -eq 'c1:1000:inline,c1:2000:inline,c2:2000:deferred')

    Write-Host "All $passed Layer 3 overlap-harvest assertions passed."
} finally {
    Remove-Item -LiteralPath $root -Recurse -Force -ErrorAction SilentlyContinue
}
