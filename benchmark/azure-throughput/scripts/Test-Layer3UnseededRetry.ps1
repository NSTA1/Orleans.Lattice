#!/usr/bin/env pwsh
#requires -Version 7.0
<#
.SYNOPSIS
    Local regression tests for how the Layer 3 sweep treats an UNSEEDED read
    cohort. Loads only function definitions, never the report's Azure-running
    Main, and drives Invoke-Layer3Cohorts against a fake cohort runner.
.DESCRIPTION
    A read cohort whose log carries no '[producer] preseed' line measured the
    miss path and is excluded from the aggregate. Until this was fixed the
    sweep also ACCEPTED such a cohort as the cell's first cohort without
    escalating, so the cell's next cohort - never allowed to escalate - ran at
    the starting rung, reached it, and the cell was published as a lower bound
    far below the escalated rung an earlier sweep had measured. The sweep now
    re-runs an unseeded cohort at the same rung, and an unseeded cohort kept
    after its retries does not use up the cell's escalation turn.
#>
[CmdletBinding()] param()
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$path = Join-Path $PSScriptRoot '..\..\performance-report.ps1'
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

# The fake cohort's log is 'seeded <rate>' or 'unseeded <rate>'; these stand in
# for the log parsers, which have their own tests.
function Read-SiloLogStats([string] $SiloLogPath, [string] $WorkloadMode, [int] $BatchSize) {
    $rate = [double]((Get-Content -Raw $SiloLogPath).Trim().Split(' ')[1])
    return [pscustomobject]@{
        SteadyMean = $rate; FinalOps = 1; FinalActiveSec = 1; FinalThroughput = $rate
        PerCallP50Ms = 1; PerCallP75Ms = 1; PerCallP90Ms = 1; PerCallP99Ms = 1
        InFlightMax = 1; Failed = 0; Verdict = 'HEALTHY'
    }
}
function Set-Layer3ProducerEvidence($Cohort, [string] $LogPath) { $Cohort['producerBound'] = $false }
function Set-Layer3PreseedEvidence($Cohort, [string] $LogPath, [string] $WorkloadMode) {
    $Cohort['unseeded'] = (Get-Content -Raw $LogPath).StartsWith('unseeded')
}

$passed = 0
function Assert-Case([string] $Name, [bool] $Condition) {
    if (-not $Condition) { throw "FAIL: $Name" }
    $script:passed++
    Write-Host "PASS: $Name"
}

$root = Join-Path ([IO.Path]::GetTempPath()) ("l3-unseeded-" + [guid]::NewGuid().ToString('N'))
$azScriptsDir = Join-Path $root 'scripts'
$runRoot = Join-Path $root 'run'
New-Item -ItemType Directory -Force $azScriptsDir, $runRoot | Out-Null
$queueFile = Join-Path $root 'queue.txt'
$callsFile = Join-Path $root 'calls.txt'
Set-Content (Join-Path $azScriptsDir 'aca-common.ps1') "function Get-AcaRunRoot { '$runRoot' }"
# Each call pops the next scripted outcome and records the per-silo rung it ran at.
Set-Content (Join-Path $azScriptsDir 'run-cohort-aca.ps1') @"
param([string]`$NamePrefix, [int]`$SiloCount, [string]`$WorkloadMode, [int]`$DurationSec,
    [int]`$VehiclesPerSilo, [int]`$TickHz, [int]`$BatchSize, [int]`$FlushConcurrencyPerSilo,
    [int]`$ShardCount, [int]`$WalPartitions, [int]`$SetManyFanOutBudgetSec,
    [int]`$WalAdmissionCallBudgetSec, [int]`$WalAppendCoalescingInFlightThreshold,
    [int]`$WalSaturationRecoveryReleaseBatch, [int]`$ClientsPerSilo, [string]`$CohortTag)
`$queue = @(Get-Content '$queueFile')
Set-Content '$queueFile' @(`$queue | Select-Object -Skip 1)
Add-Content '$callsFile' "`${CohortTag}:`$VehiclesPerSilo"
Set-Content (Join-Path '$runRoot' "`$NamePrefix.n`$SiloCount.`$WorkloadMode.`$CohortTag.log") `$queue[0]
"@

# Starting rung 1000 veh/silo x 5 Hz = 5000 keys/s offered at one silo.
$Layer3Rows = @(@{ Label = 'GetManyAsync'; WorkloadId = 'get-many'; WorkloadMode = 'get-many'; RungPerSilo = '1000:5:10' })

function Invoke-Case([string[]] $Outcomes, [int] $MaxUnseededRetries) {
    Set-Content $queueFile $Outcomes
    Set-Content $callsFile @()
    Get-ChildItem $runRoot | Remove-Item -Force
    $cells = Invoke-Layer3Cohorts -AcaPrefix t -WorkloadIds @('get-many') -SiloCounts @(1) -N 2 `
        -MaxUnseededRetries $MaxUnseededRetries 3>$null 6>$null
    return @{ Cells = @($cells['get-many']['1']); Calls = @(Get-Content $callsFile) }
}

try {
    # 1. The first cohort loses its seed once. It is re-run at the same rung,
    #    then escalates normally; the second cohort runs at the escalated rung.
    $r = Invoke-Case -MaxUnseededRetries 2 -Outcomes @(
        'unseeded 4990', 'seeded 4990', 'seeded 7000', 'seeded 7100')
    Assert-Case 'unseeded first cohort is re-run at the same rung before escalating' (
        ($r.Calls -join ',') -eq 'c1:1000,c1:1000,c1:2000,c2:2000')
    Assert-Case 'both published cohorts ran at the escalated rung' (
        $r.Cells.Count -eq 2 -and $r.Cells[0].rungVehicles -eq 2000 -and $r.Cells[1].rungVehicles -eq 2000)
    Assert-Case 'neither published cohort is unseeded or offer-bound' (
        -not $r.Cells[0].unseeded -and -not $r.Cells[1].unseeded -and
        -not $r.Cells[0].offerBound -and -not $r.Cells[1].offerBound)
    Assert-Case 'the unseeded attempt keeps its own log' (
        Test-Path (Join-Path $runRoot 't.n1.get-many.c1.unseeded1.log'))

    # 2. Retries exhausted: the unseeded cohort is kept (the aggregate drops
    #    it), but it does not use the escalation turn, so the next cohort that
    #    reaches its offered load still escalates instead of being published
    #    as an offer-bound lower bound at the starting rung.
    $r = Invoke-Case -MaxUnseededRetries 0 -Outcomes @(
        'unseeded 4990', 'seeded 4990', 'seeded 7000')
    Assert-Case 'an exhausted unseeded cohort does not stop the next cohort escalating' (
        ($r.Calls -join ',') -eq 'c1:1000,c2:1000,c2:2000')
    Assert-Case 'the kept unseeded cohort is marked for exclusion' ($r.Cells[0].unseeded)
    Assert-Case 'the escalated cohort is not graded offer-bound' (
        $r.Cells[1].rungVehicles -eq 2000 -and -not $r.Cells[1].offerBound)

    # 3. A seeded second cohort that reaches its offered load is still graded
    #    offer-bound; the fix must not let later cohorts escalate in general.
    $r = Invoke-Case -MaxUnseededRetries 2 -Outcomes @('seeded 3000', 'seeded 4990')
    Assert-Case 'a seeded later cohort that reaches offered load is still offer-bound' (
        ($r.Calls -join ',') -eq 'c1:1000,c2:1000' -and $r.Cells[1].offerBound)
}
finally {
    Remove-Item -Recurse -Force $root -ErrorAction SilentlyContinue
}
Write-Host "All $passed Layer 3 unseeded-retry cases passed."
