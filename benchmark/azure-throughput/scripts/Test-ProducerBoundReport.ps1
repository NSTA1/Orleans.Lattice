#!/usr/bin/env pwsh
#requires -Version 7.0
<#
.SYNOPSIS
    Local regression tests for producer-bound Layer 3 parsing and rendering.
    Loads only function definitions, never the report's Azure-running Main.
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

$Layer3Rows = @(@{ Label = 'GetManyAsync'; WorkloadMode = 'get-many'; ChartGroup = 'reads' })
$Layer3ChartGroups = @(@{ Id = 'reads'; Title = 'Read throughput' })
$passed = 0
function Assert-Case([string] $Name, [bool] $Condition) {
    if (-not $Condition) { throw "FAIL: $Name" }
    $script:passed++
    Write-Host "PASS: $Name"
}
function New-Cohort([double] $Rate = 400000) {
    return @{
        verdict = 'HEALTHY'; finalThroughput = $Rate; steadyMean = $Rate
        perCallP50Ms = 1; perCallP99Ms = 2; rungVehicles = 200000; rungTickHz = 5
        offerBound = $false
    }
}

$log = [IO.Path]::GetTempFileName()
try {
    $cases = @(
        @{ Name = 'legacy slip is ambiguous'; Line = '[producer] t= 30.0s sent=1 rate=4,700 msg/s slipMaxMs= 6797.0'; Bound = $false },
        @{ Name = 'legacy DONE without blocked is ambiguous'; Line = '[producer] DONE total=1 elapsed=1s avg=4,700 msg/s slipMaxMs=6797.0'; Bound = $false },
        @{ Name = 'CPU-bound generator'; Line = '[producer] DONE total=1 elapsed=1s avg=400,000 msg/s genBlockedFrac=0.010 slipMaxMs=1001.0'; Bound = $true },
        @{ Name = 'saturated cluster below offered'; Line = '[producer] DONE total=1 elapsed=1s avg=400,000 msg/s genBlockedFrac=0.800 slipMaxMs=6797.0'; Bound = $false },
        @{ Name = 'blocked threshold is not producer-bound'; Line = '[producer] DONE total=1 elapsed=1s avg=400,000 msg/s genBlockedFrac=0.200 slipMaxMs=6797.0'; Bound = $false },
        @{ Name = 'slip threshold is not producer-bound'; Line = '[producer] DONE total=1 elapsed=1s avg=400,000 msg/s genBlockedFrac=0.010 slipMaxMs=1000.0'; Bound = $false },
        @{ Name = 'healthy'; Line = '[producer] DONE total=1 elapsed=1s avg=400,000 msg/s genBlockedFrac=0.010 slipMaxMs=10.0'; Bound = $false },
        @{ Name = 'ignore non-producer'; Line = '[silo] t=1s genBlockedFrac=1.0 slipMaxMs=99999.0'; Bound = $false }
    )
    foreach ($case in $cases) {
        [IO.File]::WriteAllText($log, $case.Line)
        $cohort = New-Cohort
        $warnings = @()
        Set-Layer3ProducerEvidence -Cohort $cohort -LogPath $log -WarningVariable warnings -WarningAction SilentlyContinue
        Assert-Case $case.Name ($cohort.producerBound -eq $case.Bound)
        Assert-Case "$($case.Name) warning" (($warnings.Count -gt 0) -eq $case.Bound)
    }

    [IO.File]::WriteAllText($log, '[producer] DONE total=1 elapsed=1s avg=900,000 msg/s genBlockedFrac=0.5 slipMaxMs=20.0')
    $cohort = New-Cohort 900000
    Set-Layer3ProducerEvidence -Cohort $cohort -LogPath $log
    Assert-Case 'at 90 percent offered is not backpressure-bound' (-not $cohort.producerBound)

    [IO.File]::WriteAllText($log, "[producer] t=1s genBlockedFrac=0.8 slipMaxMs=12000.0`n[producer] DONE total=1 elapsed=1s avg=400,000 msg/s genBlockedFrac=0.1 slipMaxMs=0.0")
    $cohort = New-Cohort
    Set-Layer3ProducerEvidence -Cohort $cohort -LogPath $log -WarningAction SilentlyContinue
    Assert-Case 'DONE totals win over unrelated periodic maxima' (-not $cohort.producerBound -and $cohort.producerSlipMaxMs -eq 0 -and $cohort.producerGenBlockedFrac -eq 0.1)

    [IO.File]::WriteAllText($log, "[producer] t=1s genBlockedFrac=0.8 slipMaxMs=12000.0`n[producer] t=2s genBlockedFrac=0.0 slipMaxMs=0.0")
    Set-Layer3ProducerEvidence -Cohort $cohort -LogPath $log
    Assert-Case 'windows cannot be combined without paired DONE totals' (-not $cohort.producerBound)

    [IO.File]::WriteAllText($log, "[producer] t=1s genBlockedFrac=0.8 slipMaxMs=0.0`n[producer] DONE total=1 elapsed=1s avg=400,000 msg/s genBlockedFrac=0.1 slipMaxMs=12000.0")
    Set-Layer3ProducerEvidence -Cohort $cohort -LogPath $log -WarningAction SilentlyContinue
    Assert-Case 'CPU-bound DONE is not masked by earlier blocked window' $cohort.producerBound

    $cohort.siloLog = $log
    $healthy = New-Cohort
    $cells = @{ 'get-many' = @{ '1' = @($healthy, $cohort); '2' = @((New-Cohort 800000)) } }
    $rows = Aggregate-Layer3Cells -Cells $cells -WarningAction SilentlyContinue
    Assert-Case 'any producer-bound contributor taints median' $rows.GetManyAsync['1'].producerBound
    Assert-Case 'producer-bound anchor disables scaling claims' ($null -eq $rows.GetManyAsync['2'].speedup -and $null -eq $rows.GetManyAsync['2'].efficiency)
    $table = Render-Layer3Table -RowsAgg $rows
    Assert-Case 'table marks lower bound' ($table.Contains('>= ') -and $table.Contains('not cluster ceilings'))
    $chart = Render-Layer3Chart -RowsAgg $rows
    Assert-Case 'chart cannot publish producer ceiling' (-not $chart.Contains('```mermaid'))
    Assert-Case 'omitted curves are explained' ($chart -match 'producer-bound')

    [IO.File]::WriteAllText($log, '[producer] DONE total=1 elapsed=1s avg=400,000 msg/s genBlockedFrac=0.010 slipMaxMs=10.0')
    $rows = Aggregate-Layer3Cells -Cells $cells
    Assert-Case 'resume reparses retained logs instead of stale grading' (-not $rows.GetManyAsync['1'].producerBound)
    Assert-Case 'healthy speedup unchanged' ($rows.GetManyAsync['2'].speedup -eq 2)
    Assert-Case 'healthy table is not a lower bound' (-not (Render-Layer3Table -RowsAgg $rows).Contains('>= '))
    Assert-Case 'healthy chart still renders' ((Render-Layer3Chart -RowsAgg $rows).Contains('```mermaid'))
    Assert-Case 'healthy chart has no omission note' (-not (Render-Layer3Chart -RowsAgg $rows).Contains('Producer-bound workload curves are omitted'))
    Assert-Case 'empty chart has no omission claim' (-not (Render-Layer3Chart -RowsAgg @{}).Contains('producer-bound'))
    $healthy.offerBound = $true
    $cells['get-many']['1'] = @($healthy)
    $rows = Aggregate-Layer3Cells -Cells $cells
    Assert-Case 'existing offer-bound marker retained' ((Render-Layer3Table -RowsAgg $rows).Contains('>= '))
    Write-Host "$passed assertions passed."
} finally {
    Remove-Item -LiteralPath $log
}
