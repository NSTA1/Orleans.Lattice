#!/usr/bin/env pwsh
#requires -Version 7.0
<#
.SYNOPSIS
    Local regression tests for the Layer 3 read-mode preseed gate (#3474).
    Loads only function definitions, never the report's Azure-running Main.
.DESCRIPTION
    Until #3474 the Layer 3 cluster-mode silo skipped the read-mode preseed,
    so every get-point / get-many cohort read an empty tree and measured the
    miss path. The producer now seeds and logs a '[producer] preseed' line;
    the report must refuse a read cohort whose log has none.
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

$Layer3Rows = @(
    @{ Label = 'GetAsync'; WorkloadMode = 'get-point'; ChartGroup = 'reads' },
    @{ Label = 'SetAsync'; WorkloadMode = 'set-point'; ChartGroup = 'writes' }
)
$Layer3ChartGroups = @(@{ Id = 'reads'; Title = 'Reads' }, @{ Id = 'writes'; Title = 'Writes' })
$passed = 0
function Assert-Case([string] $Name, [bool] $Condition) {
    if (-not $Condition) { throw "FAIL: $Name" }
    $script:passed++
    Write-Host "PASS: $Name"
}
function New-Cohort([double] $Rate, [string] $Log) {
    return @{
        verdict = 'HEALTHY'; finalThroughput = $Rate; steadyMean = $Rate
        perCallP50Ms = 1; perCallP99Ms = 2; rungVehicles = 200000; rungTickHz = 5
        offerBound = $false; siloLog = $Log
    }
}

$done = '[producer] DONE total=1 elapsed=1s avg=1,000 msg/s genBlockedFrac=0.500 slipMaxMs=10.0'
$seeded = [IO.Path]::GetTempFileName()
$unseeded = [IO.Path]::GetTempFileName()
$skipped = [IO.Path]::GetTempFileName()
$zero = [IO.Path]::GetTempFileName()
$silo = [IO.Path]::GetTempFileName()
try {
    [IO.File]::WriteAllText($seeded, "[producer] preseed treeId=t entries=1200 payloadBytes=245 attempts=1 elapsedMs=10`n$done")
    [IO.File]::WriteAllText($unseeded, $done)
    [IO.File]::WriteAllText($skipped, "[producer] preseed treeId=t skipped workloadMode=get-point preseedKeyCount=0`n$done")
    [IO.File]::WriteAllText($zero, "[producer] preseed treeId=t entries=0 payloadBytes=245 attempts=1 elapsedMs=1`n$done")
    [IO.File]::WriteAllText($silo, "[silo] preseed treeId=t entries=1200 payloadBytes=245 elapsedMs=10`n$done")

    $cases = @(
        @{ Name = 'seeded read cohort accepted'; Log = $seeded; Mode = 'get-point'; Unseeded = $false },
        @{ Name = 'read cohort without preseed refused'; Log = $unseeded; Mode = 'get-point'; Unseeded = $true },
        @{ Name = 'get-many without preseed refused'; Log = $unseeded; Mode = 'get-many'; Unseeded = $true },
        @{ Name = 'skipped preseed refused for reads'; Log = $skipped; Mode = 'get-point'; Unseeded = $true },
        @{ Name = 'zero-entry preseed refused'; Log = $zero; Mode = 'get-point'; Unseeded = $true },
        @{ Name = 'silo line is not producer evidence'; Log = $silo; Mode = 'get-point'; Unseeded = $true },
        @{ Name = 'write mode not applicable'; Log = $unseeded; Mode = 'set-point'; Unseeded = $false }
    )
    foreach ($case in $cases) {
        $cohort = New-Cohort 1000 $case.Log
        $warnings = @()
        Set-Layer3PreseedEvidence -Cohort $cohort -LogPath $case.Log -WorkloadMode $case.Mode -WarningVariable warnings -WarningAction SilentlyContinue
        Assert-Case $case.Name ($cohort.unseeded -eq $case.Unseeded)
        Assert-Case "$($case.Name) warning" (($warnings.Count -gt 0) -eq $case.Unseeded)
    }

    $cohort = New-Cohort 1000 $seeded
    Set-Layer3PreseedEvidence -Cohort $cohort -LogPath $seeded -WorkloadMode 'get-point'
    Assert-Case 'entry count recorded' ($cohort.preseedEntries -eq 1200)

    # Aggregation re-parses retained logs, so a resumed report refuses a
    # legacy unseeded cohort even when its stored state says HEALTHY.
    $cells = @{
        'get-point' = @{
            '1' = @((New-Cohort 30000 $seeded), (New-Cohort 90000 $unseeded));
            '2' = @((New-Cohort 16000 $unseeded))
        };
        'set-point' = @{ '1' = @((New-Cohort 4500 $unseeded)) }
    }
    $warnings = @()
    $rows = Aggregate-Layer3Cells -Cells $cells -WarningVariable warnings -WarningAction SilentlyContinue
    Assert-Case 'unseeded cohort excluded from median' ($rows.GetAsync['1'].sustainedThroughput -eq 30000 -and $rows.GetAsync['1'].cohortN -eq 1)
    Assert-Case 'all-unseeded cell omitted' (-not $rows.GetAsync.ContainsKey('2'))
    Assert-Case 'exclusion is announced' (@($warnings | Where-Object { "$_" -match 'UNSEEDED' }).Count -gt 0)
    Assert-Case 'write cell unaffected' ($rows.SetAsync['1'].sustainedThroughput -eq 4500)

    $cells = @{ 'get-point' = @{ '1' = @((New-Cohort 16000 $unseeded)) } }
    $rows = Aggregate-Layer3Cells -Cells $cells -WarningAction SilentlyContinue
    Assert-Case 'legacy unseeded sweep publishes no read row' (-not $rows.ContainsKey('GetAsync'))
    Write-Host "$passed assertions passed."
} finally {
    foreach ($f in @($seeded, $unseeded, $skipped, $zero, $silo)) { Remove-Item -LiteralPath $f -ErrorAction SilentlyContinue }
}
