<#
.SYNOPSIS
	Offline regression for performance-report.ps1's Layer 3 scaling charts.
.DESCRIPTION
	Mermaid's xychart-beta has one shared x-axis and no null, so a workload
	missing a cell cannot be plotted with a gap. Render-Layer3Chart once
	responded by shrinking the axis to the silo counts every series shared,
	so one workload without 6- and 8-silo cells cut the speedup chart off at
	four silos for all nine. It must drop that workload from the chart instead,
	name it, and keep the full axis. Loads only the report's definitions, so it
	needs no Azure.
#>
[CmdletBinding()] param()
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$path = Join-Path $PSScriptRoot '..\..\performance-report.ps1'
$ast = [System.Management.Automation.Language.Parser]::ParseFile((Resolve-Path $path), [ref]$null, [ref]$null)
foreach ($function in $ast.FindAll({ param($n) $n -is [System.Management.Automation.Language.FunctionDefinitionAst] }, $false)) {
    . ([scriptblock]::Create($function.Extent.Text))
}
foreach ($name in '$Layer3Rows', '$Layer3ChartGroups') {
    $assign = $ast.FindAll({ param($n) $n -is [System.Management.Automation.Language.AssignmentStatementAst] -and $n.Left.Extent.Text -eq $name }, $false) | Select-Object -First 1
    if (-not $assign) { throw "performance-report.ps1 no longer assigns $name" }
    . ([scriptblock]::Create($assign.Extent.Text))
}

$failures = 0
function Assert-Case([string] $Name, [bool] $Condition) {
    if ($Condition) { Write-Host "  ok   $Name" } else { Write-Host "  FAIL $Name" -ForegroundColor Red; $script:failures++ }
}
function New-Cells([int[]] $Counts, [double] $Base) {
    $cells = @{}
    foreach ($n in $Counts) { $cells["$n"] = @{ sustainedThroughput = $Base * $n; speedup = [double]$n; producerBound = $false } }
    return $cells
}

$all = @(1, 2, 4, 6, 8)
$rowsAgg = @{}
foreach ($row in $Layer3Rows) { $rowsAgg[$row.Label] = New-Cells $all 1000 }
$getMany = ($Layer3Rows | Where-Object { $_.WorkloadMode -eq 'get-many' }).Label
$rowsAgg[$getMany] = New-Cells @(1, 2, 4) 100000

$out = Render-Layer3Chart -RowsAgg $rowsAgg
$speedup = ($out -split '```mermaid')[1]
Assert-Case 'speedup chart keeps every silo count' ($speedup -match 'x-axis "Silos" \["1", "2", "4", "6", "8"\]')
$speedupOrder = ($speedup -split "?
" | Where-Object { $_ -like 'Series order*' } | Select-Object -First 1)
Assert-Case 'speedup chart drops the workload missing cells' ($speedupOrder -and -not ($speedupOrder -match [regex]::Escape($getMany)))
Assert-Case 'a dropped workload is named' ($out -match ('Not drawn, because it was not measured at every silo count above \(its cells are in the table\): ' + [regex]::Escape($getMany)))
Assert-Case 'no chart is truncated to the shared counts' (-not ($out -match 'x-axis "Silos" \["1", "2", "4"\]'))

$complete = @{}
foreach ($row in $Layer3Rows) { $complete[$row.Label] = New-Cells $all 1000 }
$clean = Render-Layer3Chart -RowsAgg $complete
Assert-Case 'a complete sweep names nothing as dropped' (-not ($clean -match 'Not drawn'))

$sparse = @{}
foreach ($row in $Layer3Rows) { $sparse[$row.Label] = New-Cells @(1, 2, 4) 1000 }
$sparse[$getMany] = New-Cells @(1, 2, 6) 1000
$fallback = Render-Layer3Chart -RowsAgg $sparse
Assert-Case 'falls back to the shared counts when no row covers the union' ($fallback -match 'x-axis "Silos" \["1", "2"\]')

if ($failures -gt 0) { throw "$failures Layer 3 chart assertion(s) failed." }
Write-Host 'All Layer 3 chart assertions passed.'
