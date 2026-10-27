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
$getManyPlain = $getMany.Replace('`', '')
$rowsAgg[$getMany] = New-Cells @(1, 2, 4) 100000

$out = Render-Layer3Chart -RowsAgg $rowsAgg
$speedupName = 'performance-layer3-speedup.svg'
Assert-Case 'every chart is an image reference' ($out.Markdown -match [regex]::Escape("](images/$speedupName)"))
Assert-Case 'one SVG per chart' ($out.Files.Count -eq 4)
$speedup = [xml]$out.Files[$speedupName]
$ns = @{ s = 'http://www.w3.org/2000/svg' }
$xTicks = @(Select-Xml -Xml $speedup -XPath "//s:text[@class='x-tick']" -Namespace $ns | ForEach-Object { $_.Node.InnerText })
$labels = @(Select-Xml -Xml $speedup -XPath "//s:text[@class='legend']" -Namespace $ns | ForEach-Object { $_.Node.InnerText })
Assert-Case 'speedup chart keeps every silo count' (($xTicks -join ',') -eq '1,2,4,6,8')
$xPos = @(Select-Xml -Xml $speedup -XPath "//s:text[@class='x-tick']" -Namespace $ns | ForEach-Object { $_.Node.GetAttribute('x') })
$xNum = @($xPos | Where-Object { $_ -match '^\d+(\.\d+)?$' } | ForEach-Object { [double]$_ })
Assert-Case 'x ticks sit at numeric, increasing positions' ($xNum.Count -eq 5 -and (@(1..4 | Where-Object { $xNum[$_] -le $xNum[$_ - 1] }).Count -eq 0))
$points = @(Select-Xml -Xml $speedup -XPath '//s:polyline' -Namespace $ns | ForEach-Object { $_.Node.GetAttribute('points') -split ' ' })
Assert-Case 'every line point is a numeric coordinate pair' ($points.Count -gt 0 -and @($points | Where-Object { $_ -notmatch '^\d+(\.\d+)?,-?\d+(\.\d+)?$' }).Count -eq 0)
Assert-Case 'speedup chart drops the workload missing cells' ($getManyPlain -notin $labels)
Assert-Case 'speedup legend names the reference and every drawn workload' (@($labels | Where-Object { $_ -like 'Ideal:*' }).Count -eq 1 -and (@($Layer3Rows | Where-Object { $_.Label.Replace('`', '') -in $labels }).Count -eq ($Layer3Rows.Count - 1)))
Assert-Case 'one line per series' (@(Select-Xml -Xml $speedup -XPath '//s:polyline' -Namespace $ns).Count -eq $Layer3Rows.Count)
Assert-Case 'a dropped workload is named' ($out.Markdown -match ('Not drawn, because it was not measured at every silo count above \(its cells are in the table\): ' + [regex]::Escape($getMany)))
$nonAscii = @($out.Files.Values | Where-Object { $_ -match '[^\x00-\x7F]' })
Assert-Case 'SVGs are ASCII' ($nonAscii.Count -eq 0)
$parsed = @($out.Files.Values | ForEach-Object { try { [void][xml]$_; $true } catch { $false } })
Assert-Case 'every SVG is well-formed XML' (@($parsed | Where-Object { -not $_ }).Count -eq 0)
$again = Render-Layer3Chart -RowsAgg $rowsAgg
Assert-Case 'rendering is deterministic' (@($out.Files.Keys | Where-Object { $out.Files[$_] -ne $again.Files[$_] }).Count -eq 0)

$complete = @{}
foreach ($row in $Layer3Rows) { $complete[$row.Label] = New-Cells $all 1000 }
$clean = Render-Layer3Chart -RowsAgg $complete
Assert-Case 'a complete sweep names nothing as dropped' (-not ($clean.Markdown -match 'Not drawn'))

$sparse = @{}
foreach ($row in $Layer3Rows) { $sparse[$row.Label] = New-Cells @(1, 2, 4) 1000 }
$sparse[$getMany] = New-Cells @(1, 2, 6) 1000
$fallback = Render-Layer3Chart -RowsAgg $sparse
$fbTicks = @(Select-Xml -Xml ([xml]$fallback.Files[$speedupName]) -XPath "//s:text[@class='x-tick']" -Namespace $ns | ForEach-Object { $_.Node.InnerText })
Assert-Case 'falls back to the shared counts when no row covers the union' (($fbTicks -join ',') -eq '1,2')

if ($failures -gt 0) { throw "$failures Layer 3 chart assertion(s) failed." }
Write-Host 'All Layer 3 chart assertions passed.'