#!/usr/bin/env pwsh
#requires -Version 7.0
<#
.SYNOPSIS
	Offline regressions for the Layer 3 storage-side metrics and the
	parallel-rig (-Layer3Rigs) sweep: Azure Monitor response parsing and
	reduction, the '[storage-metrics]' line round trip, the per-cell storage
	aggregation and table, the workload split between rigs, the rig-state
	merge, and the child-process argument rendering. Needs no Azure.

.EXAMPLE
	pwsh -File benchmark/azure-throughput/scripts/Test-Layer3StorageMetrics.ps1
#>
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

. (Join-Path $PSScriptRoot '_storage-metrics.ps1')
$report = Read-Ast (Join-Path $PSScriptRoot '..\..\performance-report.ps1')
$common = Read-Ast (Join-Path $PSScriptRoot 'aca-common.ps1')
foreach ($ast in @($report, $common)) {
	foreach ($fn in $ast.FindAll({ param($n) $n -is [System.Management.Automation.Language.FunctionDefinitionAst] }, $false)) {
		. ([scriptblock]::Create($fn.Extent.Text))
	}
}
foreach ($var in @('$Layer3Rows', '$Layer3ChartGroups')) {
	$assignment = $report.Find({ param($n) $n -is [System.Management.Automation.Language.AssignmentStatementAst] -and $n.Left.Extent.Text -eq $var }, $true)
	. ([scriptblock]::Create($assignment.Extent.Text))
}

# --- Azure Monitor response parsing -----------------------------------------
$txJson = @{
	value = @(@{
		name = @{ value = 'Transactions' }
		timeseries = @(
			@{ metadatavalues = @(@{ name = @{ value = 'responsetype' }; value = 'Success' })
			   data = @(@{ timeStamp = '2026-10-26T10:00:00Z'; total = 1200.0 }, @{ timeStamp = '2026-10-26T10:01:00Z'; total = 6000.0 }, @{ timeStamp = '2026-10-26T10:02:00Z'; total = $null }) },
			@{ metadatavalues = @(@{ name = @{ value = 'responsetype' }; value = 'ServerBusyError' })
			   data = @(@{ timeStamp = '2026-10-26T10:01:00Z'; total = 300.0 }) },
			@{ metadatavalues = @(@{ name = @{ value = 'responsetype' }; value = 'ServerTimeoutError' })
			   data = @(@{ timeStamp = '2026-10-26T10:01:00Z'; total = 6.0 }) },
			@{ metadatavalues = @(@{ name = @{ value = 'responsetype' }; value = 'ClientOtherError' })
			   data = @(@{ timeStamp = '2026-10-26T10:00:00Z'; total = 4.0 }) }
		)
	})
} | ConvertTo-Json -Depth 10
$latJson = @{
	value = @(
		@{ name = @{ value = 'SuccessServerLatency' }; timeseries = @(@{ data = @(@{ average = 4.0; maximum = 40.0 }, @{ average = 8.0; maximum = 90.0 }, @{ average = $null; maximum = $null }) }) },
		@{ name = @{ value = 'SuccessE2ELatency' }; timeseries = @(@{ data = @(@{ average = 10.0; maximum = 120.0 }, @{ average = 20.0; maximum = 250.0 }) }) }
	)
} | ConvertTo-Json -Depth 10
$m = ConvertFrom-StorageMetricsResponse -TransactionsJson $txJson -LatencyJson $latJson
Assert-Case 'transactions sum every response type' ($m.Transactions -eq 7510)
Assert-Case 'throttling classified from ServerBusyError' ($m.Throttled -eq 300 -and $m.Timeouts -eq 6 -and $m.OtherErrors -eq 4)
Assert-Case 'peak TPS is the busiest minute across all response types / 60' ($m.PeakTps -eq [math]::Round(6306 / 60.0, 1) -and $m.Minutes -eq 2)
Assert-Case 'latency means skip minutes that reported none' ($m.ServerLatencyMs -eq 6.0 -and $m.E2ELatencyMs -eq 15.0 -and $m.E2ELatencyMaxMs -eq 250.0)
$empty = ConvertFrom-StorageMetricsResponse -TransactionsJson '{"value":[]}' -LatencyJson ''
Assert-Case 'an empty response is zero transactions and unknown latency' ($empty.Transactions -eq 0 -and $null -eq $empty.ServerLatencyMs)

# --- line round trip ---------------------------------------------------------
$line = Format-StorageMetricsLine -Account 'stabc' -Role 'wal0' -Metrics $m
Assert-Case 'line carries the storage-metrics prefix and role' ($line.StartsWith('[storage-metrics] account=stabc role=wal0 '))
$noLat = ConvertFrom-StorageMetricsResponse -TransactionsJson $txJson -LatencyJson ''
$lines = @($line, (Format-StorageMetricsLine -Account 'stabcw1' -Role 'wal1' -Metrics $noLat), (Format-StorageMetricsLine -Account 'stabcw2' -Role 'wal2' -Unavailable 'query failed'), '[silo] t= 1.0s ops= 1 ops/sec= 1 inFlight= 0')
$parsed = @(Read-StorageMetricsLines -Lines $lines)
Assert-Case 'only storage-metrics lines parse' ($parsed.Count -eq 3)
Assert-Case 'figures round-trip' ($parsed[0].Transactions -eq 7510 -and $parsed[0].Throttled -eq 300 -and $parsed[0].E2ELatencyMs -eq 15.0)
Assert-Case 'n/a latency reads back as null' ($null -eq $parsed[1].ServerLatencyMs)
Assert-Case 'unavailable is explicit and space-free' ($parsed[2].Unavailable -eq 'query_failed' -and $null -eq $parsed[2].Transactions)

# --- per-cohort summary ------------------------------------------------------
$sum = Get-StorageMetricsSummary -Accounts $parsed
Assert-Case 'summary ignores the unavailable account but counts it as missing' ($sum.Accounts -eq 2 -and $sum.MissingAccounts -eq 1)
Assert-Case 'throttle ratio is across all accounts' ($sum.ThrottleRatio -eq [math]::Round(600 / 15020.0, 4))
Assert-Case 'latency is the worst account, not an average' ($sum.E2ELatencyMs -eq 15.0 -and $sum.ServerLatencyMs -eq 6.0)
Assert-Case 'no usable account is null, never an idle zero' ($null -eq (Get-StorageMetricsSummary -Accounts @($parsed[2])) -and $null -eq (Get-StorageMetricsSummary -Accounts @()))

# --- per-cell storage and the table -------------------------------------------
$cohorts = @(
	@{ storage = @{ ThrottleRatio = 0.01; PeakAccountTps = 100.0; ServerLatencyMs = 5.0; E2ELatencyMs = 9.0 } },
	@{ storage = @{ ThrottleRatio = 0.03; PeakAccountTps = 300.0; ServerLatencyMs = 7.0; E2ELatencyMs = 11.0 } },
	@{ storage = $null }
)
$cell = Get-Layer3CellStorage -Cohorts $cohorts
Assert-Case 'cell storage is the median across cohorts that carry it' ($cell.storageN -eq 2 -and $cell.throttleRatio -eq 0.02 -and $cell.peakAccountTps -eq 200.0)
Assert-Case 'a cell with no storage evidence is null' ($null -eq (Get-Layer3CellStorage -Cohorts @(@{ storage = $null })))
$rowsAgg = @{ '`SetManyAsync` (4,096 keys/call)' = @{ '1' = @{ siloCount = 1; walAccounts = 4; storage = $cell }; '2' = @{ siloCount = 2; walAccounts = 4; storage = $null } } }
$table = Render-Layer3StorageTable -RowsAgg $rowsAgg
Assert-Case 'storage table renders measured cells only' (@($table -split "`n" | Where-Object { $_ -match '^\| `SetManyAsync`' }).Count -eq 1)
Assert-Case 'storage table shows accounts, throttle percent and latency' ($table -match '\| 1 \| 4 \| 2\.00% \| 200 tx/s \| 6\.0 ms \| 10\.0 ms \|')
Assert-Case 'a sweep with no storage evidence says so instead of an empty table' ((Render-Layer3StorageTable -RowsAgg @{ '`SetManyAsync` (4,096 keys/call)' = @{ '1' = @{ siloCount = 1; storage = $null } } }) -match '^_No storage-side metrics')
Assert-Case 'storage table stays ASCII' ($table -notmatch '[^\x00-\x7F]')

# --- workload split between rigs ---------------------------------------------
$all = @($Layer3Rows | ForEach-Object { $_.WorkloadId })
$split = Split-Layer3Workloads -WorkloadIds $all -RigCount 3 -N 3 -SiloCounts @(1, 2, 4)
$flat = @($split | ForEach-Object { $_ })
Assert-Case 'split yields one list per rig' ($split.Count -eq 3)
Assert-Case 'every workload lands on exactly one rig' ($flat.Count -eq $all.Count -and @($flat | Sort-Object -Unique).Count -eq $all.Count)
Assert-Case 'no rig is empty' (@($split | Where-Object { @($_).Count -eq 0 }).Count -eq 0)
Assert-Case 'each rig keeps doc order' (@($split | Where-Object { $o = @($_); $idx = @($o | ForEach-Object { [array]::IndexOf($all, $_) }); (($idx | Sort-Object) -join ',') -ne ($idx -join ',') }).Count -eq 0)
$again = Split-Layer3Workloads -WorkloadIds $all -RigCount 3 -N 3 -SiloCounts @(1, 2, 4)
Assert-Case 'split is deterministic, so -Resume finds each workload on its rig' ((($split | ForEach-Object { @($_) -join ',' }) -join '|') -eq (($again | ForEach-Object { @($_) -join ',' }) -join '|'))
$one = Split-Layer3Workloads -WorkloadIds @('set-many') -RigCount 1 -N 3 -SiloCounts @(1)
Assert-Case 'a single rig gets everything' ($one.Count -eq 1 -and @($one[0]) -join ',' -eq 'set-many')

# --- rig-state merge ----------------------------------------------------------
function New-RigState([string] $Mode, [string] $Region = 'westus3', [int] $Accounts = 1) {
	return @{ region = $Region; batchSize = 4096; responseTimeoutSec = 420; layer3ShardCount = 64; layer3WalPartitions = 16
		layer3WalMaxPendingBatches = 16; layer3SaturationRatio = 0.9; layer3MaxRungEscalations = 3; layer3WalAccounts = $Accounts
		startedUtc = '2026-10-26T10:00:00Z'; endedUtc = '2026-10-26T12:00:00Z'
		layer3 = @{ siloCounts = @(1, 2); cohorts = @{ $Mode = @{ '1' = @(@{ verdict = 'HEALTHY'; finalThroughput = 1000; steadyMean = 1000; perCallP50Ms = 1.0; perCallP99Ms = 2.0; rungVehicles = 10; rungTickHz = 5; walAccounts = $Accounts }) } } } }
}
$merged = Merge-Layer3RigStates -States @((New-RigState 'set-many'), (New-RigState 'set-point')) -Prefixes @('ra', 'rb')
Assert-Case 'merge unions the rigs'' workloads' ($merged.layer3.cohorts.ContainsKey('set-many') -and $merged.layer3.cohorts.ContainsKey('set-point'))
Assert-Case 'merge records the rigs' ((@($merged.layer3.rigs) -join ',') -eq 'ra,rb' -and $merged.layer3.acaPrefix -eq 'ra+rb')
Assert-Case 'merge aggregates rows' ($merged.layer3.rows.Count -eq 2)
$threw = $false
try { Merge-Layer3RigStates -States @((New-RigState 'set-many'), (New-RigState 'set-many')) -Prefixes @('ra', 'rb') | Out-Null } catch { $threw = $true }
Assert-Case 'a workload measured on two rigs is refused, never blended' $threw
$threw = $false
try { Merge-Layer3RigStates -States @((New-RigState 'set-many'), (New-RigState 'set-point' 'eastus')) -Prefixes @('ra', 'rb') | Out-Null } catch { $threw = $true }
Assert-Case 'rigs in different regions are refused' $threw
$threw = $false
try { Merge-Layer3RigStates -States @((New-RigState 'set-many'), (New-RigState 'set-point' 'westus3' 4)) -Prefixes @('ra', 'rb') | Out-Null } catch { $threw = $true }
Assert-Case 'rigs with different WAL account counts are refused' $threw

$meta = New-MetaHeaderForLayer3 -State $merged -RowsAgg $merged.layer3.rows
Assert-Case 'metadata names the rigs of a parallel sweep' ($meta['rigs'] -eq 'ra,rb')
Assert-Case 'metadata states the WAL account count from state' ($meta['walAccounts'] -eq 1 -and $meta['methodology'] -match 'ONE Azure Storage account')
$merged4 = Merge-Layer3RigStates -States @((New-RigState 'set-many' 'westus3' 4)) -Prefixes @('ra')
$meta4 = New-MetaHeaderForLayer3 -State $merged4 -RowsAgg $merged4.layer3.rows
Assert-Case 'a multi-account sweep says so in its methodology' ($meta4['walAccounts'] -eq 4 -and $meta4['methodology'] -match 'across 4 Azure Storage accounts')
Assert-Case 'a single-rig sweep carries no rigs key' (-not $meta4.ContainsKey('rigs'))
$note = Render-ProvenanceNote -Layer 'layer3' -Meta $meta4
Assert-Case 'provenance note follows the account count' ($note -match 'across 4 Azure Storage accounts' -and $note -notmatch 'share one')

# --- rendering into the real document's markers -----------------------------
$docCopy = [IO.Path]::GetTempFileName()
try {
	Copy-Item (Join-Path $PSScriptRoot '..\..\..\docs\lattice\performance-multi-silo.md') $docCopy -Force
	$withStorage = New-RigState 'set-many' 'westus3' 4
	$withStorage.layer3.cohorts['set-many']['1'][0]['storage'] = @{ ThrottleRatio = 0.05; PeakAccountTps = 1500.0; ServerLatencyMs = 12.0; E2ELatencyMs = 30.0 }
	$renderState = Merge-Layer3RigStates -States @($withStorage) -Prefixes @('ra')
	Assert-Case 'the real document renders' (Update-MultiSiloDocMarkers -DocPath $docCopy -State $renderState -WarningAction SilentlyContinue)
	$rendered = [IO.File]::ReadAllText($docCopy)
	$storageBlock = [regex]::Match($rendered, '(?s)<!-- perf-storage:layer3:start.*?<!-- perf-storage:layer3:end -->').Value
	Assert-Case 'the storage block carries the measured cell' ($storageBlock -match '\| `SetManyAsync` \(4,096 keys/call\) \| 1 \| 4 \| 5\.00% \| 1,500 tx/s \| 12\.0 ms \| 30\.0 ms \|')
	Assert-Case 'exactly one storage block remains' ([regex]::Matches($rendered, 'perf-storage:layer3:start').Count -eq 1)
	Assert-Case 'the grid metadata records the account count' ($rendered -match '(?m)^\s+walAccounts=4\s*$')
} finally {
	Remove-Item -LiteralPath $docCopy -Force -ErrorAction SilentlyContinue
}

# --- child process argument rendering ---------------------------------------
Assert-Case 'int arrays render as a PowerShell list' ((ConvertTo-ChildArgumentText @(1, 2, 4)) -eq '1,2,4')
Assert-Case 'doubles render invariantly' ((ConvertTo-ChildArgumentText 0.9) -eq '0.9')
Assert-Case 'strings are single-quoted with quotes doubled' ((ConvertTo-ChildArgumentText "it's") -eq "'it''s'")
$childDir = Join-Path ([IO.Path]::GetTempPath()) ("child-pwsh-" + [guid]::NewGuid().ToString('N'))
[IO.Directory]::CreateDirectory($childDir) | Out-Null
try {
	$childScript = Join-Path $childDir 'probe.ps1'
	[IO.File]::WriteAllText($childScript, 'param([string]$Layer, [int[]]$SiloCounts, [double]$Ratio, [string]$Workloads, [switch]$SkipDocUpdate, [switch]$Resume) Write-Host "layer=$Layer counts=$($SiloCounts -join ''+'') n=$($SiloCounts.Count) ratio=$Ratio w=$Workloads skip=$SkipDocUpdate resume=$Resume"; Write-Output "out-line"; if ($Resume) { throw "child failed" }')
	$childLog = Join-Path $childDir 'child.log'
	$proc = Start-ChildPwsh -Script $childScript -Parameters ([ordered]@{ Layer = '3'; SiloCounts = @(1, 2, 4); Ratio = 0.9; Workloads = "set-many,it's"; SkipDocUpdate = $true; Resume = $false }) -LogPath $childLog
	$proc.WaitForExit()
	$childText = [IO.File]::ReadAllText($childLog)
	Assert-Case 'child process binds arrays, doubles, quoted strings and switches' ($childText -match "layer=3 counts=1\+2\+4 n=3 ratio=0\.9 w=set-many,it's skip=True resume=False")
	Assert-Case 'child process stdout lands in its log and a clean run exits 0' ($childText -match 'out-line' -and $proc.ExitCode -eq 0)
	$proc = Start-ChildPwsh -Script $childScript -Parameters ([ordered]@{ Layer = '3'; Resume = $true }) -LogPath $childLog
	$proc.WaitForExit()
	Assert-Case 'a child that throws exits 1 and logs why' ($proc.ExitCode -eq 1 -and [IO.File]::ReadAllText($childLog) -match 'child failed')
} finally {
	Remove-Item -LiteralPath $childDir -Recurse -Force -ErrorAction SilentlyContinue
}

# --- cohort storage accounts from the run context ----------------------------
$ctx = @{ storage = 'stabc'; walExtraAccounts = @([pscustomobject]@{ name = 'stabcw1'; tableUri = 'u1' }, [pscustomobject]@{ name = 'stabcw2'; tableUri = 'u2' }) }
$accts = @(Get-AcaCohortStorageAccounts -Context $ctx -WalAccounts 2)
Assert-Case 'a 2-account cohort uses the primary and the first extra' ($accts.Count -eq 2 -and $accts[0].Role -eq 'wal0' -and $accts[1].Name -eq 'stabcw1' -and $accts[1].Role -eq 'wal1')
Assert-Case 'a context without extra accounts yields the primary only' (@(Get-AcaCohortStorageAccounts -Context @{ storage = 'stabc' } -WalAccounts 1).Count -eq 1)

Write-Host "$passed assertions passed."
