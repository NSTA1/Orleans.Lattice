<#
.SYNOPSIS
	Focused A/B re-measurement for Phase C / step C4 (Azure Table WAL
	retry-budget tuning) from scaling.md.

.DESCRIPTION
	Phase A (see benchmark/diagnostic-reports/diagnostic-report-2026-05-24T07-22-03Z.md)
	showed a 5-100x gap between wall p99 (700-1,700 ms) and Azure Tables'
	server-timing p99 (10-130 ms) on `current-state-no-replication-azuretable`.
	That gap is the canonical fingerprint of an SDK retry storm whose
	retries ultimately succeed - which is the surface step C4 targets.

	This script runs the smallest A/B that quantifies C4's impact:
	`current-state-no-replication-azuretable` is exercised twice, against
	the same WAL configuration cell (the Phase A baseline: WalPartitions=1,
	WalMaxPendingBatches=1, PipelinePhaseTwoCommits=false), once with the
	Azure SDK default retry policy and once with the C4 tuning cohort
	(RetryMaxAttempts=2, RetryDelay=40 ms, RetryMaxDelay=400 ms,
	RetryNetworkTimeout=5 s). The two results.json files are folded into
	a comparison report at
	benchmark/diagnostic-reports/c4-tuning-ab-<UTC-timestamp>.md.

	The C4 tuning knobs only take effect on the Azure Table WAL provider,
	and only when the provider is constructing its own TableServiceClient
	(i.e. not host-supplied via the ServiceClient option). The bench silo
	builds its client via ConnectionString, so this measurement exercises
	the production wiring path verbatim.

.PARAMETER ReportPath
	Override the diagnostic-report path. Default is a UTC-timestamped
	file under benchmark/diagnostic-reports/.

.PARAMETER SkipBaseline
	Skip the SDK-default arm. Used to re-run the tuning arm in isolation.

.PARAMETER SkipTuned
	Skip the tuning arm. Used to re-collect the baseline.

.PARAMETER DryRun
	Print the two cells the driver would run without executing them.

.NOTES
	Wall-clock budget per arm: roughly the scenario's
	BENCH_WARMUP_SECONDS + BENCH_DURATION_SECONDS plus docker-compose
	spin-up/tear-down, so two arms is a single-digit-minute run on a
	warm docker stack.
#>
[CmdletBinding()]
param(
	[string] $ReportPath = '',
	[switch] $SkipBaseline,
	[switch] $SkipTuned,
	[switch] $DryRun
)

$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true

$repoRoot = Split-Path -Parent $PSScriptRoot
$benchRoot = $PSScriptRoot
$reportRoot = Join-Path $benchRoot 'diagnostic-reports'
$runRoot = Join-Path $benchRoot '.run'
$scenario = 'current-state-no-replication-azuretable'

if ([string]::IsNullOrWhiteSpace($ReportPath)) {
	$stamp = (Get-Date).ToUniversalTime().ToString('yyyy-MM-ddTHH-mm-ssZ')
	$ReportPath = Join-Path $reportRoot ("c4-tuning-ab-{0}.md" -f $stamp)
}

# A/B cells. Each carries the env-var stamp the bench harness picks up
# (BENCH_WAL_RETRY_* -> Lattice:Wal:Retry* in the silo's Program.cs).
$cells = @(
	[pscustomobject]@{
		Id     = 'sdk-default'
		Label  = 'SDK default retry policy (Phase A baseline)'
		Env    = @{}
		Skip   = $SkipBaseline.IsPresent
	},
	[pscustomobject]@{
		Id     = 'c4-tuned'
		Label  = 'C4 tuning cohort (MaxAttempts=2, Delay=40 ms, MaxDelay=400 ms, NetworkTimeout=5 s)'
		Env    = @{
			BENCH_WAL_RETRY_MAX_ATTEMPTS       = '2'
			BENCH_WAL_RETRY_DELAY_MS           = '40'
			BENCH_WAL_RETRY_MAX_DELAY_MS       = '400'
			BENCH_WAL_RETRY_NETWORK_TIMEOUT_MS = '5000'
		}
		Skip   = $SkipTuned.IsPresent
	}
)

# Phase A baseline cell - hold these constant for both arms so the only
# variable that moves between SDK-default and C4-tuned is the Azure SDK
# retry policy.
$baseEnv = @{
	BENCH_LATTICE_WAL_PARTITIONS         = '1'
	BENCH_LATTICE_WAL_MAX_PENDING_BATCHES = '1'
	BENCH_WAL_PIPELINE_PHASE_TWO         = 'false'
}

Write-Host "C4 A/B re-measurement" -ForegroundColor Cyan
Write-Host "  scenario:    $scenario"
Write-Host "  report path: $ReportPath"
Write-Host ""

if ($DryRun) {
	Write-Host "DryRun: would execute the following two arms:" -ForegroundColor Yellow
	foreach ($cell in $cells) {
		if ($cell.Skip) { Write-Host "  [skipped] $($cell.Id): $($cell.Label)"; continue }
		Write-Host "  [arm]     $($cell.Id): $($cell.Label)"
		$cell.Env.GetEnumerator() | Sort-Object Key | ForEach-Object {
			Write-Host ("              {0} = {1}" -f $_.Key, $_.Value)
		}
	}
	Write-Host ""
	Write-Host "  base env (held constant across both arms):"
	$baseEnv.GetEnumerator() | Sort-Object Key | ForEach-Object {
		Write-Host ("              {0} = {1}" -f $_.Key, $_.Value)
	}
	return
}

if (-not (Test-Path $reportRoot)) {
	New-Item -ItemType Directory -Path $reportRoot -Force | Out-Null
}

function Invoke-Cell {
	param(
		[Parameter(Mandatory)] $Cell,
		[Parameter(Mandatory)] [hashtable] $BaseEnv
	)

	if ($Cell.Skip) {
		Write-Host "[$($Cell.Id)] skipped" -ForegroundColor DarkGray
		return $null
	}

	Write-Host ""
	Write-Host "[$($Cell.Id)] $($Cell.Label)" -ForegroundColor Green

	# Snapshot existing env so we can restore afterwards.
	$allKeys = @($BaseEnv.Keys) + @($Cell.Env.Keys) | Select-Object -Unique
	$saved = @{}
	foreach ($k in $allKeys) {
		$saved[$k] = [Environment]::GetEnvironmentVariable($k)
	}

	try {
		# Apply base env first, then the cell-specific overlay.
		foreach ($k in $BaseEnv.Keys) {
			[Environment]::SetEnvironmentVariable($k, [string] $BaseEnv[$k])
		}
		foreach ($k in $Cell.Env.Keys) {
			[Environment]::SetEnvironmentVariable($k, [string] $Cell.Env[$k])
		}

		# Clear any C4 vars that are NOT in the cell, so a previous arm
		# cannot leak into the next one through process env.
		$allC4Keys = @(
			'BENCH_WAL_RETRY_MAX_ATTEMPTS',
			'BENCH_WAL_RETRY_DELAY_MS',
			'BENCH_WAL_RETRY_MAX_DELAY_MS',
			'BENCH_WAL_RETRY_NETWORK_TIMEOUT_MS'
		)
		foreach ($k in $allC4Keys) {
			if (-not $Cell.Env.ContainsKey($k)) {
				[Environment]::SetEnvironmentVariable($k, $null)
				$saved[$k] = $saved[$k]  # keep the snapshot entry
			}
		}

		& (Join-Path $benchRoot 'benchmark.ps1') -Scenario $scenario
	}
	finally {
		# Restore the snapshot so subsequent arms / scripts are not polluted.
		foreach ($k in $saved.Keys) {
			[Environment]::SetEnvironmentVariable($k, $saved[$k])
		}
	}

	# Locate the most recent results.json for this scenario.
	$scenarioRun = Join-Path $runRoot $scenario
	if (-not (Test-Path $scenarioRun)) {
		Write-Warning "[$($Cell.Id)] no results.json under $scenarioRun"
		return $null
	}
	$latest = Get-ChildItem -Path $scenarioRun -Recurse -Filter results.json |
			  Sort-Object LastWriteTime -Descending |
			  Select-Object -First 1
	if (-not $latest) {
		Write-Warning "[$($Cell.Id)] no results.json discovered after run"
		return $null
	}

	Write-Host "[$($Cell.Id)] results.json: $($latest.FullName)" -ForegroundColor DarkGray
	return [pscustomobject]@{
		Cell    = $Cell
		Results = (Get-Content -Raw $latest.FullName | ConvertFrom-Json)
		Path    = $latest.FullName
	}
}

$collected = @()
foreach ($cell in $cells) {
	$r = Invoke-Cell -Cell $cell -BaseEnv $baseEnv
	if ($null -ne $r) { $collected += $r }
}

if ($collected.Count -eq 0) {
	Write-Warning "No cells produced a results.json. Report skipped."
	return
}

# ─── Report ─────────────────────────────────────────────────────────────────────
function Get-Metric {
	param($Results, [string] $Key, $Default = $null)
	$node = $Results
	foreach ($segment in ($Key -split '\.')) {
		if ($null -eq $node) { return $Default }
		$node = $node.$segment
	}
	if ($null -eq $node) { return $Default }
	return $node
}

$sb = [System.Text.StringBuilder]::new()
$null = $sb.AppendLine("# C4 retry-budget tuning A/B re-measurement")
$null = $sb.AppendLine("")
$null = $sb.AppendLine("Generated: $((Get-Date).ToUniversalTime().ToString('o'))")
$null = $sb.AppendLine("Scenario: ``$scenario``")
$null = $sb.AppendLine("")
$null = $sb.AppendLine("Phase A (see ``benchmark/diagnostic-reports/diagnostic-report-2026-05-24T07-22-03Z.md``) attributed the WAL hot-path wall-vs-server tail-latency gap on this scenario to Azure SDK retry/backoff cost. Step C4 from ``scaling.md`` adds bounded retry-budget knobs on ``AzureTableWalStorageOptions``. This report compares the SDK-default arm with the C4 tuning cohort, holding the WAL configuration cell constant (``WalPartitions=1, WalMaxPendingBatches=1, PipelinePhaseTwoCommits=false``).")
$null = $sb.AppendLine("")
$null = $sb.AppendLine("## Cells")
$null = $sb.AppendLine("")
$null = $sb.AppendLine("| Cell | Description |")
$null = $sb.AppendLine("|---|---|")
foreach ($r in $collected) {
	$null = $sb.AppendLine("| ``$($r.Cell.Id)`` | $($r.Cell.Label) |")
}
$null = $sb.AppendLine("")

$null = $sb.AppendLine("## Headline metrics")
$null = $sb.AppendLine("")
$null = $sb.AppendLine("| Cell | Throughput (ops/s) | Commit p50 (ms) | Commit p99 (ms) | Server-timing p99 (ms) | Retry attempts | Retry exhausted |")
$null = $sb.AppendLine("|---|---:|---:|---:|---:|---:|---:|")
foreach ($r in $collected) {
	$tp     = Get-Metric $r.Results 'metrics.throughput_ops_per_sec' '-'
	$p50    = Get-Metric $r.Results 'metrics.commit_p50_ms'          '-'
	$p99    = Get-Metric $r.Results 'metrics.commit_p99_ms'          '-'
	$serv99 = Get-Metric $r.Results 'metrics.azure_table_server_p99_ms' '-'
	$rAtt   = Get-Metric $r.Results 'metrics.provider_retry_attempts' '-'
	$rEx    = Get-Metric $r.Results 'metrics.provider_retry_exhausted' '-'
	$null = $sb.AppendLine("| ``$($r.Cell.Id)`` | $tp | $p50 | $p99 | $serv99 | $rAtt | $rEx |")
}
$null = $sb.AppendLine("")
$null = $sb.AppendLine("> The retry-attempts / retry-exhausted columns are sourced from the ``orleans.lattice.provider.retry.attempts`` and ``orleans.lattice.provider.retry.exhausted`` counters. A dash means the metric was not exposed in this run (legacy exporter or absent results.json key).")
$null = $sb.AppendLine("")

$null = $sb.AppendLine("## Raw results.json paths")
$null = $sb.AppendLine("")
foreach ($r in $collected) {
	$null = $sb.AppendLine("- ``$($r.Cell.Id)``: ``$($r.Path)``")
}
$null = $sb.AppendLine("")

$null = $sb.AppendLine("## Interpretation guide")
$null = $sb.AppendLine("")
$null = $sb.AppendLine("- If ``c4-tuned`` shows lower commit p99 with similar or higher throughput, C4's bounded retry budget is doing its job: long retry trains are being cut short and the per-call deadline budget (``RetryNetworkTimeout=5 s``) is preventing a stuck request from parking a WAL slot for the full SDK default of 100 s.")
$null = $sb.AppendLine("- If ``c4-tuned`` shows higher ``provider_retry_exhausted`` than ``sdk-default``, the retry budget is too tight for the cluster's transient-fault rate. Loosen ``RetryMaxAttempts`` or ``RetryMaxDelay`` and re-measure.")
$null = $sb.AppendLine("- If throughput collapses, the C4 cohort is starving genuine 503/429 retries; widen the budget.")

$dir = Split-Path -Parent $ReportPath
if (-not (Test-Path $dir)) { New-Item -ItemType Directory -Path $dir -Force | Out-Null }
[System.IO.File]::WriteAllText((Resolve-Path -LiteralPath $dir).Path + [System.IO.Path]::DirectorySeparatorChar + (Split-Path -Leaf $ReportPath), $sb.ToString())

Write-Host ""
Write-Host "Report written: $ReportPath" -ForegroundColor Cyan
