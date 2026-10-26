<#
.SYNOPSIS
	Complete a Layer 3 cohort: harvest its producer and silo logs from Log
	Analytics, append its storage-side metrics, grade it, and write the
	cohort log.

.DESCRIPTION
	The second half of run-cohort-aca.ps1, split out so it can run later than
	the cohort itself. run-cohort-aca.ps1 writes a pending-harvest descriptor
	(<cohort log>.pending.json) once the producer job has finished and the
	silos are parked, then either invokes this script immediately or, with
	-DeferHarvest, returns and leaves it to the caller - performance-report.ps1
	-Layer3OverlapHarvest runs it in a background job while the next cohort
	runs. Everything read here is pinned by the descriptor (the job execution
	name, the silo revision, the cohort window), so when it runs does not
	change what it harvests.

	Returns the same object run-cohort-aca.ps1 always returned, and deletes
	the descriptor once the cohort log is complete.

.PARAMETER PendingHarvestPath
	The descriptor run-cohort-aca.ps1 wrote.
#>
[CmdletBinding()]
param(
	[Parameter(Mandatory)][string] $PendingHarvestPath
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

. (Join-Path $PSScriptRoot 'aca-common.ps1')
# Format-CohortVerdictLogBlock: the verdict block Layer 3 appends must be
# byte-identical to Layer 2's, because the same Read-SiloLogStats parses both.
. (Join-Path $PSScriptRoot '_run-cohort-helpers.ps1')

if (-not (Test-Path $PendingHarvestPath)) { throw "No pending harvest at $PendingHarvestPath" }
$pending = Get-Content -Path $PendingHarvestPath -Raw | ConvertFrom-Json

$NamePrefix     = [string]$pending.namePrefix
$SiloCount      = [int]$pending.siloCount
$WorkloadMode   = [string]$pending.workloadMode
$DurationSec    = [int]$pending.durationSec
$TreeId         = [string]$pending.treeId
$execName       = [string]$pending.executionName
$state          = [string]$pending.executionState
$siloRevision   = [string]$pending.siloRevision
$logPath        = [string]$pending.logPath
$FreezeWedgeSec = [int]$pending.freezeWedgeSec
$walAccounts    = [int]$pending.walAccounts
$startedUtc     = [datetime]::Parse([string]$pending.startedUtc, [System.Globalization.CultureInfo]::InvariantCulture, [System.Globalization.DateTimeStyles]::RoundtripKind).ToUniversalTime()
$producerEndUtc = [datetime]::Parse([string]$pending.producerEndUtc, [System.Globalization.CultureInfo]::InvariantCulture, [System.Globalization.DateTimeStyles]::RoundtripKind).ToUniversalTime()
$ctx = Read-AcaContext -NamePrefix $NamePrefix

# A failed producer never prints its DONE marker, so the harvest would
# poll to its full deadline waiting for a line that cannot arrive - with
# N silos billing the whole time. Across an unattended sweep that is the
# difference between a cell costing minutes and costing the harvest
# timeout. Shorten the wait to a single ingestion-lag allowance, harvest
# whatever diagnostics did land (the stack trace is the reason the cell
# failed and is worth keeping), and move on.
$harvestWait = if ($state -eq 'Succeeded') { 420 } else { 120 }
if ($state -ne 'Succeeded') {
	Write-Warning "[cohort] execution state '$state' is not Succeeded; harvesting diagnostics only (no DONE marker will arrive)."
}

$lines = Get-AcaJobLog -Context $ctx -ExecutionName $execName -SinceUtc $startedUtc -TreeId $TreeId -MaxWaitSec $harvestWait
$siloLines = Get-AcaSiloLog -Context $ctx -SinceUtc $startedUtc -UntilUtc ((Get-Date).ToUniversalTime()) -RevisionName $siloRevision -IncludeDiagnostics

# Sort the silo half by its `t=` timestamp so the cohort log reads in run
# order. Read-SiloLogStats merges every [phaseA] window of every silo, so
# the order no longer changes a published figure; Log Analytics returns
# rows ordered by ingestion-stamped TimeGenerated across N replicas, which
# interleaves the replicas arbitrarily.
$siloLines = @($siloLines | Sort-Object -Property @{ Expression = {
	if ($_ -match 't=\s*([\d.]+)s') { [double]$Matches[1] } else { [double]::MaxValue }
} })

# One combined log per cohort, producer first. Read-SiloLogStats reads
# throughput from the producer's `[silo] t=` lines and per-call quantiles
# from the silos' `[phaseA]` lines, and it is the *same* function Layer 2
# uses, so the two layers cannot drift apart on what a cell means.
Set-Content -Path $logPath -Value (@($lines) + @($siloLines)) -Encoding utf8
Write-Host "[cohort] harvested $($lines.Count) producer + $($siloLines.Count) silo line(s) -> $logPath" -ForegroundColor Green

# Grade the cohort and append the verdict block the report parser reads.
#
# Layer 2's Resolve-CohortVerdict is deliberately NOT reused here, and
# the reason is a real difference in what the two layers measure rather
# than a convenience. Layer 2 hand-tunes each workload's rung to sit
# *below* the single-account ceiling, so any failure there is genuinely
# anomalous and Resolve-CohortVerdict rightly grades FailedFinal>0 as
# FAILED. Layer 3 does the opposite on purpose: it offers rung x N in
# order to find where the cluster stops converting extra silos into
# extra throughput, so the interesting cells are the saturated ones.
# Applying Layer 2's rule would exclude exactly the cells that locate
# the knee, and the curve would terminate at the last unsaturated N.
#
# So Layer 3 grades on the one shape that is genuinely unusable - the
# silent zero, where nothing completes and nothing fails because every
# call is parked past the end of the measurement window. A cell that
# delivered real throughput is HEALTHY and its failure count is carried
# as data (Read-SiloLogStats already extracts it), not as grounds for
# exclusion.
#
# The one saturated-looking shape that is NOT kept HEALTHY is a mid-run
# freeze: the cell delivers throughput, then the cumulative ops counter
# stops advancing while in-flight stays pinned, because every call is
# parked and none completes. That is a wedge that happened to start after
# some work had landed, and grading it HEALTHY lets its failure count read
# as saturation (#3475: 128 in-flight sagas frozen for ~400 s, 70% failed,
# graded HEALTHY).
$producerDone = @($lines | Where-Object { $_.Contains('[producer] DONE') }).Count -gt 0
$productive = @($lines | Where-Object {
	$_.Contains('[silo] t=') -and ($_ -match 'ops/sec=\s*([\d,]+)') -and ([long]($Matches[1] -replace ',','') -gt 0)
}).Count
$longestFreeze = Get-SiloProgressLongestStall -Lines @($lines)
# Flushes still unsettled when FINAL was emitted (the in-flight-tail
# budget expired). They are in neither ops= nor failed=, so a cell that
# abandoned work used to read failed=0 and grade HEALTHY.
$unresolvedAtFinal = 0L
$finalLine = @($lines | Where-Object { $_.Contains('[silo] FINAL ') }) | Select-Object -Last 1
if ($finalLine -and ($finalLine -match '\bunresolved=([\d,]+)')) {
	$unresolvedAtFinal = [long]($Matches[1] -replace ',','')
}

# A '[producer] WARN wal-placement read' line means the moves landed and only
# the read-back failed, which still confirms the spread.
$placementConfirmed = @($lines | Where-Object {
	$_ -and ($_.Contains("[producer] wal-placement treeId=$TreeId accounts=$walAccounts ") -or $_.Contains("[producer] WARN wal-placement read treeId=$TreeId"))
}).Count -gt 0
$verdictState = 'HEALTHY'
$verdictDetail = ''
if (-not $producerDone) {
	$verdictState = 'WEDGE'
	$verdictDetail = ' (producer emitted no DONE marker)'
} elseif ($productive -eq 0) {
	$verdictState = 'WEDGE'
	$verdictDetail = ' (no productive window: every call outlived the measurement window)'
} elseif ($FreezeWedgeSec -gt 0 -and $longestFreeze -ge $FreezeWedgeSec) {
	$verdictState = 'WEDGE'
	$verdictDetail = (' (mid-run freeze: no completion for {0:N0}s with work in flight)' -f $longestFreeze)
} elseif ($unresolvedAtFinal -gt 0) {
	$verdictState = 'WEDGE'
	$verdictDetail = (' (unresolved tail: {0:N0} entries still in flight when the in-flight-tail budget expired)' -f $unresolvedAtFinal)
} elseif ($walAccounts -gt 1 -and -not $placementConfirmed) {
	# The producer spreads the WAL before load and throws if the spread fails,
	# so this only fires on a stale producer image that never attempted it. A
	# multi-account cell measured on one account must not publish as one.
	$verdictState = 'WEDGE'
	$verdictDetail = " (WAL placement across $walAccounts accounts not confirmed: no '[producer] wal-placement' line)"
}

# Storage-side telemetry for every account the cohort used, appended as
# '[storage-metrics]' lines that performance-report.ps1 aggregates per cell.
# Never part of the grade: an unreadable account is recorded 'unavailable'.
$storageLines = @(foreach ($acct in @($pending.storageAccounts)) {
	Get-AcaStorageMetricsLine -Context $ctx -StorageAccount ([string]$acct.name) -Role ([string]$acct.role) -SinceUtc $startedUtc -UntilUtc $producerEndUtc
})
if ($storageLines.Count -gt 0) {
	Add-Content -Path $logPath -Encoding utf8 -Value $storageLines
	foreach ($l in $storageLines) { Write-Host "[cohort] $l" -ForegroundColor DarkGray }
}
Add-Content -Path $logPath -Encoding utf8 -Value (Format-CohortVerdictLogBlock `
	-VerdictState $verdictState -VerdictDetail $verdictDetail -DrainTailSamples 0)
$verdictColour = if ($verdictState -eq 'HEALTHY') { 'Green' } else { 'Red' }
Write-Host "[cohort] verdict=$verdictState productiveWindows=$productive longestFreezeSec=$([Math]::Round($longestFreeze,1))" -ForegroundColor $verdictColour

Remove-Item -Path $PendingHarvestPath -Force -ErrorAction SilentlyContinue

[pscustomobject]@{
	NamePrefix    = $NamePrefix
	SiloCount     = $SiloCount
	WorkloadMode  = $WorkloadMode
	TreeId        = $TreeId
	DurationSec   = $DurationSec
	ExecutionName = $execName
	ExecutionState= $state
	LogPath       = $logPath
	LineCount     = $lines.Count + $siloLines.Count
	WalAccounts   = $walAccounts
}
