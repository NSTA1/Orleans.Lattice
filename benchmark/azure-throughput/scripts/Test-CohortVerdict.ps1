#!/usr/bin/env pwsh
<#
.SYNOPSIS
	Regression tests for run-cohort.ps1's verdict-computation helpers.

.DESCRIPTION
	Exercises the predicates in _run-cohort-helpers.ps1 against literal
	log-line fixtures so the runner's verdict accuracy can be verified
	without provisioning an Azure VM. Pure pwsh; no Pester dependency.

	Exits with code 0 when every assertion passes and a non-zero count
	(equal to the number of failed assertions) when one or more fails,
	so it composes cleanly into any CI step or developer pre-PR loop:

		pwsh -File ./Test-CohortVerdict.ps1
		if ($LASTEXITCODE -ne 0) { throw "cohort-verdict tests failed" }

.EXAMPLE
	cd benchmark/azure-throughput/scripts
	pwsh -File Test-CohortVerdict.ps1
#>
[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$here = Split-Path -Parent $MyInvocation.MyCommand.Path
. (Join-Path $here '_run-cohort-helpers.ps1')

# Minimalist assertion harness. Each call increments either the pass
# or fail counter and emits one line of evidence so a failed run shows
# the operator exactly which case regressed.
$script:_PassCount = 0
$script:_FailCount = 0

function _Assert {
	param(
		[Parameter(Mandatory)] [string] $Name,
		[Parameter(Mandatory)] [bool]   $Condition,
		[string] $Detail = ''
	)
	if ($Condition) {
		$script:_PassCount++
		Write-Host ("  PASS  {0}" -f $Name) -ForegroundColor Green
	} else {
		$script:_FailCount++
		Write-Host ("  FAIL  {0}  {1}" -f $Name, $Detail) -ForegroundColor Red
	}
}

# Fixture: realistic cohort tree ids in the shape emitted by
# run-cohort.ps1 line 142 (`cohort-v<vehicles>-h<tickHz>-<duration>s-<utcStamp>`).
$currentTreeId = 'cohort-v4000-h5-45s-20260608093604Z'
$priorTreeId   = 'cohort-v4000-h5-45s-20260608092345Z'

Write-Host 'Test-CohortLineAttributable' -ForegroundColor Cyan

# Acceptance criteria #1 + #3: an injected cross-cohort exception line
# must NOT count toward the current cohort's tally. Real-world shape:
# the silo's grain-activation failure surfaces with the wedged grain's
# tree id in the Activation path, and the inner exception type is
# concatenated into the same log message.
$crossCohortLine = "09:36:32 fail: Orleans.Grain[23568684] Error thrown from OnActivateAsync for activation '[Activation: ...walshard/$priorTreeId/6@...]' TableTransactionFailedException 409 Conflict"
_Assert -Name 'cross-cohort line is NOT attributable' `
	-Condition (-not (Test-CohortLineAttributable -Line $crossCohortLine -CurrentTreeId $currentTreeId)) `
	-Detail "line referenced prior cohort '$priorTreeId' but was attributed to '$currentTreeId'"

# Symmetric: a current-cohort exception line MUST count.
$currentCohortLine = "09:36:32 fail: Orleans.Grain[23568684] Error thrown from OnActivateAsync for activation '[Activation: ...walshard/$currentTreeId/6@...]' TableTransactionFailedException 409 Conflict"
_Assert -Name 'current-cohort line IS attributable' `
	-Condition (Test-CohortLineAttributable -Line $currentCohortLine -CurrentTreeId $currentTreeId) `
	-Detail "line referenced current cohort '$currentTreeId' but was excluded"

# Defensive default: a silo-wide exception with no cohort token at all
# (e.g. config-load failures, startup faults) must count - it could be
# a genuine current-cohort regression we just can't tag by id.
$silosWideLine = '09:36:32 fail: Orleans.Hosting Silo failed to load options TableTransactionFailedException at startup'
_Assert -Name 'silo-wide line with no cohort token IS attributable' `
	-Condition (Test-CohortLineAttributable -Line $silosWideLine -CurrentTreeId $currentTreeId) `
	-Detail 'silo-wide line without any cohort id should not be silently suppressed'

# Edge case: a `warn:` LatticeWalUsageGrain line that names the prior
# tree by id in the body (matches the issue's observed wedge pattern).
$prevPollerWarn = "09:36:32 warn: Orleans.Lattice.BPlusTree.Grains.LatticeWalUsageGrain[0] WAL retained-byte fan-out failed for partition 6 in tree $priorTreeId TableTransactionFailedException"
_Assert -Name 'prior-tree poller warn line is NOT attributable' `
	-Condition (-not (Test-CohortLineAttributable -Line $prevPollerWarn -CurrentTreeId $currentTreeId)) `
	-Detail 'WAL retained-byte fan-out warn for prior tree was counted toward current cohort'

# Edge case: a line with BOTH a current-cohort token and a prior-cohort
# token (rare but possible in multi-tree exception messages). The
# permissive rule says "if current tree id appears anywhere, count it".
$mixedLine = "09:36:32 fail: tree $priorTreeId references current tree $currentTreeId Exception"
_Assert -Name 'mixed-cohort line IS attributable when current id appears' `
	-Condition (Test-CohortLineAttributable -Line $mixedLine -CurrentTreeId $currentTreeId) `
	-Detail 'line that contains the current tree id should always count, even alongside other ids'

Write-Host ''
Write-Host 'Get-CohortExceptionCount' -ForegroundColor Cyan

# Build a small fixture log on disk in a temp file and assert the
# end-to-end pipeline (file reader -> filter -> counts). Every line in
# this fixture contains the substring 'Exception' so the raw count is
# unambiguous (the upstream filter is a SimpleMatch on 'Exception';
# lines without that keyword never enter the pipeline regardless of
# tree-id attribution).
$tempLog = Join-Path ([System.IO.Path]::GetTempPath()) ("cohort-verdict-test-" + [Guid]::NewGuid().ToString('N') + '.log')
try {
	$lines = @(
		$crossCohortLine,           # excluded (prior tree)
		$currentCohortLine,         # counted (current tree)
		$prevPollerWarn,            # excluded (prior tree)
		$silosWideLine,             # counted (no cohort token)
		'09:36:32 info: clean line with no Exception keyword'  # excluded (matches via 'Exception' substring; classified as no-cohort -> attributable)
	) -join "`n"
	[System.IO.File]::WriteAllText($tempLog, $lines)

	$result = Get-CohortExceptionCount -LogPath $tempLog -CurrentTreeId $currentTreeId
	# Raw = every line containing 'Exception'. The 5th fixture line
	# also contains the substring 'Exception' (in the phrase "no
	# Exception keyword"), so all 5 lines match the raw filter. That's
	# accepted noise; the tree-id filter is what fixes the actual bug.
	_Assert -Name 'Raw count includes every line matching Exception' `
		-Condition ($result.Raw -eq 5) `
		-Detail "expected raw=5 (every fixture line contains 'Exception'), got raw=$($result.Raw)"
	# Filtered = current-cohort line + silo-wide line + the no-cohort
	# info line (which has no cohort id and so falls under the
	# permissive default). Excluded = the two prior-cohort lines.
	_Assert -Name 'Filtered count drops cross-cohort lines' `
		-Condition ($result.Filtered -eq 3) `
		-Detail "expected filtered=3 (current + silo-wide + info), got filtered=$($result.Filtered)"
	_Assert -Name 'Excluded count equals Raw - Filtered' `
		-Condition ($result.Excluded -eq ($result.Raw - $result.Filtered)) `
		-Detail "expected excluded=$($result.Raw - $result.Filtered), got excluded=$($result.Excluded)"
	_Assert -Name 'Excluded count equals the two cross-cohort lines' `
		-Condition ($result.Excluded -eq 2) `
		-Detail "expected excluded=2 (cross-cohort + prior poller warn), got excluded=$($result.Excluded)"

	# Defensive fallback: non-cohort-shaped tree id (e.g. ad-hoc
	# 'r25k-001') disables the filter so we never silently suppress
	# signals we can't classify.
	$adHoc = Get-CohortExceptionCount -LogPath $tempLog -CurrentTreeId 'r25k-001'
	_Assert -Name 'non-cohort-shaped tree id falls back to raw count' `
		-Condition ($adHoc.Filtered -eq $adHoc.Raw -and $adHoc.Excluded -eq 0) `
		-Detail "expected filtered==raw and excluded=0, got filtered=$($adHoc.Filtered) raw=$($adHoc.Raw) excluded=$($adHoc.Excluded)"

	# Missing file: should return zeros, not throw.
	$missing = Get-CohortExceptionCount -LogPath (Join-Path ([System.IO.Path]::GetTempPath()) ('does-not-exist-' + [Guid]::NewGuid().ToString('N') + '.log')) -CurrentTreeId $currentTreeId
	_Assert -Name 'missing log returns zeros without throwing' `
		-Condition ($missing.Raw -eq 0 -and $missing.Filtered -eq 0 -and $missing.Excluded -eq 0) `
		-Detail "expected all-zero result, got raw=$($missing.Raw) filtered=$($missing.Filtered) excluded=$($missing.Excluded)"
}
finally {
	if (Test-Path -LiteralPath $tempLog) { Remove-Item -LiteralPath $tempLog -Force -ErrorAction SilentlyContinue }
}

Write-Host ''
Write-Host 'Format-CohortVerdictLogBlock' -ForegroundColor Cyan

# The consumer contract: performance-report.ps1's Read-SiloLogStats
# recovers the per-cohort verdict from the extracted silo log with this
# exact pattern. Kept verbatim here so a drift on either side trips a
# test rather than silently regressing the report's HEALTHY-only
# aggregation (the original bug: the verdict was never written to the
# silo log, so this parse always returned empty and every cohort was
# excluded from the medians).
$consumerVerdictPattern = '^Verdict\s*:\s*([A-Z]+)'

foreach ($state in @('HEALTHY','DEGRADED','FAILED','WEDGE')) {
	$detail = if ($state -eq 'WEDGE') { ' (15-sample drain tail)' } else { '' }
	$block  = Format-CohortVerdictLogBlock -VerdictState $state -VerdictDetail $detail -DrainTailSamples 15
	$verdictLine = @($block | Where-Object { $_ -match $consumerVerdictPattern })
	$captured = if ($verdictLine.Count -eq 1 -and $verdictLine[0] -match $consumerVerdictPattern) { $Matches[1] } else { '' }
	_Assert -Name "verdict block round-trips '$state' through the consumer regex" `
		-Condition ($verdictLine.Count -eq 1 -and $captured -eq $state) `
		-Detail "expected exactly one Verdict line capturing '$state', got $($verdictLine.Count) line(s) capturing '$captured'"
}

# The reason suffix must not bleed into the captured state token: the
# '(...)' detail follows a space so '[A-Z]+' stops at the state word.
$wedgeBlock = Format-CohortVerdictLogBlock -VerdictState 'WEDGE' -VerdictDetail ' (15-sample drain tail)' -DrainTailSamples 15
$wedgeLine  = @($wedgeBlock | Where-Object { $_ -match $consumerVerdictPattern })[0]
$null = $wedgeLine -match $consumerVerdictPattern
_Assert -Name 'reason suffix does not leak into the captured verdict token' `
	-Condition ($Matches[1] -eq 'WEDGE') `
	-Detail "expected captured token 'WEDGE', got '$($Matches[1])'"

# End-to-end round-trip through a fixture log file, mirroring how
# Read-SiloLogStats reads the extracted silo log: write a representative
# silo telemetry line plus the appended verdict block, then recover the
# verdict with the consumer pattern (Select-String, last match).
$verdictLog = Join-Path ([System.IO.Path]::GetTempPath()) ("verdict-block-test-" + [Guid]::NewGuid().ToString('N') + '.log')
try {
	$fixture = @(
		'[silo] t= 30.0s ops= 1,832 ops/sec= 150 inFlight= 8 failed= 0'
		'[silo] FINAL ops=1,832 failed=0 discarded=0 elapsed=61.2s active=52.9s'
	) + (Format-CohortVerdictLogBlock -VerdictState 'HEALTHY' -VerdictDetail '' -DrainTailSamples 1)
	[System.IO.File]::WriteAllLines($verdictLog, $fixture)

	$recovered = ''
	$line = (Select-String -Path $verdictLog -Pattern $consumerVerdictPattern | Select-Object -Last 1)
	if ($line -and $line.Line -match 'Verdict\s*:\s*([A-Z]+)') { $recovered = $Matches[1] }
	_Assert -Name 'verdict recoverable from a fixture silo log via the consumer parse' `
		-Condition ($recovered -eq 'HEALTHY') `
		-Detail "expected to recover 'HEALTHY' from the appended block, got '$recovered'"
}
finally {
	if (Test-Path -LiteralPath $verdictLog) { Remove-Item -LiteralPath $verdictLog -Force -ErrorAction SilentlyContinue }
}

Write-Host ''
Write-Host 'Get-SiloInFlight' -ForegroundColor Cyan

_Assert -Name 'parses inFlight from a real [silo] t= line' `
	-Condition ((Get-SiloInFlight -Line '[silo] t= 60.0s ops= 2,430 ops/sec= 143 inFlight= 4') -eq 4) `
	-Detail 'expected inFlight=4'
_Assert -Name 'parses inFlight=0' `
	-Condition ((Get-SiloInFlight -Line '[silo] t= 61.0s ops= 2,500 ops/sec= 0 inFlight= 0') -eq 0) `
	-Detail 'expected inFlight=0'
_Assert -Name 'returns null for an empty line' `
	-Condition ($null -eq (Get-SiloInFlight -Line '')) `
	-Detail 'expected null for empty input'
_Assert -Name 'returns null when no inFlight token present' `
	-Condition ($null -eq (Get-SiloInFlight -Line '[silo] FINAL ops=2,430 failed=0')) `
	-Detail 'expected null when the line carries no inFlight= token'

Write-Host ''
Write-Host 'Update-QuiesceState' -ForegroundColor Cyan

# A single zero observation is not enough (momentary lull between flush
# batches); the default required streak is 2.
$s1 = Update-QuiesceState -InFlight 0 -ZeroStreak 0
_Assert -Name 'one zero observation does not yet declare quiesced' `
	-Condition ($s1.ZeroStreak -eq 1 -and -not $s1.Quiesced) `
	-Detail "expected streak=1 quiesced=false, got streak=$($s1.ZeroStreak) quiesced=$($s1.Quiesced)"
$s2 = Update-QuiesceState -InFlight 0 -ZeroStreak $s1.ZeroStreak
_Assert -Name 'two consecutive zero observations declare quiesced' `
	-Condition ($s2.ZeroStreak -eq 2 -and $s2.Quiesced) `
	-Detail "expected streak=2 quiesced=true, got streak=$($s2.ZeroStreak) quiesced=$($s2.Quiesced)"
# A non-zero reading resets the streak: a saga that resumes draining must
# not be stopped just because an earlier sample read zero.
$s3 = Update-QuiesceState -InFlight 4 -ZeroStreak 1
_Assert -Name 'a non-zero reading resets the zero streak' `
	-Condition ($s3.ZeroStreak -eq 0 -and -not $s3.Quiesced) `
	-Detail "expected streak reset to 0, got streak=$($s3.ZeroStreak)"
# A null reading (no fresh progress line yet) leaves the streak unchanged.
$s4 = Update-QuiesceState -InFlight $null -ZeroStreak 1
_Assert -Name 'a null reading leaves the zero streak unchanged' `
	-Condition ($s4.ZeroStreak -eq 1 -and -not $s4.Quiesced) `
	-Detail "expected streak unchanged at 1, got streak=$($s4.ZeroStreak)"

Write-Host ''
Write-Host 'Resolve-CohortVerdict' -ForegroundColor Cyan

# Baseline: a clean cohort (FINAL emitted, no tail, no failures) is HEALTHY.
$clean = @{
	SawFinal = $true; DrainTailSamples = 2; DrainWedgeThreshold = 10
	IsReadOnlyMode = $false; SiloQuiesced = $true
	FailedFinal = 0; FailedSamples = 0; Watchdog = 0; WalSlot = 0; WalAppend = 0
	ExceptionCount = 0; BenignShutdownExceptions = 0
}
$vClean = Resolve-CohortVerdict @clean
_Assert -Name 'clean cohort is HEALTHY' `
	-Condition ($vClean.State -eq 'HEALTHY' -and $vClean.Reasons.Count -eq 0) `
	-Detail "got state=$($vClean.State) reasons=$($vClean.Reasons -join '; ')"

# The core regression: a long drain tail that was preceded by a clean
# pre-stop quiesce (in-flight reached 0) is the benign graceful-shutdown
# flush window, NOT a wedge. This is the false positive that excluded
# every small-batch cross-tree atomic cohort from the report.
$quiescedTail = $clean.Clone(); $quiescedTail.DrainTailSamples = 12; $quiescedTail.SiloQuiesced = $true
$vQT = Resolve-CohortVerdict @quiescedTail
_Assert -Name 'long drain tail after a clean quiesce is HEALTHY (benign shutdown flush)' `
	-Condition ($vQT.State -eq 'HEALTHY') `
	-Detail "got state=$($vQT.State) reasons=$($vQT.Reasons -join '; ')"
_Assert -Name 'the benign-tail reason is surfaced for diagnostics' `
	-Condition ((@($vQT.Reasons) -match 'benign shutdown flush').Count -ge 1) `
	-Detail "reasons=$($vQT.Reasons -join '; ')"

# A long drain tail WITHOUT a clean quiesce (in-flight still > 0 at stop)
# is a real undrained backlog and must still WEDGE.
$wedgeTail = $clean.Clone(); $wedgeTail.DrainTailSamples = 12; $wedgeTail.SiloQuiesced = $false
$vWT = Resolve-CohortVerdict @wedgeTail
_Assert -Name 'long drain tail without a quiesce is WEDGE' `
	-Condition ($vWT.State -eq 'WEDGE') `
	-Detail "got state=$($vWT.State) reasons=$($vWT.Reasons -join '; ')"

# Quiesce disabled (-QuiesceTimeoutSec 0 -> $null): fall back to the
# unconditional tail rule so behaviour is unchanged when the wait is off.
$nullQuiesceTail = $clean.Clone(); $nullQuiesceTail.DrainTailSamples = 12; $nullQuiesceTail.SiloQuiesced = $null
$vNQ = Resolve-CohortVerdict @nullQuiesceTail
_Assert -Name 'long drain tail with quiesce disabled falls back to WEDGE' `
	-Condition ($vNQ.State -eq 'WEDGE') `
	-Detail "got state=$($vNQ.State) reasons=$($vNQ.Reasons -join '; ')"

# Read-only modes never enqueue WAL writes, so their tail is ignored
# regardless of the quiesce signal.
$readOnlyTail = $clean.Clone(); $readOnlyTail.DrainTailSamples = 12; $readOnlyTail.IsReadOnlyMode = $true; $readOnlyTail.SiloQuiesced = $false
$vRO = Resolve-CohortVerdict @readOnlyTail
_Assert -Name 'long drain tail in a read-only mode is HEALTHY' `
	-Condition ($vRO.State -eq 'HEALTHY') `
	-Detail "got state=$($vRO.State) reasons=$($vRO.Reasons -join '; ')"

# Missing FINAL is always a WEDGE.
$noFinal = $clean.Clone(); $noFinal.SawFinal = $false
$vNF = Resolve-CohortVerdict @noFinal
_Assert -Name 'a missing FINAL is WEDGE' `
	-Condition ($vNF.State -eq 'WEDGE' -and (@($vNF.Reasons) -match 'no FINAL emitted').Count -ge 1) `
	-Detail "got state=$($vNF.State) reasons=$($vNF.Reasons -join '; ')"

# FINAL failed>0 -> FAILED.
$failedF = $clean.Clone(); $failedF.FailedFinal = 525
$vFF = Resolve-CohortVerdict @failedF
_Assert -Name 'FINAL failed>0 is FAILED' `
	-Condition ($vFF.State -eq 'FAILED' -and (@($vFF.Reasons) -match 'FINAL failed=525').Count -ge 1) `
	-Detail "got state=$($vFF.State) reasons=$($vFF.Reasons -join '; ')"

# Per-second failed sample -> FAILED.
$failedS = $clean.Clone(); $failedS.FailedSamples = 3
$vFS = Resolve-CohortVerdict @failedS
_Assert -Name 'per-second failed sample is FAILED' `
	-Condition ($vFS.State -eq 'FAILED') `
	-Detail "got state=$($vFS.State) reasons=$($vFS.Reasons -join '; ')"

# Watchdog / WAL counters -> DEGRADED.
$degraded = $clean.Clone(); $degraded.Watchdog = 1
$vDG = Resolve-CohortVerdict @degraded
_Assert -Name 'watchdog>0 is DEGRADED' `
	-Condition ($vDG.State -eq 'DEGRADED') `
	-Detail "got state=$($vDG.State) reasons=$($vDG.Reasons -join '; ')"

# Cohort-attributable exceptions -> DEGRADED.
$exc = $clean.Clone(); $exc.ExceptionCount = 2
$vEX = Resolve-CohortVerdict @exc
_Assert -Name 'exception lines are DEGRADED' `
	-Condition ($vEX.State -eq 'DEGRADED' -and (@($vEX.Reasons) -match '2 exception line').Count -ge 1) `
	-Detail "got state=$($vEX.State) reasons=$($vEX.Reasons -join '; ')"

# Benign warmup-retry exclusion: the runner subtracts pre-load warmup
# OrleansMessageRejectionException retries from ExceptionCount before
# calling this function, so a cohort whose only "exceptions" were warmup
# retries arrives here with ExceptionCount=0 and stays HEALTHY, while the
# excluded count is surfaced as a diagnostic reason.
$warmupOnly = $clean.Clone(); $warmupOnly.ExceptionCount = 0; $warmupOnly.BenignWarmupExceptions = 2
$vWU = Resolve-CohortVerdict @warmupOnly
_Assert -Name 'a cohort whose only exceptions were benign warmup retries is HEALTHY' `
	-Condition ($vWU.State -eq 'HEALTHY') `
	-Detail "got state=$($vWU.State) reasons=$($vWU.Reasons -join '; ')"
_Assert -Name 'the excluded benign warmup-retry count is surfaced for diagnostics' `
	-Condition ((@($vWU.Reasons) -match 'benign warmup-retry line').Count -ge 1) `
	-Detail "reasons=$($vWU.Reasons -join '; ')"

# A genuine load-time exception still DEGRADEs even when benign warmup
# retries were also excluded (the warmup note rides alongside the count).
$warmupPlusReal = $clean.Clone(); $warmupPlusReal.ExceptionCount = 1; $warmupPlusReal.BenignWarmupExceptions = 2
$vWR = Resolve-CohortVerdict @warmupPlusReal
_Assert -Name 'a real exception alongside excluded warmup retries is still DEGRADED' `
	-Condition ($vWR.State -eq 'DEGRADED' -and (@($vWR.Reasons) -match '1 exception line').Count -ge 1 -and (@($vWR.Reasons) -match 'benign warmup-retry line').Count -ge 1) `
	-Detail "got state=$($vWR.State) reasons=$($vWR.Reasons -join '; ')"

# Guard the exact line signature the runner uses to identify benign warmup
# retries (run-cohort.ps1 'warmup\b.*\bREJECTED\b'). A pre-load warmup
# rejection must match; a load-time storage exception must NOT.
$warmupSig = 'warmup\b.*\bREJECTED\b'
$warmupLine = '23:32:01 info: [silo] warmup treeId=cohort-v450-h5-45s-20260611233135Z attempt=5 REJECTED (OrleansMessageRejectionException: Forwarding failed: ...)'
$loadExcLine = '21:55:52 warn: TcpIngestService[0] [silo] flush of 4096 failed (mode=set-many) Azure.Data.Tables.TableTransactionFailedException: Operation could not be completed'
_Assert -Name 'warmup-retry signature matches a pre-load warmup rejection' `
	-Condition ($warmupLine -match $warmupSig) `
	-Detail "pattern '$warmupSig' did not match the warmup line"
_Assert -Name 'warmup-retry signature does NOT match a load-time storage exception' `
	-Condition (-not ($loadExcLine -match $warmupSig)) `
	-Detail "pattern '$warmupSig' wrongly matched a load-time exception"

# Precedence: an un-quiesced drain tail (WEDGE) outranks a concurrent
# FINAL failure (FAILED) since WEDGE is the most severe state.
$wedgeOverFailed = $clean.Clone(); $wedgeOverFailed.DrainTailSamples = 12; $wedgeOverFailed.SiloQuiesced = $false; $wedgeOverFailed.FailedFinal = 10
$vWF = Resolve-CohortVerdict @wedgeOverFailed
_Assert -Name 'WEDGE outranks a concurrent FAILED' `
	-Condition ($vWF.State -eq 'WEDGE') `
	-Detail "got state=$($vWF.State) reasons=$($vWF.Reasons -join '; ')"

# Precedence: when the tail is benign (quiesced) but FINAL failed>0, the
# verdict is FAILED - the benign tail does not mask a real failure.
$failedDespiteQuiesce = $clean.Clone(); $failedDespiteQuiesce.DrainTailSamples = 12; $failedDespiteQuiesce.SiloQuiesced = $true; $failedDespiteQuiesce.FailedFinal = 7
$vFQ = Resolve-CohortVerdict @failedDespiteQuiesce
_Assert -Name 'a benign tail does not mask a concurrent FAILED' `
	-Condition ($vFQ.State -eq 'FAILED') `
	-Detail "got state=$($vFQ.State) reasons=$($vFQ.Reasons -join '; ')"

Write-Host ''
$summaryColor = if ($script:_FailCount -eq 0) { 'Green' } else { 'Red' }
Write-Host ("Total: {0} passed, {1} failed" -f $script:_PassCount, $script:_FailCount) `
	-ForegroundColor $summaryColor

exit $script:_FailCount
