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
$summaryColor = if ($script:_FailCount -eq 0) { 'Green' } else { 'Red' }
Write-Host ("Total: {0} passed, {1} failed" -f $script:_PassCount, $script:_FailCount) `
	-ForegroundColor $summaryColor

exit $script:_FailCount
