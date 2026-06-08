<#
.SYNOPSIS
	Verdict-computation helpers for run-cohort.ps1.

.DESCRIPTION
	Pure, side-effect-free functions consumed by run-cohort.ps1's verdict
	pipeline. Extracted into a dot-sourceable module so the regression
	tests in Test-CohortVerdict.ps1 can exercise the filtering logic in
	isolation, without spinning up an Azure VM.

	The verdict-relevant entry point is Get-CohortExceptionCount, which
	filters the raw `Exception` line count by the current cohort's tree
	id. The motivation is that the silo runs for the lifetime of
	performance-report.ps1 (not per cohort), so wedged WAL grains from a
	prior cohort's tree continue to throw inside the current cohort's
	wall-clock window. Counting those throws toward the current cohort's
	verdict misattributes prior-cohort failures, inflating HEALTHY runs
	to DEGRADED.

	The filter is intentionally conservative: a line is excluded only
	when it explicitly references a *different* cohort tree id. Silo-wide
	exceptions with no cohort attribution still count, so genuine
	current-cohort regressions are never silently suppressed.
#>

# Pattern that matches the cohort-tree-id token embedded in
# `BENCH_TREE_ID=cohort-v<vehicles>-h<tickHz>-<duration>s-<utcStamp>`.
# The stamp shape is `yyyyMMddHHmmssZ` (14 digits + literal Z) as
# emitted by run-cohort.ps1's $stamp = (Get-Date).ToUniversalTime()
# .ToString('yyyyMMddHHmmssZ'). Pinned to the exact shape so the
# pattern won't accidentally match unrelated `cohort-` tokens in log
# prose.
$script:_CohortTreeIdPattern = 'cohort-v\d+-h\d+-\d+s-\d{14}Z'

<#
.SYNOPSIS
	Counts silo-log lines containing `Exception` that are attributable to
	the current cohort.

.DESCRIPTION
	A line is counted when at least one of the following holds:
	  1. It explicitly references $CurrentTreeId.
	  2. It contains no cohort-tree-id token at all (silo-wide noise
		 that is not provably cross-cohort).
	A line is excluded when it references some cohort-shaped tree id
	that is NOT $CurrentTreeId - this is the cross-cohort residual case.

	The raw count (every `Exception` line in the log) is also returned
	for diagnostic completeness; the verdict pipeline drives off the
	filtered count.

.PARAMETER LogPath
	Absolute or relative path to the silo log file.

.PARAMETER CurrentTreeId
	The BENCH_TREE_ID assigned to the current cohort. When this matches
	the script's cohort-tree-id pattern, the filter activates; otherwise
	(e.g. caller supplied a non-cohort-shaped id like 'r25k-001') the
	filter falls back to counting every Exception line so we never
	silently suppress signals we don't know how to classify.

.OUTPUTS
	Hashtable with keys:
	  Filtered = [int]  - count attributable to the current cohort
	  Raw      = [int]  - count of every Exception line
	  Excluded = [int]  - Raw - Filtered (number of cross-cohort lines)
#>
function Get-CohortExceptionCount {
	[CmdletBinding()]
	param(
		[Parameter(Mandatory)] [string] $LogPath,
		[Parameter(Mandatory)] [AllowEmptyString()] [string] $CurrentTreeId
	)

	if (-not (Test-Path -LiteralPath $LogPath)) {
		return @{ Filtered = 0; Raw = 0; Excluded = 0 }
	}

	$allExceptionLines = @(Select-String -Path $LogPath -Pattern 'Exception' -SimpleMatch)
	$rawCount = $allExceptionLines.Count

	# Defensive: if the caller's tree id is not cohort-shaped, we have
	# no safe way to identify "other cohort" lines, so we count
	# everything. This keeps the runner safe under ad-hoc tree ids
	# (e.g. the -ExtraSiloEnv BENCH_TREE_ID = 'r25k-001' shape from the
	# script's .EXAMPLE block).
	if ($CurrentTreeId -notmatch ('^' + $script:_CohortTreeIdPattern + '$')) {
		return @{ Filtered = $rawCount; Raw = $rawCount; Excluded = 0 }
	}

	$filtered = 0
	foreach ($m in $allExceptionLines) {
		if (Test-CohortLineAttributable -Line $m.Line -CurrentTreeId $CurrentTreeId) {
			$filtered++
		}
	}

	return @{ Filtered = $filtered; Raw = $rawCount; Excluded = $rawCount - $filtered }
}

<#
.SYNOPSIS
	Returns $true if a log line is attributable to the current cohort.

.DESCRIPTION
	Pure predicate. Exposed (rather than inlined into
	Get-CohortExceptionCount) so the regression test can exercise the
	classification rules directly against literal strings without
	staging temp files.

	Rules:
	  - Line contains $CurrentTreeId           -> attributable (true)
	  - Line contains some other cohort tree   -> NOT attributable (false)
	  - Line contains no cohort tree id at all -> attributable (true)

	The "no token at all" default is deliberately permissive: silo-wide
	exceptions (config-load failures, network-stack faults, startup
	errors) are not cohort-attributable but are still legitimate
	degradation signals for whichever cohort happens to be running.
#>
function Test-CohortLineAttributable {
	[CmdletBinding()]
	param(
		[Parameter(Mandatory)] [string] $Line,
		[Parameter(Mandatory)] [string] $CurrentTreeId
	)

	if ($Line.Contains($CurrentTreeId)) { return $true }

	$otherCohortTokens = [regex]::Matches($Line, $script:_CohortTreeIdPattern)
	if ($otherCohortTokens.Count -eq 0) { return $true }

	# Any cohort-shaped token present and none of them equal the
	# current cohort -> cross-cohort residual, exclude.
	foreach ($t in $otherCohortTokens) {
		if ($t.Value -eq $CurrentTreeId) { return $true }
	}
	return $false
}
