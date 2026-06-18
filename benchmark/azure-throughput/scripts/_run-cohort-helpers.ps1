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

<#
.SYNOPSIS
	Formats the verdict summary block that run-cohort.ps1 appends to the
	silo log so downstream consumers can recover the per-cohort verdict.

.DESCRIPTION
	The verdict is computed by the runner, not emitted by the silo, so the
	extracted silo log carries the silo's [silo]/[phaseA] telemetry but no
	verdict line. performance-report.ps1's Read-SiloLogStats recovers the
	per-cohort verdict by parsing the first line this block emits with the
	pattern '^Verdict\s*:\s*([A-Z]+)'; without the appended block that parse
	returns empty and the report's HEALTHY-only aggregation cannot tell
	healthy cohorts from wedged ones (it then excludes every cohort).

	Pure and side-effect-free (returns the lines; the caller does the
	append) so Test-CohortVerdict.ps1 can assert the emitted Verdict line
	round-trips through the consumer's regex without staging a VM run.

	The leading '#' delimiter keeps the block from colliding with the
	silo's own log grammar (anchored '^[silo]' / '^[phaseA]' lines).

.PARAMETER VerdictState
	The computed verdict state (HEALTHY / DEGRADED / FAILED / WEDGE).

.PARAMETER VerdictDetail
	The pre-formatted reason suffix (e.g. ' (15-sample drain tail)'), or
	an empty string when there are no reasons.

.PARAMETER DrainTailSamples
	Count of trailing rate=0 per-second samples post-producer.

.OUTPUTS
	[string[]] the lines to append to the silo log, verbatim.
#>
function Format-CohortVerdictLogBlock {
	[CmdletBinding()]
	param(
		[Parameter(Mandatory)] [string] $VerdictState,
		[Parameter(Mandatory)] [AllowEmptyString()] [string] $VerdictDetail,
		[Parameter(Mandatory)] [int] $DrainTailSamples
	)

	return @(
		'# === run-cohort verdict (appended post-drain; not silo-emitted) ==='
		("Verdict      : {0}{1}" -f $VerdictState, $VerdictDetail)
		("Drain tail   : {0} trailing rate=0 sample(s) post-producer" -f $DrainTailSamples)
	)
}

<#
.SYNOPSIS
	Parses the in-flight gauge from a single '[silo] t=' per-second
	progress line, or $null when the line has no parseable gauge.

.DESCRIPTION
	The silo's reporter emits one line per second of the shape
	'[silo] t= 60.0s ops= 2,430 ops/sec= 143 inFlight= 4'. run-cohort.ps1
	polls the journal tail for the latest such line to decide when the
	silo has quiesced (drained its in-flight work) after the producer
	stops, so it can stop the silo without aborting still-draining
	in-flight sagas. Exposed as a pure parser so Test-CohortVerdict.ps1
	can pin the gauge extraction without a live silo.

.PARAMETER Line
	A candidate '[silo] t=' progress line.

.OUTPUTS
	[int] the inFlight value, or $null when $Line is null/empty or has no
	'inFlight=' token.
#>
function Get-SiloInFlight {
	[CmdletBinding()]
	param([AllowNull()] [AllowEmptyString()] [string] $Line)

	if ([string]::IsNullOrWhiteSpace($Line)) { return $null }
	if ($Line -match 'inFlight=\s*(\d+)') { return [int]$Matches[1] }
	return $null
}

<#
.SYNOPSIS
	Decides whether the silo has quiesced enough to stop, given the
	latest parsed in-flight value and a running count of consecutive
	zero observations.

.DESCRIPTION
	Pure decision helper so the quiesce loop in run-cohort.ps1 stays
	thin and the rule is unit-testable. The silo is considered quiesced
	once the in-flight gauge has been observed at zero for
	$RequiredZeroStreak consecutive samples - a single zero can be a
	momentary lull between flush batches, so a short streak avoids
	stopping mid-saga. A $null reading (no fresh progress line yet) does
	not advance or reset the streak.

.PARAMETER InFlight
	The latest parsed in-flight value, or $null when none is available.

.PARAMETER ZeroStreak
	Consecutive prior zero observations.

.PARAMETER RequiredZeroStreak
	Number of consecutive zero observations required to declare quiesced.

.OUTPUTS
	Hashtable with keys:
	  ZeroStreak = [int]  - updated streak
	  Quiesced   = [bool] - whether the required streak has been reached
#>
function Update-QuiesceState {
	[CmdletBinding()]
	param(
		[AllowNull()] [Nullable[int]] $InFlight,
		[int] $ZeroStreak,
		[int] $RequiredZeroStreak = 2
	)

	$streak = $ZeroStreak
	if ($null -eq $InFlight) {
		# No fresh reading; leave the streak unchanged.
	} elseif ($InFlight -le 0) {
		$streak++
	} else {
		$streak = 0
	}
	return @{ ZeroStreak = $streak; Quiesced = ($streak -ge $RequiredZeroStreak) }
}

<#
.SYNOPSIS
	Computes the cohort verdict (state + reasons) from the collected
	signals, applying the worst-first precedence.

.DESCRIPTION
	Pure decision function extracted from run-cohort.ps1 so the verdict
	precedence is unit-testable without a live VM. The runner gathers the
	raw signals (FINAL line, drain tail, per-second/FINAL failures,
	watchdog + WAL counters, filtered exception count, quiesce outcome)
	and this function maps them to a single verdict.

	State precedence (worst-first; the most severe applicable state wins):
	  WEDGE     - no FINAL emitted at all, OR a long post-producer
	              zero-rate drain tail that does NOT correspond to a clean
	              pre-stop quiesce (i.e. real undrained backlog).
	  FAILED    - any per-second failed>0 sample OR FINAL failed>0.
	  DEGRADED  - watchdog / wal-slot / wal-append counters, OR
	              cohort-attributable exception lines (net of benign
	              shutdown-race and warmup-retry lines, which the runner
	              subtracts before calling this function).
	  HEALTHY   - none of the above; FINAL emitted, no failures, clean drain.

	Drain-tail nuance: a trailing run of rate=0 per-second samples is the
	drain-wedge phenotype ONLY when it represents work the silo could not
	surface to the SIGTERM-driven drain. Two cases make such a tail benign
	and are surfaced as info reasons without downgrading:
	  - read-only modes (no WAL backlog can exist), and
	  - cohorts whose pre-stop quiesce confirmed the in-flight gauge
	    reached 0 before the stop ($SiloQuiesced -eq $true): the tail is
	    then the silo's graceful-shutdown WAL-flush window, not a wedge.
	A genuinely wedged silo never quiesces ($SiloQuiesced -eq $false), so
	its drain tail still WEDGEs. When quiesce was disabled
	($SiloQuiesced -eq $null) the rule falls back to the unconditional
	tail check.

.OUTPUTS
	Hashtable with keys:
	  State   = [string]   - HEALTHY / DEGRADED / FAILED / WEDGE
	  Reasons = [string[]] - ordered diagnostic reasons (may be empty)
#>
function Resolve-CohortVerdict {
	[CmdletBinding()]
	param(
		[bool] $SawFinal,
		[int] $DrainTailSamples,
		[int] $DrainWedgeThreshold,
		[bool] $IsReadOnlyMode,
		[AllowNull()] [Nullable[bool]] $SiloQuiesced,
		[long] $FailedFinal,
		[int] $FailedSamples,
		[int] $Watchdog,
		[int] $WalSlot,
		[int] $WalAppend,
		[int] $ExceptionCount,
		[int] $BenignShutdownExceptions,
		[int] $BenignWarmupExceptions = 0
	)

	$order = @{ 'HEALTHY' = 0; 'DEGRADED' = 1; 'FAILED' = 2; 'WEDGE' = 3 }
	$byRank = @('HEALTHY', 'DEGRADED', 'FAILED', 'WEDGE')
	$rank = 0
	$reasons = @()

	if (-not $SawFinal) {
		if ($order['WEDGE'] -gt $rank) { $rank = $order['WEDGE'] }
		$reasons += 'no FINAL emitted'
	}
	if ($DrainTailSamples -ge $DrainWedgeThreshold) {
		if ($IsReadOnlyMode) {
			$reasons += "$DrainTailSamples-sample drain tail (read-only, ignored)"
		} elseif ($SiloQuiesced -eq $true) {
			$reasons += "$DrainTailSamples-sample drain tail (quiesced before stop, benign shutdown flush)"
		} else {
			if ($order['WEDGE'] -gt $rank) { $rank = $order['WEDGE'] }
			$reasons += "$DrainTailSamples-sample drain tail"
		}
	}
	if ($FailedFinal -gt 0 -or $FailedSamples -gt 0) {
		if ($order['FAILED'] -gt $rank) { $rank = $order['FAILED'] }
		if ($FailedFinal -gt 0) { $reasons += "FINAL failed=$FailedFinal" }
		elseif ($FailedSamples -gt 0) { $reasons += "$FailedSamples per-second sample(s) carried failed>0" }
	}
	if ($Watchdog -gt 0 -or $WalSlot -gt 0 -or $WalAppend -gt 0) {
		if ($order['DEGRADED'] -gt $rank) { $rank = $order['DEGRADED'] }
		$reasons += "watchdog=$Watchdog wal-slot=$WalSlot wal-append=$WalAppend"
	}
	if ($ExceptionCount -gt 0) {
		if ($order['DEGRADED'] -gt $rank) { $rank = $order['DEGRADED'] }
		$reason = "$ExceptionCount exception line(s)"
		$excludedNotes = @()
		if ($BenignShutdownExceptions -gt 0) { $excludedNotes += "$BenignShutdownExceptions benign shutdown-race line(s)" }
		if ($BenignWarmupExceptions -gt 0)   { $excludedNotes += "$BenignWarmupExceptions benign warmup-retry line(s)" }
		if ($excludedNotes.Count -gt 0) {
			$reason += " ($($excludedNotes -join ', ') excluded)"
		}
		$reasons += $reason
	} else {
		$excludedNotes = @()
		if ($BenignShutdownExceptions -gt 0) { $excludedNotes += "$BenignShutdownExceptions benign shutdown-race line(s)" }
		if ($BenignWarmupExceptions -gt 0)   { $excludedNotes += "$BenignWarmupExceptions benign warmup-retry line(s)" }
		if ($excludedNotes.Count -gt 0) {
			$reasons += "$($excludedNotes -join ', ') excluded"
		}
	}

	return @{ State = $byRank[$rank]; Reasons = $reasons }
}

<#
.SYNOPSIS
	Counts the stall-watchdog / WAL wedge-diagnostic signals in a silo log.

.DESCRIPTION
	Pure scrape function extracted from run-cohort.ps1 so the token-matching
	is unit-testable against literal log fixtures (Test-CohortVerdict.ps1)
	rather than only ever exercised against a live VM's log. The runner feeds
	the returned counts straight into Resolve-CohortVerdict's DEGRADED rule.

	Two regressions this function exists to prevent recurring:
	  - The patterns are regex-escaped ('\[...\]'). They MUST NOT be passed
	    with -SimpleMatch: SimpleMatch treats the pattern as a literal string,
	    so '\[stall-watchdog\]' would search for a backslash character that
	    never appears in the log and always return 0 - silently scoring every
	    genuine wedge as watchdog=0 wal-slot=0 wal-append=0.
	  - The stall-watchdog dumps a large multi-line burst (every line prefixed
	    '[stall-watchdog] '); counting raw prefix lines reports dump verbosity,
	    not wedge events, and is fragile under journald burst rate-limiting.
	    Count the single canonical 'WEDGE DETECTED' header - the true per-fire
	    signal. The wal-slot / wal-append introspection uses a family of tokens
	    ('[wal-slot]', '[wal-slot-grain]', '[wal-slot-debug]', '[wal-slot-probe]'
	    and '[wal-append]', '[wal-append-tracker]', '[wal-append-debug]'), so
	    anchor on the '[wal-slot' / '[wal-append' prefix, not the bare token.

.OUTPUTS
	Hashtable with integer keys: Watchdog, WalSlot, WalAppend.
#>
function Measure-CohortWedgeDiagnostics {
	[CmdletBinding()]
	param(
		[Parameter(Mandatory)] [string] $SiloLogPath
	)

	return @{
		Watchdog  = @(Select-String -Path $SiloLogPath -Pattern '\[stall-watchdog\] WEDGE DETECTED').Count
		WalSlot   = @(Select-String -Path $SiloLogPath -Pattern '\[wal-slot').Count
		WalAppend = @(Select-String -Path $SiloLogPath -Pattern '\[wal-append').Count
	}
}
