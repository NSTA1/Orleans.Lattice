#!/usr/bin/env pwsh
<#
.SYNOPSIS
	Pure adjudication helpers for the tuning overlay's resource knobs.

.DESCRIPTION
	Issue #2863. Every resource knob in docker-compose.tuning.yml is declared as
	a variable reference whose message promises an explicitly derived value. That
	guard checks PRESENCE. It cannot check MEANING, and for all seven knobs the
	two come apart at the same value:

		VAR:?msg    errors when VAR is unset OR empty
		VAR?msg     errors when VAR is unset only

	Neither form inspects the value, so `0` is non-empty and satisfies BOTH.
	Switching between them is not the remedy and would weaken the guard.

	THE DEFECT IS THAT `0` IS OVERLOADED, NOT THAT IT IS ACCEPTED. For every
	knob here `0` carries two meanings at once that no observer can separate:

		"I forgot to set this"            - an operator error
		"run the automatic derivation"    - a deliberate operating mode

	Both are true of the same byte. Rejecting `0` outright would remove the
	second meaning along with the first and make a reasoned configuration
	inexpressible; accepting it leaves a forgotten export indistinguishable from
	a choice. No guard can recover information the encoding has already
	destroyed, so the fix is at the ENCODING: `0` is retired, and the deliberate
	case gets its own token.

	THE GENERAL RULE, which outlives this issue: a value that means "I did not
	choose" and a value that means "I chose the automatic behaviour" must not be
	the same value.

	WHY ONLY TWO KNOBS GET `auto`. A token is only offerable where BOTH hold:

		1. a runtime derivation actually exists for that knob, and
		2. WE OWN THE PARSER that finally reads the string.

	Compose interpolation has no mapping operator, so `auto` cannot be rewritten
	in transit - whatever finally reads the variable must understand it. For
	REPOCONTEXT_MAX_CONCURRENT_REPLAYS that reader is our own
	RepoContextReplayConcurrency.ResolveMaxConcurrentReplays, and for
	EMBEDDER_INTRA_THREADS it is our own EmbedServerOptions.ResolveIntraOpThreads.
	For the other five the reader is Docker or the CLR, whose vocabulary is not
	ours to extend; inventing a token there would produce a string the consumer
	does not recognise, failing in whatever way that consumer happens to fail -
	usually silently. So those five accept no `auto` at all, and `0` is simply
	refused with a message that asks for a real number.

	Compose cannot express a value predicate, so the check lives here.

	CONVENTIONS, matching _provenance.ps1 beside it:

	- Every function is PURE. It takes readings as parameters and returns
	  violations. Nothing here reads a file, an environment variable, or a
	  process. Assert-TuningEnv.ps1 does the acquisition and calls these, so the
	  FAILING direction can be demonstrated against fabricated readings rather
	  than only asserted.
	- Every function returns a (possibly empty) array of violation strings and
	  never throws for a violation. The caller decides what a violation means.
	- A violation names the value it objected to, what that value would silently
	  re-enter, AND what to write instead. The migration text is the highest
	  value string in this file: an operator meeting this check has a working
	  configuration that just stopped working, and "0 is invalid" without "write
	  auto" converts a correct guard into an outage with no way forward.

	WHY THE REFUSALS ARE DISTINGUISHABLE. An unset knob and a knob pinned to a
	retired sentinel are different operator errors with different remedies - one
	forgot to supply a value, the other supplied one that used to mean "do not
	pin me" - and the whole defect in #2863 is that the presence check collapses
	them into a single verdict it then only fires half of. Reporting them under
	one reason here would reproduce that collapse one layer down.
#>

Set-StrictMode -Version Latest

<#
.SYNOPSIS
	The token that selects a knob's runtime derivation deliberately.

.DESCRIPTION
	One spelling, applied to every knob that supports it. A second accepted
	spelling would be a second thing to keep in step across the scripts, the
	overlay, the two parsers and the docs, for no gain.
#>
$script:TuningAutoToken = 'auto'

<#
.SYNOPSIS
	The resource knobs the tuning overlay requires, the sentinel each one must
	not carry, and whether it accepts the `auto` token.

.DESCRIPTION
	This list is the single source of truth for what Assert-TuningEnv.ps1
	validates, and it is ENUMERATED rather than counted on purpose. The sibling
	failure this guard exists to prevent is not "the known knob regressed" but
	"a knob nobody listed had the same defect all along": #2863 was reported
	against REPOCONTEXT_MAX_CONCURRENT_REPLAYS alone, and all seven knobs turned
	out to share it. An occurrence count over the overlay would have returned a
	clean 1.

	The list is anchored to an INDEPENDENT artefact by
	TuningEnvSentinelHygieneTests, which asserts it matches both the variable
	references docker-compose.tuning.yml actually declares and the settings
	LocalDeploymentRunbookHygieneTests already enumerates. Without that anchor
	the list would be both the thing checked and the thing checking, and quietly
	dropping an entry would let its knob go back to accepting a sentinel with
	every assertion keyed on this list still green.

	KINDS, because the sentinel is not always the literal string `0`:

	  Count  a positive integer. `0` is the sentinel.
	  Cpus   a positive decimal. `0` and `0.0` are the sentinel.
	  Bytes  a Docker size, optionally suffixed b/k/m/g. `0`, `0m`, `0g` are all
	         the sentinel, which is why this is a parsed check and not a string
	         comparison against "0". The sibling site here is a SPELLING, not a
	         call site, and a naive -eq '0' would pass every one of them.

	AUTOTOKEN is the token that selects the derivation, or $null where no
	derivation is reachable. DERIVATION says what `auto` actually runs, so the
	message can promise something specific rather than gesturing at automatic
	behaviour.
#>
function Get-TuningKnob {
	[CmdletBinding()]
	[OutputType([pscustomobject])]
	param()

	return @(
		[pscustomobject]@{
			Name = 'REPOCONTEXT_CPUS'
			Service = 'repocontext'
			Setting = 'cpus'
			Kind = 'Cpus'
			AutoToken = $null
			Derivation = $null
			Consumer = 'Docker'
			Sentinel = 'Docker reads a CPU quota of 0 as NO CPU LIMIT, so the service is entitled to the whole host'
		},
		[pscustomobject]@{
			Name = 'REPOCONTEXT_MEM_LIMIT'
			Service = 'repocontext'
			Setting = 'mem_limit'
			Kind = 'Bytes'
			AutoToken = $null
			Derivation = $null
			Consumer = 'Docker'
			Sentinel = 'Docker reads a memory limit of 0 as UNLIMITED, so .NET sizes its heap hard limit from the host instead of from the grant'
		},
		[pscustomobject]@{
			Name = 'REPOCONTEXT_GC_HEAP_COUNT'
			Service = 'repocontext'
			Setting = 'DOTNET_GCHeapCount'
			Kind = 'Count'
			AutoToken = $null
			Derivation = $null
			Consumer = 'the CLR'
			Sentinel = '0 is not a valid heap count, so the GC discards it and sizes the heap count from the processor count - the coupling DOTNET_GCHeapCount was introduced to break (issue #2596)'
		},
		[pscustomobject]@{
			Name = 'REPOCONTEXT_MAX_CONCURRENT_REPLAYS'
			Service = 'repocontext'
			Setting = 'LATTICE_WAL_MAX_CONCURRENT_REPLAYS'
			Kind = 'Count'
			AutoToken = $script:TuningAutoToken
			Derivation = 'the library sizes the WAL replay gate from the lesser of Environment.ProcessorCount and the enforced cgroup CPU grant (ResolveGateSizing, issue #2821)'
			Consumer = 'RepoContextReplayConcurrency.ResolveMaxConcurrentReplays'
			Sentinel = '0 is LatticeOptions.DefaultWalMaterialiserMaxConcurrentReplays, which resolves the WAL replay gate at runtime instead of pinning it - this is the value that produced 625 OutOfMemoryException on acceptance run 10 (issue #2863)'
		},
		[pscustomobject]@{
			Name = 'EMBEDDER_CPUS'
			Service = 'embedder'
			Setting = 'cpus'
			Kind = 'Cpus'
			AutoToken = $null
			Derivation = $null
			Consumer = 'Docker'
			Sentinel = 'Docker reads a CPU quota of 0 as NO CPU LIMIT, so the service is entitled to the whole host'
		},
		[pscustomobject]@{
			Name = 'EMBEDDER_MEM_LIMIT'
			Service = 'embedder'
			Setting = 'mem_limit'
			Kind = 'Bytes'
			AutoToken = $null
			Derivation = $null
			Consumer = 'Docker'
			Sentinel = 'Docker reads a memory limit of 0 as UNLIMITED, so the two services stop summing to a figure the host can honour'
		},
		[pscustomobject]@{
			Name = 'EMBEDDER_INTRA_THREADS'
			Service = 'embedder'
			Setting = 'EMBED_INTRA_THREADS'
			Kind = 'Count'
			AutoToken = $script:TuningAutoToken
			Derivation = 'the embedding server sizes the ONNX intra-op pool from the enforced cgroup CPU grant (ResolveIntraOpThreads, issue #2610)'
			Consumer = 'EmbedServerOptions.ResolveIntraOpThreads'
			Sentinel = '0 is EmbedServerOptions.LetRuntimeChoose, and OnnxEmbedder applies the pin only when it is positive - so the pin is never applied and ONNX Runtime sizes its intra-op pool from HOST cores, ignoring the cgroup quota (issue #2610)'
		}
	)
}

<#
.SYNOPSIS
	Decides what one raw knob reading means.

.DESCRIPTION
	Returns a verdict object rather than a boolean, because the failing outcomes
	call for different operator actions and the point of this whole file is that
	a single verdict cannot say which applies.

	  Ok              the value pins the knob
	  Auto            the value selects the knob's runtime derivation on purpose
	  Unset           no value at all; the overlay's own presence check already
	                  catches this one, and it is re-checked here so the
	                  preflight is a SUPERSET of that guard rather than a
	                  replacement that quietly drops half of it
	  Sentinel        a retired value that parses but used to mean "decide at
	                  runtime"
	  AutoUnsupported an `auto` token on a knob whose consumer would never
	                  understand it
	  Unparseable     a value that is none of the above

	AutoUnsupported is its own verdict rather than an Unparseable because the
	operator's mistake is specific and so is the remedy: they generalised a
	token that is real elsewhere in this same file. Telling them "unparseable"
	would be true and useless.
#>
function Test-TuningKnobValue {
	[CmdletBinding()]
	[OutputType([pscustomobject])]
	param(
		[Parameter(Mandatory)] [pscustomobject] $Knob,
		[AllowNull()] [AllowEmptyString()] [string] $Value
	)

	$raw = if ($null -eq $Value) { '' } else { $Value.Trim() }

	function New-Verdict([string] $verdict, [nullable[double]] $magnitude) {
		return [pscustomobject]@{
			Name = $Knob.Name
			Verdict = $verdict
			Value = $raw
			Magnitude = $magnitude
		}
	}

	if ($raw.Length -eq 0) {
		return New-Verdict 'Unset' $null
	}

	# The token is matched case-insensitively so AUTO and Auto are not a third
	# way to fail, but it is matched EXACTLY otherwise: `automatic` is a typo,
	# not a synonym, and treating it as one would reintroduce exactly the
	# parseability-for-intent substitution this check exists to remove.
	$looksAuto = @('auto', 'derive', 'derived', 'automatic') -contains $raw.ToLowerInvariant()

	if ($null -ne $Knob.AutoToken) {
		if ($raw -ieq $Knob.AutoToken) {
			return New-Verdict 'Auto' $null
		}

		if ($looksAuto) {
			return New-Verdict 'AutoUnsupported' $null
		}
	}
	elseif ($looksAuto) {
		return New-Verdict 'AutoUnsupported' $null
	}

	# Strip a Docker size suffix before parsing. Deliberately tolerant of the
	# forms Docker itself accepts (`512m`, `2g`, `1024`, and the `mi`/`gi`
	# spellings), because a check that only understood the bare integer would
	# call a perfectly ordinary `0m` unparseable and report the wrong reason for
	# the right refusal.
	$magnitudeText = $raw

	if ($Knob.Kind -eq 'Bytes') {
		$suffixMatch = [regex]::Match($raw, '^(?<number>[0-9]*\.?[0-9]+)\s*(?<unit>[bkmg](?:i?b?)?)?$', 'IgnoreCase')

		if (-not $suffixMatch.Success) {
			return New-Verdict 'Unparseable' $null
		}

		$magnitudeText = $suffixMatch.Groups['number'].Value
	}

	$magnitude = 0.0

	if (-not [double]::TryParse(
			$magnitudeText,
			[Globalization.NumberStyles]::Float,
			[Globalization.CultureInfo]::InvariantCulture,
			[ref] $magnitude)) {
		return New-Verdict 'Unparseable' $null
	}

	# A negative grant is not a sentinel, it is nonsense, and Docker rejects it
	# outright. Reported as unparseable so the message does not claim a meaning
	# the value does not have.
	if ($magnitude -lt 0) {
		return New-Verdict 'Unparseable' $magnitude
	}

	if ($magnitude -eq 0) {
		return New-Verdict 'Sentinel' $magnitude
	}

	# A Count knob is a number of permits, heaps or threads. A fractional one is
	# not a sentinel and is not nonsense either, but it is not a value any of
	# these consumers can honour, so it is refused rather than truncated.
	if ($Knob.Kind -eq 'Count' -and $magnitude -ne [Math]::Floor($magnitude)) {
		return New-Verdict 'Unparseable' $magnitude
	}

	return New-Verdict 'Ok' $magnitude
}

<#
.SYNOPSIS
	Renders the "write this instead" half of a refusal.

.DESCRIPTION
	Separated so that the sentinel refusal and the unset refusal quote the SAME
	remedy without one of them drifting, and so a test can assert the migration
	text is present per knob rather than trusting that it was written twice
	identically.
#>
function Get-TuningKnobRemedy {
	[CmdletBinding()]
	[OutputType([string])]
	param(
		[Parameter(Mandatory)] [pscustomobject] $Knob
	)

	$expected = switch ($Knob.Kind) {
		'Count' { 'a positive integer' }
		'Cpus' { 'a positive number of CPUs, for example 6 or 5.5' }
		default { 'a positive size, for example 12g or 12288m' }
	}

	if ($null -ne $Knob.AutoToken) {
		return "Write '$($Knob.AutoToken)' to run the derivation deliberately - $($Knob.Derivation) - " +
			"or pin $expected. Derive a pinned value with ./scripts/New-TuningEnv.ps1."
	}

	return "Pin $expected; this knob has no runtime derivation to select, because $($Knob.Setting) is read by " +
		"$($Knob.Consumer) and its vocabulary is not ours to extend. " +
		'Derive a pinned value with ./scripts/New-TuningEnv.ps1.'
}

<#
.SYNOPSIS
	Adjudicates a whole set of knob readings and returns the violations.

.DESCRIPTION
	`Reading` is a hashtable of raw values keyed by knob name. A key that is
	absent is treated exactly as a key present with an empty value: from the
	overlay's point of view both are "the variable does not supply anything",
	which is the state the presence check fires on.

	Every knob is adjudicated on every call and the violations are returned
	together, rather than throwing at the first. An operator who has just
	regenerated a .env wants the whole list, and refusing one knob at a time
	turns one fix into seven round trips against a deploy that takes tens of
	minutes.
#>
function Get-TuningEnvViolation {
	[CmdletBinding()]
	[OutputType([string])]
	param(
		[Parameter(Mandatory)] [hashtable] $Reading
	)

	$violations = @()

	foreach ($knob in Get-TuningKnob) {
		$value = if ($Reading.ContainsKey($knob.Name)) { [string] $Reading[$knob.Name] } else { '' }
		$verdict = Test-TuningKnobValue -Knob $knob -Value $value
		$remedy = Get-TuningKnobRemedy -Knob $knob

		if ($verdict.Verdict -eq 'Ok' -or $verdict.Verdict -eq 'Auto') {
			continue
		}

		if ($verdict.Verdict -eq 'Unset') {
			$violations += "$($knob.Name) is UNSET. It supplies $($knob.Service).$($knob.Setting), " +
				'which the tuning overlay declares with no default by design (issue #2779). ' +
				$remedy
			continue
		}

		if ($verdict.Verdict -eq 'Sentinel') {
			$violations += "$($knob.Name) is set to '$($verdict.Value)', which PARSES BUT MEANS UNSET. " +
				"It supplies $($knob.Service).$($knob.Setting), and $($knob.Sentinel). " +
				'The overlay guards this knob with a presence check, and a presence check ' +
				'accepts this value - the deployment then runs unpinned while every layer ' +
				'reports success (issue #2863). ' +
				$remedy
			continue
		}

		if ($verdict.Verdict -eq 'AutoUnsupported') {
			$violations += "$($knob.Name) is set to '$($verdict.Value)'. " +
				"$($knob.Setting) is read by $($knob.Consumer), which would not recognise that token. " +
				$remedy
			continue
		}

		$violations += "$($knob.Name) is set to '$($verdict.Value)', which is not a value " +
			"$($knob.Service).$($knob.Setting) can take. " +
			$remedy
	}

	# Returned through the comma operator so the array survives PowerShell's
	# output unrolling. Without it a CLEAN run returns $null rather than an empty
	# array, and every caller that asks a clean result for its .Count throws
	# under Set-StrictMode - which would make the passing direction the fragile
	# one, in a file whose entire purpose is that both directions are checkable.
	return ,$violations
}
