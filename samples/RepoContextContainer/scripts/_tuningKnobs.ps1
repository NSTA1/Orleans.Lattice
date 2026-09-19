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

	RADIX exists because ONE knob is read in base 16 and the rest are not, and
	getting that boundary wrong in either direction is a new defect rather than
	a fix (issue #2928).

	  Decimal  the consumer parses base 10. `0x6` is meaningless to it.
	  Hex      the consumer parses base 16 WHETHER OR NOT the value is
	           0x-prefixed. Only DOTNET_GCHeapCount is in this class, because
	           only the CLR reads it, and the CLR's numeric knobs go through a
	           base-16 conversion.

	The hazard is exact and was MEASURED on .NET 10 with server GC on a 16-CPU
	host rather than argued from documentation, because a claim about a runtime
	is a claim about an artefact:

	    DOTNET_GCHeapCount=6     -> 6 heaps
	    DOTNET_GCHeapCount=0x6   -> 6 heaps
	    DOTNET_GCHeapCount=10    -> 16 heaps   (0x10)
	    DOTNET_GCHeapCount=0x0C  -> 12 heaps

	So values below 10 read identically in both bases and everything from 10 up
	does not. THE BOUNDARY IS THE FINDING. On the live rig the derived value is
	6, which is why this is currently LATENT and must not be described as a live
	misconfiguration; it becomes live on any host whose derivation crosses 10,
	which is 25 or more logical CPUs at the current 0.4 share. The remedy is to
	EMIT the 0x form, which is unambiguous, and to refuse a bare value at or
	above 10 because nobody can tell which base its author meant.

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
			Radix = 'Decimal'
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
			Radix = 'Decimal'
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
			Radix = 'Hex'
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
			Radix = 'Decimal'
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
			Radix = 'Decimal'
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
			Radix = 'Decimal'
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
			Radix = 'Decimal'
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
	  HexUnsupported  an 0x-prefixed value on a knob whose consumer parses base
	                  10, where it would not fail cleanly but would be read as
	                  0 or rejected deep inside a runtime we do not own
	  AmbiguousRadix  a BARE value of 10 or more on a knob its consumer reads as
	                  hexadecimal, so the author's intended base is unrecoverable
	  Unparseable     a value that is none of the above

	AutoUnsupported is its own verdict rather than an Unparseable because the
	operator's mistake is specific and so is the remedy: they generalised a
	token that is real elsewhere in this same file. Telling them "unparseable"
	would be true and useless.

	AmbiguousRadix is likewise its own verdict, and it is NOT a claim that the
	value is wrong - `12` is a perfectly good hex number and the CLR will honour
	it as 18. The objection is that nobody can tell whether its author meant 12
	or 18, and a configuration whose meaning depends on a fact about the reader
	is not one an acceptance run can be scored against (issue #2928).
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

	# ---- Radix, before any numeric parse (issue #2928) ------------------
	#
	# Whether these digits mean what they look like is a property of the
	# CONSUMER, not of the string, so it has to be settled before parsing and
	# not after. Getting the boundary wrong in either direction is a new defect:
	# emitting 0x on a decimal knob would hand our own int.TryParse a string it
	# rejects, and treating a hex knob as decimal is the original bug.
	$hexPrefixed = $raw.StartsWith('0x', [StringComparison]::OrdinalIgnoreCase)

	if ($hexPrefixed -and $Knob.Radix -ne 'Hex') {
		return New-Verdict 'HexUnsupported' $null
	}

	if ($Knob.Radix -eq 'Hex') {
		$digits = if ($hexPrefixed) { $raw.Substring(2) } else { $raw }
		$hexValue = 0

		if ($digits.Length -eq 0 -or -not [int]::TryParse(
				$digits,
				[Globalization.NumberStyles]::HexNumber,
				[Globalization.CultureInfo]::InvariantCulture,
				[ref] $hexValue)) {
			return New-Verdict 'Unparseable' $null
		}

		if ($hexValue -eq 0) {
			return New-Verdict 'Sentinel' ([double] $hexValue)
		}

		# A bare value BELOW 10 reads identically in both bases, so there is
		# nothing to disambiguate and refusing it would be pedantry that broke
		# the live rig's current, correct `6`. At 10 and above the two readings
		# diverge and the author's intent is unrecoverable from the file.
		if (-not $hexPrefixed -and $hexValue -ge 10) {
			return New-Verdict 'AmbiguousRadix' ([double] $hexValue)
		}

		return New-Verdict 'Ok' ([double] $hexValue)
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

	if ($Knob.Radix -eq 'Hex') {
		$expected = "a positive count WRITTEN IN THE 0x FORM, for example 0x6 or 0x18, because $($Knob.Consumer) " +
			'reads this knob in base 16 (issue #2928)'
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
	Renders a derived numeric value in the notation its consumer actually reads.

.DESCRIPTION
	Issue #2928, the WRITE side. New-TuningEnv.ps1 derives a heap count as an
	ordinary integer and used to write it verbatim, which is correct for every
	knob whose consumer parses base 10 and wrong for the one whose consumer does
	not. The derived value on the reference host is 6, which reads the same in
	both bases - so the defect never surfaced there, and a fix that only ever
	ran against that host could not demonstrate itself.

	Pure, and separated from the script that calls it, precisely so a test can
	assert 24 renders as `0x18` without needing a 60-CPU host to derive 24 on.
	That is the whole reason this is a function rather than a format string at
	the call site: the interesting inputs are unreachable on the machine the
	code runs on.

	The 0x form is emitted even when the value is below 10 and would read
	identically either way. Emitting it conditionally would mean the file's
	notation silently changed the first time a host crossed the boundary, which
	is a worse property than a slightly redundant `0x6`: an operator who has
	learned to read `6` would meet `0x10` exactly once, on the run where it
	mattered.
#>
function Format-TuningKnobValue {
	[CmdletBinding()]
	[OutputType([string])]
	param(
		[Parameter(Mandatory)] [pscustomobject] $Knob,
		[Parameter(Mandatory)] [int] $Value
	)

	if ($Knob.Radix -eq 'Hex') {
		if ($Value -lt 0) {
			throw "Cannot render a negative value ($Value) for $($Knob.Name)."
		}

		return '0x{0:X}' -f $Value
	}

	return [string] $Value
}

<#
.SYNOPSIS
	The commits that taught each consumer the token its .env may carry.

.DESCRIPTION
	A DEPLOYED ARTEFACT CHECK, and the reason it exists is worth stating because
	the guard looks like belt-and-braces until you have been caught by it.

	`auto` is understood by the two consumers we own. That is a fact about
	SOURCE. What runs is a binary in an image, and the image can predate the
	commit that taught it the token - which was exactly the state of the live
	rig when this was written: the running image was built from a commit that
	does not contain the string `auto` in that file at all, so migrating the
	.env to `auto` before rebuilding would not have fallen back, it would have
	thrown InvalidOperationException at silo configuration and the stack would
	not have started.

	The general pattern, which is the most expensive thing this epic has
	learned: SOURCE AND ARTEFACT ARE DIFFERENT OBJECTS, and a claim has to say
	which one it is about. Three separate defects in this same rig are instances
	of it - a value (#2928), a checkout (#2930), and this, an image.

	SCOPE, stated so the next reader is not misled about what is covered: this
	guard checks the `auto` token on the two knobs that accept it. It is not a
	general "every token the .env uses" check, because every token the .env
	currently uses IS that one. When a second token is added, add its row here;
	the shape is ready for it and the adjudicator iterates the table.

	VERIFIEDTOKEN is a literal on purpose, and is the part that makes this fail
	loudly rather than quietly stop applying. The adjudicator asserts it still
	equals the knob's live AutoToken, so renaming the token without revisiting
	the introducing commit is a reported staleness rather than a guard that
	silently matches nothing.
#>
function Get-TokenAncestryRequirement {
	[CmdletBinding()]
	[OutputType([pscustomobject])]
	param()

	return @(
		[pscustomobject]@{
			KnobName = 'REPOCONTEXT_MAX_CONCURRENT_REPLAYS'
			VerifiedToken = 'auto'
			IntroducedIn = '52d5cd2ea0ebbb2641b3d0d542db67c5f633c350'
			IntroducedBy = 'PR #2896, fixing issue #2863'
			Consumer = 'RepoContextReplayConcurrency.ResolveMaxConcurrentReplays'
			Consequence = 'an image built before this commit throws InvalidOperationException at silo configuration and the stack does not start'
		},
		[pscustomobject]@{
			KnobName = 'EMBEDDER_INTRA_THREADS'
			VerifiedToken = 'auto'
			IntroducedIn = '52d5cd2ea0ebbb2641b3d0d542db67c5f633c350'
			IntroducedBy = 'PR #2896, fixing issue #2863'
			Consumer = 'EmbedServerOptions.ResolveIntraOpThreads'
			Consequence = 'an image built before this commit does not recognise the token'
		}
	)
}

<#
.SYNOPSIS
	Adjudicates whether the image about to run understands the tokens the .env
	uses.

.DESCRIPTION
	PURE. `AncestryResult` is a hashtable mapping a commit sha to $true when it
	is an ancestor of the commit being built, $false when it is not, and absent
	when the caller could not determine it. Assert-TuningEnv.ps1 supplies it by
	running `git merge-base --is-ancestor`; this function never shells out, so a
	test can drive the REFUSING direction without a repository in a particular
	state.

	An UNDETERMINED ancestry is reported rather than assumed either way. This is
	the same fail-closed choice as everywhere else in this rig: the guard exists
	because a confident wrong answer about a deployed artefact cost a stack
	restart, and a guard that quietly passes when it cannot tell would reproduce
	that with extra steps.
#>
function Get-TokenAncestryViolation {
	[CmdletBinding()]
	[OutputType([string])]
	param(
		[Parameter(Mandatory)] [hashtable] $Reading,
		[Parameter(Mandatory)] [hashtable] $AncestryResult,
		[Parameter(Mandatory)] [string] $BuildCommit
	)

	$violations = @()
	$knobsByName = @{}

	foreach ($knob in Get-TuningKnob) {
		$knobsByName[$knob.Name] = $knob
	}

	foreach ($requirement in Get-TokenAncestryRequirement) {
		if (-not $knobsByName.ContainsKey($requirement.KnobName)) {
			$violations += "The token-ancestry table names $($requirement.KnobName), which is not a knob. " +
				'The table has drifted from Get-TuningKnob and is no longer checking what it claims to.'
			continue
		}

		$knob = $knobsByName[$requirement.KnobName]

		if ($knob.AutoToken -ne $requirement.VerifiedToken) {
			$violations += "The token-ancestry table records '$($requirement.VerifiedToken)' for " +
				"$($requirement.KnobName), but that knob's token is now '$($knob.AutoToken)'. The token was " +
				'renamed without revisiting the commit that introduced it, so this guard would silently stop ' +
				'applying. Update the table, verifying the new introducing commit.'
			continue
		}

		$value = if ($Reading.ContainsKey($knob.Name)) { ([string] $Reading[$knob.Name]).Trim() } else { '' }

		if ($value -ine $requirement.VerifiedToken) {
			continue
		}

		if (-not $AncestryResult.ContainsKey($requirement.IntroducedIn)) {
			$violations += "$($knob.Name) is set to '$($requirement.VerifiedToken)', but whether the commit " +
				"about to be built ($BuildCommit) contains $($requirement.IntroducedIn) could not be " +
				'determined. The token is understood by the SOURCE; whether it is understood by the ARTEFACT ' +
				'is the question, and an undetermined answer is not a yes.'
			continue
		}

		if (-not $AncestryResult[$requirement.IntroducedIn]) {
			$violations += "$($knob.Name) is set to '$($requirement.VerifiedToken)', which " +
				"$($requirement.Consumer) only learned in $($requirement.IntroducedIn) " +
				"($($requirement.IntroducedBy)). That commit is NOT an ancestor of $BuildCommit, so " +
				"$($requirement.Consequence). Rebuild from a commit that contains it BEFORE putting this " +
				'value in .env - never the other way round, and never with a container start in between.'
		}
	}

	return ,$violations
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

		if ($verdict.Verdict -eq 'HexUnsupported') {
			$violations += "$($knob.Name) is set to '$($verdict.Value)', which is HEXADECIMAL NOTATION on a " +
				"knob that is read in base 10. $($knob.Setting) is read by $($knob.Consumer), which parses " +
				'decimal, so the 0x form is not a more precise way of writing this value - it is a different ' +
				'value or a parse failure. Only DOTNET_GCHeapCount is read as hexadecimal, because only the ' +
				'CLR reads it; generalising the 0x form from there to here is the same class of mistake as ' +
				'generalising the auto token (issue #2928). ' +
				$remedy
			continue
		}

		if ($verdict.Verdict -eq 'AmbiguousRadix') {
			$asHex = [int] $verdict.Magnitude
			$asDecimal = [int] $verdict.Value
			$violations += "$($knob.Name) is set to '$($verdict.Value)', and $($knob.Setting) is read by " +
				"$($knob.Consumer) IN BASE 16. So this is $asHex heaps, not $asDecimal - and nobody reading " +
				'the file can tell which was meant. The value is not necessarily wrong; it is ' +
				'unattributable, which is what an acceptance run cannot be scored against. ' +
				"Write '0x$('{0:X}' -f $asHex)' to mean $asHex, or '0x$('{0:X}' -f $asDecimal)' to mean " +
				"$asDecimal. Values below 10 are exempt because they read identically in both bases, which " +
				'is why the live rig has been safe at 6 (issue #2928).'
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
