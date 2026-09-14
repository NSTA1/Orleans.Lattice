#!/usr/bin/env pwsh
<#
.SYNOPSIS
	Pure helpers that record a deployment's attribution-relevant configuration
	and name what moved since the previous run.

.DESCRIPTION
	Issue #2931. A measured movement is only evidence about a cause if no OTHER
	sufficient cause moved in the same step. The WAL replay gate's 16 -> 6 was
	lost exactly there: the gate's own knob changed, and DOTNET_PROCESSOR_COUNT
	was removed, in one deploy. Both are sufficient to produce that movement, so
	the reading attributes to neither. Nothing was mis-set; a measurement was
	destroyed.

	The remedy is NOT to restore a particular value. A pinned constant with no
	assertion is what produced this: the pin was believed to be in force for
	several runs after it had been deleted, because nothing ever read it back.
	The remedy is to make the rig incapable of changing two attribution-relevant
	variables SILENTLY in one step - deliberately is fine and is sometimes
	necessary, silently is what voids a comparison.

	So this file does three things, and the third is the one that matters:

	  1. enumerates the variables a comparison's validity depends on;
	  2. records each one's value AND where that value came from;
	  3. diffs a run against its predecessor and refuses a multi-variable step
	     that nobody acknowledged.

	DECLARED AND EFFECTIVE ARE DIFFERENT OBJECTS, and conflating them is the
	failure this epic keeps rediscovering - at a value (#2928), at a checkout
	(#2930), and at an image (the `auto` token, which the deployed binary did
	not understand while the source did). A manifest that recorded only what the
	compose file DECLARES would repeat it one layer up, so every record carries
	both and a divergence between them is itself a finding.

	CONVENTIONS, matching _tuningKnobs.ps1 and _provenance.ps1 beside it:

	- Every function is PURE. Nothing here reads a file, an environment
	  variable, a container, or a process. Assert-DeployManifest.ps1 does the
	  acquisition. The split is what makes the FAILING direction demonstrable:
	  a test can drive a two-variable step, an indeterminate provenance, or a
	  declared/effective divergence against fabricated readings, with no daemon
	  and no deploy.
	- Nothing here throws for a finding. Findings are returned; the caller
	  decides what they mean.
	- Every finding NAMES the variable, both values, and why it matters. A diff
	  that says "3 variables changed" is a scrollback entry. One that says which
	  three, from what, to what, is evidence someone who was not there can cite.
#>

Set-StrictMode -Version Latest

<#
.SYNOPSIS
	The provenance labels a resolved value can carry.

.DESCRIPTION
	Named constants rather than bare strings so a typo in a comparison is a
	missing-property error under Set-StrictMode instead of a silently false
	branch. `Indeterminate` is a first-class outcome, not an error case: the
	whole point is that "we could not tell where this came from" must be
	reportable, because the alternative is guessing and presenting the guess.
#>
$script:ProvenanceExplicit = 'ExplicitEnvironment'
$script:ProvenanceCgroup = 'CgroupDerivation'
$script:ProvenanceIndeterminate = 'Indeterminate'

<#
.SYNOPSIS
	Whether the DECLARED half was obtained, and if not, why not.

.DESCRIPTION
	Three states, because #2983 is a case of the first two being rendered
	identically. A manifest that prints `<absent>` for a key cannot, without
	this, be read to mean either "compose resolved and does not declare it" or
	"compose was never resolved, so nothing is known" - and those license
	opposite conclusions. The second is not a weaker form of the first; it is
	an absence of evidence being recorded in the notation reserved for evidence
	of absence.

	  Available     resolution succeeded. Every `<absent>` below it is a
	                positive finding: the key is genuinely not declared.
	  Unreadable    resolution was attempted and FAILED. Nothing is known about
	                any key, and the reason is recorded beside it.
	  NotAttempted  no resolution was asked for - the caller supplied a reading
	                directly, or asked for the effective half alone.

	Only `Available` licenses the divergence check. The other two suppress it,
	which is correct, but they must SAY they suppressed it: a suppressed check
	that renders as a passing one is the defect this file was written to stop,
	and #2983 is that defect occurring inside this file.
#>
$script:DeclarationAvailable = 'Available'
$script:DeclarationUnreadable = 'Unreadable'
$script:DeclarationNotAttempted = 'NotAttempted'

<#
.SYNOPSIS
	The compose files that must ALL be resolved for a declared reading to be
	complete, in overlay order.

.DESCRIPTION
	Pure on purpose, and separated from the code that runs `docker compose`, so
	the overlay requirement is assertable without a daemon. The requirement is
	not cosmetic: a bare `docker compose config` resolves only
	docker-compose.yml and docker-compose.override.yml, and EVERY
	attribution-relevant knob in Get-AttributionVariable is set by
	docker-compose.tuning.yml. Verified 2026-09-14 against the live rig - a bare
	resolution yields zero matches for LATTICE_WAL_MAX_CONCURRENT_REPLAYS, and
	the same resolution with the overlay yields "0".

	So a fix that resolved compose WITHOUT the overlay would report `<absent>`
	for every knob while now claiming to have looked, which is strictly worse
	than not looking: it converts an admitted gap into a false negative. Keeping
	the list here means a test can assert the overlay is in it without starting
	anything.
#>
function Get-DeployComposeFile {
	[CmdletBinding()]
	[OutputType([string])]
	param()

	return @('docker-compose.yml', 'docker-compose.tuning.yml')
}

<#
.SYNOPSIS
	The variables whose movement invalidates a run-to-run comparison.

.DESCRIPTION
	ENUMERATED, for the same reason Get-TuningKnob is: the failure to guard
	against is not "the known variable regressed" but "a variable nobody listed
	moved too". #2931 is precisely that - DOTNET_PROCESSOR_COUNT was not a
	lattice knob, was not in anybody's list, and is the one that broke the
	attribution.

	ATTRIBUTION says what a movement in this variable is sufficient to cause, in
	the words a reader scoring a run needs. It is not decoration: a diff has to
	tell someone who was not present WHY the delta threatens their reading, or
	they will see a changed number and move on.

	CONSUMER records who finally reads the string, which is what decides whether
	a value can be validated locally at all. Where it is a runtime we do not
	own, the manifest can record the value but cannot vouch for its meaning.
#>
function Get-AttributionVariable {
	[CmdletBinding()]
	[OutputType([pscustomobject])]
	param()

	return @(
		[pscustomobject]@{
			Name = 'DOTNET_PROCESSOR_COUNT'
			Service = 'repocontext'
			Consumer = 'the CLR, and every Environment.ProcessorCount consumer in the process'
			Attribution = 'moves the thread pool, Orleans scheduling, the GC heap count where it is not pinned, AND any gate sized from ProcessorCount - so it is sufficient on its own to move almost any throughput or concurrency reading'
		},
		[pscustomobject]@{
			Name = 'LATTICE_WAL_MAX_CONCURRENT_REPLAYS'
			Service = 'repocontext'
			Consumer = 'RepoContextReplayConcurrency.ResolveMaxConcurrentReplays'
			Attribution = 'sets the WAL replay gate directly, so it is sufficient to move replay concurrency, activation-storm CPU, and the peak buffer footprint that produced the OutOfMemoryException wave in #2692'
		},
		[pscustomobject]@{
			Name = 'DOTNET_GCHeapCount'
			Service = 'repocontext'
			Consumer = 'the CLR'
			Attribution = 'sets server GC heap count, so it is sufficient to move pause distribution and peak managed footprint - the axis gate run 2 measured at 20.9% of wall-clock in stop-the-world pauses'
		},
		[pscustomobject]@{
			Name = 'DOTNET_gcServer'
			Service = 'repocontext'
			Consumer = 'the CLR'
			Attribution = 'selects the collector outright, so it is sufficient to move every latency reading taken on the service'
		},
		[pscustomobject]@{
			Name = 'EMBED_INTRA_THREADS'
			Service = 'embedder'
			Consumer = 'EmbedServerOptions.ResolveIntraOpThreads'
			Attribution = 'sizes the ONNX intra-op pool, so it is sufficient to move vectorising throughput and the kernel throttling rate under a fractional grant'
		}
	)
}

<#
.SYNOPSIS
	The resource grants whose movement invalidates a comparison just as surely.

.DESCRIPTION
	Kept apart from Get-AttributionVariable because they are read from a
	different place - the resolved service definition rather than its
	environment block - and a caller that can see one may not be able to see
	the other. Merging them would force a caller to supply both or report a
	false absence for the half it cannot reach, and a false absence is the one
	outcome this whole file exists to prevent.
#>
function Get-AttributionGrant {
	[CmdletBinding()]
	[OutputType([pscustomobject])]
	param()

	return @(
		[pscustomobject]@{
			Name = 'repocontext.cpus'
			Service = 'repocontext'
			Consumer = 'Docker'
			Attribution = 'bounds the CPU the process can obtain, and is the number the cgroup-aware derivations read when their variable is absent - so it is sufficient to move a reading BOTH directly and through every derivation downstream of it'
		},
		[pscustomobject]@{
			Name = 'repocontext.mem_limit'
			Service = 'repocontext'
			Consumer = 'Docker'
			Attribution = 'sets the .NET heap hard limit, so it is sufficient to move collection frequency, and below the working set it produces a STORAGE fault rather than a container kill - a movement here can invalidate a run without any resource event to mark it'
		},
		[pscustomobject]@{
			Name = 'embedder.cpus'
			Service = 'embedder'
			Consumer = 'Docker'
			Attribution = 'bounds embedder CPU and is the grant the intra-op pool derives from when EMBED_INTRA_THREADS is absent'
		},
		[pscustomobject]@{
			Name = 'embedder.mem_limit'
			Service = 'embedder'
			Consumer = 'Docker'
			Attribution = 'bounds embedder memory, and with repocontext.mem_limit determines whether the pair still sums to something the host can honour'
		}
	)
}

<#
.SYNOPSIS
	Decides the resolved processor count and, more importantly, where it came
	from.

.DESCRIPTION
	The PM's requirement on #2931, in his words: a loud 6 is worth more than a
	silent 16. So this returns the count AND its provenance, and treats "I
	cannot tell" as a reportable outcome rather than defaulting quietly.

	The three outcomes and why each is distinct:

	  ExplicitEnvironment  DOTNET_PROCESSOR_COUNT is set and usable. The value
	                       is whatever it says, and it OVERRIDES the cgroup - so
	                       a reader must know the quota is no longer the source.
	  CgroupDerivation     the variable is absent, so Environment.ProcessorCount
	                       resolves from the enforced quota. This is the current
	                       state of the live rig and it is CORRECT; it simply
	                       has to be visible, because it was invisible for
	                       several runs while a predicate asserted otherwise.
	  Indeterminate        the variable is present but unusable, or absent with
	                       no quota reading to derive from. FAIL CLOSED. A
	                       manifest that guessed here would be asserting
	                       provenance it does not have, which is worse than
	                       declining to, because it would be believed.

	A present-but-unusable value is Indeterminate rather than falling back to
	the cgroup on purpose. The CLR's own handling of a malformed value is not
	something this script should model and then present as fact - that is a
	claim about an artefact, made from a script, which is the exact seam this
	epic keeps losing.
#>
function Resolve-ProcessorCountProvenance {
	[CmdletBinding()]
	[OutputType([pscustomobject])]
	param(
		[AllowNull()] [AllowEmptyString()] [string] $Declared,
		[AllowNull()] [nullable[double]] $CgroupCpuQuota
	)

	$raw = if ($null -eq $Declared) { '' } else { $Declared.Trim() }

	if ($raw.Length -gt 0) {
		$parsed = 0

		if ([int]::TryParse(
				$raw,
				[Globalization.NumberStyles]::Integer,
				[Globalization.CultureInfo]::InvariantCulture,
				[ref] $parsed) -and $parsed -gt 0) {
			return [pscustomobject]@{
				Resolved = $parsed
				Source = $script:ProvenanceExplicit
				Declared = $raw
				Reason = "DOTNET_PROCESSOR_COUNT is set to '$raw', which OVERRIDES the cgroup quota process-wide."
			}
		}

		return [pscustomobject]@{
			Resolved = $null
			Source = $script:ProvenanceIndeterminate
			Declared = $raw
			Reason = "DOTNET_PROCESSOR_COUNT is set to '$raw', which is not a positive integer. " +
				'What the CLR does with it is not something this script will guess at and then report as fact.'
		}
	}

	if ($null -eq $CgroupCpuQuota) {
		return [pscustomobject]@{
			Resolved = $null
			Source = $script:ProvenanceIndeterminate
			Declared = ''
			Reason = 'DOTNET_PROCESSOR_COUNT is absent, so the count derives from the enforced CPU quota - ' +
				'but no quota reading was supplied, so the resolved count is unknown.'
		}
	}

	# Ceiling, matching what .NET itself reports for a fractional quota, so a
	# recorded count never disagrees with the runtime in the unsafe direction.
	$derived = [Math]::Max(1, [int][Math]::Ceiling($CgroupCpuQuota))

	return [pscustomobject]@{
		Resolved = $derived
		Source = $script:ProvenanceCgroup
		Declared = ''
		Reason = "DOTNET_PROCESSOR_COUNT is absent, so Environment.ProcessorCount derives from the enforced " +
			"CPU quota of $CgroupCpuQuota, giving $derived. This is the live rig's current state and it is " +
			'correct; it is recorded because it was invisible while a predicate asserted a pinned 16.'
	}
}

<#
.SYNOPSIS
	Builds an ordered manifest from declared and effective readings.

.DESCRIPTION
	`Declared` is what the resolved compose configuration says. `Effective` is
	what the running container actually carries. Both are hashtables keyed by
	variable name; a key absent from either is recorded as absent rather than
	as empty, because those are different states and #2931 is a case of exactly
	that difference going unrecorded.

	Order is the enumeration order of Get-AttributionVariable then
	Get-AttributionGrant, NOT hashtable order, so two manifests of the same
	deployment are byte-identical and a diff of the files is a diff of the
	configuration. A manifest whose line order wandered would show spurious
	deltas and train its reader to skim them.
#>
function New-DeployManifest {
	[CmdletBinding()]
	[OutputType([pscustomobject])]
	param(
		[Parameter(Mandatory)] [hashtable] $Declared,
		[Parameter(Mandatory)] [hashtable] $Effective,
		[AllowNull()] [nullable[double]] $CgroupCpuQuota,
		[AllowNull()] [AllowEmptyString()] [string] $Label,
		[ValidateSet('Available', 'Unreadable', 'NotAttempted')]
		[AllowNull()] [AllowEmptyString()] [string] $DeclarationStatus,
		[AllowNull()] [AllowEmptyString()] [string] $DeclarationReason
	)

	$records = @()

	# An omitted status is inferred, so every existing caller keeps working. The
	# inference is the OLD behaviour and is deliberately the conservative one: an
	# empty reading becomes NotAttempted, never Available, so a caller that forgot
	# to say cannot accidentally license the divergence check against nothing.
	$status = if (-not [string]::IsNullOrWhiteSpace($DeclarationStatus)) {
		$DeclarationStatus
	}
	elseif ($Declared.Keys.Count -gt 0) {
		$script:DeclarationAvailable
	}
	else {
		$script:DeclarationNotAttempted
	}

	foreach ($variable in @(Get-AttributionVariable) + @(Get-AttributionGrant)) {
		$declaredValue = if ($Declared.ContainsKey($variable.Name)) {
			$raw = $Declared[$variable.Name]
			if ($null -eq $raw) { $null } else { [string] $raw }
		}
		else { $null }

		$effectiveValue = if ($Effective.ContainsKey($variable.Name)) {
			$raw = $Effective[$variable.Name]
			if ($null -eq $raw) { $null } else { [string] $raw }
		}
		else { $null }

		$records += [pscustomobject]@{
			Name = $variable.Name
			Service = $variable.Service
			Declared = $declaredValue
			Effective = $effectiveValue
			Attribution = $variable.Attribution
			Consumer = $variable.Consumer
		}
	}

	$provenance = Resolve-ProcessorCountProvenance `
		-Declared ($(if ($Effective.ContainsKey('DOTNET_PROCESSOR_COUNT')) { [string] $Effective['DOTNET_PROCESSOR_COUNT'] } else { '' })) `
		-CgroupCpuQuota $CgroupCpuQuota

	return [pscustomobject]@{
		Label = if ([string]::IsNullOrWhiteSpace($Label)) { 'unlabelled' } else { $Label }
		Records = @($records)
		ProcessorCount = $provenance
		# Whether a DECLARED reading was obtained, and if not why not. Inferring this
		# from ($Declared.Keys.Count -gt 0) - which is what this did until #2983 -
		# cannot tell "resolved, and the file declares nothing" from "never resolved",
		# and the caller's own acquisition gap made the second case 100% of real
		# invocations while it rendered as the first. An explicit status cannot be
		# satisfied by forgetting to acquire.
		DeclarationStatus = $status
		DeclarationReason = if ([string]::IsNullOrWhiteSpace($DeclarationReason)) { '' } else { $DeclarationReason }
		# Retained so existing readers keep working. Now DERIVED from the status
		# rather than from emptiness, so it cannot disagree with it.
		DeclarationAvailable = ($status -eq $script:DeclarationAvailable)
	}
}

<#
.SYNOPSIS
	Reports where a manifest's declared and effective readings disagree.

.DESCRIPTION
	A divergence is not automatically a defect - a null-valued compose entry
	DECLARES nothing and correctly yields nothing - so the check is asymmetric
	and only reports the cases that can mislead:

	  declared something, effective absent    the value did not reach the
	                                          container; the deployment is not
	                                          the one the file describes
	  declared X, effective Y                 something between the file and the
	                                          container rewrote it

	The reverse - declared absent, effective present - is reported too, because
	it means the value came from somewhere this manifest cannot see, and an
	unattributable value is the thing being hunted.
#>
function Get-DeployManifestDivergence {
	[CmdletBinding()]
	[OutputType([string])]
	param(
		[Parameter(Mandatory)] [pscustomobject] $Manifest
	)

	$divergences = @()

	# Only an AVAILABLE declaration licenses this check. Without a declared half
	# there is nothing to diverge FROM, and comparing against one would report every
	# variable the container carries as unattributable - the guard accusing the
	# deployment of the guard's own missing input.
	#
	# Silence here is correct and is not a weakening: the ATTRIBUTION check
	# (Get-AttributionVerdict) runs regardless and is the half that gates a deploy.
	# But silence must be ATTRIBUTABLE, which is what #2983 was about - the caller
	# never acquired a declaration, so this returned empty on every real invocation
	# and the script printed OK. The status now says which of the two silences it
	# is, and Get-DeclarationSuppression below turns that into a line the operator
	# reads. A check that cannot fire must say so where a passing one would not.
	if ($Manifest.PSObject.Properties.Name -contains 'DeclarationStatus') {
		if ($Manifest.DeclarationStatus -ne $script:DeclarationAvailable) {
			return ,$divergences
		}
	}
	elseif ($Manifest.PSObject.Properties.Name -contains 'DeclarationAvailable' -and
		-not $Manifest.DeclarationAvailable) {
		return ,$divergences
	}

	foreach ($record in $Manifest.Records) {
		$declaredAbsent = $null -eq $record.Declared -or $record.Declared.Length -eq 0
		$effectiveAbsent = $null -eq $record.Effective -or $record.Effective.Length -eq 0

		if ($declaredAbsent -and $effectiveAbsent) {
			continue
		}

		if ($declaredAbsent) {
			$divergences += "$($record.Name) is ABSENT from the resolved configuration but PRESENT in the " +
				"container as '$($record.Effective)'. It came from somewhere this manifest cannot see, so " +
				'its value is not attributable to any tracked file.'
			continue
		}

		if ($effectiveAbsent) {
			$divergences += "$($record.Name) is DECLARED as '$($record.Declared)' but is ABSENT from the " +
				'container. The running deployment is not the one the configuration describes.'
			continue
		}

		if ($record.Declared -ne $record.Effective) {
			$divergences += "$($record.Name) is DECLARED as '$($record.Declared)' but the container carries " +
				"'$($record.Effective)'. Something between the file and the container rewrote it."
		}
	}

	return ,$divergences
}

<#
.SYNOPSIS
	Reports, in one line, that the divergence check did NOT run and why.

.DESCRIPTION
	Returns an empty string when the check ran. Otherwise it names the reason,
	so a suppressed check is visible in exactly the place a reader looks for its
	verdict.

	This exists because #2983 was not, at bottom, a missing `docker compose`
	call. It was that the absence of one was INDISTINGUISHABLE from a clean
	result: Get-DeployManifestDivergence returned zero divergences, the caller
	printed OK, and nothing anywhere said the comparison had not happened. The
	acquisition fix stops that arising; this stops it being silent if it ever
	arises again by another route - a resolution failure, a caller that supplies
	only the effective half, a future overlay that will not resolve.

	The general form is worth stating, because it recurs across this epic: a
	check that can be suppressed needs a channel for "suppressed" that is not
	the same channel as "passed". Zero findings and no findings possible are
	different facts, and rendering them identically is what makes a guard
	report success for work it never did.
#>
function Get-DeclarationSuppression {
	[CmdletBinding()]
	[OutputType([string])]
	param(
		[Parameter(Mandatory)] [pscustomobject] $Manifest
	)

	if (-not ($Manifest.PSObject.Properties.Name -contains 'DeclarationStatus')) {
		return ''
	}

	$status = $Manifest.DeclarationStatus
	$reason = if ($Manifest.PSObject.Properties.Name -contains 'DeclarationReason') { [string] $Manifest.DeclarationReason } else { '' }
	$suffix = if ([string]::IsNullOrWhiteSpace($reason)) { '' } else { " Reason: $reason" }

	switch ($status) {
		'Available' { return '' }
		'Unreadable' {
			return 'DECLARED HALF UNREADABLE: the compose configuration could not be resolved, so the ' +
				'declared/effective divergence check DID NOT RUN. Every declared value below is ' +
				"<unreadable>, which is not the same claim as <absent>.$suffix"
		}
		default {
			return 'DECLARED HALF NOT RESOLVED: no compose resolution was attempted, so the ' +
				'declared/effective divergence check DID NOT RUN. Declared values below are ' +
				"<not-resolved> and assert nothing about the configuration.$suffix"
		}
	}
}

<#
.SYNOPSIS
	Names every attribution-relevant variable that moved between two manifests.

.DESCRIPTION
	Compares EFFECTIVE values, because effective is what a measurement was taken
	under. Comparing declared values would produce a diff of intentions.

	Each delta carries the variable's Attribution string, so the diff says not
	merely that something moved but what a reader's conclusion is now exposed
	to. That is the difference between a log line and evidence.

	A variable absent from the baseline entirely - because the baseline predates
	its being recorded - is reported as Kind 'Unrecorded' rather than as a
	movement. Calling it a change would manufacture a delta out of an
	improvement to the instrument, which is its own way of voiding a comparison.
#>
function Compare-DeployManifest {
	[CmdletBinding()]
	[OutputType([pscustomobject])]
	param(
		[Parameter(Mandatory)] [pscustomobject] $Baseline,
		[Parameter(Mandatory)] [pscustomobject] $Current
	)

	$deltas = @()
	$baselineByName = @{}

	foreach ($record in $Baseline.Records) {
		$baselineByName[$record.Name] = $record
	}

	foreach ($record in $Current.Records) {
		if (-not $baselineByName.ContainsKey($record.Name)) {
			$deltas += [pscustomobject]@{
				Name = $record.Name
				Kind = 'Unrecorded'
				Was = $null
				Now = $record.Effective
				Attribution = $record.Attribution
			}
			continue
		}

		$was = $baselineByName[$record.Name].Effective
		$now = $record.Effective

		if ($was -eq $now) {
			continue
		}

		$kind = if ($null -eq $was -or $was.Length -eq 0) { 'Pinned' }
			elseif ($null -eq $now -or $now.Length -eq 0) { 'Unpinned' }
			else { 'Changed' }

		$deltas += [pscustomobject]@{
			Name = $record.Name
			Kind = $kind
			Was = $was
			Now = $now
			Attribution = $record.Attribution
		}
	}

	return ,$deltas
}

<#
.SYNOPSIS
	Decides whether a step is attributable, and says why not when it is not.

.DESCRIPTION
	THIS IS THE POINT OF THE FILE. Everything above records; this adjudicates.

	A step that moves ONE attribution-relevant variable supports attributing a
	measured movement to it. A step that moves TWO supports attributing it to
	neither, which is what happened to the 16 -> 6 reading: the gate knob moved
	and the processor-count pin was removed together, and both are sufficient.

	'Unrecorded' deltas are excluded from the count. They are an artefact of the
	instrument improving, not of the deployment changing, and counting them
	would make the first run after any addition here permanently unattributable
	- a guard that cries wolf on its own installation is a guard that gets
	switched off.

	This never throws and never blocks by itself. It returns a verdict, and
	Assert-DeployManifest.ps1 turns a NotAttributable verdict into a non-zero
	exit UNLESS the operator acknowledged the multi-variable step. That is the
	whole mechanism: changing two variables at once stays POSSIBLE, because
	sometimes it is necessary, and stops being SILENT, which is what voids a
	comparison.
#>
function Get-AttributionVerdict {
	[CmdletBinding()]
	[OutputType([pscustomobject])]
	param(
		[Parameter(Mandatory)] [AllowEmptyCollection()] [array] $Deltas
	)

	$moved = @($Deltas | Where-Object { $_.Kind -ne 'Unrecorded' })

	if ($moved.Count -eq 0) {
		return [pscustomobject]@{
			Attributable = $true
			MovedCount = 0
			Summary = 'No attribution-relevant variable moved. A measured movement is attributable to the code under test.'
			Detail = @()
		}
	}

	if ($moved.Count -eq 1) {
		$one = $moved[0]
		return [pscustomobject]@{
			Attributable = $true
			MovedCount = 1
			Summary = "Exactly one attribution-relevant variable moved: $($one.Name) ($($one.Kind), " +
				"'$($one.Was)' -> '$($one.Now)'). A measured movement is attributable to it OR to the code " +
				'under test, and those two are separable only if one of them is held across a further run.'
			Detail = @("$($one.Name): $($one.Attribution)")
		}
	}

	$names = ($moved | ForEach-Object { $_.Name }) -join ', '

	return [pscustomobject]@{
		Attributable = $false
		MovedCount = $moved.Count
		Summary = "$($moved.Count) attribution-relevant variables moved in one step: $names. " +
			'Any measured movement now has more than one sufficient cause and is attributable to NONE of them. ' +
			'This is the defect recorded in issue #2931, where a WAL replay gate moving 16 -> 6 could not be ' +
			'attributed because a processor-count pin was removed in the same step.'
		Detail = @($moved | ForEach-Object {
			"$($_.Name) ($($_.Kind), '$($_.Was)' -> '$($_.Now)'): $($_.Attribution)"
		})
	}
}

<#
.SYNOPSIS
	Renders a manifest as the stable, citable text the run captures.

.DESCRIPTION
	Deterministic and ordered, so two manifests of the same configuration are
	byte-identical and `diff` is a meaningful operation on them. Written as a
	file rather than only to the console because the PM has to CITE it when
	scoring a run, and a scrollback is not evidence - it is not addressable, it
	is not durable, and it is not something a second reader can check.

	Absent is rendered as `<absent>` rather than as an empty field. An empty
	field beside a name reads as a value that happens to be blank, and this
	whole issue is a case of absent and empty being confused.
#>
function Format-DeployManifest {
	[CmdletBinding()]
	[OutputType([string])]
	param(
		[Parameter(Mandatory)] [pscustomobject] $Manifest,
		[AllowNull()] [AllowEmptyString()] [string] $GeneratedAt
	)

	$stamp = if ([string]::IsNullOrWhiteSpace($GeneratedAt)) { '<unrecorded>' } else { $GeneratedAt }
	$lines = @()

	$lines += '# DEPLOY MANIFEST - attribution-relevant configuration (issue #2931)'
	$lines += '#'
	$lines += '# Every variable below is sufficient ON ITS OWN to move a throughput,'
	$lines += '# latency or concurrency reading. Two of them moving in one step makes a'
	$lines += '# measured movement attributable to neither. Diff this file between runs'
	$lines += '# BEFORE scoring one against the other.'
	$lines += '#'
	$lines += "# label        : $($Manifest.Label)"
	$lines += "# generated at : $stamp"
	$lines += ''
	$lines += "PROCESSOR_COUNT_RESOLVED=$(if ($null -eq $Manifest.ProcessorCount.Resolved) { '<indeterminate>' } else { $Manifest.ProcessorCount.Resolved })"
	$lines += "PROCESSOR_COUNT_SOURCE=$($Manifest.ProcessorCount.Source)"

	# Recorded IN the manifest, not merely on the console, because the file is what
	# gets cited when a run is scored. A reader who cannot tell from the file alone
	# whether the declared column is evidence or an unfilled gap will read it as
	# evidence - which is how #2983 survived thirteen deployments.
	$status = if ($Manifest.PSObject.Properties.Name -contains 'DeclarationStatus' -and
		-not [string]::IsNullOrWhiteSpace($Manifest.DeclarationStatus)) { $Manifest.DeclarationStatus } else { 'NotAttempted' }
	$reason = if ($Manifest.PSObject.Properties.Name -contains 'DeclarationReason') { [string] $Manifest.DeclarationReason } else { '' }
	$lines += "DECLARATION_STATUS=$status"
	$lines += "DECLARATION_REASON=$(if ([string]::IsNullOrWhiteSpace($reason)) { '<none>' } else { $reason })"
	$lines += ''

	# `<absent>` asserts the key is not declared. That is only true when the
	# declaration was actually read, so when it was not, the declared column renders
	# as `<unreadable>` or `<not-resolved>` instead. Same width, different claim.
	$declaredPlaceholder = switch ($status) {
		'Available' { '<absent>' }
		'Unreadable' { '<unreadable>' }
		default { '<not-resolved>' }
	}

	foreach ($record in $Manifest.Records) {
		$declared = if ($null -eq $record.Declared -or $record.Declared.Length -eq 0) { $declaredPlaceholder } else { $record.Declared }
		$effective = if ($null -eq $record.Effective -or $record.Effective.Length -eq 0) { '<absent>' } else { $record.Effective }
		$lines += "$($record.Name)|declared=$declared|effective=$effective"
	}

	return ($lines -join "`n") + "`n"
}

<#
.SYNOPSIS
	Reads back a manifest rendered by Format-DeployManifest.

.DESCRIPTION
	The inverse of the renderer, and deliberately tolerant in exactly one
	direction: a line it does not understand is skipped, so a manifest written
	by a later version with extra fields still yields a usable baseline instead
	of refusing to compare at all. A baseline that cannot be read is a
	comparison that silently does not happen, which is the failure mode this
	file exists to remove - so the tolerant choice is the safe one HERE, though
	it would not be in the adjudicating functions above.

	Round-trip fidelity on the fields it does understand is asserted by
	Test-DeployManifest.ps1; without that the renderer and this could drift and
	every diff would be against a subtly different object.
#>
function Read-DeployManifest {
	[CmdletBinding()]
	[OutputType([pscustomobject])]
	param(
		[Parameter(Mandatory)] [AllowEmptyString()] [string] $Text
	)

	$records = @()
	$label = 'unlabelled'
	$resolved = $null
	$source = $script:ProvenanceIndeterminate
	$attribution = @{}
	# A manifest written before #2983 carries no DECLARATION_STATUS line. Defaulting
	# it to NotAttempted is the honest reading of such a file: its declared column
	# was never acquired. Defaulting to Available would retroactively promote every
	# historical `<absent>` into a positive finding it never was.
	$declarationStatus = $script:DeclarationNotAttempted
	$declarationReason = 'no DECLARATION_STATUS recorded; manifest predates #2983'

	foreach ($variable in @(Get-AttributionVariable) + @(Get-AttributionGrant)) {
		$attribution[$variable.Name] = $variable
	}

	foreach ($line in ($Text -split "`r?`n")) {
		$trimmed = $line.Trim()

		if ($trimmed.StartsWith('# label')) {
			$separator = $trimmed.IndexOf(':')
			if ($separator -ge 0) {
				$label = $trimmed.Substring($separator + 1).Trim()
			}
			continue
		}

		if ($trimmed.Length -eq 0 -or $trimmed.StartsWith('#')) {
			continue
		}

		if ($trimmed.StartsWith('PROCESSOR_COUNT_RESOLVED=')) {
			$value = $trimmed.Substring('PROCESSOR_COUNT_RESOLVED='.Length)
			$parsed = 0
			$resolved = if ([int]::TryParse($value, [ref] $parsed)) { $parsed } else { $null }
			continue
		}

		if ($trimmed.StartsWith('PROCESSOR_COUNT_SOURCE=')) {
			$source = $trimmed.Substring('PROCESSOR_COUNT_SOURCE='.Length)
			continue
		}

		if ($trimmed.StartsWith('DECLARATION_STATUS=')) {
			$declarationStatus = $trimmed.Substring('DECLARATION_STATUS='.Length)
			continue
		}

		if ($trimmed.StartsWith('DECLARATION_REASON=')) {
			$value = $trimmed.Substring('DECLARATION_REASON='.Length)
			$declarationReason = if ($value -eq '<none>') { '' } else { $value }
			continue
		}

		$parts = $trimmed -split '\|'

		if ($parts.Count -ne 3 -or -not $parts[1].StartsWith('declared=') -or -not $parts[2].StartsWith('effective=')) {
			continue
		}

		$name = $parts[0]
		$declared = $parts[1].Substring('declared='.Length)
		$effective = $parts[2].Substring('effective='.Length)

		# All three placeholders read back as $null - there is no VALUE in any of
		# them. Which of the three it was is carried by DeclarationStatus, not by
		# the per-record field, so a reader cannot get the two out of step.
		$declaredValue = if ($declared -in @('<absent>', '<unreadable>', '<not-resolved>')) { $null } else { $declared }

		$records += [pscustomobject]@{
			Name = $name
			Service = if ($attribution.ContainsKey($name)) { $attribution[$name].Service } else { '<unknown>' }
			Declared = $declaredValue
			Effective = if ($effective -eq '<absent>') { $null } else { $effective }
			Attribution = if ($attribution.ContainsKey($name)) { $attribution[$name].Attribution } else { 'not enumerated by this version of the manifest' }
			Consumer = if ($attribution.ContainsKey($name)) { $attribution[$name].Consumer } else { '<unknown>' }
		}
	}

	return [pscustomobject]@{
		Label = $label
		Records = @($records)
		DeclarationStatus = $declarationStatus
		DeclarationReason = $declarationReason
		DeclarationAvailable = ($declarationStatus -eq $script:DeclarationAvailable)
		ProcessorCount = [pscustomobject]@{
			Resolved = $resolved
			Source = $source
			Declared = ''
			Reason = 'read back from a recorded manifest'
		}
	}
}
