#!/usr/bin/env pwsh
<#
.SYNOPSIS
	Pure adjudication helpers for the RepoContext container provenance check.

.DESCRIPTION
	Every function here is PURE: it takes readings as parameters and returns
	violations. Nothing in this file runs `docker`, runs `git`, or touches the
	filesystem. Assert-ContainerProvenance.ps1 does the acquisition and calls
	these; Test-ContainerProvenance.ps1 drives these against literal fixtures.

	That split is deliberate and is what makes the acceptance criterion
	reachable. The check exists to be run against a RUNNING container, which
	means the interesting failure - a container launched from a different
	checkout than the operator believes - cannot be manufactured on demand in a
	test. Separating the adjudication from the acquisition moves the whole
	decision into functions that can be handed a fabricated disagreement, so the
	FAILING direction is demonstrated rather than asserted. A check only ever
	observed passing is indistinguishable from one that cannot fail.

	Two conventions hold throughout:

	- Every function returns a (possibly empty) array of violation strings. It
	  never throws for a violation and never writes to the host. The caller
	  decides what a violation means.
	- A violation message names the DISAGREEING VALUES, not just the fact of
	  disagreement, because the operator's next question is always "then what IS
	  it running?" and a check that forces them to go and find out separately has
	  done half its job.
#>

Set-StrictMode -Version Latest

<#
.SYNOPSIS
	Canonicalises a filesystem path for comparison, without touching the disk.

.DESCRIPTION
	Unifies directory separators and strips trailing ones, so that
	`C:/dev/lattice/samples/RepoContextContainer/` and
	`C:\dev\lattice\samples\RepoContextContainer` compare equal. Deliberately
	does NOT resolve `.`, `..`, or symlinks: this function is pure, and a
	resolution that silently consulted the filesystem would make the comparison
	depend on the machine the check runs from rather than on the two values
	being compared.

	The caller is responsible for supplying already-absolute paths. In the
	acquisition script both sides come from a canonical source (a Docker label
	and `git rev-parse --show-toplevel`), so neither is relative.
#>
function ConvertTo-ProvenancePath {
	[CmdletBinding()]
	[OutputType([string])]
	param(
		[AllowNull()] [AllowEmptyString()] [string] $Path
	)

	if ([string]::IsNullOrWhiteSpace($Path)) { return '' }

	$normalised = $Path.Trim().Replace('/', '\')
	# Trim trailing separators, but never past a drive root ("C:\") or the
	# filesystem root ("\"), where the separator is part of the path rather
	# than a decoration on it.
	while ($normalised.Length -gt 1 -and $normalised.EndsWith('\')) {
		if ($normalised.Length -eq 3 -and $normalised[1] -eq ':') { break }
		$normalised = $normalised.Substring(0, $normalised.Length - 1)
	}

	return $normalised
}

<#
.SYNOPSIS
	Compares two paths for equality under the platform's case rules.

.DESCRIPTION
	Case sensitivity is an explicit PARAMETER rather than a fact this function
	discovers for itself, for a reason worth stating: comparing
	case-insensitively where the filesystem is case-sensitive makes two DIFFERENT
	paths compare EQUAL, which turns a real disagreement into a pass. That is the
	dangerous direction for a check whose entire job is to notice a
	disagreement, so the decision is surfaced to the caller and pinned by tests
	in both settings rather than inferred at the point of use.
#>
function Test-ProvenancePathsEqual {
	[CmdletBinding()]
	[OutputType([bool])]
	param(
		[AllowNull()] [AllowEmptyString()] [string] $Left,
		[AllowNull()] [AllowEmptyString()] [string] $Right,
		[bool] $CaseSensitive
	)

	$l = ConvertTo-ProvenancePath -Path $Left
	$r = ConvertTo-ProvenancePath -Path $Right

	if ($l -eq '' -or $r -eq '') { return $false }

	if ($CaseSensitive) { return $l -ceq $r }
	return $l -ieq $r
}

<#
.SYNOPSIS
	Check 1 of 4. The container was composed from the checkout the operator means.

.DESCRIPTION
	`docker compose up` reads its OWN working directory's compose files, whatever
	branch produced the image, and prints nothing that names a branch. So the
	image and the runtime configuration are two independent inputs and only the
	first is obviously version-controlled. This check reads the answer out of the
	channel that actually holds it: the labels Compose stamps onto the container.

	Refuses when the project working directory is absent or disagrees with the
	expected checkout, and when any resolved config file lies outside that
	working directory - an override file pulled in from elsewhere is exactly how
	the merged document stops matching the tracked one.
#>
function Get-ComposeProvenanceViolation {
	[CmdletBinding()]
	[OutputType([string[]])]
	param(
		[AllowNull()] [AllowEmptyString()] [string] $WorkingDirectory,
		[AllowNull()] [string[]] $ConfigFiles,
		[Parameter(Mandatory)] [string] $ExpectedCheckout,
		[bool] $CaseSensitive
	)

	$violations = [System.Collections.Generic.List[string]]::new()

	if ([string]::IsNullOrWhiteSpace($WorkingDirectory)) {
		$violations.Add("the container carries no com.docker.compose.project.working_dir label, so it cannot be shown to have come from '$ExpectedCheckout'; it was probably not started by Docker Compose")
		return ,$violations.ToArray()
	}

	if (-not (Test-ProvenancePathsEqual -Left $WorkingDirectory -Right $ExpectedCheckout -CaseSensitive $CaseSensitive)) {
		$violations.Add("compose project working directory is '$WorkingDirectory' but the expected checkout is '$ExpectedCheckout'; the runtime configuration under test came from a different working tree than the one being verified")
	}

	if ($null -eq $ConfigFiles -or $ConfigFiles.Count -eq 0) {
		$violations.Add('the container carries no com.docker.compose.project.config_files label, so which compose files were merged into the running configuration is unknown')
		return ,$violations.ToArray()
	}

	$normalisedWorkingDirectory = ConvertTo-ProvenancePath -Path $WorkingDirectory
	foreach ($configFile in $ConfigFiles) {
		if ([string]::IsNullOrWhiteSpace($configFile)) { continue }

		$normalisedFile = ConvertTo-ProvenancePath -Path $configFile
		$prefix = $normalisedWorkingDirectory + '\'
		$isUnder = if ($CaseSensitive) {
			$normalisedFile.StartsWith($prefix, [System.StringComparison]::Ordinal)
		}
		else {
			$normalisedFile.StartsWith($prefix, [System.StringComparison]::OrdinalIgnoreCase)
		}

		if (-not $isUnder) {
			$violations.Add("compose config file '$configFile' lies outside the project working directory '$WorkingDirectory'; a file merged in from elsewhere is not the tracked configuration")
		}
	}

	return ,$violations.ToArray()
}

<#
.SYNOPSIS
	Check 2 of 4. That checkout is at the commit the operator means.

.DESCRIPTION
	Reported as a value the operator reads rather than an inference they must
	make: knowing the container came from a given directory says nothing about
	which commit that directory was sitting on when `up` ran, and a worktree can
	move under a running container at any time.

	NOTE the shape of the comparison. The expected commit must be sourced
	INDEPENDENTLY of the resolved checkout - if it were defaulted to the HEAD of
	the directory the container resolved to, this check would compare a value
	against itself and pass unconditionally. That would be strictly worse than
	omitting it, because it would report as checked. The caller reads the
	expected commit from the operator's own checkout, which is the thing they
	believe they deployed.

	A short commit is accepted as a prefix of a long one, which is how operators
	actually paste them, but never below seven characters: a shorter prefix
	starts matching commits it did not mean.
#>
function Get-GitProvenanceViolation {
	[CmdletBinding()]
	[OutputType([string[]])]
	param(
		[AllowNull()] [AllowEmptyString()] [string] $ResolvedCommit,
		[AllowNull()] [AllowEmptyString()] [string] $ExpectedCommit
	)

	$violations = [System.Collections.Generic.List[string]]::new()

	if ([string]::IsNullOrWhiteSpace($ResolvedCommit)) {
		$violations.Add('the compose project working directory did not resolve to a git commit, so the deployed source revision is unknown; it may not be a git worktree at all')
		return ,$violations.ToArray()
	}

	if ([string]::IsNullOrWhiteSpace($ExpectedCommit)) {
		$violations.Add('no expected commit was supplied, so the deployed revision cannot be adjudicated')
		return ,$violations.ToArray()
	}

	$resolved = $ResolvedCommit.Trim()
	$expected = $ExpectedCommit.Trim()

	$shorter = if ($resolved.Length -le $expected.Length) { $resolved } else { $expected }
	$longer = if ($resolved.Length -le $expected.Length) { $expected } else { $resolved }

	if ($shorter.Length -lt 7) {
		$violations.Add("commit '$shorter' is shorter than seven characters, which is too short to identify a commit; supply a longer revision")
		return ,$violations.ToArray()
	}

	if (-not $longer.StartsWith($shorter, [System.StringComparison]::OrdinalIgnoreCase)) {
		$violations.Add("the deployed checkout is at commit '$resolved' but the expected commit is '$expected'; the running configuration predates or diverges from the revision being verified")
	}

	return ,$violations.ToArray()
}

<#
.SYNOPSIS
	Check 3 of 4. The container is running the image its tag currently names.

.DESCRIPTION
	Catches the stale container: an image rebuilt from a newer commit moves the
	tag, but a container started before that rebuild keeps running the old image
	id for as long as it is not recreated. `docker compose up -d` with no changed
	service definition will happily leave it in place, so a rebuild can appear to
	have been deployed when nothing was replaced.

	Compares image IDS, never tags. A tag is a mutable pointer, so comparing tag
	to tag is a comparison of two names for whatever is current and can never
	detect this.
#>
function Get-ImageProvenanceViolation {
	[CmdletBinding()]
	[OutputType([string[]])]
	param(
		[AllowNull()] [AllowEmptyString()] [string] $RunningImageId,
		[AllowNull()] [AllowEmptyString()] [string] $ExpectedImageId,
		[AllowNull()] [AllowEmptyString()] [string] $ImageReference
	)

	$violations = [System.Collections.Generic.List[string]]::new()
	$label = if ([string]::IsNullOrWhiteSpace($ImageReference)) { 'the expected image' } else { "image reference '$ImageReference'" }

	if ([string]::IsNullOrWhiteSpace($RunningImageId)) {
		$violations.Add('the running container reports no image id, so what it is executing cannot be established')
		return ,$violations.ToArray()
	}

	if ([string]::IsNullOrWhiteSpace($ExpectedImageId)) {
		$violations.Add("$label does not resolve to an image id on this host, so the running image cannot be adjudicated; the image may have been pruned or never built here")
		return ,$violations.ToArray()
	}

	if ($RunningImageId.Trim() -ine $ExpectedImageId.Trim()) {
		$violations.Add("the container is running image id '$RunningImageId' but $label now resolves to '$ExpectedImageId'; the container predates the current build and was never recreated")
	}

	return ,$violations.ToArray()
}

<#
.SYNOPSIS
	Check 4 of 4, and the only one that reads the channel the answer lives in.

.DESCRIPTION
	Checks 1 to 3 can ALL pass while the setting under test never reached the
	process. An override file merged into the resolved document, a variable
	dropped during an edit, or a container that predates the setting entirely
	will each leave the value unset in an environment whose compose provenance,
	git provenance, and image provenance are all impeccable.

	So this check asserts the value is present in the CONTAINER'S OWN
	ENVIRONMENT. The expected value is read from the intended checkout's compose
	file; the actual value is read from the running container. That the
	comparison crosses from tracked file to running process is the whole point:
	a check comparing tracked files to other tracked files would have been green
	throughout both failed gate runs, because the repository agreed with itself
	perfectly and the deployment simply resolved a different checkout.

	ABSENT and PRESENT-BUT-DIFFERENT are reported as distinct violations. They
	have different causes and different remedies, and collapsing them would
	reproduce in this check the very defect the check exists to catch.
#>
function Get-EnvironmentProvenanceViolation {
	[CmdletBinding()]
	[OutputType([string[]])]
	param(
		[AllowNull()] [string[]] $ContainerEnvironment,
		[Parameter(Mandatory)] [hashtable] $ExpectedSettings
	)

	$violations = [System.Collections.Generic.List[string]]::new()

	if ($ExpectedSettings.Count -eq 0) {
		$violations.Add('no candidate-only setting was supplied to assert, so this check would pass regardless of what the container holds; supply at least one setting that differs between the candidate and the baseline')
		return ,$violations.ToArray()
	}

	$actual = @{}
	foreach ($entry in @($ContainerEnvironment)) {
		if ([string]::IsNullOrWhiteSpace($entry)) { continue }
		$split = $entry.IndexOf('=')
		if ($split -lt 1) { continue }
		$actual[$entry.Substring(0, $split)] = $entry.Substring($split + 1)
	}

	foreach ($name in ($ExpectedSettings.Keys | Sort-Object)) {
		$expected = "$($ExpectedSettings[$name])"

		if (-not $actual.ContainsKey($name)) {
			$violations.Add("$name is ABSENT from the container's environment but the intended checkout declares it as '$expected'; the merged compose document that produced this container did not carry it")
			continue
		}

		$observed = "$($actual[$name])"
		if ($observed -cne $expected) {
			$violations.Add("$name is '$observed' in the container's environment but the intended checkout declares '$expected'; the running process is configured differently from the source being verified")
		}
	}

	return ,$violations.ToArray()
}

<#
.SYNOPSIS
	Runs all four checks over a set of readings and returns a full report.

.DESCRIPTION
	Returns an object carrying BOTH the violations and every value that was
	read, whether or not anything disagreed. Emitting the readings
	unconditionally is a requirement of the check rather than a convenience: a
	bare pass/fail forces the operator to go and find the values themselves at
	precisely the moment they have least reason to trust their own assumptions
	about where they live.
#>
function Get-ContainerProvenanceReport {
	[CmdletBinding()]
	param(
		[Parameter(Mandatory)] [hashtable] $Readings,
		[Parameter(Mandatory)] [string] $ExpectedCheckout,
		[Parameter(Mandatory)] [hashtable] $ExpectedSettings,
		[bool] $CaseSensitive
	)

	$violations = [System.Collections.Generic.List[string]]::new()

	$violations.AddRange([string[]] (Get-ComposeProvenanceViolation `
				-WorkingDirectory $Readings['ComposeWorkingDirectory'] `
				-ConfigFiles $Readings['ComposeConfigFiles'] `
				-ExpectedCheckout $ExpectedCheckout `
				-CaseSensitive $CaseSensitive))

	$violations.AddRange([string[]] (Get-GitProvenanceViolation `
				-ResolvedCommit $Readings['ResolvedCommit'] `
				-ExpectedCommit $Readings['ExpectedCommit']))

	$violations.AddRange([string[]] (Get-ImageProvenanceViolation `
				-RunningImageId $Readings['RunningImageId'] `
				-ExpectedImageId $Readings['ExpectedImageId'] `
				-ImageReference $Readings['ImageReference']))

	$violations.AddRange([string[]] (Get-EnvironmentProvenanceViolation `
				-ContainerEnvironment $Readings['ContainerEnvironment'] `
				-ExpectedSettings $ExpectedSettings))

	return [pscustomobject] @{
		Readings    = $Readings
		Violations  = $violations.ToArray()
		IsSatisfied = ($violations.Count -eq 0)
	}
}
