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
	Parses a Compose duration ("120s", "2m", "1m30s") to whole seconds.

.DESCRIPTION
	Returns $null when the value is absent or is not a duration the Compose
	specification would accept, so a caller can distinguish "not a duration" from
	"zero seconds". A bare number is seconds, matching Compose.

	This exists because COMPOSE NORMALISES DURATIONS AND A LITERAL COMPARISON
	THEREFORE PRODUCES FALSE ACCUSATIONS. A file declaring `120s` resolves to
	`2m0s`, so a check comparing text reports the setting as wrong - or absent -
	on a stack that is correctly configured. That failure mode is worse than the
	one this whole script exists to catch: it accuses a good deployment of
	precisely the defect that invalidated two gate runs, and the obvious remedy
	for the accusation is to change a deployment that was already right.

	Semantics deliberately match ConvertFrom-RigComposeDuration in
	benchmark/coldstart-rig/scripts/_rig-helpers.ps1. The two guards are
	different instruments (see the header of Assert-ContainerProvenance.ps1), but
	agreeing on what a duration MEANS is not a coupling, and disagreeing would
	mean one of them refusing a stack the other accepts for no reason an operator
	could act on.
#>
function ConvertFrom-ProvenanceDuration {
	[CmdletBinding()]
	param([AllowNull()] $Value)

	$text = "$Value".Trim()
	if ([string]::IsNullOrWhiteSpace($text)) { return $null }

	if ($text -match '^\d+$') { return [int] $text }

	$matched = [regex]::Matches($text, '(?<n>\d+)(?<u>h|m|s)')
	if ($matched.Count -eq 0) { return $null }
	# Reject trailing junk: the units must account for the whole string, or a
	# typo like "120sec onds" would silently parse as 120.
	if ((($matched | ForEach-Object { $_.Value }) -join '') -ne $text) { return $null }

	$total = 0
	foreach ($m in $matched) {
		$n = [int] $m.Groups['n'].Value
		switch ($m.Groups['u'].Value) {
			'h' { $total += $n * 3600 }
			'm' { $total += $n * 60 }
			's' { $total += $n }
		}
	}
	return $total
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
	expected checkout, when any resolved config file lies outside that working
	directory, when a named file is not on disk, and when the NUMBER of resolved
	files is not the number expected.

	THE COUNT ASSERTION IS THE LOAD-BEARING PART, and it is why this check reads
	the label rather than walking the repository. The sample stack's real
	deployment is TWO files: the tracked `docker-compose.yml`, which carries a
	`build:` stanza and no `image:`, and a `docker-compose.override.yml` that is
	UNTRACKED AND GITIGNORED (see .gitignore, where it is ignored deliberately
	because it is machine-local). That override is load-bearing: it supplies the
	`image:` pin the tracked file does not have, the memory limit, the CPU caps,
	and the scan-cadence variables every prior measurement on a given box was
	taken against.

	So a check that enumerated tracked files would not merely be incomplete, it
	would be WRONG IN THE DANGEROUS DIRECTION. Relaunching from a worktree that
	lacks the override silently drops the memory limit and the image pin while
	the tree looks perfectly correct, and a tracked-file check would pass that
	stack. The count is what fails on a file git has never heard of.

	Note the corollary: fixing a provenance defect by changing the launch
	directory is itself a provenance change, and it is not self-verifying.
#>
function Get-ComposeProvenanceViolation {
	[CmdletBinding()]
	[OutputType([string[]])]
	param(
		[AllowNull()] [AllowEmptyString()] [string] $WorkingDirectory,
		[AllowNull()] [string[]] $ConfigFiles,
		[Parameter(Mandatory)] [string] $ExpectedCheckout,
		[int] $ExpectedConfigFileCount = 0,
		[AllowNull()] [string[]] $MissingConfigFiles,
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

	# A file the label names but the disk does not have means the container was
	# composed from a document that can no longer be reproduced, which is not a
	# lesser problem than a wrong one.
	foreach ($missing in @($MissingConfigFiles)) {
		if ([string]::IsNullOrWhiteSpace($missing)) { continue }
		$violations.Add("compose config file '$missing' is named by the container but is not present on disk; the running configuration cannot be reproduced from this checkout")
	}

	# Deliberately compares the COUNT, not the tracked set. The stack's real
	# deployment includes a gitignored override supplying the image pin, the
	# memory limit and the CPU caps, so a stack launched from a directory that
	# lacks it resolves fewer files while every remaining path still looks right.
	if ($ExpectedConfigFileCount -gt 0 -and $ConfigFiles.Count -ne $ExpectedConfigFileCount) {
		$named = ($ConfigFiles -join ', ')
		$violations.Add("the container resolved $($ConfigFiles.Count) compose file(s) but $ExpectedConfigFileCount were expected ($named); a missing override drops settings such as the image pin and the memory limit while leaving every remaining file correct")
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
		if ($observed -ceq $expected) { continue }

		# Literal inequality is not disagreement when both sides are durations:
		# Compose normalises `120s` to `2m0s`, so a text comparison accuses a
		# correctly configured stack of the exact defect this script exists to
		# find. Fall back to the literal comparison only when the values are not
		# both durations, so a genuine mismatch is still refused.
		$expectedSeconds = ConvertFrom-ProvenanceDuration -Value $expected
		$observedSeconds = ConvertFrom-ProvenanceDuration -Value $observed
		if ($null -ne $expectedSeconds -and $null -ne $observedSeconds) {
			if ($expectedSeconds -eq $observedSeconds) { continue }
			$violations.Add("$name is '$observed' ($observedSeconds s) in the container's environment but the intended checkout declares '$expected' ($expectedSeconds s); the running process is configured differently from the source being verified")
			continue
		}

		$violations.Add("$name is '$observed' in the container's environment but the intended checkout declares '$expected'; the running process is configured differently from the source being verified")
	}

	return ,$violations.ToArray()
}

<#
.SYNOPSIS
	Rewrites a Docker Desktop host-mount path back into the host path an operator
	would recognise.

.DESCRIPTION
	Docker Desktop reports some bind sources through its own Linux VM view, as
	`/run/desktop/mnt/host/c/dev/x`, rather than as `C:\dev\x`. Both name the same
	directory, and the first is what `docker inspect` returned for the archive
	bind in issue #2627 while the very same container reported `C:\dev` for the
	workspace bind.

	That asymmetry matters here rather than being cosmetic. A check that compared
	the reported source against a Windows path, or asked whether it was absolute
	in Windows terms, would silently take the VM form for something else and reach
	a wrong verdict on the one reading it exists to adjudicate. So the form is
	normalised once, at the boundary, and everything downstream sees one shape.

	Pure: it rewrites a string and never consults the filesystem. A path that does
	not carry the prefix is returned unchanged, so a genuine Linux host path is
	left alone.
#>
function ConvertFrom-DockerDesktopHostPath {
	[CmdletBinding()]
	[OutputType([string])]
	param(
		[AllowNull()] [AllowEmptyString()] [string] $Path
	)

	if ([string]::IsNullOrWhiteSpace($Path)) { return '' }

	$value = $Path.Trim()
	$prefix = '/run/desktop/mnt/host/'
	if (-not $value.StartsWith($prefix, [System.StringComparison]::OrdinalIgnoreCase)) { return $value }

	$remainder = $value.Substring($prefix.Length)
	if ($remainder.Length -eq 0) { return $value }

	$drive = $remainder.Substring(0, 1)
	if ($drive -notmatch '^[A-Za-z]$') { return $value }

	$rest = if ($remainder.Length -gt 1) { $remainder.Substring(1) } else { '' }
	if ($rest.Length -gt 0 -and $rest[0] -ne '/') { return $value }

	return ($drive.ToUpperInvariant() + ':' + $rest).Replace('/', '\')
}

<#
.SYNOPSIS
	Whether a path names a location absolutely, on either platform's rules.

.DESCRIPTION
	Accepts a drive-rooted Windows path (`C:\x`), a UNC path (`\\server\share`),
	and a rooted POSIX path (`/x`). Everything else - `./memory-archive`,
	`memory-archive`, `..\x` - is relative and is exactly the shape whose meaning
	depends on where the operator happened to be standing.

	Deliberately accepts BOTH platforms' forms rather than branching on the host
	this script runs from: the value being judged came out of a container's mount
	table, not out of this process, and it can legitimately be either.
#>
function Test-ProvenancePathIsAbsolute {
	[CmdletBinding()]
	[OutputType([bool])]
	param(
		[AllowNull()] [AllowEmptyString()] [string] $Path
	)

	if ([string]::IsNullOrWhiteSpace($Path)) { return $false }

	$value = $Path.Trim()

	if ($value.StartsWith('\\') -or $value.StartsWith('//')) { return $true }
	if ($value.StartsWith('/')) { return $true }
	if ($value -match '^[A-Za-z]:[\\/]') { return $true }

	return $false
}

<#
.SYNOPSIS
	Check 5 of 5. The archive holding durable memory did not land somewhere a
	routine cleanup deletes.

.DESCRIPTION
	This is the only check here that reads the WRITE path, and the reason it
	exists is that the other four read inputs. Checks 1 to 3 establish where the
	stack was composed from, what revision it carries, and which image it runs.
	Not one of them looks at a bind DESTINATION, so all four can agree perfectly
	on a container whose durable output is being written into a directory that
	`git worktree remove` deletes.

	Worse than not covering it: check 2 REQUIRES the compose directory to be a
	git worktree, correctly, because a gate run deliberately composes from the
	candidate worktree and composing from elsewhere is the #2617 defect. Compose
	resolved the archive's relative default against that same directory. So the
	single fact "this stack was composed from a git worktree" was simultaneously
	the certified-correct state for the source tree and the cause of the archive
	landing somewhere deletable (issue #2627).

	THIS CHECK THEREFORE KEYS ON THE ARCHIVE PATH AND NOTHING ELSE. It is handed
	no compose directory and no expected checkout, so it cannot be tempted into
	refusing a worktree working directory - which would contradict check 2 and
	refuse every legitimate gate run. The parameter list is the guarantee, not a
	comment about one.

	The readings are supplied by the caller, which is what keeps this pure and
	testable: `ArchiveSource` and `ArchiveMountType` come from the container's own
	mount table, and `GitToplevel` / `IsLinkedWorktree` from a git query run
	against that source on the host. `GitToplevel` empty means the archive is not
	inside any git checkout, which is the state this check wants.

	A linked worktree and an ordinary checkout are reported as DISTINCT
	violations. They have different lifetimes and different remedies - a worktree
	is removed wholesale by a single command, a checkout survives until someone
	deletes it but still loses the directory to `git clean -xdf` - and an operator
	who is told only "inside git" will reach for the wrong one.
#>
function Get-ArchiveDurabilityViolation {
	[CmdletBinding()]
	[OutputType([string[]])]
	param(
		[AllowNull()] [AllowEmptyString()] [string] $ArchiveDestination,
		[AllowNull()] [AllowEmptyString()] [string] $ArchiveSource,
		[AllowNull()] [AllowEmptyString()] [string] $ArchiveMountType,
		[AllowNull()] [AllowEmptyString()] [string] $GitToplevel,
		[bool] $IsLinkedWorktree,
		[bool] $SourceExistsOnHost = $true
	)

	$violations = [System.Collections.Generic.List[string]]::new()
	$destination = if ([string]::IsNullOrWhiteSpace($ArchiveDestination)) { '/memory-archive' } else { $ArchiveDestination.Trim() }

	if ([string]::IsNullOrWhiteSpace($ArchiveSource)) {
		$violations.Add("nothing is mounted at '$destination', so the only copy of durable agent memory that survives 'docker compose down -v' does not exist; the live tree in the /data volume is all there is")
		return ,$violations.ToArray()
	}

	$source = ConvertFrom-DockerDesktopHostPath -Path $ArchiveSource

	if (-not [string]::IsNullOrWhiteSpace($ArchiveMountType) -and $ArchiveMountType.Trim() -ine 'bind') {
		$violations.Add("'$destination' is a '$($ArchiveMountType.Trim())' mount sourced from '$source', not a bind mount; 'docker compose down -v' removes every volume the project declares, so this archive dies in the same command as the store it exists to outlive")
		return ,$violations.ToArray()
	}

	if (-not (Test-ProvenancePathIsAbsolute -Path $source)) {
		$violations.Add("the archive bound at '$destination' resolved to the relative source '$source'; a relative bind source is resolved against the directory 'docker compose' was invoked from, so the location of the only surviving copy of durable memory is a function of where the operator was standing")
		return ,$violations.ToArray()
	}

	if (-not $SourceExistsOnHost) {
		$violations.Add("the archive bound at '$destination' is at '$source', which does not exist from where this check is running, so whether it sits inside a git worktree CANNOT BE ESTABLISHED; either this is not the docker host, or the directory has already been deleted. This is reported rather than passed over, because a check that cannot look must not report clean")
		return ,$violations.ToArray()
	}

	if ([string]::IsNullOrWhiteSpace($GitToplevel)) {
		return ,$violations.ToArray()
	}
	$toplevel = ConvertFrom-DockerDesktopHostPath -Path $GitToplevel

	if ($IsLinkedWorktree) {
		$violations.Add("the archive bound at '$destination' is at '$source', which is inside the LINKED GIT WORKTREE rooted at '$toplevel'; 'git worktree remove', a session cleanup, or a tidy of a worktree collection deletes that tree and the only surviving copy of durable agent memory with it, without warning. Set REPOCONTEXT_MEMORY_ARCHIVE_PATH to an absolute path outside every checkout")
		return ,$violations.ToArray()
	}

	$violations.Add("the archive bound at '$destination' is at '$source', which is inside the GIT CHECKOUT rooted at '$toplevel'; 'git clean -xdf' removes it and deleting the checkout takes it, so the only surviving copy of durable agent memory shares the lifetime of a working tree. Set REPOCONTEXT_MEMORY_ARCHIVE_PATH to an absolute path outside every checkout")
	return ,$violations.ToArray()
}

<#
.SYNOPSIS
	Runs all five checks over a set of readings and returns a full report.

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
		[int] $ExpectedConfigFileCount = 0,
		[bool] $CaseSensitive
	)

	$violations = [System.Collections.Generic.List[string]]::new()

	$violations.AddRange([string[]] (Get-ComposeProvenanceViolation `
				-WorkingDirectory $Readings['ComposeWorkingDirectory'] `
				-ConfigFiles $Readings['ComposeConfigFiles'] `
				-ExpectedCheckout $ExpectedCheckout `
				-ExpectedConfigFileCount $ExpectedConfigFileCount `
				-MissingConfigFiles $Readings['MissingConfigFiles'] `
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

	# Check 5 is handed ONLY archive readings. Not the compose working directory,
	# not the expected checkout: the composite is where a guard over the write
	# path would most easily acquire a dependency on the read path, and check 2
	# requires the compose directory to be a worktree.
	$violations.AddRange([string[]] (Get-ArchiveDurabilityViolation `
				-ArchiveDestination $Readings['ArchiveDestination'] `
				-ArchiveSource $Readings['ArchiveSource'] `
				-ArchiveMountType $Readings['ArchiveMountType'] `
				-GitToplevel $Readings['ArchiveGitToplevel'] `
				-IsLinkedWorktree ([bool] $Readings['ArchiveIsLinkedWorktree']) `
				-SourceExistsOnHost ([bool] $Readings['ArchiveSourceExistsOnHost'])))

	return [pscustomobject] @{
		Readings    = $Readings
		Violations  = $violations.ToArray()
		IsSatisfied = ($violations.Count -eq 0)
	}
}
