#!/usr/bin/env pwsh
<#
.SYNOPSIS
	Regression tests for the RepoContext container provenance check.

.DESCRIPTION
	Exercises the pure adjudication functions in _provenance.ps1 against literal
	fixtures, so the check's correctness can be verified without Docker, without
	a running container, and without a second checkout. Pure pwsh; no Pester
	dependency, matching benchmark/coldstart-rig/scripts/Test-RigHelpers.ps1.

	EVERY CHECK IS TESTED IN BOTH DIRECTIONS, and that is the point of this file
	rather than a courtesy. A check only ever observed passing is
	indistinguishable from one that cannot fail: both are consistent with the
	mechanism working and with the mechanism being unwired, and only a case that
	is supposed to REFUSE separates them. So each of the five checks has at least
	one fixture it accepts and at least one it refuses, and the refusals assert
	on the violation TEXT, since a violation that does not name the disagreeing
	values leaves the operator to go and find them.

	Three fixtures reconstruct real defects. `Compose provenance REFUSES the
	gate run 1/2 shape` and `Composite REFUSES the reconstructed gate run 2
	readings` use the actual observed values: a container whose compose label
	resolved to the main checkout while the operator stood in the candidate
	worktree, with LATTICE_REPOCONTEXT_STOP_GRACE_PERIOD absent from its
	environment despite being merged on the candidate branch. `Archive
	durability REFUSES the measured issue #2627 reading` uses the mount source
	`docker inspect` actually returned for the archive bind, VM path form and
	all.

	Check 5's section carries one fixture the others do not need, and it is the
	load-bearing one: a composite whose compose working directory IS a git
	worktree - the certified-correct gate state that check 2 REQUIRES - and whose
	archive is elsewhere must be ACCEPTED. Without it, a check 5 that refused on
	the compose directory rather than the archive path would satisfy every
	refusal fixture in this file while contradicting check 2 and refusing every
	legitimate gate run.

	Exits with code 0 when every assertion passes and a non-zero count (equal to
	the number of failed assertions) when one or more fails:

		pwsh -File ./Test-ContainerProvenance.ps1
		if ($LASTEXITCODE -ne 0) { throw "container provenance tests failed" }

.EXAMPLE
	cd samples/RepoContextContainer/scripts
	pwsh -File Test-ContainerProvenance.ps1

.NOTES
	Needs no Docker, no git, and no running container, so unlike the script it
	tests it is safe to run anywhere - including CI, where
	RepoContextContainerProvenanceScriptTests drives it. Run it by hand after
	changing _provenance.ps1.

	The suite is worth exactly as much as its own discrimination, which is not
	established by it passing. To measure that, break the code and count: commit
	first, then make one function return no violations unconditionally, re-run,
	and confirm the failures are the ones that check that function and NOT the
	others. Identical Totals across the two arms prove both runs executed the
	same set of assertions rather than one arm silently running fewer.
#>
[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$here = Split-Path -Parent $MyInvocation.MyCommand.Path
. (Join-Path $here '_provenance.ps1')

$script:_PassCount = 0
$script:_FailCount = 0

function _Assert {
	param(
		[Parameter(Mandatory)] [string] $Name,
		[Parameter(Mandatory)] [bool] $Condition,
		[string] $Detail = ''
	)
	if ($Condition) {
		$script:_PassCount++
		Write-Host ("  PASS  {0}" -f $Name) -ForegroundColor Green
	}
	else {
		$script:_FailCount++
		Write-Host ("  FAIL  {0}  {1}" -f $Name, $Detail) -ForegroundColor Red
	}
}

function _Section {
	param([Parameter(Mandatory)] [string] $Name)
	Write-Host ''
	Write-Host $Name -ForegroundColor Cyan
}

# The checkout the operator believes they deployed, and the one the container
# actually resolved to. These are the two real paths from the epic #2368 gate
# runs, and the entire defect is that nothing in `docker compose up`'s output
# distinguishes them.
$candidateCheckout = 'C:\dev\copilot-worktrees\lattice\bucket4\samples\RepoContextContainer'
$mainCheckout = 'C:\dev\lattice\samples\RepoContextContainer'

$candidateCommit = '9fcaaa998e1d4b7a2c5f0839ab61de7740c2b1a5'
$mainCommit = '3ab77c10f4e2d9856bb0143fcae62d99017b4e8a'

# The archive readings, in both directions. The refusing pair is the mount
# source `docker inspect` ACTUALLY returned in issue #2627 - note the Docker
# Desktop VM path form, which is what the same container reported for this bind
# while reporting a plain `C:\dev` for the workspace bind.
$worktreeArchiveSource = '/run/desktop/mnt/host/c/dev/copilot-worktrees/lattice/bucket4-merge/samples/RepoContextContainer/memory-archive'
$worktreeArchiveToplevel = 'C:\dev\copilot-worktrees\lattice\bucket4-merge'
$durableArchiveSource = 'C:\dev\repocontext-memory-archive'

# Join a leaf onto one of the FIXTURE paths above.
#
# `Join-Path` resolves a drive qualifier through the PSDrive provider, so
# `Join-Path 'C:\dev\x' 'y'` is a terminating error on Linux ("Cannot find
# drive. A drive with the name 'C' does not exist."), which stops this suite
# dead five assertions in. The fixtures are deliberately Windows-shaped - they
# are the readings `docker inspect` actually returned during the epic #2368 gate
# runs, and rewriting them to POSIX shapes would destroy the evidence the suite
# exists to encode - so they are joined textually instead, with the separator
# the base path already implies. Nothing here touches the filesystem.
function Join-FixturePath {
	param(
		[Parameter(Mandatory)][string] $Base,
		[Parameter(Mandatory)][string] $Leaf
	)

	$separator = if ($Base.Contains('\')) { '\' } else { '/' }
	return ($Base.TrimEnd('\', '/') + $separator + $Leaf)
}

function New-AgreeingReadings {
	return @{
		ContainerName             = 'repocontext'
		ComposeWorkingDirectory   = $candidateCheckout
		ComposeConfigFiles        = @((Join-FixturePath $candidateCheckout 'docker-compose.yml'))
		ResolvedCommit            = $candidateCommit
		ExpectedCommit            = $candidateCommit
		RunningImageId            = 'sha256:11112222333344445555666677778888999900001111222233334444555566667'
		ExpectedImageId           = 'sha256:11112222333344445555666677778888999900001111222233334444555566667'
		ImageReference            = 'orleans-lattice/repocontext:local'
		ContainerEnvironment      = @('PATH=/usr/bin', 'LATTICE_REPOCONTEXT_STOP_GRACE_PERIOD=120s')
		ArchiveDestination        = '/memory-archive'
		ArchiveSource             = $durableArchiveSource
		ArchiveMountType          = 'bind'
		ArchiveSourceExistsOnHost = $true
		ArchiveGitReadingExaminable = $true
		ArchiveGitToplevel        = ''
		ArchiveIsLinkedWorktree   = $false
	}
}

$expectedSettings = @{ 'LATTICE_REPOCONTEXT_STOP_GRACE_PERIOD' = '120s' }

# ---------------------------------------------------------------------------
_Section 'Path normalisation'
# ---------------------------------------------------------------------------

_Assert -Name 'separators and trailing separator are normalised away' `
	-Condition ((ConvertTo-ProvenancePath -Path 'C:/dev/lattice/samples/RepoContextContainer/') -eq 'C:\dev\lattice\samples\RepoContextContainer')

_Assert -Name 'a drive root keeps its separator' `
	-Condition ((ConvertTo-ProvenancePath -Path 'C:\') -eq 'C:\')

_Assert -Name 'case-insensitive comparison ACCEPTS a case-only difference' `
	-Condition (Test-ProvenancePathsEqual -Left 'C:\Dev\Lattice' -Right 'c:\dev\lattice' -CaseSensitive $false)

# The dangerous direction. Comparing case-insensitively where the filesystem is
# case-sensitive makes two DIFFERENT directories compare equal, which converts a
# real disagreement into a pass in the one check whose only job is to notice a
# disagreement.
_Assert -Name 'case-sensitive comparison REFUSES a case-only difference' `
	-Condition (-not (Test-ProvenancePathsEqual -Left '/srv/Lattice' -Right '/srv/lattice' -CaseSensitive $true))

_Assert -Name 'an empty path is never equal to anything, including another empty path' `
	-Condition (-not (Test-ProvenancePathsEqual -Left '' -Right '' -CaseSensitive $false))

# ---------------------------------------------------------------------------
_Section 'Check 1 of 5: compose provenance'
# ---------------------------------------------------------------------------

$accepted = Get-ComposeProvenanceViolation `
	-WorkingDirectory $candidateCheckout `
	-ConfigFiles @((Join-FixturePath $candidateCheckout 'docker-compose.yml')) `
	-ExpectedCheckout $candidateCheckout -CaseSensitive $false
_Assert -Name 'ACCEPTS a container composed from the expected checkout' `
	-Condition ($accepted.Count -eq 0) -Detail ($accepted -join '; ')

# The real gate run 1/2 shape: the operator stood in the candidate worktree and
# the container had been composed from the main checkout.
$refused = Get-ComposeProvenanceViolation `
	-WorkingDirectory $mainCheckout `
	-ConfigFiles @((Join-FixturePath $mainCheckout 'docker-compose.yml')) `
	-ExpectedCheckout $candidateCheckout -CaseSensitive $false
_Assert -Name 'REFUSES the gate run 1/2 shape (composed from a different checkout)' `
	-Condition ($refused.Count -eq 1)
_Assert -Name 'and names BOTH the resolved and the expected directory' `
	-Condition ($refused.Count -ge 1 -and $refused[0].Contains($mainCheckout) -and $refused[0].Contains($candidateCheckout)) `
	-Detail ($refused -join '; ')

$refused = Get-ComposeProvenanceViolation -WorkingDirectory '' -ConfigFiles @() `
	-ExpectedCheckout $candidateCheckout -CaseSensitive $false
_Assert -Name 'REFUSES a container carrying no compose working_dir label' `
	-Condition ($refused.Count -eq 1 -and $refused[0].Contains('working_dir'))

$refused = Get-ComposeProvenanceViolation -WorkingDirectory $candidateCheckout -ConfigFiles $null `
	-ExpectedCheckout $candidateCheckout -CaseSensitive $false
_Assert -Name 'REFUSES a container carrying no compose config_files label' `
	-Condition ($refused.Count -eq 1 -and $refused[0].Contains('config_files'))

# An override file merged in from outside the project directory is how the
# resolved document stops matching the tracked one while every path still looks
# plausible.
$refused = Get-ComposeProvenanceViolation `
	-WorkingDirectory $candidateCheckout `
	-ConfigFiles @((Join-FixturePath $candidateCheckout 'docker-compose.yml'), 'C:\tmp\docker-compose.override.yml') `
	-ExpectedCheckout $candidateCheckout -CaseSensitive $false
_Assert -Name 'REFUSES a config file merged in from outside the project directory' `
	-Condition ($refused.Count -eq 1 -and $refused[0].Contains('override'))

# --- the untracked-override hazard, both directions ------------------------
# The real stack is base + a gitignored override supplying the image pin, the
# memory limit and the CPU caps. Every fixture below therefore uses a file git
# has never heard of: a check that could only reason about tracked files would
# be green throughout, which is the property that makes this class need its own
# instrument.
$base = Join-FixturePath $candidateCheckout 'docker-compose.yml'
$override = Join-FixturePath $candidateCheckout 'docker-compose.override.yml'

$accepted = Get-ComposeProvenanceViolation `
	-WorkingDirectory $candidateCheckout -ConfigFiles @($base, $override) `
	-ExpectedCheckout $candidateCheckout -ExpectedConfigFileCount 2 `
	-MissingConfigFiles @() -CaseSensitive $false
_Assert -Name 'ACCEPTS the documented two-file stack including the untracked override' `
	-Condition ($accepted.Count -eq 0) -Detail ($accepted -join '; ')

# The hazard the PM was one command away from creating: relaunching from a
# worktree that lacks the gitignored override. The working directory is right,
# the one file resolved is right, and the tree agrees with itself perfectly.
# Only the count dissents.
$refused = Get-ComposeProvenanceViolation `
	-WorkingDirectory $candidateCheckout -ConfigFiles @($base) `
	-ExpectedCheckout $candidateCheckout -ExpectedConfigFileCount 2 `
	-MissingConfigFiles @() -CaseSensitive $false
_Assert -Name 'REFUSES a stack that silently dropped its untracked override' `
	-Condition ($refused.Count -eq 1) -Detail ($refused -join '; ')
_Assert -Name 'and names BOTH counts so the operator knows what is missing' `
	-Condition ($refused.Count -ge 1 -and $refused[0].Contains('1 compose file') -and $refused[0].Contains('2 were expected')) `
	-Detail ($refused -join '; ')

$refused = Get-ComposeProvenanceViolation `
	-WorkingDirectory $candidateCheckout -ConfigFiles @($base, $override, (Join-FixturePath $candidateCheckout 'docker-compose.extra.yml')) `
	-ExpectedCheckout $candidateCheckout -ExpectedConfigFileCount 2 `
	-MissingConfigFiles @() -CaseSensitive $false
_Assert -Name 'REFUSES an UNEXPECTED EXTRA file as well as a missing one' `
	-Condition ($refused.Count -eq 1 -and $refused[0].Contains('3 compose file')) `
	-Detail ($refused -join '; ')

$refused = Get-ComposeProvenanceViolation `
	-WorkingDirectory $candidateCheckout -ConfigFiles @($base, $override) `
	-ExpectedCheckout $candidateCheckout -ExpectedConfigFileCount 2 `
	-MissingConfigFiles @($override) -CaseSensitive $false
_Assert -Name 'REFUSES a named config file that is no longer on disk' `
	-Condition ($refused.Count -eq 1 -and $refused[0].Contains('not present on disk')) `
	-Detail ($refused -join '; ')

# The paired negative for the count assertion itself. Left unpinned the check
# must not invent an expectation, or an operator who genuinely runs one file
# would be refused for it - and a check that refuses correct stacks gets
# switched off, taking the three checks that were working with it.
$accepted = Get-ComposeProvenanceViolation `
	-WorkingDirectory $candidateCheckout -ConfigFiles @($base) `
	-ExpectedCheckout $candidateCheckout -MissingConfigFiles @() -CaseSensitive $false
_Assert -Name 'does NOT assert a count when none was pinned' `
	-Condition ($accepted.Count -eq 0) -Detail ($accepted -join '; ')

$accepted = Get-ComposeProvenanceViolation `
	-WorkingDirectory $candidateCheckout -ConfigFiles @($base) `
	-ExpectedCheckout $candidateCheckout -ExpectedConfigFileCount 1 `
	-MissingConfigFiles @() -CaseSensitive $false
_Assert -Name 'ACCEPTS one file when running without an override was DECLARED' `
	-Condition ($accepted.Count -eq 0) -Detail ($accepted -join '; ')

# ---------------------------------------------------------------------------
_Section 'Check 2 of 5: git provenance'
# ---------------------------------------------------------------------------

_Assert -Name 'ACCEPTS a checkout sitting on the expected commit' `
	-Condition ((Get-GitProvenanceViolation -ResolvedCommit $candidateCommit -ExpectedCommit $candidateCommit).Count -eq 0)

_Assert -Name 'ACCEPTS a short commit that prefixes the resolved one' `
	-Condition ((Get-GitProvenanceViolation -ResolvedCommit $candidateCommit -ExpectedCommit '9fcaaa9').Count -eq 0)

$refused = Get-GitProvenanceViolation -ResolvedCommit $mainCommit -ExpectedCommit $candidateCommit
_Assert -Name 'REFUSES a checkout sitting on a different commit' -Condition ($refused.Count -eq 1)
_Assert -Name 'and names BOTH commits' `
	-Condition ($refused.Count -ge 1 -and $refused[0].Contains($mainCommit) -and $refused[0].Contains($candidateCommit)) `
	-Detail ($refused -join '; ')

_Assert -Name 'REFUSES a working directory that is not a git worktree' `
	-Condition ((Get-GitProvenanceViolation -ResolvedCommit '' -ExpectedCommit $candidateCommit).Count -eq 1)

_Assert -Name 'REFUSES an expected commit that was never supplied' `
	-Condition ((Get-GitProvenanceViolation -ResolvedCommit $candidateCommit -ExpectedCommit '').Count -eq 1)

# A prefix short enough to match commits it did not mean is refused rather than
# quietly accepted, because a loose match here reports as a verified revision.
_Assert -Name 'REFUSES a commit prefix shorter than seven characters' `
	-Condition ((Get-GitProvenanceViolation -ResolvedCommit $candidateCommit -ExpectedCommit '9fcaa').Count -eq 1)

# ---------------------------------------------------------------------------
_Section 'Check 3 of 5: image provenance'
# ---------------------------------------------------------------------------

_Assert -Name 'ACCEPTS a container running the image its reference resolves to' `
	-Condition ((Get-ImageProvenanceViolation -RunningImageId 'sha256:aaaa' -ExpectedImageId 'sha256:aaaa' -ImageReference 'repocontext:local').Count -eq 0)

$refused = Get-ImageProvenanceViolation -RunningImageId 'sha256:aaaa' -ExpectedImageId 'sha256:bbbb' -ImageReference 'repocontext:local'
_Assert -Name 'REFUSES a stale container left in place across a rebuild' -Condition ($refused.Count -eq 1)
_Assert -Name 'and names both image ids and the reference' `
	-Condition ($refused.Count -ge 1 -and $refused[0].Contains('sha256:aaaa') -and $refused[0].Contains('sha256:bbbb') -and $refused[0].Contains('repocontext:local')) `
	-Detail ($refused -join '; ')

_Assert -Name 'REFUSES a reference that resolves to no image on this host' `
	-Condition ((Get-ImageProvenanceViolation -RunningImageId 'sha256:aaaa' -ExpectedImageId '' -ImageReference 'repocontext:local').Count -eq 1)

_Assert -Name 'REFUSES a container reporting no image id' `
	-Condition ((Get-ImageProvenanceViolation -RunningImageId '' -ExpectedImageId 'sha256:bbbb' -ImageReference 'repocontext:local').Count -eq 1)

# ---------------------------------------------------------------------------
_Section 'Check 4 of 5: environment provenance'
# ---------------------------------------------------------------------------

_Assert -Name 'ACCEPTS a setting present in the container with the expected value' `
	-Condition ((Get-EnvironmentProvenanceViolation `
			-ContainerEnvironment @('PATH=/usr/bin', 'LATTICE_REPOCONTEXT_STOP_GRACE_PERIOD=120s') `
			-ExpectedSettings $expectedSettings).Count -eq 0)

# The gate run 1/2 reading: merged on the candidate branch, absent from the
# process. This is the case no comparison between tracked files could catch.
$refused = Get-EnvironmentProvenanceViolation `
	-ContainerEnvironment @('PATH=/usr/bin') `
	-ExpectedSettings $expectedSettings
_Assert -Name 'REFUSES a setting ABSENT from the container environment' -Condition ($refused.Count -eq 1)
_Assert -Name 'and reports it as ABSENT, naming the setting and the expected value' `
	-Condition ($refused.Count -ge 1 -and $refused[0].Contains('ABSENT') -and $refused[0].Contains('LATTICE_REPOCONTEXT_STOP_GRACE_PERIOD') -and $refused[0].Contains('120s')) `
	-Detail ($refused -join '; ')

# Absent and present-but-different have different causes and different remedies.
# Collapsing them would reproduce inside this check the exact defect the check
# exists to catch, so the distinctness is asserted rather than assumed.
$refused = Get-EnvironmentProvenanceViolation `
	-ContainerEnvironment @('LATTICE_REPOCONTEXT_STOP_GRACE_PERIOD=10s') `
	-ExpectedSettings $expectedSettings
_Assert -Name 'REFUSES a setting present with the wrong value' -Condition ($refused.Count -eq 1)
_Assert -Name 'and does NOT report a wrong value as ABSENT' `
	-Condition ($refused.Count -ge 1 -and -not $refused[0].Contains('ABSENT') -and $refused[0].Contains('10s') -and $refused[0].Contains('120s')) `
	-Detail ($refused -join '; ')

# A check with nothing to assert would pass against any container at all, which
# is the false green this whole item exists to prevent.
_Assert -Name 'REFUSES being asked to assert no settings at all' `
	-Condition ((Get-EnvironmentProvenanceViolation -ContainerEnvironment @('PATH=/usr/bin') -ExpectedSettings @{}).Count -eq 1)

_Assert -Name 'a setting whose value contains "=" is read whole' `
	-Condition ((Get-EnvironmentProvenanceViolation `
			-ContainerEnvironment @('CONNECTION=Host=db;Port=5432') `
			-ExpectedSettings @{ 'CONNECTION' = 'Host=db;Port=5432' }).Count -eq 0)

# --- duration normalisation, both directions -------------------------------
# Compose normalises durations: a file saying `120s` resolves to `2m0s`. A
# literal comparison therefore accuses a CORRECTLY configured stack of exactly
# the defect that invalidated two gate runs - and the obvious response to the
# accusation is to change a deployment that was already right. That is a worse
# failure than the blindness this script was written to fix, because it does not
# merely fail to answer, it answers wrongly with apparent authority.
foreach ($spelling in @('2m0s', '2m', '1m60s', '120')) {
	_Assert -Name "ACCEPTS '$spelling' as equal to the declared 120s" `
		-Condition ((Get-EnvironmentProvenanceViolation `
				-ContainerEnvironment @("LATTICE_REPOCONTEXT_STOP_GRACE_PERIOD=$spelling") `
				-ExpectedSettings $expectedSettings).Count -eq 0)
}

# The paired negative. Parsing durations must not become "any two durations
# agree" - a check that accepts every spelling AND every value has stopped
# being a check while still reporting green.
$refused = Get-EnvironmentProvenanceViolation `
	-ContainerEnvironment @('LATTICE_REPOCONTEXT_STOP_GRACE_PERIOD=2m1s') `
	-ExpectedSettings $expectedSettings
_Assert -Name 'REFUSES a duration that is genuinely different, however spelled' `
	-Condition ($refused.Count -eq 1) -Detail ($refused -join '; ')
_Assert -Name 'and reports both parsed values so the difference is legible' `
	-Condition ($refused.Count -ge 1 -and $refused[0].Contains('121 s') -and $refused[0].Contains('120 s')) `
	-Detail ($refused -join '; ')

# Non-durations must keep the strict literal comparison. Silently widening
# equality for values that merely look numeric would let a real drift pass.
$refused = Get-EnvironmentProvenanceViolation `
	-ContainerEnvironment @('LATTICE_REPOCONTEXT_MODE=Fast') `
	-ExpectedSettings @{ 'LATTICE_REPOCONTEXT_MODE' = 'fast' }
_Assert -Name 'REFUSES a case-only difference in a NON-duration setting' `
	-Condition ($refused.Count -eq 1) -Detail ($refused -join '; ')

_Assert -Name 'a duration-looking expected value against junk stays a refusal' `
	-Condition ((Get-EnvironmentProvenanceViolation `
			-ContainerEnvironment @('LATTICE_REPOCONTEXT_STOP_GRACE_PERIOD=120sec onds') `
			-ExpectedSettings $expectedSettings).Count -eq 1)

_Assert -Name 'ConvertFrom-ProvenanceDuration returns null for a non-duration' `
	-Condition ($null -eq (ConvertFrom-ProvenanceDuration -Value 'always'))
_Assert -Name 'ConvertFrom-ProvenanceDuration distinguishes absent from zero' `
	-Condition (($null -eq (ConvertFrom-ProvenanceDuration -Value '')) -and (0 -eq (ConvertFrom-ProvenanceDuration -Value '0s')))
_Assert -Name 'ConvertFrom-ProvenanceDuration handles compound units' `
	-Condition (5400 -eq (ConvertFrom-ProvenanceDuration -Value '1h30m'))

# ---------------------------------------------------------------------------
_Section 'Check 5 of 5: archive durability'
# ---------------------------------------------------------------------------

# --- the git reading must distinguish "no" from "no answer" -----------------
# The three stderr strings below are REAL git output, captured from git on this
# repository, not paraphrases. The orphaned-worktree case is the one that makes
# the parenthetical load-bearing: it contains the bare phrase and must NOT be
# recognised, because an orphaned linked worktree is precisely what check 5
# exists to catch.
$_gitMissStderr = 'fatal: not a git repository (or any of the parent directories): .git'
$_gitOrphanStderr = 'fatal: not a git repository: C:/dev/main/.git/worktrees/gone'
$_gitCorruptStderr = 'fatal: invalid gitfile format: C:/dev/broken/.git'

_Assert -Name 'a clean toplevel reading is examinable' `
	-Condition (Test-GitReadingIsExaminable -ExitCode 0 -StandardError '' -Toplevel 'C:\dev\lattice')

_Assert -Name 'a genuine not-a-repository answer is examinable' `
	-Condition (Test-GitReadingIsExaminable -ExitCode 128 -StandardError $_gitMissStderr -Toplevel '')

_Assert -Name 'an ORPHANED LINKED WORKTREE is NOT examinable, though its stderr carries the bare phrase' `
	-Condition (-not (Test-GitReadingIsExaminable -ExitCode 128 -StandardError $_gitOrphanStderr -Toplevel ''))

_Assert -Name 'and that is not incidental: the bare phrase IS present in that reading' `
	-Condition ($_gitOrphanStderr -match 'not a git repository')

_Assert -Name 'a corrupt gitfile is NOT examinable' `
	-Condition (-not (Test-GitReadingIsExaminable -ExitCode 128 -StandardError $_gitCorruptStderr -Toplevel ''))

_Assert -Name 'a safe.directory refusal is NOT examinable' `
	-Condition (-not (Test-GitReadingIsExaminable -ExitCode 128 `
			-StandardError "fatal: detected dubious ownership in repository at 'C:/archive'" -Toplevel ''))

_Assert -Name 'an unrecognised failure is NOT examinable, so an unenumerated fault fails closed' `
	-Condition (-not (Test-GitReadingIsExaminable -ExitCode 1 -StandardError 'something nobody predicted' -Toplevel ''))

_Assert -Name 'exit 0 with NO toplevel is NOT examinable' `
	-Condition (-not (Test-GitReadingIsExaminable -ExitCode 0 -StandardError '' -Toplevel ''))

# The exit-code conjunct of the first promotion guard is load-bearing on its own.
# Nothing else in this suite drives a FAILING exit that nevertheless carries a
# toplevel, so without this assertion the guard could be relaxed to promote on any
# non-blank toplevel and every other fixture here would still pass.
_Assert -Name 'a FAILING exit carrying a toplevel is NOT examinable' `
	-Condition (-not (Test-GitReadingIsExaminable -ExitCode 128 -StandardError 'fatal: a fault this suite does not enumerate' -Toplevel 'C:\dev\lattice'))

# --- the VM path form must be understood before anything else is asked ------
_Assert -Name 'a Docker Desktop VM host path is rewritten to the operator path' `
	-Condition ((ConvertFrom-DockerDesktopHostPath -Path '/run/desktop/mnt/host/c/dev/x') -eq 'C:\dev\x')

_Assert -Name 'a genuine Linux host path is left alone' `
	-Condition ((ConvertFrom-DockerDesktopHostPath -Path '/srv/repocontext-memory') -eq '/srv/repocontext-memory')

_Assert -Name 'absoluteness accepts both platforms and refuses a relative source' `
	-Condition ((Test-ProvenancePathIsAbsolute -Path 'C:\dev\x') `
		-and (Test-ProvenancePathIsAbsolute -Path '/srv/x') `
		-and (Test-ProvenancePathIsAbsolute -Path '\\server\share\x') `
		-and -not (Test-ProvenancePathIsAbsolute -Path './memory-archive') `
		-and -not (Test-ProvenancePathIsAbsolute -Path 'memory-archive'))

# --- the accepting direction ------------------------------------------------
_Assert -Name 'ACCEPTS an absolute bind source outside every git checkout' `
	-Condition ((Get-ArchiveDurabilityViolation `
			-ArchiveDestination '/memory-archive' -ArchiveSource $durableArchiveSource `
			-ArchiveMountType 'bind' -GitToplevel '' -IsLinkedWorktree $false `
			-SourceExistsOnHost $true -GitReadingExaminable $true).Count -eq 0)

# --- the measured defect ----------------------------------------------------
$refused = Get-ArchiveDurabilityViolation `
	-ArchiveDestination '/memory-archive' -ArchiveSource $worktreeArchiveSource `
	-ArchiveMountType 'bind' -GitToplevel $worktreeArchiveToplevel -IsLinkedWorktree $true `
	-SourceExistsOnHost $true
_Assert -Name 'REFUSES the measured issue #2627 reading' -Condition ($refused.Count -eq 1) -Detail ($refused -join '; ')
_Assert -Name 'and names the worktree root and the command that deletes it' `
	-Condition ($refused.Count -ge 1 -and $refused[0].Contains($worktreeArchiveToplevel) -and $refused[0].Contains('git worktree remove')) `
	-Detail ($refused -join '; ')
_Assert -Name 'and names the variable the operator has to set' `
	-Condition ($refused.Count -ge 1 -and $refused[0].Contains('REPOCONTEXT_MEMORY_ARCHIVE_PATH')) `
	-Detail ($refused -join '; ')

# An ordinary checkout and a linked worktree have different lifetimes and
# different remedies. Reporting both as "inside git" would send the operator to
# the wrong one, so the messages are asserted to be distinguishable.
$refusedCheckout = Get-ArchiveDurabilityViolation `
	-ArchiveDestination '/memory-archive' -ArchiveSource 'C:\dev\lattice\samples\RepoContextContainer\memory-archive' `
	-ArchiveMountType 'bind' -GitToplevel 'C:\dev\lattice' -IsLinkedWorktree $false `
	-SourceExistsOnHost $true
_Assert -Name 'REFUSES an archive inside an ordinary checkout too' -Condition ($refusedCheckout.Count -eq 1) -Detail ($refusedCheckout -join '; ')
_Assert -Name 'and does NOT describe an ordinary checkout as a linked worktree' `
	-Condition ($refusedCheckout.Count -ge 1 -and -not $refusedCheckout[0].Contains('LINKED GIT WORKTREE') -and $refusedCheckout[0].Contains('git clean -xdf')) `
	-Detail ($refusedCheckout -join '; ')

# --- the remaining shapes ---------------------------------------------------
$refused = Get-ArchiveDurabilityViolation `
	-ArchiveDestination '/memory-archive' -ArchiveSource './memory-archive' `
	-ArchiveMountType 'bind' -GitToplevel '' -IsLinkedWorktree $false -SourceExistsOnHost $true
_Assert -Name 'REFUSES a relative bind source, whatever it resolved to' -Condition ($refused.Count -eq 1)
_Assert -Name 'and says the location depends on the invocation directory' `
	-Condition ($refused.Count -ge 1 -and $refused[0].Contains('invoked from')) -Detail ($refused -join '; ')

_Assert -Name 'REFUSES a container with nothing bound at the archive destination' `
	-Condition ((Get-ArchiveDurabilityViolation `
			-ArchiveDestination '/memory-archive' -ArchiveSource '' -ArchiveMountType '' `
			-GitToplevel '' -IsLinkedWorktree $false -SourceExistsOnHost $true).Count -eq 1)

$refused = Get-ArchiveDurabilityViolation `
	-ArchiveDestination '/memory-archive' -ArchiveSource 'repocontext-memory' `
	-ArchiveMountType 'volume' -GitToplevel '' -IsLinkedWorktree $false -SourceExistsOnHost $true
_Assert -Name 'REFUSES a NAMED VOLUME at the archive destination' -Condition ($refused.Count -eq 1)
_Assert -Name 'and says down -v removes every declared volume' `
	-Condition ($refused.Count -ge 1 -and $refused[0].Contains('down -v')) -Detail ($refused -join '; ')

# A check that cannot look must not report clean. An unexaminable source is the
# exact shape this bucket exists to catch, so it is a refusal rather than a pass.
$refused = Get-ArchiveDurabilityViolation `
	-ArchiveDestination '/memory-archive' -ArchiveSource $durableArchiveSource `
	-ArchiveMountType 'bind' -GitToplevel '' -IsLinkedWorktree $false -SourceExistsOnHost $false
_Assert -Name 'REFUSES a source it cannot examine rather than passing it' -Condition ($refused.Count -eq 1)
_Assert -Name 'and says so, rather than reporting the archive as durable' `
	-Condition ($refused.Count -ge 1 -and $refused[0].Contains('CANNOT BE ESTABLISHED')) -Detail ($refused -join '; ')

# A check can be blind in TWO ways: the path is not visible (above), or the git
# query against a perfectly visible path never produced an answer. An empty
# toplevel is the PASSING state, so collapsing "git said no" into "git did not
# answer" means the check reports clean exactly when it has been blinded.
#
# This is not exotic here. The archive is written by the container as root while
# this check runs as the operator, so an ownership split is the normal steady
# state, and `git rev-parse` exits 128 under safe.directory for exactly that.
$refused = Get-ArchiveDurabilityViolation `
	-ArchiveDestination '/memory-archive' -ArchiveSource $durableArchiveSource `
	-ArchiveMountType 'bind' -GitToplevel '' -IsLinkedWorktree $false `
	-SourceExistsOnHost $true -GitReadingExaminable $false
_Assert -Name 'REFUSES an empty toplevel when the git query DID NOT COMPLETE' -Condition ($refused.Count -eq 1) -Detail ($refused -join '; ')
_Assert -Name 'and says the containment CANNOT BE ESTABLISHED rather than reporting durable' `
	-Condition ($refused.Count -ge 1 -and $refused[0].Contains('CANNOT BE ESTABLISHED')) -Detail ($refused -join '; ')

# The companion, and it is not redundant: a test that only pins the refusal
# cannot tell you the clean path still works, so a fix that refused everything
# would look identical to a correct one.
_Assert -Name 'ACCEPTS the same empty toplevel when the query DID complete' `
	-Condition ((Get-ArchiveDurabilityViolation `
			-ArchiveDestination '/memory-archive' -ArchiveSource $durableArchiveSource `
			-ArchiveMountType 'bind' -GitToplevel '' -IsLinkedWorktree $false `
			-SourceExistsOnHost $true -GitReadingExaminable $true).Count -eq 0)

# Fail-closed by DEFAULT, not merely when the caller remembers to say so. If the
# parameter's default were flipped to true, every unanticipated git failure
# would silently rejoin the clean path, so the default itself is pinned.
_Assert -Name 'and treats an OMITTED examinability reading as unexaminable, not as clean' `
	-Condition ((Get-ArchiveDurabilityViolation `
			-ArchiveDestination '/memory-archive' -ArchiveSource $durableArchiveSource `
			-ArchiveMountType 'bind' -GitToplevel '' -IsLinkedWorktree $false `
			-SourceExistsOnHost $true).Count -eq 1)

# ---------------------------------------------------------------------------
_Section 'Composite report'
# ---------------------------------------------------------------------------

$report = Get-ContainerProvenanceReport -Readings (New-AgreeingReadings) `
	-ExpectedCheckout $candidateCheckout -ExpectedSettings $expectedSettings -CaseSensitive $false
_Assert -Name 'ACCEPTS readings on which all five checks agree' `
	-Condition ($report.IsSatisfied -and $report.Violations.Count -eq 0) -Detail ($report.Violations -join '; ')

# THE GUARD-DIRECTION FIXTURE. New-AgreeingReadings composes from
# C:\dev\copilot-worktrees\lattice\bucket4\... - a git WORKTREE, which is the
# certified-correct gate state and which check 2 REQUIRES. The assertion above
# therefore already proves check 5 does not key on the compose directory, but
# only implicitly, so it is stated here as its own claim: a check 5 keyed on the
# working directory would refuse every legitimate gate run while satisfying
# every refusal fixture in this file.
_Assert -Name 'and does so even though the compose directory IS a git worktree' `
	-Condition ($report.IsSatisfied -and $report.Readings.ComposeWorkingDirectory.Contains('copilot-worktrees')) `
	-Detail ($report.Violations -join '; ')

_Assert -Name 'and returns every reading it was given, so the operator reads the values' `
	-Condition ($report.Readings.ComposeWorkingDirectory -eq $candidateCheckout -and $report.Readings.RunningImageId.StartsWith('sha256:'))

# The reconstructed gate run 2 container: composed from the main checkout, on
# the main commit, and missing the setting that was merged on the candidate.
$gateRun2 = New-AgreeingReadings
$gateRun2.ComposeWorkingDirectory = $mainCheckout
$gateRun2.ComposeConfigFiles = @((Join-FixturePath $mainCheckout 'docker-compose.yml'))
$gateRun2.ResolvedCommit = $mainCommit
$gateRun2.ContainerEnvironment = @('PATH=/usr/bin')

$report = Get-ContainerProvenanceReport -Readings $gateRun2 `
	-ExpectedCheckout $candidateCheckout -ExpectedSettings $expectedSettings -CaseSensitive $false
_Assert -Name 'REFUSES the reconstructed gate run 2 readings' -Condition (-not $report.IsSatisfied)

# Three separate checks disagree on this container, and all three must be
# reported. A composite that stopped at the first would leave the operator
# fixing the working directory and re-running to discover the next one.
_Assert -Name 'and reports all three disagreements, not merely the first' `
	-Condition ($report.Violations.Count -eq 3) -Detail ($report.Violations -join '; ')

_Assert -Name 'and the environment disagreement is among them' `
	-Condition (@($report.Violations | Where-Object { $_.Contains('ABSENT') }).Count -eq 1) `
	-Detail ($report.Violations -join '; ')

# Each check is independently reachable through the composite: a container that
# is otherwise impeccable but stale on ONE input is refused for that input
# alone. Without these, a composite could satisfy every fixture above while
# three of its four checks were never wired in.
$imageOnly = New-AgreeingReadings
$imageOnly.RunningImageId = 'sha256:9999'
$report = Get-ContainerProvenanceReport -Readings $imageOnly `
	-ExpectedCheckout $candidateCheckout -ExpectedSettings $expectedSettings -CaseSensitive $false
_Assert -Name 'REFUSES on image provenance alone, reporting exactly one disagreement' `
	-Condition ((-not $report.IsSatisfied) -and $report.Violations.Count -eq 1) -Detail ($report.Violations -join '; ')

$commitOnly = New-AgreeingReadings
$commitOnly.ResolvedCommit = $mainCommit
$report = Get-ContainerProvenanceReport -Readings $commitOnly `
	-ExpectedCheckout $candidateCheckout -ExpectedSettings $expectedSettings -CaseSensitive $false
_Assert -Name 'REFUSES on git provenance alone, reporting exactly one disagreement' `
	-Condition ((-not $report.IsSatisfied) -and $report.Violations.Count -eq 1) -Detail ($report.Violations -join '; ')

$environmentOnly = New-AgreeingReadings
$environmentOnly.ContainerEnvironment = @('LATTICE_REPOCONTEXT_STOP_GRACE_PERIOD=10s')
$report = Get-ContainerProvenanceReport -Readings $environmentOnly `
	-ExpectedCheckout $candidateCheckout -ExpectedSettings $expectedSettings -CaseSensitive $false
_Assert -Name 'REFUSES on environment provenance alone, reporting exactly one disagreement' `
	-Condition ((-not $report.IsSatisfied) -and $report.Violations.Count -eq 1) -Detail ($report.Violations -join '; ')

$archiveOnly = New-AgreeingReadings
$archiveOnly.ArchiveSource = $worktreeArchiveSource
$archiveOnly.ArchiveGitToplevel = $worktreeArchiveToplevel
$archiveOnly.ArchiveIsLinkedWorktree = $true
$report = Get-ContainerProvenanceReport -Readings $archiveOnly `
	-ExpectedCheckout $candidateCheckout -ExpectedSettings $expectedSettings -CaseSensitive $false
_Assert -Name 'REFUSES on archive durability alone, reporting exactly one disagreement' `
	-Condition ((-not $report.IsSatisfied) -and $report.Violations.Count -eq 1) -Detail ($report.Violations -join '; ')
_Assert -Name 'and that disagreement is the archive one, not a knock-on from another check' `
	-Condition ($report.Violations.Count -ge 1 -and $report.Violations[0].Contains('LINKED GIT WORKTREE')) `
	-Detail ($report.Violations -join '; ')

# Readings that carry NO archive keys at all must refuse rather than pass. This
# is what stops check 5 being silently unwired from the composite: a report that
# treated absent archive readings as "nothing to complain about" would accept
# every fixture in this file while examining nothing.
$noArchive = New-AgreeingReadings
$noArchive.Remove('ArchiveSource')
$noArchive.Remove('ArchiveMountType')
$noArchive.Remove('ArchiveSourceExistsOnHost')
$noArchive.Remove('ArchiveGitToplevel')
$noArchive.Remove('ArchiveIsLinkedWorktree')
$noArchive.Remove('ArchiveGitReadingExaminable')
$report = Get-ContainerProvenanceReport -Readings $noArchive `
	-ExpectedCheckout $candidateCheckout -ExpectedSettings $expectedSettings -CaseSensitive $false
_Assert -Name 'REFUSES readings that carry no archive reading at all' `
	-Condition ((-not $report.IsSatisfied) -and $report.Violations.Count -eq 1) -Detail ($report.Violations -join '; ')

# An unexaminable git reading has to survive the trip through the COMPOSITE, not
# merely be handled by the pure function. These two fixtures are what would go
# red if check 5 were left intact but its new reading were never wired through.
$blindGit = New-AgreeingReadings
$blindGit['ArchiveGitReadingExaminable'] = $false
$report = Get-ContainerProvenanceReport -Readings $blindGit `
	-ExpectedCheckout $candidateCheckout -ExpectedSettings $expectedSettings -CaseSensitive $false
_Assert -Name 'REFUSES a report whose archive git query did not complete' `
	-Condition ((-not $report.IsSatisfied) -and $report.Violations.Count -eq 1) -Detail ($report.Violations -join '; ')
_Assert -Name 'and reports it as unestablished rather than as a containment finding' `
	-Condition ($report.Violations.Count -ge 1 -and $report.Violations[0].Contains('CANNOT BE ESTABLISHED')) `
	-Detail ($report.Violations -join '; ')

# The examinability key MISSING is not the same statement as it being false, and
# the composite must treat it the same way regardless. Asserted explicitly
# because otherwise the fail-closed direction rests on the unstated PowerShell
# detail that [bool] $null is $false - a guard kept honest by an accident of the
# language is the shape this bucket exists to stop.
$missingKey = New-AgreeingReadings
$missingKey.Remove('ArchiveGitReadingExaminable')
$report = Get-ContainerProvenanceReport -Readings $missingKey `
	-ExpectedCheckout $candidateCheckout -ExpectedSettings $expectedSettings -CaseSensitive $false
_Assert -Name 'REFUSES readings that OMIT the archive examinability key entirely' `
	-Condition ((-not $report.IsSatisfied) -and $report.Violations.Count -eq 1) -Detail ($report.Violations -join '; ')

# ---------------------------------------------------------------------------
Write-Host ''
Write-Host ('  Total {0}   Passed {1}   Failed {2}' -f ($script:_PassCount + $script:_FailCount), $script:_PassCount, $script:_FailCount)
Write-Host ''

exit $script:_FailCount
