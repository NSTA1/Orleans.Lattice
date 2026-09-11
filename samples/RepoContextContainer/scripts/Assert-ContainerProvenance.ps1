#!/usr/bin/env pwsh
<#
.SYNOPSIS
	Refuses a RUNNING RepoContext container that cannot be shown to have been
	launched from the checkout the operator believes it was.

.DESCRIPTION
	`docker compose up` reads the compose files in its OWN working directory,
	regardless of which branch built the image it starts. The image and the
	runtime configuration are therefore two independent inputs, and only the
	first is obviously version-controlled. Nothing in `docker compose up`'s
	output names a branch, a commit, or a directory, so an operator standing in
	one checkout can deploy a candidate image under a different checkout's
	configuration and see no sign of it.

	That is not hypothetical. Epic #2368's gate runs 1 and 2 did exactly this:
	the candidate image ran under the baseline's runtime config, the container's
	own compose label resolved to the main checkout, and
	`LATTICE_REPOCONTEXT_STOP_GRACE_PERIOD` was absent from the process
	environment while sitting merged on the candidate branch. Both runs observed
	the absence of the fix's effect and concluded the fix was absent. The
	observation was real and correctly made; the discriminator was in a channel
	nobody was reading.

	This script reads that channel. It performs five checks and REFUSES unless
	all five agree:

	  1. COMPOSE provenance. The container's own
	     `com.docker.compose.project.working_dir` label resolves to the expected
	     checkout, and every file in `com.docker.compose.project.config_files`
	     lies under it.
	  2. GIT provenance. That resolved directory is a git worktree, and its HEAD
	     is the expected commit - reported as a value you read, not an inference
	     you make.
	  3. IMAGE provenance. The image id the container is actually executing still
	     matches what its image reference resolves to now, so a container left in
	     place across a rebuild is caught.
	  4. ENVIRONMENT provenance. A candidate-only setting is PRESENT in the
	     container's own environment with the expected value.
	  5. ARCHIVE durability. The bind mount holding durable agent memory resolves
	     to an absolute host path that is OUTSIDE every git worktree and checkout.

	Check 4 is the non-redundant one and the reason the other three are not
	sufficient. Checks 1 to 3 can all pass while an override file, an edit, or a
	stale container leaves the value unset. More importantly, no arrangement of
	checks over TRACKED FILES could have caught the original defect at all: the
	repository agreed with itself perfectly throughout both failed runs. Only a
	reading taken from the running process separates "the source does not carry
	the fix" from "the source carries it and this container never received it".

	Check 5 is the only one that reads the WRITE path, and it was added because
	checks 1 to 4 are all about INPUTS - which checkout, which commit, which
	image, which settings - and durable output flows the other way (issue #2627).
	The archive's bind source used to default to a relative path, which compose
	resolves against its INVOCATION directory, and check 2 requires that
	directory to be a git worktree. So the single fact "this stack was composed
	from a git worktree" was simultaneously the certified-correct state for the
	source tree and the cause of the only surviving copy of durable memory
	landing in a directory `git worktree remove` deletes.

	CHECK 5 THEREFORE KEYS ON THE ARCHIVE PATH ALONE. It is never given the
	compose directory. Refusing a worktree working directory would contradict
	check 2 and refuse every legitimate gate run, so the two are kept apart by
	the adjudicating function's parameter list rather than by a convention.

.PARAMETER ContainerName
	The running container to interrogate. Defaults to the compose service name
	used by this sample's stack.

.PARAMETER ExpectedCheckout
	The directory the operator believes the stack was composed from. Defaults to
	the directory containing this script's compose files, which is the checkout
	you are running this script out of - deliberately, since that is precisely
	the belief being tested.

.PARAMETER ExpectedCommit
	The commit the operator believes is deployed. Defaults to the HEAD of
	-ExpectedCheckout.

	This default is sourced from the OPERATOR's checkout, never from the
	directory the container resolved to. Defaulting it to the latter would
	compare a value against itself and pass unconditionally, which is worse than
	omitting the check because it would report as checked.

.PARAMETER ExpectedSetting
	Candidate-only settings to assert in the container's environment, as a
	hashtable of name to expected value. Defaults to
	LATTICE_REPOCONTEXT_STOP_GRACE_PERIOD read from -ExpectedCheckout's
	docker-compose.yml, which is the exact setting that was missing in gate runs
	1 and 2.

.PARAMETER ExpectedImageId
	Overrides check 3's expectation with an explicit image id, for when you know
	which build should be deployed. By default the check compares the running
	image id against what the container's own image reference resolves to now,
	which detects a stale container but does NOT establish that the image was
	built from -ExpectedCommit.

.EXAMPLE
	cd samples/RepoContextContainer
	pwsh -File ./scripts/Assert-ContainerProvenance.ps1

.EXAMPLE
	pwsh -File ./scripts/Assert-ContainerProvenance.ps1 -ExpectedCommit 9fcaaa998

.NOTES
	This is an OPERATOR check, not a CI gate, and it is not wired into any
	workflow. It needs a running container, and a fixture that skipped when
	Docker was absent would produce exactly the false green this check exists to
	prevent - a green result meaning only that nothing was examined.

	What a green run does NOT establish:
	  - That the image was built from the expected commit. Check 3 as defaulted
	    detects a stale container, not a mislabelled build. Pass -ExpectedImageId
	    if you need that.
	  - That the checkout was clean when `up` ran. HEAD is a commit, not a
	    working tree; uncommitted edits to a compose file are invisible here.
	  - That any setting other than those in -ExpectedSetting reached the
	    process. Check 4 is an existence proof for the settings you name, and
	    says nothing about the ones you do not.
	  - That the archive CONTENT is good. Check 5 adjudicates where the archive
	    landed, not what is in it, and a durable path holding a stale or empty
	    snapshot passes it.
	  - Anything about a container other than -ContainerName.

	The rig's `Assert-RigComposeIsolation` (benchmark/coldstart-rig) is a
	DIFFERENT instrument and neither subsumes this one. It validates the
	DECLARATION - what `docker compose config` resolved - before anything runs.
	This validates the DEPLOYMENT - what a container that is already running
	actually received. The rig has never had this failure mode, because
	`Get-RigComposeFile` pins the file it resolves, so the different-checkout
	drift cannot occur there. A green rig guard therefore says nothing about this
	class, and the two must not be collapsed into one instrument. The gap was
	drawn deliberately by issue #2576, whose handoff named both the remedy and
	its location: a post-up precondition on the container's own environment.
	This script is that precondition.
#>
[CmdletBinding()]
param(
	[string] $ContainerName = 'repocontext',
	[string] $ExpectedCheckout,
	[string] $ExpectedCommit,
	[hashtable] $ExpectedSetting,
	[string] $ExpectedImageId,

	# Defaults to 2 because the documented deployment of this sample IS two
	# files: the tracked `docker-compose.yml`, which carries a `build:` stanza
	# and no `image:`, and a `docker-compose.override.yml` that is untracked and
	# gitignored on purpose, and which supplies the image pin, the memory limit,
	# the CPU caps and the scan cadence. A stack launched from a directory that
	# lacks the override resolves one file and every path it does resolve is
	# still correct, so only the count catches it. Passing 1 here is a
	# deliberate statement that you meant to run without an override, which is
	# the point: dropping it should be an act, not an accident.
	[int] $ExpectedConfigFileCount = 2,

	# The container path the durable-memory archive is bound at. A parameter
	# rather than a constant so a host that relocates the mount can still be
	# adjudicated, but it must name a mount that EXISTS: check 5 refuses a
	# container with nothing bound here, because an absent archive is the
	# strongest form of the defect it looks for.
	[string] $ArchiveDestination = '/memory-archive'
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$here = Split-Path -Parent $MyInvocation.MyCommand.Path
. (Join-Path $here '_provenance.ps1')

function Invoke-Docker {
	param([Parameter(Mandatory)] [string[]] $DockerArgument)

	$output = & docker @DockerArgument 2>&1
	if ($LASTEXITCODE -ne 0) {
		throw "docker $($DockerArgument -join ' ') failed with exit code $LASTEXITCODE`n$output"
	}
	return ($output | Out-String).Trim()
}

function Get-HeadCommit {
	param([Parameter(Mandatory)] [string] $Directory)

	if (-not (Test-Path -LiteralPath $Directory)) { return '' }

	$commit = & git -C $Directory rev-parse HEAD 2>$null
	if ($LASTEXITCODE -ne 0) { return '' }
	return ("$commit").Trim()
}

function Get-DeclaredSetting {
	param(
		[Parameter(Mandatory)] [string] $ComposeFile,
		[Parameter(Mandatory)] [string] $Name
	)

	if (-not (Test-Path -LiteralPath $ComposeFile)) {
		throw "cannot read the expected value of $Name because '$ComposeFile' does not exist; pass -ExpectedSetting explicitly"
	}

	foreach ($line in (Get-Content -LiteralPath $ComposeFile)) {
		if ($line -match "^\s*$([regex]::Escape($Name))\s*:\s*(.+?)\s*$") {
			return $Matches[1].Trim("'", '"')
		}
	}

	throw "cannot read the expected value of $Name because '$ComposeFile' does not declare it; pass -ExpectedSetting explicitly"
}

# Impure half of check 5. Asks git, on THIS host, whether the archive's bind
# source lies inside a checkout - and if so, whether that checkout is a linked
# worktree. The adjudication itself stays pure in _provenance.ps1; this only
# takes the reading.
#
# Only the archive path is passed in. Deliberately: the compose directory is
# legitimately a worktree and check 2 requires it to be one, so a probe that
# could see it is a probe that could be keyed on it by a later edit.
#
# `Examinable` is the reading that says whether this probe got an ANSWER, as
# distinct from whether the answer was "no". It is promoted to true only on
# positive recognition - a clean success, or the exact not-a-repository
# signature - and never merely because nothing appeared to go wrong. An empty
# toplevel with `Examinable` false means the query failed and the pure half
# refuses to call that clean.
function Get-ArchiveGitReading {
	param([Parameter(Mandatory)] [AllowEmptyString()] [string] $Path)

	$reading = @{ Exists = $false; Toplevel = ''; IsLinkedWorktree = $false; Examinable = $false }
	if ([string]::IsNullOrWhiteSpace($Path)) { return $reading }

	if (-not (Test-Path -LiteralPath $Path)) { return $reading }
	$reading.Exists = $true

	# git's stderr is matched below, so its locale is pinned for this call only.
	# A recognition that silently stops matching under a translated git is the
	# same false clean wearing a different hat.
	$savedLcAll = $env:LC_ALL
	$savedLang = $env:LANG
	$stderrFile = [System.IO.Path]::GetTempFileName()

	try {
		$env:LC_ALL = 'C'
		$env:LANG = 'C'

		$toplevel = ''
		$exitCode = $null

		try {
			$toplevel = & git -C $Path rev-parse --show-toplevel 2>$stderrFile
			$exitCode = $LASTEXITCODE
		}
		catch {
			# git absent from PATH. No answer, so the reading stays unexaminable.
			return $reading
		}

		$stderr = if (Test-Path -LiteralPath $stderrFile) { [string] (Get-Content -Raw -LiteralPath $stderrFile -ErrorAction SilentlyContinue) } else { '' }

		if ($exitCode -eq 0 -and -not [string]::IsNullOrWhiteSpace($toplevel)) {
			$reading.Examinable = $true
			$reading.Toplevel = ("$toplevel").Trim()
		}
		elseif ($exitCode -eq 128 -and $stderr -match 'not a git repository \(or any of the parent directories\)') {
			# The ONE negative answer that is an answer, and the parenthetical is
			# load-bearing rather than decorative. git emits the BARE phrase
			# "fatal: not a git repository: <admin dir>" for an ORPHANED LINKED
			# WORKTREE - one whose .git/worktrees entry has been removed - which is
			# precisely a state check 5 exists to catch. Matching the bare phrase
			# would promote that to durable. Only the parenthetical form, emitted
			# when discovery genuinely walked to the root and found nothing, means
			# "outside every checkout". Verified against git on all three inputs:
			# genuine miss (parenthetical), orphaned worktree (bare only), and a
			# corrupt gitfile ("invalid gitfile format", neither).
			#
			# Every other 128 - a dubious-ownership refusal under safe.directory, a
			# locked or corrupt repository, an orphaned worktree - leaves this
			# false, because those are failures to look.
			$reading.Examinable = $true
		}
		else {
			return $reading
		}
	}
	finally {
		$env:LC_ALL = $savedLcAll
		$env:LANG = $savedLang
		Remove-Item -LiteralPath $stderrFile -Force -ErrorAction SilentlyContinue
	}

	if ([string]::IsNullOrWhiteSpace($reading.Toplevel)) { return $reading }

	# A LINKED worktree carries a `.git` FILE pointing at the main repository's
	# admin directory; a primary checkout carries a `.git` DIRECTORY. That is the
	# distinction, and it decides which remedy the operator is told about.
	$dotGit = Join-Path $reading.Toplevel '.git'
	$reading.IsLinkedWorktree = (Test-Path -LiteralPath $dotGit -PathType Leaf)

	return $reading
}

# The expected checkout defaults to the compose directory this script ships in,
# which is the checkout the operator is standing in. That is the belief under
# test, so it is the right default and must never be derived from the container.
if ([string]::IsNullOrWhiteSpace($ExpectedCheckout)) {
	$ExpectedCheckout = (Resolve-Path (Join-Path $here '..')).Path
}
if ([string]::IsNullOrWhiteSpace($ExpectedCommit)) {
	$ExpectedCommit = Get-HeadCommit -Directory $ExpectedCheckout
}
if ($null -eq $ExpectedSetting -or $ExpectedSetting.Count -eq 0) {
	$composeFile = Join-Path $ExpectedCheckout 'docker-compose.yml'
	$ExpectedSetting = @{
		'LATTICE_REPOCONTEXT_STOP_GRACE_PERIOD' = (Get-DeclaredSetting -ComposeFile $composeFile -Name 'LATTICE_REPOCONTEXT_STOP_GRACE_PERIOD')
	}
}

$inspected = Invoke-Docker -DockerArgument @('inspect', $ContainerName) | ConvertFrom-Json
if ($null -eq $inspected -or @($inspected).Count -eq 0) {
	throw "container '$ContainerName' was not found; start the stack before running this check"
}
$container = @($inspected)[0]

$labels = $container.Config.Labels
$workingDirectory = if ($null -ne $labels -and $labels.PSObject.Properties.Name -contains 'com.docker.compose.project.working_dir') {
	"$($labels.'com.docker.compose.project.working_dir')"
}
else { '' }

$configFiles = @()
if ($null -ne $labels -and $labels.PSObject.Properties.Name -contains 'com.docker.compose.project.config_files') {
	$configFiles = @("$($labels.'com.docker.compose.project.config_files')".Split(',') | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne '' })
}

$imageReference = "$($container.Config.Image)"
$runningImageId = "$($container.Image)"

if ([string]::IsNullOrWhiteSpace($ExpectedImageId)) {
	# Resolve the container's OWN reference to whatever it names now. A tag is a
	# mutable pointer, so this asks "has the thing this container was started
	# from moved since?", which is the stale-container question. It is not the
	# same as "was this image built from the expected commit".
	try { $ExpectedImageId = Invoke-Docker -DockerArgument @('image', 'inspect', $imageReference, '--format', '{{.Id}}') }
	catch { $ExpectedImageId = '' }
}

$resolvedCommit = if ([string]::IsNullOrWhiteSpace($workingDirectory)) { '' } else { Get-HeadCommit -Directory $workingDirectory }

# The archive bind, read from the container's own mount table. Docker Desktop
# reports some sources through its VM view (/run/desktop/mnt/host/c/...), which
# is the exact form issue #2627 observed, so the source is normalised back to a
# host path before anything is asked of it.
$archiveMount = @($container.Mounts) | Where-Object { "$($_.Destination)" -eq $ArchiveDestination } | Select-Object -First 1
$archiveSource = if ($null -eq $archiveMount) { '' } else { ConvertFrom-DockerDesktopHostPath -Path "$($archiveMount.Source)" }
$archiveMountType = if ($null -eq $archiveMount) { '' } else { "$($archiveMount.Type)" }
$archiveGit = Get-ArchiveGitReading -Path $archiveSource

# Existence is resolved here, in the impure half, and adjudicated in the pure
# one. Deliberately Test-Path and not a git query: the override this stack needs
# is gitignored, so asking git whether a config file exists would report the
# load-bearing one as absent and the dropped one as fine.
$missingConfigFiles = @($configFiles | Where-Object { -not (Test-Path -LiteralPath $_) })

$readings = @{
	ContainerName              = $ContainerName
	ComposeWorkingDirectory    = $workingDirectory
	ComposeConfigFiles         = $configFiles
	MissingConfigFiles         = $missingConfigFiles
	ResolvedCommit             = $resolvedCommit
	ExpectedCommit             = $ExpectedCommit
	RunningImageId             = $runningImageId
	ExpectedImageId            = $ExpectedImageId
	ImageReference             = $imageReference
	ContainerEnvironment       = @($container.Config.Env)
	ArchiveDestination         = $ArchiveDestination
	ArchiveSource              = $archiveSource
	ArchiveMountType           = $archiveMountType
	ArchiveSourceExistsOnHost  = $archiveGit.Exists
	ArchiveGitReadingExaminable = $archiveGit.Examinable
	ArchiveGitToplevel         = $archiveGit.Toplevel
	ArchiveIsLinkedWorktree    = $archiveGit.IsLinkedWorktree
}

$report = Get-ContainerProvenanceReport `
	-Readings $readings `
	-ExpectedCheckout $ExpectedCheckout `
	-ExpectedSettings $ExpectedSetting `
	-ExpectedConfigFileCount $ExpectedConfigFileCount `
	-CaseSensitive (-not $IsWindows)

# Emit every reading unconditionally, pass or fail. A bare verdict sends the
# operator away to find these values themselves at exactly the moment they have
# least reason to trust their own assumptions about where they live.
Write-Host ''
Write-Host 'RepoContext container provenance' -ForegroundColor Cyan
Write-Host '--------------------------------'
Write-Host ("  container                 : {0}" -f $ContainerName)
Write-Host ("  expected checkout         : {0}" -f $ExpectedCheckout)
Write-Host ("  compose working_dir       : {0}" -f $(if ($workingDirectory) { $workingDirectory } else { '<absent>' }))
Write-Host ("  compose config_files      : {0}" -f $(if ($configFiles.Count) { $configFiles -join ', ' } else { '<absent>' }))
Write-Host ("  config_files count        : {0} (expected {1})" -f $configFiles.Count, $ExpectedConfigFileCount)
Write-Host ("  config_files missing      : {0}" -f $(if ($missingConfigFiles.Count) { $missingConfigFiles -join ', ' } else { '<none>' }))
Write-Host ("  expected commit           : {0}" -f $(if ($ExpectedCommit) { $ExpectedCommit } else { '<unresolved>' }))
Write-Host ("  deployed checkout commit  : {0}" -f $(if ($resolvedCommit) { $resolvedCommit } else { '<unresolved>' }))
Write-Host ("  image reference           : {0}" -f $imageReference)
Write-Host ("  running image id          : {0}" -f $runningImageId)
Write-Host ("  reference resolves to     : {0}" -f $(if ($ExpectedImageId) { $ExpectedImageId } else { '<unresolved>' }))
foreach ($name in ($ExpectedSetting.Keys | Sort-Object)) {
	$observed = @($container.Config.Env) | Where-Object { $_ -like "$name=*" } | Select-Object -First 1
	Write-Host ("  {0}" -f $name)
	Write-Host ("      expected              : {0}" -f $ExpectedSetting[$name])
	Write-Host ("      in container          : {0}" -f $(if ($observed) { $observed.Substring($observed.IndexOf('=') + 1) } else { '<ABSENT>' }))
}
Write-Host ("  memory archive mount      : {0}" -f $(if ($archiveSource) { "$archiveSource -> $ArchiveDestination ($archiveMountType)" } else { "<NOTHING BOUND AT $ArchiveDestination>" }))
Write-Host ("  archive inside git        : {0}" -f $(if ($archiveGit.Toplevel) { "$($archiveGit.Toplevel)$(if ($archiveGit.IsLinkedWorktree) { ' (LINKED WORKTREE)' } else { ' (checkout)' })" } elseif (-not $archiveGit.Exists) { '<UNEXAMINABLE - source not present on this host>' } elseif (-not $archiveGit.Examinable) { '<UNEXAMINABLE - the git query DID NOT COMPLETE>' } else { '<no - outside every checkout>' }))
Write-Host ''

if (-not $report.IsSatisfied) {
	$detail = ($report.Violations | ForEach-Object { "  - $_" }) -join [Environment]::NewLine
	throw ("Container provenance REFUSED for '$ContainerName': this container cannot be shown to have been launched from '$ExpectedCheckout'." + [Environment]::NewLine + $detail)
}

Write-Host ("  OK  all five provenance checks agree for '{0}'" -f $ContainerName) -ForegroundColor Green
