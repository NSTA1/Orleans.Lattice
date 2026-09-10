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

	This script reads that channel. It performs four checks and REFUSES unless
	all four agree:

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

	Check 4 is the non-redundant one and the reason the other three are not
	sufficient. Checks 1 to 3 can all pass while an override file, an edit, or a
	stale container leaves the value unset. More importantly, no arrangement of
	checks over TRACKED FILES could have caught the original defect at all: the
	repository agreed with itself perfectly throughout both failed runs. Only a
	reading taken from the running process separates "the source does not carry
	the fix" from "the source carries it and this container never received it".

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
	[int] $ExpectedConfigFileCount = 2
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

# Existence is resolved here, in the impure half, and adjudicated in the pure
# one. Deliberately Test-Path and not a git query: the override this stack needs
# is gitignored, so asking git whether a config file exists would report the
# load-bearing one as absent and the dropped one as fine.
$missingConfigFiles = @($configFiles | Where-Object { -not (Test-Path -LiteralPath $_) })

$readings = @{
	ContainerName            = $ContainerName
	ComposeWorkingDirectory  = $workingDirectory
	ComposeConfigFiles       = $configFiles
	MissingConfigFiles       = $missingConfigFiles
	ResolvedCommit           = $resolvedCommit
	ExpectedCommit           = $ExpectedCommit
	RunningImageId           = $runningImageId
	ExpectedImageId          = $ExpectedImageId
	ImageReference           = $imageReference
	ContainerEnvironment     = @($container.Config.Env)
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
Write-Host ''

if (-not $report.IsSatisfied) {
	$detail = ($report.Violations | ForEach-Object { "  - $_" }) -join [Environment]::NewLine
	throw ("Container provenance REFUSED for '$ContainerName': this container cannot be shown to have been launched from '$ExpectedCheckout'." + [Environment]::NewLine + $detail)
}

Write-Host ("  OK  all four provenance checks agree for '{0}'" -f $ContainerName) -ForegroundColor Green
