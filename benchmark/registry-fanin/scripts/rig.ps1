#!/usr/bin/env pwsh
<#
.SYNOPSIS
	Drives the isolated registry fan-in rig: build, up, ready, driver, down, reset.

.DESCRIPTION
	Every verb runs the fail-closed isolation guard first, and every verb that
	resolves a container id runs the container-target guard before that id
	reaches a docker command. The second guard is the load-bearing one: the
	sidecar is launched with `--network container:<id>`, and a wrong id there
	would attach a synthetic load generator to the PROTECTED deployment's
	network namespace and succeed.

.PARAMETER Verb
	build   Build the silo image from the CURRENT checkout, under the rig tag.
	        The instrument added by this workstream must be in the RUNNING
	        binary or the measurement is vacuous, so this is not optional.
	tag     Apply the rig's additional tag to the already-built embedder image.
	up      Start the stack (creates the external volumes if absent).
	ready   Block until /health/ready answers, and print the ready moment.
	driver  Run the sidecar driver in the silo's network namespace.
	down    Stop and remove the stack, leaving the volumes.
	reset   down, then REMOVE the rig's work volume, for a clean cold start.
	status  Print container provenance: StartedAt, RestartCount, image, digest.

.EXAMPLE
	pwsh scripts/rig.ps1 build
	pwsh scripts/rig.ps1 reset
	pwsh scripts/rig.ps1 up
	pwsh scripts/rig.ps1 driver -DriverArgs 'create --trees 40'

	Driver arguments travel as ONE string that this script splits on
	whitespace, rather than as remaining arguments. Under `pwsh -File` a bare
	`--` is read as an ambiguous parameter name and a comma-joined list binds
	as a single literal token, so both of the obvious spellings silently
	mis-parse - one fails outright, the other reaches the driver as one
	unrecognised verb.
#>

[CmdletBinding()]
param(
	[Parameter(Mandatory, Position = 0)]
	[ValidateSet('build', 'tag', 'up', 'ready', 'driver', 'down', 'reset', 'status')]
	[string] $Verb,

	[string] $ParametersFile,
	[string] $NuGetConfigFile,
	[int] $ReadyTimeoutSec,

	[string] $DriverArgs,

	[Parameter(ValueFromRemainingArguments)]
	[string[]] $Rest = @()
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

. (Join-Path $PSScriptRoot '_fanin-helpers.ps1')

$RigRoot = Split-Path -Parent $PSScriptRoot
$RepoRoot = Split-Path -Parent (Split-Path -Parent $RigRoot)
$ComposeFile = Join-Path $RigRoot 'docker-compose.rig.yml'

# An unbound [int] parameter arrives as 0, which is a perfectly valid-looking
# value rather than an absent one, so it cannot be handed to the override map
# unconditionally: doing so replaced the committed 900s ready timeout with 0
# and made every `ready` call fail instantly. Numeric overrides are therefore
# admitted only when the caller actually supplied them.
$overrides = @{ NuGetConfigFile = $NuGetConfigFile }
if ($PSBoundParameters.ContainsKey('ReadyTimeoutSec')) { $overrides['ReadyTimeoutSec'] = $ReadyTimeoutSec }

$config = Get-FanInConfig -ScriptRoot $PSScriptRoot -ParametersFile $ParametersFile -Override $overrides
$null = Assert-FanInIsolation -Config $config

function Invoke-Docker {
	param([string[]] $DockerArgs, [switch] $AllowFailure)

	$output = & docker @DockerArgs 2>&1
	if ($LASTEXITCODE -ne 0 -and -not $AllowFailure) {
		throw "docker $($DockerArgs -join ' ') failed with exit code $LASTEXITCODE`n$($output -join "`n")"
	}
	return $output
}

function Get-ComposeEnvironment {
	# Every compose variable is REQUIRED by the compose file (`:?`), so an
	# unset one fails loudly at resolution rather than silently defaulting to
	# something live.
	return @{
		FANIN_PROJECT        = $config.ProjectName
		FANIN_MCP_IMAGE      = $config.McpImage
		FANIN_EMBEDDER_IMAGE = $config.EmbedderImage
		FANIN_HOST_PORT      = "$($config.HostPort)"
		FANIN_WORK_VOLUME    = $config.WorkVolume
		FANIN_HF_VOLUME      = $config.HfCacheVolume
		FANIN_WORKSPACE      = $config.WorkspaceRoot
	}
}

function Invoke-Compose {
	param([string[]] $ComposeArgs, [switch] $AllowFailure)

	foreach ($pair in (Get-ComposeEnvironment).GetEnumerator()) {
		Set-Item -Path "env:$($pair.Key)" -Value $pair.Value
	}

	$full = @('compose', '-p', $config.ProjectName, '-f', $ComposeFile) + $ComposeArgs
	return Invoke-Docker -DockerArgs $full -AllowFailure:$AllowFailure
}

<#
.SYNOPSIS
	Resolves the rig's silo container and REFUSES anything that is not one.
#>
function Get-RigSiloContainer {
	$ids = @(Invoke-Compose -ComposeArgs @('ps', '-q', 'repocontext') | Where-Object { "$_".Trim() -ne '' })
	if ($ids.Count -eq 0) { throw 'The rig silo container is not running. Run `rig.ps1 up` first.' }
	if ($ids.Count -gt 1) { throw "Expected one rig silo container, found $($ids.Count)." }

	$id = "$($ids[0])".Trim()
	$json = (Invoke-Docker -DockerArgs @('inspect', $id)) -join "`n"
	$inspect = ($json | ConvertFrom-Json)[0]

	$project = $null
	if ($inspect.Config.PSObject.Properties.Name -contains 'Labels' -and $null -ne $inspect.Config.Labels) {
		$labels = $inspect.Config.Labels
		if ($labels.PSObject.Properties.Name -contains 'com.docker.compose.project') {
			$project = $labels.'com.docker.compose.project'
		}
	}

	$name = "$($inspect.Name)".TrimStart('/')

	# The guard runs BEFORE this id is used for anything, and in particular
	# before it reaches `--network container:`.
	$null = Assert-FanInContainerTarget -Config $config -ContainerId $id -ContainerName $name -ComposeProject $project

	return [pscustomobject] @{
		Id           = $id
		Name         = $name
		Image        = $inspect.Config.Image
		ImageId      = $inspect.Image
		# StartedAt is the authority for the restart-boundary check. A compose
		# recreation yields RestartCount=0 on a brand-new container, so
		# RestartCount can only ever ADD a restart, never rule one out.
		StartedAtUtc = ([datetime] $inspect.State.StartedAt).ToUniversalTime()
		RestartCount = [int] $inspect.RestartCount
		Running      = [bool] $inspect.State.Running
	}
}

function Wait-RigReady {
	param([int] $TimeoutSec)

	$deadline = [datetime]::UtcNow.AddSeconds($TimeoutSec)
	$url = "http://localhost:$($config.HostPort)/health/ready"
	$interval = [int] $config.ProbeIntervalMs

	while ([datetime]::UtcNow -lt $deadline) {
		try {
			$response = Invoke-WebRequest -Uri $url -TimeoutSec 5 -SkipHttpErrorCheck
			if ($response.StatusCode -eq 200) { return [datetime]::UtcNow }
		}
		catch { }
		Start-Sleep -Milliseconds $interval
	}

	throw "The rig silo did not report ready within $TimeoutSec seconds."
}

switch ($Verb) {

	'build' {
		# The one rig operation that creates an image. Its destination carries
		# the rig tag, which the isolation guard has already checked is not a
		# live reference, so a build can never move a live tag.
		$nuget = "$($config.NuGetConfigFile)"
		if ($nuget -eq '') {
			$candidate = Join-Path $env:APPDATA 'NuGet\NuGet.Config'
			if (Test-Path -LiteralPath $candidate) { $nuget = $candidate }
		}

		$sha = (& git -C $RepoRoot rev-parse HEAD).Trim()
		Write-Host "Building $($config.McpImage) from $RepoRoot @ $($sha.Substring(0,9))" -ForegroundColor Cyan

		$buildArgs = @(
			'build',
			'-f', (Join-Path $RepoRoot 'apps/repocontext/Dockerfile'),
			'--build-arg', "GIT_COMMIT=$sha",
			'-t', $config.McpImage
		)
		if ($nuget -ne '') {
			# Passed as a BuildKit secret and never written into an image layer.
			$buildArgs += @('--secret', "id=nugetcfg,src=$nuget")
		}
		$buildArgs += $RepoRoot

		$env:DOCKER_BUILDKIT = '1'
		$null = Invoke-Docker -DockerArgs $buildArgs
		Write-Host "Built $($config.McpImage)" -ForegroundColor Green
	}

	'tag' {
		$source = "$($config.SourceEmbedderImage)"
		Write-Host "Tagging $source as $($config.EmbedderImage)" -ForegroundColor Cyan
		$null = Invoke-Docker -DockerArgs @('tag', $source, $config.EmbedderImage)
		Write-Host 'Tagged.' -ForegroundColor Green
	}

	'up' {
		foreach ($volume in @($config.WorkVolume, $config.HfCacheVolume)) {
			$existing = Invoke-Docker -DockerArgs @('volume', 'ls', '-q', '--filter', "name=^$volume`$") -AllowFailure
			if (-not (@($existing | Where-Object { "$_".Trim() -ne '' }).Count)) {
				Write-Host "Creating volume $volume" -ForegroundColor Cyan
				$null = Invoke-Docker -DockerArgs @('volume', 'create', $volume)
			}
		}

		$null = Invoke-Compose -ComposeArgs @('up', '-d')
		Start-Sleep -Seconds ([int] $config.StartupSettleSec)

		$silo = Get-RigSiloContainer
		Write-Host "Up: $($silo.Name) ($($silo.Id.Substring(0,12))) started $($silo.StartedAtUtc.ToString('o'))" -ForegroundColor Green
	}

	'ready' {
		$timeout = if ($config.ContainsKey('ReadyTimeoutSec')) { [int] $config.ReadyTimeoutSec } else { 900 }
		$silo = Get-RigSiloContainer
		$readyAt = Wait-RigReady -TimeoutSec $timeout
		[pscustomobject] @{
			ContainerId  = $silo.Id
			StartedAtUtc = $silo.StartedAtUtc.ToString('o')
			ReadyAtUtc   = $readyAt.ToString('o')
			StartupSeconds = ($readyAt - $silo.StartedAtUtc).TotalSeconds
			RestartCount = $silo.RestartCount
		} | ConvertTo-Json -Depth 4
	}

	'driver' {
		$silo = Get-RigSiloContainer

		# -DriverArgs is the supported spelling; -Rest is kept only so a single
		# bare verb (`rig.ps1 driver list`) still works. Splitting one string on
		# whitespace is deliberate: it is the only spelling that survives
		# `pwsh -File` intact, where a bare `--` is an ambiguous parameter name
		# and a comma-joined list arrives as one literal token.
		# NOTE the local is NOT named $driverArgs. PowerShell variable names are
		# case-insensitive, so $driverArgs and the [string] $DriverArgs
		# parameter are the SAME variable: assigning an array to it coerces
		# silently to a string, which then fails at `.Count` under StrictMode
		# with an error that points nowhere near the cause.
		$argv = @()
		if ($DriverArgs) {
			$argv = @($DriverArgs -split '\s+' | Where-Object { "$_" -ne '' })
		}
		elseif ($Rest) {
			$argv = @($Rest | Where-Object { "$_" -ne '--' -and "$_" -ne '' })
		}

		if ($argv.Count -eq 0) { $argv = @('list') }

		# Cluster identity is passed from configuration rather than left to the
		# driver's defaults. A mismatch does not fail as a connection error: the
		# gateway completes the TCP handshake and then refuses on identity, so
		# it reads as a networking fault and sends you looking in the wrong
		# place entirely.
		if ($argv -notcontains '--cluster-id') {
			$argv += @('--cluster-id', "$($config.ClusterId)")
		}

		if ($argv -notcontains '--service-id') {
			$argv += @('--service-id', "$($config.ServiceId)")
		}

		# --network container:<id> is the whole reason for the container guard
		# above: the silo binds its Orleans gateway to loopback inside its own
		# container, so no published port reaches it, and a mistaken id here
		# would succeed against the wrong container rather than fail.
		$runArgs = @(
			'run', '--rm',
			'--network', "container:$($silo.Id)",
			'-v', "$(Join-Path $RigRoot 'results'):/reports",
			$config.DriverImage
		) + $argv

		& docker @runArgs
		if ($LASTEXITCODE -ne 0) { throw "The sidecar driver exited with code $LASTEXITCODE." }
	}

	'down' {
		$null = Invoke-Compose -ComposeArgs @('down', '--remove-orphans') -AllowFailure
		Write-Host 'Down.' -ForegroundColor Green
	}

	'reset' {
		$null = Invoke-Compose -ComposeArgs @('down', '--remove-orphans') -AllowFailure
		# Re-checked here rather than trusted from the guard above, because this
		# is the only destructive verb and it takes a volume name straight to
		# `docker volume rm`.
		$violations = Test-FanInVolumeName -Volume $config.WorkVolume -Config $config -Label 'WorkVolume'
		if ($violations.Count -gt 0) {
			throw ('REFUSING to remove a volume that is not a rig volume: ' + ($violations -join '; '))
		}
		$null = Invoke-Docker -DockerArgs @('volume', 'rm', '-f', $config.WorkVolume) -AllowFailure
		Write-Host "Reset: removed $($config.WorkVolume)." -ForegroundColor Green
	}

	'status' {
		$silo = Get-RigSiloContainer
		[pscustomobject] @{
			ContainerId   = $silo.Id
			Name          = $silo.Name
			Image         = $silo.Image
			ImageId       = $silo.ImageId
			StartedAtUtc  = $silo.StartedAtUtc.ToString('o')
			ReadingAtUtc  = ([datetime]::UtcNow).ToString('o')
			RestartCount  = $silo.RestartCount
			Running       = $silo.Running
			# Stated on every status read because RestartCount=0 is the single
			# most misleading field on this object.
			RestartNote   = 'RestartCount=0 does NOT prove no restart occurred; a compose recreation yields a fresh container with RestartCount=0 and a new StartedAt. Compare StartedAtUtc against the time any counter baseline was taken.'
		} | ConvertTo-Json -Depth 4
	}
}
