#!/usr/bin/env pwsh
<#
.SYNOPSIS
	Day-to-day helper for the isolated cold-start rig.

.DESCRIPTION
	Thin, guarded wrappers around the operations you want between cohorts.
	Every subcommand that could bind anything runs the fail-closed isolation
	guard first, so there is no "quick" path that skips it.

	Subcommands:

	  guard    Run BOTH halves of the isolation guard and print what the rig
	           would actually bind, as resolved by Docker. Use this to satisfy
	           yourself that the rig cannot reach the live deployment. Also
	           reports LIVE IMAGE DRIFT: whether the live deployment's running
	           code and its image tag have diverged, so a restart of it would
	           silently swap what it runs.
	  build    Build the RepoContext host image from a git ref into the rig's
	           own build tag (repocontext-mcp:coldstart-<sha>) and record it as
	           the run's source, e.g.
	           ./rig.ps1 build feat/my-branch
	           It NEVER writes a live tag, so testing new code never requires
	           the deploy script (which promotes to production as a side
	           effect). Behind a private or corporate NuGet feed, set
	           $env:NUGET_CONFIG_FILE (or pass -NuGetConfigFile) to your own
	           NuGet.Config; it is passed as a BuildKit secret and never
	           written into an image layer.
	  tag      Apply the rig's additional image tags to already-built images.
	           Prefers the image the last `build` recorded, when there is one.
	  clone    Recreate the working volume as a fresh clone of the master.
	  up       Bring the stack up on the CURRENT working volume.
	  down     Stop and remove the rig's containers (volumes are kept).
	  status   Show the rig's containers, volumes and images.
	  logs     Tail the rig host's container log.
	  mcp      Call one MCP tool against the rig, e.g.
	           ./rig.ps1 mcp repocontext_list_repos '{}'
	  clean    Remove the rig's WORKING volume and containers. Add -All to
	           remove the master, scale master and model cache too.

.EXAMPLE
	./rig.ps1 guard

.EXAMPLE
	./rig.ps1 build feat/bounded-cold-start-at-scale

.EXAMPLE
	./rig.ps1 mcp repocontext_search '{"repoId":"lattice","query":"wal replay","k":3}'
#>
[CmdletBinding()]
param(
	[Parameter(Mandatory, Position = 0)]
	[ValidateSet('guard', 'build', 'tag', 'clone', 'up', 'down', 'status', 'logs', 'mcp', 'clean')]
	[string] $Command,
	[Parameter(Position = 1)] [string] $Argument1,
	[Parameter(Position = 2)] [string] $Argument2,
	[string] $Ref,
	[string] $NuGetConfigFile,
	[string] $ParametersFile,
	[switch] $All
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$here = Split-Path -Parent $MyInvocation.MyCommand.Path
. (Join-Path $here '_rig-docker.ps1')

$config = Get-RigConfig -ParametersFile $ParametersFile -ScriptRoot $here
Assert-RigIsolation -Config $config | Out-Null

switch ($Command) {
	'guard' {
		$document = Assert-RigDockerIsolation -Config $config
		Write-Host 'Isolation guard PASSED, both halves.' -ForegroundColor Green
		Write-Host ''
		Write-Host 'What the rig would bind, as resolved by Docker:' -ForegroundColor Cyan
		Write-Host "  compose project : $($document.name)"
		foreach ($serviceName in (Get-RigMemberNames -Object $document.services)) {
			$service = Get-RigMember -Object $document.services -Name $serviceName
			Write-Host "  service $serviceName"
			Write-Host "    image        : $(Get-RigMember -Object $service -Name 'image')"
			foreach ($port in @(Get-RigMember -Object $service -Name 'ports')) {
				if ($null -eq $port) { continue }
				Write-Host "    publishes    : host $(Get-RigMember -Object $port -Name 'published') -> container $(Get-RigMember -Object $port -Name 'target')"
			}
			foreach ($mount in @(Get-RigMember -Object $service -Name 'volumes')) {
				if ($null -eq $mount) { continue }
				$readOnly = if ((Get-RigMember -Object $mount -Name 'read_only') -eq $true) { ' (read-only)' } else { '' }
				Write-Host "    mounts       : $(Get-RigMember -Object $mount -Name 'type') $(Get-RigMember -Object $mount -Name 'source') -> $(Get-RigMember -Object $mount -Name 'target')$readOnly"
			}
		}
		Write-Host ''
		Write-Host 'Refused by construction: the repocontextcontainer project, any repocontextcontainer_* volume, the repocontext-mcp:local tag, and host port 8080.' -ForegroundColor DarkGray

		# The rig cannot move a live tag, but something else on this host can.
		# The pin check is read-only and says whether that already happened.
		Write-Host ''
		Write-Host 'Live deployment image pin:' -ForegroundColor Cyan
		Write-RigLiveImagePin -Report (Get-RigLiveImagePin -Config $config) | Out-Null
	}

	'build' {
		$reference = if ($Ref) { $Ref } elseif ($Argument1) { $Argument1 } else { 'HEAD' }
		$record = Invoke-RigBuild -Config $config -Ref $reference -NuGetConfigFile $NuGetConfigFile -ScriptRoot $here
		Write-Host "Recorded as the run's source image; `./rig.ps1 tag` and run-cohort.ps1 will now tag from $($record.image)." -ForegroundColor Green
	}

	'tag' {
		# A recorded build is the source when there is one: an operator who
		# just built a branch means to measure THAT, not whatever the live tag
		# holds.
		$built = Get-RigBuildSource -ScriptRoot $here
		$mcpSource = if ($built) { "$($built.image)" } else { "$($config.SourceMcpImage)" }
		if ($built) {
			Write-Host "Using the recorded build source $mcpSource (commit $($built.commitSha))." -ForegroundColor Cyan
		}
		Add-RigImageTag -Config $config -Source $mcpSource -Destination "$($config.McpImage)" | Out-Null
		Add-RigImageTag -Config $config -Source "$($config.SourceEmbedderImage)" -Destination "$($config.EmbedderImage)" | Out-Null
		Write-Host "Tagged $($config.McpImage) and $($config.EmbedderImage)." -ForegroundColor Green
	}

	'clone' {
		$source = if ($Argument1) { $Argument1 } else { "$($config.MasterVolume)" }
		Invoke-RigCompose -Config $config -ComposeArgs @('down', '--remove-orphans') -AllowFailure | Out-Null
		New-RigVolume -Config $config -Name "$($config.HfCacheVolume)" | Out-Null
		Copy-RigVolume -Config $config -Source $source -Destination "$($config.WorkVolume)" | Out-Null
		Write-Host "Working volume '$($config.WorkVolume)' is a fresh clone of '$source'." -ForegroundColor Green
	}

	'up' {
		Assert-RigDockerIsolation -Config $config | Out-Null
		Invoke-RigCompose -Config $config -ComposeArgs @('up', '-d')
		Write-Host "Rig is up on http://localhost:$($config.HostPort)/" -ForegroundColor Green
	}

	'down' {
		Invoke-RigCompose -Config $config -ComposeArgs @('down', '--remove-orphans') -AllowFailure
		Write-Host 'Rig is down (volumes kept).' -ForegroundColor Green
	}

	'status' {
		Write-Host 'Containers' -ForegroundColor Cyan
		Invoke-RigCompose -Config $config -ComposeArgs @('ps', '-a') -AllowFailure
		Write-Host ''
		Write-Host 'Volumes' -ForegroundColor Cyan
		foreach ($volume in @("$($config.MasterVolume)", "$($config.ScaleMasterVolume)", "$($config.WorkVolume)", "$($config.HfCacheVolume)")) {
			$state = if (Test-RigVolumeExists -Name $volume) { 'present' } else { 'absent ' }
			Write-Host "  $state  $volume"
		}
		Write-Host ''
		Write-Host 'Images' -ForegroundColor Cyan
		foreach ($image in @("$($config.McpImage)", "$($config.EmbedderImage)")) {
			$id = (Invoke-RigDocker -DockerArgs @('image', 'inspect', $image, '--format', '{{.Id}}') -AllowFailure | Out-String).Trim()
			$state = if ($id -like 'sha256:*') { 'present' } else { 'absent ' }
			Write-Host "  $state  $image"
		}
	}

	'logs' {
		$container = Get-RigContainerName -Config $config -Service 'repocontext'
		& docker logs -f --tail 200 $container
	}

	'mcp' {
		if (-not $Argument1) { throw "Usage: ./rig.ps1 mcp <toolName> '<jsonArguments>'" }
		$arguments = @{}
		if ($Argument2) {
			$parsed = $Argument2 | ConvertFrom-Json
			foreach ($property in $parsed.PSObject.Properties) { $arguments[$property.Name] = $property.Value }
		}
		$result = Invoke-RigMcpTool -BaseUri "http://localhost:$($config.HostPort)/" -Name $Argument1 -Arguments $arguments
		Write-Host ("ok={0} status={1} duration={2}ms" -f $result.Ok, $result.StatusCode, $result.DurationMs) -ForegroundColor Cyan
		if ($result.Error) { Write-Host $result.Error -ForegroundColor Red }
		if ($result.Text) { Write-Output $result.Text }
	}

	'clean' {
		Invoke-RigCompose -Config $config -ComposeArgs @('down', '--remove-orphans') -AllowFailure | Out-Null
		Remove-RigVolume -Config $config -Name "$($config.WorkVolume)"
		Write-Host "Removed working volume '$($config.WorkVolume)'." -ForegroundColor Green
		if ($All) {
			foreach ($volume in @("$($config.MasterVolume)", "$($config.ScaleMasterVolume)", "$($config.HfCacheVolume)")) {
				Remove-RigVolume -Config $config -Name $volume
				Write-Host "Removed '$volume'." -ForegroundColor Green
			}
		}
	}
}
