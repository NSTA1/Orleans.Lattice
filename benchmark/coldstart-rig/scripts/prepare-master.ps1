#!/usr/bin/env pwsh
<#
.SYNOPSIS
	Restores a backup tarball into the rig's PRISTINE master volume and applies
	the rig's own additional image tags. Idempotent.

.DESCRIPTION
	Step one of the rig, run once per backup. It does three things:

	  1. Extracts the tarball to a host staging directory, which is what the
	     OFFLINE state census walks (a host directory is far faster to walk
	     from PowerShell than a Docker bind mount, and needs no container).
	  2. Loads the SAME tarball into the rig's master volume, by untarring it
	     INSIDE a throwaway container so the 1.8 GB of small files are written
	     into the Docker VM's own filesystem rather than dragged back and
	     forth across a host bind mount.
	  3. Applies the rig's additional image tags to already-built images.

	The master volume is written here and NEVER AGAIN: run-cohort.ps1 clones it
	to a working volume per run and the compose stack cannot bind the master at
	all (the isolation guard refuses it), so every run starts from byte-identical
	durable state.

	Nothing here can touch a live deployment: the tarball is a COPY of durable
	state, the master volume name must pass the isolation guard, and the image
	tags are ADDITIONAL tags applied to existing images (the guard refuses a
	live tag as a destination).

.PARAMETER BackupTarball
	Path to the volume backup tarball. Defaults to the BackupTarball entry in
	parameters(.local).ps1. It is only ever READ.

.PARAMETER Force
	Rebuild the master volume and re-extract the staging directory even when a
	matching one already exists.

.PARAMETER SkipImages
	Do not (re-)apply the rig image tags. Useful when the source images have
	not changed and you only want to refresh durable state.

.EXAMPLE
	./prepare-master.ps1

.EXAMPLE
	./prepare-master.ps1 -BackupTarball D:\backups\volume-backup-2026-08-29T1000.tar -Force
#>
[CmdletBinding()]
param(
	[string] $BackupTarball,
	[string] $ParametersFile,
	[switch] $Force,
	[switch] $SkipImages
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$here = Split-Path -Parent $MyInvocation.MyCommand.Path
. (Join-Path $here '_rig-docker.ps1')

$config = Get-RigConfig -ParametersFile $ParametersFile -Override @{ BackupTarball = $BackupTarball } -ScriptRoot $here
Assert-RigIsolation -Config $config | Out-Null
Write-Host "Isolation guard passed: project '$($config.ProjectName)', master volume '$($config.MasterVolume)', host port $($config.HostPort)." -ForegroundColor Green

$tarball = "$($config.BackupTarball)"
if (-not (Test-Path -LiteralPath $tarball)) {
	throw "Backup tarball not found: $tarball. Pass -BackupTarball, or set BackupTarball in parameters.local.ps1."
}
$tarballItem = Get-Item -LiteralPath $tarball
$stem = [System.IO.Path]::GetFileNameWithoutExtension($tarballItem.Name)

$runRoot = Get-RigRunRoot -ScriptRoot $here
$stagingRoot = Join-Path $runRoot 'state'
$staging = Join-Path $stagingRoot $stem
$manifestPath = Join-Path $staging '.rig-manifest.json'

$manifest = [ordered] @{
	tarballPath           = $tarballItem.FullName
	tarballSizeBytes      = $tarballItem.Length
	# Stored as ticks, not as an ISO-8601 string: ConvertFrom-Json rehydrates a
	# date-shaped string into a DateTime, so a string round-trip would compare
	# two different types and quietly re-extract 1.8 GB on every run.
	tarballLastWriteTicks = $tarballItem.LastWriteTimeUtc.Ticks
	tarballLastWriteUtc   = $tarballItem.LastWriteTimeUtc.ToString('o')
	stagingPath           = $staging
	masterVolume          = "$($config.MasterVolume)"
	preparedUtc           = [datetime]::UtcNow.ToString('o')
}

$stagingCurrent = $false
if (-not $Force -and (Test-Path -LiteralPath $manifestPath)) {
	$existing = Get-Content -LiteralPath $manifestPath -Raw | ConvertFrom-Json
	$stagingCurrent = Test-RigStagingManifestCurrent `
		-Manifest $existing `
		-TarballSizeBytes $tarballItem.Length `
		-TarballLastWriteTicks $tarballItem.LastWriteTimeUtc.Ticks
}

if ($stagingCurrent) {
	Write-Host "Staging copy is current: $staging (use -Force to re-extract)." -ForegroundColor DarkGray
}
else {
	Write-Host "Extracting $([Math]::Round($tarballItem.Length / 1GB, 2)) GB to $staging ..." -ForegroundColor Cyan
	if (Test-Path -LiteralPath $staging) { Remove-Item -LiteralPath $staging -Recurse -Force }
	New-Item -ItemType Directory -Force -Path $staging | Out-Null
	& tar -xf $tarballItem.FullName -C $staging
	if ($LASTEXITCODE -ne 0) { throw "tar exited with code $LASTEXITCODE while extracting '$tarball'." }
}

# --- Master volume -------------------------------------------------------
$masterExists = Test-RigVolumeExists -Name "$($config.MasterVolume)"
if ($masterExists -and -not $Force -and $stagingCurrent) {
	Write-Host "Master volume '$($config.MasterVolume)' already exists (use -Force to rebuild)." -ForegroundColor DarkGray
}
else {
	Write-Host "Rebuilding master volume '$($config.MasterVolume)' from the tarball ..." -ForegroundColor Cyan
	Remove-RigVolume -Config $config -Name "$($config.MasterVolume)"
	New-RigVolume -Config $config -Name "$($config.MasterVolume)" | Out-Null

	# Untar INSIDE the container: the tarball's directory is bind-mounted
	# read-only and the extraction target is the fast Docker-side volume, so
	# the many small files never cross the host bind mount individually.
	Invoke-RigDocker -DockerArgs @(
		'run', '--rm',
		'-v', "$($tarballItem.DirectoryName):/backup:ro",
		'-v', "$($config.MasterVolume):/to",
		'busybox:latest',
		'sh', '-c', "tar -xf '/backup/$($tarballItem.Name)' -C /to"
	) | Out-Null
}

# The embedder cache volume is created empty; the model download populates it
# on the first run and every later run reuses it.
New-RigVolume -Config $config -Name "$($config.HfCacheVolume)" | Out-Null

# --- Image tags ----------------------------------------------------------
if (-not $SkipImages) {
	Write-Host 'Applying the rig image tags (additional tags on already-built images) ...' -ForegroundColor Cyan
	Add-RigImageTag -Config $config -Source "$($config.SourceMcpImage)" -Destination "$($config.McpImage)" | Out-Null
	Add-RigImageTag -Config $config -Source "$($config.SourceEmbedderImage)" -Destination "$($config.EmbedderImage)" | Out-Null
	Write-Host "  $($config.SourceMcpImage) -> $($config.McpImage)"
	Write-Host "  $($config.SourceEmbedderImage) -> $($config.EmbedderImage)"
}

$manifest | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $manifestPath -Encoding ascii

# --- Final gate ----------------------------------------------------------
# Resolve the compose document now, so a misconfiguration is caught during
# preparation rather than in the middle of a measured cohort.
Assert-RigDockerIsolation -Config $config | Out-Null
Write-Host 'Resolved compose document passed the isolation guard.' -ForegroundColor Green

Write-Host ''
Write-Host 'Master prepared.' -ForegroundColor Green
Write-Host "  staging  : $staging"
Write-Host "  master   : $($config.MasterVolume)"
Write-Host "  images   : $($config.McpImage), $($config.EmbedderImage)"
Write-Host ''
Write-Host 'Next: ./run-cohort.ps1' -ForegroundColor Cyan
