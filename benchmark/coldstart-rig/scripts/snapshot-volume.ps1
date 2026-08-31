#!/usr/bin/env pwsh
<#
.SYNOPSIS
	Extracts a rig volume to a host staging directory so the OFFLINE census can
	walk it, without disturbing the pristine master.

.DESCRIPTION
	inspect-state.ps1 answers a durable-state question from a host staging
	directory (the WAL framing walk) plus a SQLite database (grain state). Both
	default to the copy that prepare-master.ps1 laid down from the backup
	tarball, which is the PRISTINE "before" state and must stay that way.

	To census a volume that has since MOVED ON - most importantly the WORKING
	volume after a box has been left running long enough to heal itself - that
	volume first has to become a staging directory of its own. That is all this
	script does:

	  1. tar the volume's contents INSIDE a throwaway container (so 1.8 GB of
	     small files are read from the Docker VM's own filesystem rather than
	     dragged across a bind mount file by file, exactly as prepare-master.ps1
	     does in the opposite direction);
	  2. extract that tarball into <runRoot>/state/<Name>/ on the host;
	  3. copy the grain-state database to where a `-SqliteSource staging` census
	     expects it.

	Then:

	  ./inspect-state.ps1 -StagingPath <printed path> -SqliteSource staging -SkipExpectations

	The -SkipExpectations is not optional in spirit: census-expectations.json
	pins the PRE-epic figures from a specific backup, so a healed volume is
	SUPPOSED to differ from them and the known-answer check would correctly
	report a mismatch.

	ISOLATION. The volume name is put through the same guard every other
	binding operation uses, so this can only ever read a volume inside the rig's
	own namespace. The mount is read-only.

.PARAMETER Volume
	The rig volume to snapshot. Defaults to the working volume.

.PARAMETER Name
	The staging directory name to create under <runRoot>/state/. Defaults to
	the volume name plus a UTC stamp.

.EXAMPLE
	./snapshot-volume.ps1 -Name after-heal
#>
[CmdletBinding()]
param(
	[string] $Volume,
	[string] $Name,
	[string] $ParametersFile,
	[switch] $Force
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$here = Split-Path -Parent $MyInvocation.MyCommand.Path
. (Join-Path $here '_rig-docker.ps1')

$config = Get-RigConfig -ParametersFile $ParametersFile -ScriptRoot $here
Assert-RigIsolation -Config $config | Out-Null

if (-not $Volume) { $Volume = "$($config.WorkVolume)" }

# The same name guard every binding operation uses. A volume outside the rig's
# prefix - or any live deployment volume - is refused here, not merely avoided.
$violations = Test-RigVolumeName -Volume $Volume -Config $config -Label 'volume'
if ($violations.Count -gt 0) {
	throw ("Rig isolation guard REFUSED to snapshot: " + ($violations -join '; ') + '.')
}
if (-not (Test-RigVolumeExists -Name $Volume)) {
	throw "Volume '$Volume' does not exist."
}

if (-not $Name) { $Name = "$Volume-{0}" -f ([datetime]::UtcNow.ToString('yyyyMMddTHHmmssZ')) }

$runRoot = Get-RigRunRoot -ScriptRoot $here
$staging = Join-Path (Join-Path $runRoot 'state') $Name
if ((Test-Path -LiteralPath $staging) -and -not $Force) {
	throw "Staging directory already exists: $staging (pass -Force to replace it)."
}
if (Test-Path -LiteralPath $staging) { Remove-Item -LiteralPath $staging -Recurse -Force }
New-Item -ItemType Directory -Force -Path $staging | Out-Null

$transferRoot = Join-Path $runRoot 'transfer'
New-Item -ItemType Directory -Force -Path $transferRoot | Out-Null
$tarball = Join-Path $transferRoot "$Name.tar"
if (Test-Path -LiteralPath $tarball) { Remove-Item -LiteralPath $tarball -Force }

Write-Host "Snapshotting volume '$Volume' ..." -ForegroundColor Cyan
$watch = [System.Diagnostics.Stopwatch]::StartNew()

# READ-ONLY on the source. The only writable mount is the host transfer
# directory the tarball lands in.
Invoke-RigDocker -DockerArgs @(
	'run', '--rm',
	'-v', "${Volume}:/data:ro",
	'-v', "${transferRoot}:/out",
	'busybox:latest',
	'tar', '-cf', "/out/$Name.tar", '-C', '/data', '.'
) | Out-Null

if (-not (Test-Path -LiteralPath $tarball)) {
	throw "Snapshot tarball was not produced at $tarball."
}

& tar -xf $tarball -C $staging
if ($LASTEXITCODE -ne 0) { throw "tar exited with code $LASTEXITCODE while extracting '$tarball'." }
Remove-Item -LiteralPath $tarball -Force

$watch.Stop()

$databases = @(Get-ChildItem -LiteralPath $staging -Filter '*.db' -File -ErrorAction SilentlyContinue)
$sizeBytes = (Get-ChildItem -LiteralPath $staging -Recurse -File | Measure-Object -Property Length -Sum).Sum

$manifest = [ordered] @{
	schemaVersion  = 1
	kind           = 'coldstart-rig/volume-snapshot'
	volume         = $Volume
	stagingPath    = $staging
	snapshotUtc    = [datetime]::UtcNow.ToString('o')
	elapsedSeconds = [Math]::Round($watch.Elapsed.TotalSeconds, 1)
	sizeBytes      = $sizeBytes
	databases      = @($databases | ForEach-Object { $_.Name })
}
$manifest | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath (Join-Path $staging '.rig-snapshot.json') -Encoding utf8

Write-Host ("  {0:N2} GB in {1:N1}s" -f ($sizeBytes / 1GB), $watch.Elapsed.TotalSeconds) -ForegroundColor DarkGray
Write-Host ''
Write-Host 'Snapshot ready.' -ForegroundColor Green
Write-Host "  staging : $staging"
Write-Host "  next    : ./inspect-state.ps1 -StagingPath '$staging' -SqliteSource staging -SkipExpectations"
