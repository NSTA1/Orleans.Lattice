#!/usr/bin/env pwsh
<#
.SYNOPSIS
	Records this deployment's attribution-relevant configuration and refuses a
	multi-variable step nobody acknowledged.

.DESCRIPTION
	Issue #2931. Run this immediately after `docker compose up`, before taking
	any reading you intend to compare against a previous run.

	WHAT IT IS FOR. A pre-registered predicate that asserts grant equality as
	prose, verified by hand after the deploy, is checked by the person most
	motivated to find it clean. This moves that check into the rig: the
	configuration is recorded as a file, the file is diffed against the previous
	run's, and a step that moved two attribution-relevant variables at once is
	REFUSED unless the operator said so in advance and gave a reason.

	It does not stop anyone changing two variables. It stops them doing it
	SILENTLY, which is the difference between a deliberate experiment and a void
	comparison.

	WHY A FILE AND NOT JUST CONSOLE OUTPUT. The manifest has to be citable when
	scoring a run. A scrollback is not evidence: it is not addressable, not
	durable, and not checkable by a second reader. -ManifestPath writes it.

	DECLARED AND EFFECTIVE ARE BOTH RECORDED. What the resolved compose
	configuration says and what the running container carries are different
	objects, and this epic has now lost three claims to that distinction - at a
	value (#2928), at a checkout (#2930), and at an image (a token the source
	understood and the deployed binary did not). A manifest of intentions would
	repeat the mistake one layer up, so a divergence between the two is itself
	reported.

	THE PURE HALF is _deployManifest.ps1 beside this file. Every decision -
	what counts as attribution-relevant, what a delta is, whether a step is
	attributable, how a manifest renders and reads back - is made there, by
	functions that touch no file, no container and no environment. This script
	only acquires. That split is what lets Test-DeployManifest.ps1 demonstrate
	the REFUSING direction without a daemon.

.PARAMETER ComposeDirectory
	Directory holding the compose files. Defaults to the parent of this script.

.PARAMETER ManifestPath
	Where to write this run's manifest. Defaults to
	.deploy/manifests/<label>.manifest beside the compose files.

.PARAMETER BaselinePath
	A previous run's manifest to diff against. When omitted, the most recent
	manifest already in the manifest directory is used, and when there is none
	the run is recorded as a baseline with nothing to compare.

.PARAMETER Label
	Names this run in the manifest, for example `run-14`. Defaults to a
	timestamp.

.PARAMETER AcceptMultipleDeltas
	Acknowledge, IN ADVANCE, that this step moves more than one
	attribution-relevant variable. Requires -Reason. Without it a multi-variable
	step exits 2.

.PARAMETER Reason
	Why the multi-variable step is intended. Recorded in the manifest so the
	acknowledgement is as durable as the thing it excuses.

.PARAMETER DeclaredReading
	A pre-built hashtable used instead of reading compose. For tests, and for an
	operator checking a candidate configuration before deploying it.

.PARAMETER EffectiveReading
	A pre-built hashtable used instead of inspecting the container.

.PARAMETER CgroupCpuQuota
	The enforced CPU quota, used to derive the processor count when
	DOTNET_PROCESSOR_COUNT is absent. Read from the container when not supplied.

.OUTPUTS
	Exit code, matching the vocabulary the sibling assert scripts already use:

	  0  recorded; at most one attribution-relevant variable moved
	  2  REFUSED: a multi-variable step nobody acknowledged, a declared/effective
	     divergence, or an indeterminate processor-count provenance
	  4  the deployment could not be read

	Indeterminate provenance is a REFUSAL and not a warning. The requirement on
	this script is that the resolved count and its source are observable; a run
	that cannot say where its processor count came from has not met it, and
	recording "unknown" while exiting 0 would be the instrument reporting
	success for the one outcome it exists to prevent.

.EXAMPLE
	./scripts/Assert-DeployManifest.ps1 -Label run-14

.EXAMPLE
	./scripts/Assert-DeployManifest.ps1 -Label run-15 -AcceptMultipleDeltas -Reason 'retuning both grants together'
#>
[CmdletBinding()]
param(
	[string] $ComposeDirectory,
	[string] $ManifestPath,
	[string] $BaselinePath,
	[string] $Label,
	[switch] $AcceptMultipleDeltas,
	[string] $Reason,
	[hashtable] $DeclaredReading,
	[hashtable] $EffectiveReading,
	[AllowNull()] [nullable[double]] $CgroupCpuQuota,
	[string] $ContainerName = 'repocontextcontainer-repocontext-1',
	[switch] $Quiet
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

. (Join-Path $PSScriptRoot '_deployManifest.ps1')

if (-not $ComposeDirectory) {
	$ComposeDirectory = Split-Path -Parent $PSScriptRoot
}

if (-not $Label) {
	$Label = Get-Date -Format 'yyyyMMdd-HHmmss'
}

<#
.SYNOPSIS
	Reads the attribution-relevant environment the running container carries.

.DESCRIPTION
	`docker inspect` rather than `docker compose config`, because this is the
	EFFECTIVE half: what the process can actually see. Returns $null when the
	container cannot be read at all, which the caller turns into exit 4 - as
	distinct from reading it successfully and finding a variable absent, which
	is a legitimate recorded state.
#>
function Get-EffectiveReading {
	[CmdletBinding()]
	param([Parameter(Mandatory)] [string] $Container)

	$raw = & docker inspect $Container --format '{{json .Config.Env}}' 2>&1

	if ($LASTEXITCODE -ne 0) {
		return $null
	}

	$reading = @{}

	foreach ($entry in ($raw | ConvertFrom-Json)) {
		$separator = ([string] $entry).IndexOf('=')

		if ($separator -lt 1) {
			continue
		}

		$reading[([string] $entry).Substring(0, $separator)] = ([string] $entry).Substring($separator + 1)
	}

	$grants = & docker inspect $Container --format '{{.HostConfig.Memory}} {{.HostConfig.NanoCpus}}' 2>&1

	if ($LASTEXITCODE -eq 0) {
		$parts = ([string] $grants).Trim() -split '\s+'

		if ($parts.Count -eq 2) {
			$memory = 0L
			$nanoCpus = 0L

			if ([long]::TryParse($parts[0], [ref] $memory) -and $memory -gt 0) {
				$reading['repocontext.mem_limit'] = "$([int][Math]::Round($memory / 1MB))m"
			}

			if ([long]::TryParse($parts[1], [ref] $nanoCpus) -and $nanoCpus -gt 0) {
				$reading['repocontext.cpus'] = ('{0:0.##}' -f ($nanoCpus / 1e9))
			}
		}
	}

	return $reading
}

<#
.SYNOPSIS
	Reads the enforced CPU quota from the container, in cores.

.DESCRIPTION
	Returns $null rather than a default when it cannot be read. A default here
	would be this script inventing the very provenance it is supposed to
	establish, and the resulting manifest would assert a source it does not
	have.
#>
function Get-CgroupCpuQuota {
	[CmdletBinding()]
	param([Parameter(Mandatory)] [string] $Container)

	$raw = & docker inspect $Container --format '{{.HostConfig.NanoCpus}}' 2>&1

	if ($LASTEXITCODE -ne 0) {
		return $null
	}

	$nanoCpus = 0L

	if (-not [long]::TryParse(([string] $raw).Trim(), [ref] $nanoCpus) -or $nanoCpus -le 0) {
		return $null
	}

	return [double] ($nanoCpus / 1e9)
}

if ($null -ne $EffectiveReading) {
	$effective = $EffectiveReading
}
else {
	$effective = Get-EffectiveReading -Container $ContainerName

	if ($null -eq $effective) {
		Write-Host "DEPLOY MANIFEST UNREADABLE: could not inspect container '$ContainerName'."
		Write-Host 'Start the stack first, or pass -EffectiveReading to check a candidate configuration.'
		exit 4
	}
}

if ($null -ne $DeclaredReading) {
	$declared = $DeclaredReading
}
else {
	# The declared half is best-effort ON PURPOSE, and this is the one place the
	# script tolerates a gap. Resolving compose requires every :?-guarded
	# variable to be supplied, which is not true of a shell that merely wants to
	# record what is running. A missing declared half costs the divergence check
	# and nothing else, whereas refusing outright would mean the manifest - the
	# thing that has to exist for the comparison to be scoreable - does not get
	# written at all.
	$declared = @{}
}

if ($null -eq $CgroupCpuQuota -and $null -eq $EffectiveReading) {
	$CgroupCpuQuota = Get-CgroupCpuQuota -Container $ContainerName
}

$manifest = New-DeployManifest `
	-Declared $declared `
	-Effective $effective `
	-CgroupCpuQuota $CgroupCpuQuota `
	-Label $Label

$rendered = Format-DeployManifest -Manifest $manifest -GeneratedAt (Get-Date -Format 'yyyy-MM-dd HH:mm:ss')

if (-not $ManifestPath) {
	$manifestDirectory = Join-Path $ComposeDirectory '.deploy/manifests'
	$ManifestPath = Join-Path $manifestDirectory "$Label.manifest"
}
else {
	$manifestDirectory = Split-Path -Parent $ManifestPath
}

if ($manifestDirectory -and -not (Test-Path -LiteralPath $manifestDirectory)) {
	New-Item -ItemType Directory -Path $manifestDirectory -Force | Out-Null
}

[IO.File]::WriteAllText($ManifestPath, $rendered)

if (-not $Quiet) {
	Write-Host ''
	Write-Host 'PROCESSOR COUNT' -ForegroundColor Cyan
	Write-Host ("  resolved : {0}" -f $(if ($null -eq $manifest.ProcessorCount.Resolved) { '<INDETERMINATE>' } else { $manifest.ProcessorCount.Resolved }))
	Write-Host ("  source   : {0}" -f $manifest.ProcessorCount.Source)
	Write-Host ("  because  : {0}" -f $manifest.ProcessorCount.Reason)
	Write-Host ''
	Write-Host "Wrote $ManifestPath"
}

$refusals = @()

if ($manifest.ProcessorCount.Source -eq 'Indeterminate') {
	$refusals += 'PROCESSOR COUNT PROVENANCE IS INDETERMINATE. ' + $manifest.ProcessorCount.Reason +
		' The requirement on this script (issue #2931) is that the resolved count AND its source are' +
		' observable at deploy time, so recording "unknown" and exiting 0 would be this instrument' +
		' reporting success for the exact outcome it exists to prevent.'
}

$divergences = Get-DeployManifestDivergence -Manifest $manifest

foreach ($divergence in $divergences) {
	$refusals += "DECLARED/EFFECTIVE DIVERGENCE: $divergence"
}

if (-not $BaselinePath) {
	$candidates = @()

	if (Test-Path -LiteralPath $manifestDirectory) {
		$candidates = @(Get-ChildItem -LiteralPath $manifestDirectory -Filter '*.manifest' -File |
			Where-Object { $_.FullName -ne (Resolve-Path -LiteralPath $ManifestPath).Path } |
			Sort-Object LastWriteTimeUtc -Descending)
	}

	if ($candidates.Count -gt 0) {
		$BaselinePath = $candidates[0].FullName
	}
}

if ($BaselinePath -and (Test-Path -LiteralPath $BaselinePath -PathType Leaf)) {
	$baseline = Read-DeployManifest -Text ([IO.File]::ReadAllText($BaselinePath))
	$deltas = Compare-DeployManifest -Baseline $baseline -Current $manifest
	$verdict = Get-AttributionVerdict -Deltas $deltas

	if (-not $Quiet) {
		Write-Host ''
		Write-Host 'ATTRIBUTION' -ForegroundColor Cyan
		Write-Host ("  baseline : {0}" -f $BaselinePath)
		Write-Host ("  verdict  : {0}" -f $verdict.Summary)

		foreach ($detail in $verdict.Detail) {
			Write-Host "    - $detail"
		}
	}

	if (-not $verdict.Attributable) {
		if ($AcceptMultipleDeltas -and -not [string]::IsNullOrWhiteSpace($Reason)) {
			if (-not $Quiet) {
				Write-Host ''
				Write-Host "ACKNOWLEDGED multi-variable step: $Reason" -ForegroundColor Yellow
			}

			[IO.File]::AppendAllText($ManifestPath, "`n# ACKNOWLEDGED MULTI-VARIABLE STEP: $Reason`n")
		}
		elseif ($AcceptMultipleDeltas) {
			$refusals += '-AcceptMultipleDeltas was passed without -Reason. The acknowledgement has to be as ' +
				'durable as the thing it excuses, or the manifest records that somebody waved it through and ' +
				'not why - which is the state this whole file exists to leave behind.'
		}
		else {
			$refusals += $verdict.Summary
		}
	}
}
elseif (-not $Quiet) {
	Write-Host ''
	Write-Host 'ATTRIBUTION: no baseline manifest found; this run is recorded as the baseline.'
}

if ($refusals.Count -eq 0) {
	if (-not $Quiet) {
		Write-Host ''
		Write-Host 'DEPLOY MANIFEST OK.' -ForegroundColor Green
	}

	exit 0
}

Write-Host ''
Write-Host "DEPLOY MANIFEST REFUSED: $($refusals.Count) finding(s)."
Write-Host ''

foreach ($refusal in $refusals) {
	Write-Host "  - $refusal"
	Write-Host ''
}

Write-Host 'The manifest has still been written, so the configuration is on record either way.'
Write-Host 'What is refused is treating this run as comparable to the baseline without saying so.'

exit 2
