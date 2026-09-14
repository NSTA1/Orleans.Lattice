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

	THE DECLARED HALF IS RESOLVED WITH THE TUNING OVERLAY, and this script
	resolves it - #2983 was that it never did. A bare `docker compose config`
	reads only docker-compose.yml and docker-compose.override.yml, while every
	knob this manifest tracks is set by docker-compose.tuning.yml, so resolving
	without the overlay would report `<absent>` for all of them while now
	claiming to have looked. The file list lives in Get-DeployComposeFile in the
	pure library so that the requirement is assertable without a daemon.

	WHEN THE DECLARED HALF CANNOT BE RESOLVED, THE MANIFEST SAYS SO. `<absent>`
	means compose was read and does not declare the key; `<unreadable>` means it
	could not be read and NOTHING is known. Conflating them is what let #2983
	survive thirteen deployments - every key reported `<absent>`, which reads as
	a positive finding, when the truth was that no resolution had been attempted.
	A suppressed divergence check also announces itself, because zero findings
	and no findings possible are different facts and must not print alike.

	THE PURE HALF is _deployManifest.ps1 beside this file. Every decision -
	what counts as attribution-relevant, what a delta is, whether a step is
	attributable, how a manifest renders and reads back - is made there, by
	functions that touch no file, no container and no environment. This script
	does the acquisition, and ONLY the acquisition. That split is what lets
	Test-DeployManifest.ps1 demonstrate the REFUSING direction without a daemon.

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
	A pre-built hashtable used INSTEAD OF resolving compose. For tests, and for
	an operator checking a candidate configuration before deploying it. Omitting
	it is the normal path and resolves the compose files for real.

.PARAMETER EffectiveReading
	A pre-built hashtable used instead of inspecting the containers.

.PARAMETER ContainerName
	The repocontext container to inspect for the effective half.

.PARAMETER EmbedContainerName
	The embedder container to inspect. Read because EMBED_INTRA_THREADS and both
	embedder grants are attribution-relevant; inspecting only repocontext left
	them permanently effective-absent, which would read as a divergence once the
	declared half started resolving them. An embedder that is not running is not
	fatal - its keys are simply recorded absent.

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
	[string] $EmbedContainerName = 'repocontextcontainer-embedder-1',
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
	param(
		[Parameter(Mandatory)] [string] $Container,
		[AllowNull()] [AllowEmptyString()] [string] $EmbedContainer
	)

	$reading = Read-ContainerReading -Container $Container -Prefix 'repocontext'

	if ($null -eq $reading) {
		return $null
	}

	# The embedder is read too, because Get-AttributionVariable tracks
	# EMBED_INTRA_THREADS, embedder.cpus and embedder.mem_limit. Inspecting only
	# repocontext made those three permanently effective-absent, which was
	# harmless while the declared half was ALSO empty - the two gaps cancelled.
	# Acquiring the declared half without this would uncancel them and report
	# three divergences on a healthy deploy, so a partial fix to #2983 is worse
	# than the defect: it converts a silent no-op into a guard that refuses
	# correct deployments, which is how guards get removed.
	#
	# A missing embedder is NOT fatal. The repocontext container is what the
	# script exists to record; an embedder that is not running leaves its keys
	# effective-absent, which the divergence check reports honestly rather than
	# the script refusing to write a manifest at all.
	if (-not [string]::IsNullOrWhiteSpace($EmbedContainer)) {
		$embed = Read-ContainerReading -Container $EmbedContainer -Prefix 'embedder'

		if ($null -ne $embed) {
			# ONLY the keys the attribution table assigns to the embedder are taken
			# from it. A flat merge is wrong and is not a theoretical risk: this
			# repository declares DOTNET_gcServer as '1' on repocontext and '0' on
			# embedder, so merging every key let the embedder's 0 overwrite
			# repocontext's 1 and the very first live run reported a
			# DECLARED/EFFECTIVE DIVERGENCE on a correct deployment.
			#
			# The declared half is already read per service. This is the same
			# discipline on the effective half, and the invariant is worth naming
			# because it is not visible in the manifest's shape: records are keyed
			# by bare variable NAME, so nothing in the key says which container the
			# value must come from. Get-AttributionVariable's Service is the only
			# thing that does.
			$owned = @(@(Get-AttributionVariable) + @(Get-AttributionGrant) |
				Where-Object { $_.Service -eq 'embedder' } |
				ForEach-Object { $_.Name })

			foreach ($key in $owned) {
				if ($embed.ContainsKey($key)) {
					$reading[$key] = $embed[$key]
				}
			}
		}
	}

	return $reading
}

<#
.SYNOPSIS
	Reads one container's environment and grants, keying the grants by service.

.DESCRIPTION
	The grant keys carry a service prefix because `cpus` and `mem_limit` exist
	per service and a manifest that recorded a bare `cpus` could not say whose.
	The rendering - MiB with an `m` suffix, cores to two decimals - is the
	NORMAL FORM both halves are converted into, so that a declared
	`mem_limit: "12884901888"` and an effective 12884901888 bytes compare equal
	instead of reporting a divergence that is purely a difference of units.
#>
function Read-ContainerReading {
	[CmdletBinding()]
	param(
		[Parameter(Mandatory)] [string] $Container,
		[Parameter(Mandatory)] [string] $Prefix
	)

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
				$reading["$Prefix.mem_limit"] = ConvertTo-MemoryGrant -Bytes $memory
			}

			if ([long]::TryParse($parts[1], [ref] $nanoCpus) -and $nanoCpus -gt 0) {
				$reading["$Prefix.cpus"] = ConvertTo-CpuGrant -Cores ($nanoCpus / 1e9)
			}
		}
	}

	return $reading
}

<#
.SYNOPSIS
	Renders a byte count as the manifest's normal form for a memory grant.

.DESCRIPTION
	Shared by both halves so the comparison is between values, not notations.
	Compose declares `mem_limit: "12884901888"` while docker reports 12884901888
	bytes; without a shared normal form those render as `12884901888` and
	`12288m` and the divergence check reports a finding that is entirely an
	artefact of the instrument. A false divergence is not a safe failure here -
	it exits 2 on a correct deployment, and a guard that refuses correct
	deployments is one an operator learns to bypass.
#>
function ConvertTo-MemoryGrant {
	[CmdletBinding()]
	[OutputType([string])]
	param([Parameter(Mandatory)] [long] $Bytes)

	return "$([int][Math]::Round($Bytes / 1MB))m"
}

<#
.SYNOPSIS
	Renders a core count as the manifest's normal form for a CPU grant.
#>
function ConvertTo-CpuGrant {
	[CmdletBinding()]
	[OutputType([string])]
	param([Parameter(Mandatory)] [double] $Cores)

	return ('{0:0.##}' -f $Cores)
}

<#
.SYNOPSIS
	Resolves the compose configuration and returns the DECLARED half.

.DESCRIPTION
	Returns $null when the configuration cannot be resolved, which the caller
	records as Unreadable WITH the reason rather than as an absence. The two are
	rendered differently in the manifest on purpose - see the DeclarationStatus
	block in _deployManifest.ps1.

	THE OVERLAY IS NOT OPTIONAL. The file list comes from Get-DeployComposeFile
	in the pure library, so the requirement is unit-testable without a daemon. A
	bare `docker compose config` resolves only docker-compose.yml and
	docker-compose.override.yml, and every attribution-relevant knob is set by
	docker-compose.tuning.yml: resolving without it yields `<absent>` for all of
	them while now claiming to have looked.

	VALUES ARE EXTRACTED PER SERVICE. DOTNET_gcServer is `1` on repocontext and
	`0` on embedder in this very repository, so a key-only scan over the
	resolved document picks whichever service it met first and can manufacture a
	divergence out of two correct declarations. Each record's Service says which
	one it means, and that is what is read.
#>
function Get-DeclaredReading {
	[CmdletBinding()]
	param(
		[Parameter(Mandatory)] [string] $Directory,
		[Parameter(Mandatory)] [ref] $Reason
	)

	$arguments = @()

	foreach ($file in Get-DeployComposeFile) {
		$path = Join-Path $Directory $file

		if (-not (Test-Path -LiteralPath $path)) {
			$Reason.Value = "compose file '$file' not found in '$Directory'"
			return $null
		}

		$arguments += @('-f', $path)
	}

	$previous = $PWD
	$raw = $null

	try {
		# Resolved from the compose directory so the project's .env is picked up.
		# Several knobs are `${VAR:?}`-guarded and the resolution fails without it -
		# which is a legitimate Unreadable, not a crash.
		Set-Location -LiteralPath $Directory
		$raw = & docker compose @arguments config --format json 2>&1
	}
	catch {
		$Reason.Value = "docker compose config could not be started: $($_.Exception.Message)"
		return $null
	}
	finally {
		Set-Location -LiteralPath $previous
	}

	if ($LASTEXITCODE -ne 0) {
		$Reason.Value = "docker compose config exited $LASTEXITCODE : $((($raw | Out-String) -split "`n" | Where-Object { $_.Trim() } | Select-Object -First 1))".Trim()
		return $null
	}

	try {
		$document = ($raw | Out-String) | ConvertFrom-Json
	}
	catch {
		$Reason.Value = "docker compose config returned output that is not JSON: $($_.Exception.Message)"
		return $null
	}

	if ($null -eq $document -or -not ($document.PSObject.Properties.Name -contains 'services')) {
		$Reason.Value = 'docker compose config returned no services block'
		return $null
	}

	$reading = @{}

	foreach ($record in @(Get-AttributionVariable) + @(Get-AttributionGrant)) {
		$service = $document.services.PSObject.Properties |
			Where-Object { $_.Name -eq $record.Service } |
			Select-Object -First 1

		if ($null -eq $service) {
			continue
		}

		$definition = $service.Value

		if ($record.Name -like '*.mem_limit') {
			$bytes = 0L

			if ($definition.PSObject.Properties.Name -contains 'mem_limit' -and
				[long]::TryParse([string] $definition.mem_limit, [ref] $bytes) -and $bytes -gt 0) {
				$reading[$record.Name] = ConvertTo-MemoryGrant -Bytes $bytes
			}

			continue
		}

		if ($record.Name -like '*.cpus') {
			$cores = 0.0

			if ($definition.PSObject.Properties.Name -contains 'cpus' -and
				[double]::TryParse([string] $definition.cpus, [ref] $cores) -and $cores -gt 0) {
				$reading[$record.Name] = ConvertTo-CpuGrant -Cores $cores
			}

			continue
		}

		if (-not ($definition.PSObject.Properties.Name -contains 'environment')) {
			continue
		}

		$entry = $definition.environment.PSObject.Properties |
			Where-Object { $_.Name -eq $record.Name } |
			Select-Object -First 1

		# A bare `KEY:` with no value resolves to null and creates NO variable in
		# the container - it is a pass-through from a host environment that does
		# not set it. Recording that as a declared empty string would assert a
		# declaration the deployment does not make.
		if ($null -ne $entry -and $null -ne $entry.Value) {
			$reading[$record.Name] = [string] $entry.Value
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
	$effective = Get-EffectiveReading -Container $ContainerName -EmbedContainer $EmbedContainerName

	if ($null -eq $effective) {
		Write-Host "DEPLOY MANIFEST UNREADABLE: could not inspect container '$ContainerName'."
		Write-Host 'Start the stack first, or pass -EffectiveReading to check a candidate configuration.'
		exit 4
	}
}

if ($null -ne $DeclaredReading) {
	$declared = $DeclaredReading
	$declarationStatus = 'Available'
	$declarationReason = 'supplied by the caller via -DeclaredReading'
}
else {
	# #2983: this branch used to set $declared = @{} and never resolve anything.
	# The comment described TOLERATING a resolution failure; the code never
	# attempted a resolution, so the declared half was empty in 100% of real
	# invocations, the divergence check short-circuited, and the script printed
	# OK. Thirteen deployments, never once run on real data.
	#
	# The tolerance the old comment argued for is real and is kept - a shell with
	# no .env cannot resolve the `${VAR:?}`-guarded knobs, and refusing outright
	# would mean the manifest does not get written at all. What changes is that
	# the failure is now RECORDED as Unreadable with its reason, instead of being
	# indistinguishable from a clean resolution that found nothing.
	$reason = ''
	$declared = Get-DeclaredReading -Directory $ComposeDirectory -Reason ([ref] $reason)

	if ($null -eq $declared) {
		$declared = @{}
		$declarationStatus = 'Unreadable'
		$declarationReason = $reason
	}
	else {
		$declarationStatus = 'Available'
		$declarationReason = "resolved from $((Get-DeployComposeFile) -join ', ')"
	}
}

if ($null -eq $CgroupCpuQuota -and $null -eq $EffectiveReading) {
	$CgroupCpuQuota = Get-CgroupCpuQuota -Container $ContainerName
}

$manifest = New-DeployManifest `
	-Declared $declared `
	-Effective $effective `
	-CgroupCpuQuota $CgroupCpuQuota `
	-Label $Label `
	-DeclarationStatus $declarationStatus `
	-DeclarationReason $declarationReason

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

# Zero divergences and NO divergence check possible are different facts, and until
# #2983 they printed identically. The suppression line is emitted whenever the
# check did not run, so the operator cannot read an empty findings list as a clean
# comparison. This is not a refusal: a manifest still gets written, which is the
# whole reason the tolerance exists. It is a refusal to let the silence pass as a
# verdict.
$suppression = Get-DeclarationSuppression -Manifest $manifest

if (-not [string]::IsNullOrWhiteSpace($suppression) -and -not $Quiet) {
	Write-Host ''
	Write-Host $suppression -ForegroundColor Yellow
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

		# The verdict NAMES what it covers. "OK" unqualified over a suppressed
		# divergence check is the #2983 shape exactly: a green line standing for a
		# comparison that never happened, syntactically identical to one that did.
		if ([string]::IsNullOrWhiteSpace($suppression)) {
			Write-Host 'DEPLOY MANIFEST OK.' -ForegroundColor Green
		}
		else {
			Write-Host 'DEPLOY MANIFEST OK (ATTRIBUTION ONLY - the declared/effective check did not run).' -ForegroundColor Yellow
		}
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
