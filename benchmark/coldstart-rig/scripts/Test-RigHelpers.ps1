#!/usr/bin/env pwsh
<#
.SYNOPSIS
	Regression tests for the cold-start rig's isolation guard, its offline
	file-WAL framing parser, and its deterministic statistics helpers.

.DESCRIPTION
	Exercises the pure functions in _rig-helpers.ps1 (and the pure parsing
	helpers in _rig-docker.ps1) against literal fixtures, so the rig's
	correctness can be verified without Docker, without a restored volume,
	and without any dependence on wall-clock timing. Pure pwsh; no Pester
	dependency, matching benchmark/azure-throughput/scripts/Test-CohortVerdict.ps1.

	The isolation-guard cases are the important ones. Decision D11 of epic
	#1830 requires the rig to be structurally incapable of touching the live
	deployment, so every one of the four live identities (compose project,
	volume, image tag, host port) has a test that asserts the guard REFUSES
	it - at the configuration layer and again at the resolved-compose layer.

	Exits with code 0 when every assertion passes and a non-zero count
	(equal to the number of failed assertions) when one or more fails:

		pwsh -File ./Test-RigHelpers.ps1
		if ($LASTEXITCODE -ne 0) { throw "cold-start rig tests failed" }

.EXAMPLE
	cd benchmark/coldstart-rig/scripts
	pwsh -File Test-RigHelpers.ps1
#>
[CmdletBinding()]
param(
	# The live-image-drift section constructs a REAL divergence out of two
	# throwaway images in the rig's own namespace, so it needs a Docker daemon.
	# It skips itself when there is none; pass this to skip it deliberately.
	[switch] $SkipDockerTests
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$here = Split-Path -Parent $MyInvocation.MyCommand.Path
. (Join-Path $here '_rig-docker.ps1')

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

# Asserts that a scriptblock throws, and that the message mentions the
# supplied fragment, so a test cannot pass on an unrelated failure.
function _AssertRefuses {
	param(
		[Parameter(Mandatory)] [string] $Name,
		[Parameter(Mandatory)] [scriptblock] $Action,
		[string] $Fragment = 'REFUSED'
	)
	$message = $null
	try { & $Action | Out-Null }
	catch { $message = $_.Exception.Message }

	if ($null -eq $message) {
		_Assert -Name $Name -Condition $false -Detail 'the guard ACCEPTED a configuration it must refuse'
		return
	}
	_Assert -Name $Name -Condition ($message -like "*$Fragment*") -Detail "threw, but the message did not mention '$Fragment': $message"
}

# Evaluates a scriptblock, returning $null when it throws, so a helper that is
# missing or broken is reported as a FAILED assertion rather than aborting the
# suite before it reaches its summary line.
function _Try {
	param([Parameter(Mandatory)] [scriptblock] $Action)
	try { return & $Action } catch { return $null }
}

# A baseline configuration with the same shape parameters.ps1 produces.
function New-TestConfig {
	param([hashtable] $Override = @{})

	$config = @{
		ProjectName             = 'lattice-coldstart'
		MasterVolume            = 'lattice-coldstart-master'
		ScaleMasterVolume       = 'lattice-coldstart-scale-master'
		WorkVolume              = 'lattice-coldstart-work'
		HfCacheVolume           = 'lattice-coldstart-hf'
		HostPort                = 18080
		McpImage                = 'repocontext-mcp:coldstart-rig'
		EmbedderImage           = 'rc-embedder:coldstart-rig'
		SourceMcpImage          = 'repocontext-mcp:local'
		SourceEmbedderImage     = 'repocontextcontainer-embedder:latest'
		BuildImageTagPrefix     = 'coldstart-'
		RequiredProjectPrefix   = 'lattice-coldstart'
		RequiredVolumePrefix    = 'lattice-coldstart'
		RequiredImageTag        = 'coldstart-rig'
		ForbiddenProjects       = @('repocontextcontainer')
		ForbiddenVolumePrefixes = @('repocontextcontainer_')
		ForbiddenVolumes        = @('repocontextcontainer_repocontext-data', 'repocontextcontainer_hf-cache')
		ForbiddenImages         = @('repocontext-mcp:local', 'repocontextcontainer-repocontext:latest', 'repocontextcontainer-embedder:latest')
		ForbiddenPorts          = @(8080)
	}
	foreach ($key in $Override.Keys) { $config[$key] = $Override[$key] }
	return $config
}

# A resolved compose document in the shape `docker compose config --format json`
# emits for the rig's own compose file.
function New-TestComposeDocument {
	param([string] $Json)

	if (-not $Json) {
		# The exact shape `docker compose config --format json` emits for the
		# rig's compose file. Note that a service mount's `source` is the
		# compose-file volume KEY, and the real Docker volume name lives on the
		# top-level `volumes` entry - the guard must resolve one to the other or
		# an innocent-looking key could point at a live volume.
		$Json = @'
{
  "name": "lattice-coldstart",
  "services": {
    "embedder": {
      "image": "rc-embedder:coldstart-rig",
      "restart": "no",
      "volumes": [ { "type": "volume", "source": "hf-cache", "target": "/app/.cache/huggingface", "volume": {} } ]
    },
    "repocontext": {
      "image": "repocontext-mcp:coldstart-rig",
      "restart": "no",
      "ports": [ { "mode": "ingress", "target": 8080, "published": "18080", "protocol": "tcp" } ],
      "volumes": [
        { "type": "volume", "source": "work", "target": "/data", "volume": {} },
        { "type": "bind", "source": "C:\\dev", "target": "/workspace", "read_only": true, "bind": {} }
      ]
    }
  },
  "volumes": {
    "hf-cache": { "name": "lattice-coldstart-hf", "external": true },
    "work": { "name": "lattice-coldstart-work", "external": true }
  }
}
'@
	}
	return ($Json | ConvertFrom-Json)
}

# ---------------------------------------------------------------------------
Write-Host 'Assert-RigIsolation (configuration layer)' -ForegroundColor Cyan
# ---------------------------------------------------------------------------

$baseline = New-TestConfig
$accepted = $true
try { Assert-RigIsolation -Config $baseline | Out-Null } catch { $accepted = $false; $baselineError = $_.Exception.Message }
_Assert -Name 'the shipped default configuration is accepted' -Condition $accepted `
	-Detail $(if ($accepted) { '' } else { $baselineError })

_AssertRefuses -Name 'REFUSES the live compose project' -Fragment 'LIVE project' `
	-Action { Assert-RigIsolation -Config (New-TestConfig @{ ProjectName = 'repocontextcontainer' }) }

_AssertRefuses -Name 'REFUSES a project outside the rig prefix' -Fragment 'required rig prefix' `
	-Action { Assert-RigIsolation -Config (New-TestConfig @{ ProjectName = 'something-else' }) }

_AssertRefuses -Name 'REFUSES the live data volume by exact name' -Fragment 'LIVE volume' `
	-Action { Assert-RigIsolation -Config (New-TestConfig @{ WorkVolume = 'repocontextcontainer_repocontext-data' }) }

_AssertRefuses -Name 'REFUSES any volume carrying the live prefix' -Fragment 'LIVE volume prefix' `
	-Action { Assert-RigIsolation -Config (New-TestConfig @{ MasterVolume = 'repocontextcontainer_anything-at-all' }) }

_AssertRefuses -Name 'REFUSES a volume outside the rig prefix' -Fragment 'required rig prefix' `
	-Action { Assert-RigIsolation -Config (New-TestConfig @{ HfCacheVolume = 'some-other-cache' }) }

_AssertRefuses -Name 'REFUSES the live image tag' -Fragment 'LIVE image tag' `
	-Action { Assert-RigIsolation -Config (New-TestConfig @{ McpImage = 'repocontext-mcp:local' }) }

_AssertRefuses -Name 'REFUSES an image without the rig tag' -Fragment 'required rig tag' `
	-Action { Assert-RigIsolation -Config (New-TestConfig @{ EmbedderImage = 'rc-embedder:something' }) }

_AssertRefuses -Name 'REFUSES the live host port' -Fragment 'LIVE host port' `
	-Action { Assert-RigIsolation -Config (New-TestConfig @{ HostPort = 8080 }) }

_AssertRefuses -Name 'REFUSES a non-numeric host port' -Fragment 'not an integer' `
	-Action { Assert-RigIsolation -Config (New-TestConfig @{ HostPort = 'eighty-eighty' }) }

_AssertRefuses -Name 'REFUSES a master volume equal to its working clone' -Fragment 'must be distinct' `
	-Action { Assert-RigIsolation -Config (New-TestConfig @{ WorkVolume = 'lattice-coldstart-master' }) }

_AssertRefuses -Name 'REFUSES tagging an image onto itself' -Fragment 'ADDITIONAL tag' `
	-Action { Assert-RigIsolation -Config (New-TestConfig @{ SourceMcpImage = 'repocontext-mcp:coldstart-rig' }) }

# Fail-closed: an absent or blank value must never be treated as "no constraint".
$missing = New-TestConfig
$missing.Remove('ForbiddenPorts') | Out-Null
_AssertRefuses -Name 'REFUSES a configuration missing a required key' -Fragment "required key 'ForbiddenPorts' is missing" `
	-Action { Assert-RigIsolation -Config $missing }

_AssertRefuses -Name 'REFUSES a configuration with an empty required key' -Fragment 'null or empty' `
	-Action { Assert-RigIsolation -Config (New-TestConfig @{ ProjectName = '   ' }) }

# Shape contract. Test-RigVolumeName is consumed by every docker-side helper
# with `$v = Test-RigVolumeName ...; if ($v.Count -gt 0) { refuse }`, so it
# must return a COLLECTION whose Count is the violation count - never a
# single-element wrapper around an empty array (which would make every clean
# name look like a violation) and never $null (which would make every dirty
# name look clean).
$cleanViolations = Test-RigVolumeName -Volume 'lattice-coldstart-work' -Config $baseline -Label 'volume'
_Assert -Name 'a clean volume name yields exactly zero violations' -Condition ($cleanViolations.Count -eq 0) `
	-Detail "Count was $($cleanViolations.Count): $($cleanViolations -join '; ')"
$dirtyViolations = Test-RigVolumeName -Volume 'repocontextcontainer_repocontext-data' -Config $baseline -Label 'volume'
_Assert -Name 'a live volume name yields at least one violation' -Condition ($dirtyViolations.Count -ge 1) `
	-Detail "Count was $($dirtyViolations.Count)"

# ---------------------------------------------------------------------------
Write-Host 'Assert-RigBuildImage (build destinations)' -ForegroundColor Cyan
# ---------------------------------------------------------------------------

# A build is the ONLY rig operation that creates an image, so it is the one
# place a live tag could be moved. Its destination guard gets the same
# treatment as the tagging guard: one test per way it could go wrong.

$buildReference = _Try { Get-RigBuildImageReference -Config $baseline -Sha 'a1b2c3d4e5f60718293a4b5c6d7e8f9012345678' }
_Assert -Name 'a build reference is the rig repository plus a build tag for the commit' `
	-Condition ($buildReference -eq 'repocontext-mcp:coldstart-a1b2c3d4e5f6') -Detail "got '$buildReference'"

$buildAccepted = $true
$buildError = ''
try { Assert-RigBuildImage -Config $baseline -Destination $buildReference | Out-Null }
catch { $buildAccepted = $false; $buildError = $_.Exception.Message }
_Assert -Name "the rig's own build destination is accepted" -Condition $buildAccepted -Detail $buildError

_AssertRefuses -Name 'REFUSES building the LIVE image tag' -Fragment 'LIVE image tag' `
	-Action { Assert-RigBuildImage -Config $baseline -Destination 'repocontext-mcp:local' }

_AssertRefuses -Name 'REFUSES building a live embedder tag' -Fragment 'LIVE image tag' `
	-Action { Assert-RigBuildImage -Config $baseline -Destination 'repocontextcontainer-embedder:latest' }

_AssertRefuses -Name 'REFUSES building an image without the rig build-tag prefix' -Fragment 'build-tag prefix' `
	-Action { Assert-RigBuildImage -Config $baseline -Destination 'repocontext-mcp:candidate-abc123' }

# An untagged reference means ':latest', which is exactly how a build would
# clobber a default tag by accident.
_AssertRefuses -Name 'REFUSES building an untagged reference' -Fragment 'build-tag prefix' `
	-Action { Assert-RigBuildImage -Config $baseline -Destination 'repocontext-mcp' }

# The build produces a SOURCE the rig then applies its additional run tag to.
# Building straight into the run tag would overwrite the image a rig may be
# running and would collapse the two-step layering that keeps the tag additive.
_AssertRefuses -Name 'REFUSES building into the tag the rig RUNS' -Fragment 'tag the rig RUNS' `
	-Action { Assert-RigBuildImage -Config $baseline -Destination 'repocontext-mcp:coldstart-rig' }

_AssertRefuses -Name 'REFUSES an empty build destination' -Fragment 'null or empty' `
	-Action { Assert-RigBuildImage -Config $baseline -Destination '' }

# The prefix is OPTIONAL configuration, so a parameters.local.ps1 written
# before the build command existed still loads instead of refusing everything.
$withoutPrefix = New-TestConfig
$withoutPrefix.Remove('BuildImageTagPrefix') | Out-Null
_Assert -Name 'an absent build-tag prefix falls back to the rig default' `
	-Condition ((_Try { Get-RigBuildTagPrefix -Config $withoutPrefix }) -eq 'coldstart-') `
	-Detail "got '$(_Try { Get-RigBuildTagPrefix -Config $withoutPrefix })'"

# ---------------------------------------------------------------------------
Write-Host 'Resolve-RigNuGetConfigFile (private / corporate feed)' -ForegroundColor Cyan
# ---------------------------------------------------------------------------

# A build on a host behind a corporate NuGet proxy cannot reach nuget.org, so
# the rig has to be able to restore through the operator's own NuGet.Config -
# using the SAME env var the local-dev reference architecture uses, so a proxy
# is configured once and works in both places.

$nugetRoot = Join-Path ([System.IO.Path]::GetTempPath()) ("rig-nuget-{0}" -f [guid]::NewGuid().ToString('N').Substring(0, 8))
New-Item -ItemType Directory -Force -Path $nugetRoot | Out-Null
$nugetExplicit = Join-Path $nugetRoot 'explicit.config'
$nugetEnv = Join-Path $nugetRoot 'env.config'
$nugetParam = Join-Path $nugetRoot 'parameters.config'
foreach ($f in @($nugetExplicit, $nugetEnv, $nugetParam)) { Set-Content -LiteralPath $f -Value '<configuration />' -Encoding ascii }

try {
	# Nothing configured must stay the SDK default, so the common case (a host
	# that can reach nuget.org) needs no configuration at all.
	_Assert -Name 'no configured NuGet.Config resolves to the SDK default' `
		-Condition ((_Try { Resolve-RigNuGetConfigFile -Config (New-TestConfig) -Explicit '' -EnvironmentValue '' }) -eq '') `
		-Detail "got '$(_Try { Resolve-RigNuGetConfigFile -Config (New-TestConfig) -Explicit '' -EnvironmentValue '' })'"

	$fromParameters = _Try { Resolve-RigNuGetConfigFile -Config (New-TestConfig @{ NuGetConfigFile = $nugetParam }) -Explicit '' -EnvironmentValue '' }
	_Assert -Name 'the rig parameters can name a NuGet.Config' -Condition ($fromParameters -eq $nugetParam) -Detail "got '$fromParameters'"

	$fromEnv = _Try { Resolve-RigNuGetConfigFile -Config (New-TestConfig @{ NuGetConfigFile = $nugetParam }) -Explicit '' -EnvironmentValue $nugetEnv }
	_Assert -Name 'NUGET_CONFIG_FILE overrides the rig parameters' -Condition ($fromEnv -eq $nugetEnv) -Detail "got '$fromEnv'"

	$fromExplicit = _Try { Resolve-RigNuGetConfigFile -Config (New-TestConfig @{ NuGetConfigFile = $nugetParam }) -Explicit $nugetExplicit -EnvironmentValue $nugetEnv }
	_Assert -Name 'an explicit -NuGetConfigFile overrides both' -Condition ($fromExplicit -eq $nugetExplicit) -Detail "got '$fromExplicit'"

	# Silently falling back to nuget.org here would restore from a feed the host
	# cannot reach and fail minutes later inside the image build, blaming the
	# wrong thing entirely.
	_AssertRefuses -Name 'REFUSES a NuGet.Config path that does not exist' -Fragment 'does not exist' `
		-Action { Resolve-RigNuGetConfigFile -Config (New-TestConfig) -Explicit (Join-Path $nugetRoot 'absent.config') -EnvironmentValue '' }

	_AssertRefuses -Name 'REFUSES a NUGET_CONFIG_FILE that does not exist' -Fragment 'does not exist' `
		-Action { Resolve-RigNuGetConfigFile -Config (New-TestConfig) -Explicit '' -EnvironmentValue (Join-Path $nugetRoot 'absent.config') }
}
finally { Remove-Item -Recurse -Force -LiteralPath $nugetRoot -ErrorAction SilentlyContinue }

# ---------------------------------------------------------------------------
Write-Host 'Compare-RigLiveImagePin (live image drift, pure comparison)' -ForegroundColor Cyan
# ---------------------------------------------------------------------------

# A container is pinned to an image ID at create time, so moving its tag leaves
# it running but arms the NEXT restart to adopt different code. These cases fix
# the two things that make such a detector worth having: it must fire on a real
# divergence, and it must stay quiet on everything else - including a box where
# the live deployment simply is not there.

$pinDrift = _Try {
	Compare-RigLiveImagePin -Container 'repocontextcontainer-repocontext-1' -ImageReference 'repocontext-mcp:local' `
		-PinnedImageId 'sha256:aaaa1111' -TagImageId 'sha256:bbbb2222'
}
_Assert -Name 'differing pinned and tag ids are reported as DRIFT' `
	-Condition ($null -ne $pinDrift -and $pinDrift.status -eq 'drift' -and $pinDrift.drifted -and $pinDrift.checked) `
	-Detail ($pinDrift | ConvertTo-Json -Compress)
$pinDriftMessage = if ($null -ne $pinDrift) { "$($pinDrift.message)" } else { '' }
_Assert -Name 'the drift message names both ids and says a restart would replace the code' `
	-Condition ($pinDriftMessage -like '*LIVE IMAGE DRIFT*' -and $pinDriftMessage -like '*sha256:aaaa1111*' -and $pinDriftMessage -like '*sha256:bbbb2222*' -and $pinDriftMessage -like '*REPLACE*') `
	-Detail $pinDriftMessage

$pinClean = _Try {
	Compare-RigLiveImagePin -Container 'repocontextcontainer-repocontext-1' -ImageReference 'repocontext-mcp:local' `
		-PinnedImageId 'sha256:aaaa1111' -TagImageId 'sha256:aaaa1111'
}
_Assert -Name 'matching ids are reported as CLEAN, not drift' `
	-Condition ($null -ne $pinClean -and $pinClean.status -eq 'clean' -and -not $pinClean.drifted -and $pinClean.checked) `
	-Detail ($pinClean | ConvertTo-Json -Compress)

# Docker prints ids in a single case, but a caller trimming or upper-casing one
# of them must not manufacture an alarm out of nothing.
$pinCase = _Try {
	Compare-RigLiveImagePin -Container 'c' -ImageReference 'i' -PinnedImageId ' SHA256:AAAA1111 ' -TagImageId 'sha256:aaaa1111'
}
_Assert -Name 'case and whitespace differences are not mistaken for drift' `
	-Condition ($null -ne $pinCase -and $pinCase.status -eq 'clean') -Detail ($pinCase | ConvertTo-Json -Compress)

# Fail-SAFE, not fail-closed: this is a read-only advisory about a deployment
# the rig does not own. A clean box must produce a quiet skip - never a crash,
# and never a false alarm that trains an operator to ignore the check.
$pinNoContainer = _Try { Compare-RigLiveImagePin -Container '' -ImageReference '' -PinnedImageId '' -TagImageId '' }
_Assert -Name 'no live container yields a quiet SKIP, not drift' `
	-Condition ($null -ne $pinNoContainer -and $pinNoContainer.status -eq 'skipped' -and -not $pinNoContainer.drifted -and -not $pinNoContainer.checked) `
	-Detail ($pinNoContainer | ConvertTo-Json -Compress)

$pinNoTag = _Try {
	Compare-RigLiveImagePin -Container 'repocontextcontainer-repocontext-1' -ImageReference 'repocontext-mcp:local' `
		-PinnedImageId 'sha256:aaaa1111' -TagImageId ''
}
_Assert -Name 'an image reference that does not resolve yields a SKIP, not drift' `
	-Condition ($null -ne $pinNoTag -and $pinNoTag.status -eq 'skipped' -and -not $pinNoTag.drifted) `
	-Detail ($pinNoTag | ConvertTo-Json -Compress)

$pinNoPin = _Try {
	Compare-RigLiveImagePin -Container 'repocontextcontainer-repocontext-1' -ImageReference 'repocontext-mcp:local' `
		-PinnedImageId '' -TagImageId 'sha256:aaaa1111'
}
_Assert -Name 'an unreadable pinned image id yields a SKIP, not drift' `
	-Condition ($null -ne $pinNoPin -and $pinNoPin.status -eq 'skipped' -and -not $pinNoPin.drifted) `
	-Detail ($pinNoPin | ConvertTo-Json -Compress)

# ---------------------------------------------------------------------------
Write-Host 'Get-RigConfig (override semantics)' -ForegroundColor Cyan
# ---------------------------------------------------------------------------

# The committed defaults must themselves satisfy the guard, or the rig ships
# refusing to start.
$shipped = Get-RigConfig -ParametersFile (Join-Path $here 'parameters.ps1') -ScriptRoot $here
$shippedAccepted = $true
try { Assert-RigIsolation -Config $shipped | Out-Null } catch { $shippedAccepted = $false; $shippedError = $_.Exception.Message }
_Assert -Name 'the committed parameters.ps1 passes the isolation guard' -Condition $shippedAccepted `
	-Detail $(if ($shippedAccepted) { '' } else { $shippedError })

$overridden = Get-RigConfig -ParametersFile (Join-Path $here 'parameters.ps1') -ScriptRoot $here -Override @{ WarmQueryCount = 9 }
_Assert -Name 'a supplied override is applied' -Condition ($overridden.WarmQueryCount -eq 9) -Detail "got $($overridden.WarmQueryCount)"

# An unbound string parameter arrives as '' and an unbound switch as $null.
# Neither may erase a committed default - that is how -RepoId '' would silently
# blank the workload.
$blanked = Get-RigConfig -ParametersFile (Join-Path $here 'parameters.ps1') -ScriptRoot $here -Override @{ RepoId = ''; SemanticQuery = $null }
_Assert -Name 'an empty override does not erase a committed default' `
	-Condition ($blanked.RepoId -eq $shipped.RepoId -and $blanked.SemanticQuery -eq $shipped.SemanticQuery) `
	-Detail "RepoId='$($blanked.RepoId)' SemanticQuery='$($blanked.SemanticQuery)'"

# ---------------------------------------------------------------------------
Write-Host 'Assert-RigComposeIsolation (resolved-compose layer)' -ForegroundColor Cyan
# ---------------------------------------------------------------------------

$accepted = $true
try { Assert-RigComposeIsolation -Document (New-TestComposeDocument) -Config $baseline | Out-Null }
catch { $accepted = $false; $composeError = $_.Exception.Message }
_Assert -Name "the rig's own resolved compose document is accepted" -Condition $accepted `
	-Detail $(if ($accepted) { '' } else { $composeError })

_AssertRefuses -Name 'REFUSES a resolved document naming the live project' -Fragment 'rig project' `
	-Action {
	$document = New-TestComposeDocument
	$document.name = 'repocontextcontainer'
	Assert-RigComposeIsolation -Document $document -Config $baseline
}

_AssertRefuses -Name 'REFUSES a resolved document binding a live volume' -Fragment 'LIVE volume' `
	-Action {
	$document = New-TestComposeDocument
	$document.services.repocontext.volumes[0].source = 'repocontextcontainer_repocontext-data'
	Assert-RigComposeIsolation -Document $document -Config $baseline
}

# The dangerous case: an innocent-looking compose volume KEY whose declared
# name is a live volume. A guard that validated the mount source verbatim
# would wave this straight through.
_AssertRefuses -Name 'REFUSES a rig-looking volume key that resolves to a live volume' -Fragment 'LIVE volume' `
	-Action {
	$document = New-TestComposeDocument
	$document.volumes.work.name = 'repocontextcontainer_repocontext-data'
	Assert-RigComposeIsolation -Document $document -Config $baseline
}

_AssertRefuses -Name 'REFUSES a resolved document running a live image' -Fragment 'not one of the rig' `
	-Action {
	$document = New-TestComposeDocument
	$document.services.repocontext.image = 'repocontext-mcp:local'
	Assert-RigComposeIsolation -Document $document -Config $baseline
}

_AssertRefuses -Name 'REFUSES a resolved document publishing the live host port' -Fragment 'LIVE host port' `
	-Action {
	$document = New-TestComposeDocument
	$document.services.repocontext.ports[0].published = '8080'
	Assert-RigComposeIsolation -Document $document -Config $baseline
}

_AssertRefuses -Name 'REFUSES a service that declares a build' -Fragment 'declares a build' `
	-Action {
	$document = New-TestComposeDocument
	$document.services.repocontext | Add-Member -NotePropertyName 'build' -NotePropertyValue ([pscustomobject] @{ context = '../..' })
	Assert-RigComposeIsolation -Document $document -Config $baseline
}

_AssertRefuses -Name 'REFUSES a writable bind mount' -Fragment 'writable' `
	-Action {
	$document = New-TestComposeDocument
	$document.services.repocontext.volumes[1].read_only = $false
	Assert-RigComposeIsolation -Document $document -Config $baseline
}

# The pristine master must not be bindable by the running stack, or a run
# would mutate the baseline that makes two runs comparable.
_AssertRefuses -Name 'REFUSES the running stack binding the pristine master volume' -Fragment 'not one of the rig' `
	-Action {
	$document = New-TestComposeDocument
	$document.volumes.work.name = 'lattice-coldstart-master'
	Assert-RigComposeIsolation -Document $document -Config $baseline
}

_AssertRefuses -Name 'REFUSES a declared volume outside the rig set' -Fragment 'declared volume' `
	-Action {
	$document = New-TestComposeDocument
	$document.volumes.work.name = 'some-other-volume'
	Assert-RigComposeIsolation -Document $document -Config $baseline
}

_AssertRefuses -Name 'REFUSES an empty resolved compose document' -Fragment 'empty' `
	-Action { Assert-RigComposeIsolation -Document $null -Config $baseline }

# ---------------------------------------------------------------------------
Write-Host 'Test-RigStagingManifestCurrent (staging cache validity)' -ForegroundColor Cyan
# ---------------------------------------------------------------------------

# The manifest arrives from ConvertFrom-Json, so it is a PSCustomObject whose
# numbers may be Int64 or String depending on how it was written. Exercise the
# real shape, not a hashtable stand-in.
$manifestJson = '{"tarballPath":"x.tar","tarballSizeBytes":1841364992,"tarballLastWriteTicks":638939999999999999,"tarballLastWriteUtc":"2026-08-29T09:59:36.2231117Z"}'
$currentManifest = $manifestJson | ConvertFrom-Json

_Assert -Name 'a manifest matching size and last-write ticks is current' `
	-Condition (Test-RigStagingManifestCurrent -Manifest $currentManifest -TarballSizeBytes 1841364992 -TarballLastWriteTicks 638939999999999999)

_Assert -Name 'a different tarball size is NOT current' `
	-Condition (-not (Test-RigStagingManifestCurrent -Manifest $currentManifest -TarballSizeBytes 1841364993 -TarballLastWriteTicks 638939999999999999))

_Assert -Name 'a different last-write time is NOT current' `
	-Condition (-not (Test-RigStagingManifestCurrent -Manifest $currentManifest -TarballSizeBytes 1841364992 -TarballLastWriteTicks 638939999999999998))

# THE REGRESSION. A manifest written by an older revision of the rig has no
# tarballLastWriteTicks field at all. Under Set-StrictMode -Version Latest,
# reading it directly is a TERMINATING error, so prepare-master.ps1 crashed
# ("The property 'tarballLastWriteTicks' cannot be found on this object")
# instead of treating an unreadable manifest as a cache miss. Encountered for
# real by S14 (#1844) against a staging copy left by an earlier S8 run.
$legacyManifest = '{"tarballPath":"x.tar","tarballSizeBytes":1841364992,"tarballLastWriteUtc":"2026-08-29T09:59:36.2231117Z"}' | ConvertFrom-Json
_Assert -Name 'a legacy manifest missing tarballLastWriteTicks reports NOT current instead of throwing' `
	-Condition (-not (Test-RigStagingManifestCurrent -Manifest $legacyManifest -TarballSizeBytes 1841364992 -TarballLastWriteTicks 638939999999999999))

$sizelessManifest = '{"tarballPath":"x.tar","tarballLastWriteTicks":638939999999999999}' | ConvertFrom-Json
_Assert -Name 'a manifest missing tarballSizeBytes reports NOT current instead of throwing' `
	-Condition (-not (Test-RigStagingManifestCurrent -Manifest $sizelessManifest -TarballSizeBytes 1841364992 -TarballLastWriteTicks 638939999999999999))

$garbledManifest = '{"tarballSizeBytes":"not-a-number","tarballLastWriteTicks":638939999999999999}' | ConvertFrom-Json
_Assert -Name 'an unparseable field reports NOT current instead of throwing' `
	-Condition (-not (Test-RigStagingManifestCurrent -Manifest $garbledManifest -TarballSizeBytes 1841364992 -TarballLastWriteTicks 638939999999999999))

_Assert -Name 'a null manifest reports NOT current' `
	-Condition (-not (Test-RigStagingManifestCurrent -Manifest $null -TarballSizeBytes 1 -TarballLastWriteTicks 1))

# A hashtable manifest is accepted too, because Get-RigMember handles both
# shapes and a caller may hand one in without a JSON round trip.
_Assert -Name 'a hashtable manifest is read through the same accessor' `
	-Condition (Test-RigStagingManifestCurrent -Manifest @{ tarballSizeBytes = 7; tarballLastWriteTicks = 9 } -TarballSizeBytes 7 -TarballLastWriteTicks 9)

# ---------------------------------------------------------------------------
Write-Host 'Get-RigWalSegmentCensus (offline file-WAL framing)' -ForegroundColor Cyan
# ---------------------------------------------------------------------------

$empty = Get-RigWalSegmentCensus -Bytes ([byte[]] @())
_Assert -Name 'an empty segment yields a zeroed, intact census' `
	-Condition ($empty.DataRecords -eq 0 -and $empty.CommitRecords -eq 0 -and $empty.TrimRecords -eq 0 -and $empty.Intact -and $empty.TruncatedTailBytes -eq 0) `
	-Detail ($empty | ConvertTo-Json -Compress)

$payload = [byte[]] @(10, 20, 30, 40)
$single = Get-RigWalSegmentCensus -Bytes (New-RigWalRecordBytes -Type 'Data' -Offset 42 -Payload $payload)
_Assert -Name 'a single data record is counted with its offset and payload length' `
	-Condition ($single.DataRecords -eq 1 -and $single.MinDataOffset -eq 42 -and $single.MaxDataOffset -eq 42 -and $single.PayloadBytes -eq 4 -and $single.Intact) `
	-Detail ($single | ConvertTo-Json -Compress)

# A realistic batch: three data records sealed by a commit trailer, then a
# trim marker, then a second batch. Exactly the shape FileWalShard appends.
$bytes = [System.Collections.Generic.List[byte]]::new()
foreach ($offset in 100, 101, 102) {
	$bytes.AddRange((New-RigWalRecordBytes -Type 'Data' -Offset $offset -Payload ([byte[]] @(1, 2, 3))))
}
$bytes.AddRange((New-RigWalRecordBytes -Type 'Commit' -Count 3))
$bytes.AddRange((New-RigWalRecordBytes -Type 'Trim' -Offset 100))
$bytes.AddRange((New-RigWalRecordBytes -Type 'Data' -Offset 103 -Payload ([byte[]] @(9))))
$bytes.AddRange((New-RigWalRecordBytes -Type 'Commit' -Count 1))
$mixed = Get-RigWalSegmentCensus -Bytes $bytes.ToArray()

_Assert -Name 'data, commit and trim records are counted separately' `
	-Condition ($mixed.DataRecords -eq 4 -and $mixed.CommitRecords -eq 2 -and $mixed.TrimRecords -eq 1) `
	-Detail ($mixed | ConvertTo-Json -Compress)
_Assert -Name 'the lowest and highest data offsets are reported' `
	-Condition ($mixed.MinDataOffset -eq 100 -and $mixed.MaxDataOffset -eq 103) `
	-Detail ($mixed | ConvertTo-Json -Compress)
_Assert -Name 'the trim watermark is the highest trim-through offset' `
	-Condition ($mixed.LastTrimThroughOffset -eq 100) `
	-Detail ($mixed | ConvertTo-Json -Compress)
_Assert -Name 'payload bytes exclude the framing and the offset prefix' `
	-Condition ($mixed.PayloadBytes -eq 10) `
	-Detail ($mixed | ConvertTo-Json -Compress)
_Assert -Name 'a well-formed segment reports no truncated tail' `
	-Condition ($mixed.Intact -and $mixed.TruncatedTailBytes -eq 0) `
	-Detail ($mixed | ConvertTo-Json -Compress)

# A crash leaves a torn trailing record. The census must count everything
# before it and report the tail, not throw and not lose the earlier records.
$torn = [System.Collections.Generic.List[byte]]::new()
$torn.AddRange((New-RigWalRecordBytes -Type 'Data' -Offset 7 -Payload ([byte[]] @(1, 2, 3, 4, 5))))
$torn.AddRange((New-RigWalRecordBytes -Type 'Data' -Offset 8 -Payload ([byte[]] @(1, 2, 3, 4, 5))))
$tornBytes = $torn.ToArray()
$tornBytes = $tornBytes[0..($tornBytes.Length - 4)]           # lop the tail off the last record
$tornCensus = Get-RigWalSegmentCensus -Bytes ([byte[]] $tornBytes)
_Assert -Name 'a torn trailing record is reported, not thrown, and earlier records survive' `
	-Condition ($tornCensus.DataRecords -eq 1 -and -not $tornCensus.Intact -and $tornCensus.TruncatedTailBytes -gt 0) `
	-Detail ($tornCensus | ConvertTo-Json -Compress)

$garbage = [System.Collections.Generic.List[byte]]::new()
$garbage.AddRange((New-RigWalRecordBytes -Type 'Data' -Offset 1 -Payload ([byte[]] @(1))))
$garbage.AddRange([byte[]] @(99, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))    # unknown type tag
$unknown = Get-RigWalSegmentCensus -Bytes $garbage.ToArray()
_Assert -Name 'an unknown record type stops the walk and is reported as a tail' `
	-Condition ($unknown.DataRecords -eq 1 -and -not $unknown.Intact) `
	-Detail ($unknown | ConvertTo-Json -Compress)

$oversized = [byte[]] @(1, 0xFF, 0xFF, 0xFF, 0x7F, 0, 0, 0, 0)            # bodyLen = int.MaxValue
$oversizedCensus = Get-RigWalSegmentCensus -Bytes $oversized
_Assert -Name 'a body length running past the end of the segment is refused' `
	-Condition ($oversizedCensus.DataRecords -eq 0 -and -not $oversizedCensus.Intact) `
	-Detail ($oversizedCensus | ConvertTo-Json -Compress)

$negative = [byte[]] @(1, 0xFF, 0xFF, 0xFF, 0xFF, 0, 0, 0, 0)             # bodyLen = -1
$negativeCensus = Get-RigWalSegmentCensus -Bytes $negative
_Assert -Name 'a negative body length is refused' `
	-Condition ($negativeCensus.DataRecords -eq 0 -and -not $negativeCensus.Intact) `
	-Detail ($negativeCensus | ConvertTo-Json -Compress)

# The on-disk path STREAMS rather than loading a segment (a real deployment's
# segments run to tens of megabytes each), so it is exercised against a real
# file to prove the streaming walk agrees with the in-memory one byte for byte.
$segmentPath = Join-Path ([System.IO.Path]::GetTempPath()) ("rig-wal-{0}.log" -f [guid]::NewGuid())
try {
	[System.IO.File]::WriteAllBytes($segmentPath, $bytes.ToArray())
	$fileCensus = Get-RigWalFileCensus -Path $segmentPath
	_Assert -Name 'the streaming on-disk walk agrees with the in-memory walk' `
		-Condition ($fileCensus.DataRecords -eq $mixed.DataRecords -and
			$fileCensus.CommitRecords -eq $mixed.CommitRecords -and
			$fileCensus.TrimRecords -eq $mixed.TrimRecords -and
			$fileCensus.PayloadBytes -eq $mixed.PayloadBytes -and
			$fileCensus.MinDataOffset -eq $mixed.MinDataOffset -and
			$fileCensus.MaxDataOffset -eq $mixed.MaxDataOffset -and
			$fileCensus.LastTrimThroughOffset -eq $mixed.LastTrimThroughOffset -and
			$fileCensus.Intact -eq $mixed.Intact) `
		-Detail ($fileCensus | ConvertTo-Json -Compress)
	_Assert -Name 'the streaming walk reports the real file size' `
		-Condition ($fileCensus.SizeBytes -eq (Get-Item -LiteralPath $segmentPath).Length)
}
finally { Remove-Item -LiteralPath $segmentPath -Force -ErrorAction SilentlyContinue }

# A torn tail on disk must behave exactly as it does in memory: earlier records
# survive, the tail is reported, nothing throws.
$tornPath = Join-Path ([System.IO.Path]::GetTempPath()) ("rig-wal-torn-{0}.log" -f [guid]::NewGuid())
try {
	[System.IO.File]::WriteAllBytes($tornPath, [byte[]] $tornBytes)
	$tornFileCensus = Get-RigWalFileCensus -Path $tornPath
	_Assert -Name 'the streaming walk reports a torn tail without throwing' `
		-Condition ($tornFileCensus.DataRecords -eq 1 -and -not $tornFileCensus.Intact -and $tornFileCensus.TruncatedTailBytes -gt 0) `
		-Detail ($tornFileCensus | ConvertTo-Json -Compress)
}
finally { Remove-Item -LiteralPath $tornPath -Force -ErrorAction SilentlyContinue }

# ---------------------------------------------------------------------------
Write-Host 'ConvertFrom-RigEncodedPathSegment (tree-id decoding)' -ForegroundColor Cyan
# ---------------------------------------------------------------------------

_Assert -Name 'an unreserved tree id round-trips unchanged' `
	-Condition ((ConvertFrom-RigEncodedPathSegment -Segment 'repo-context-vector-metadata') -eq 'repo-context-vector-metadata')
_Assert -Name 'a percent-encoded byte is decoded back to its character' `
	-Condition ((ConvertFrom-RigEncodedPathSegment -Segment 'view%2Dsys%2Fauth') -eq 'view-sys/auth') `
	-Detail (ConvertFrom-RigEncodedPathSegment -Segment 'view%2Dsys%2Fauth')

# ---------------------------------------------------------------------------
Write-Host 'Image reference normalisation' -ForegroundColor Cyan
# ---------------------------------------------------------------------------

_Assert -Name 'an untagged image normalises to :latest' `
	-Condition ((ConvertTo-RigNormalisedImage -Image 'repocontext-mcp') -eq 'repocontext-mcp:latest')
_Assert -Name 'a registry port is not mistaken for a tag' `
	-Condition ((ConvertTo-RigNormalisedImage -Image 'registry.local:5000/repocontext-mcp') -eq 'registry.local:5000/repocontext-mcp:latest') `
	-Detail (ConvertTo-RigNormalisedImage -Image 'registry.local:5000/repocontext-mcp')
_Assert -Name 'the tag of a tagged image is returned' `
	-Condition ((Get-RigImageTag -Image 'repocontext-mcp:coldstart-rig') -eq 'coldstart-rig')

# ---------------------------------------------------------------------------
Write-Host 'Deterministic statistics and log counters' -ForegroundColor Cyan
# ---------------------------------------------------------------------------

$samples = [double[]] @(10, 20, 30, 40)
_Assert -Name 'nearest-rank p50 is deterministic' -Condition ((Get-RigPercentile -Samples $samples -Percentile 50) -eq 20)
_Assert -Name 'nearest-rank p100 is the maximum' -Condition ((Get-RigPercentile -Samples $samples -Percentile 100) -eq 40)
_Assert -Name 'an empty sample yields no percentile rather than an error' -Condition ($null -eq (Get-RigPercentile -Samples @() -Percentile 50))

_Assert -Name 'relative spread of two samples is max-min over the mean' `
	-Condition ((Get-RigRelativeSpread -Samples ([double[]] @(80, 100))) -eq 22.22) `
	-Detail "got $(Get-RigRelativeSpread -Samples ([double[]] @(80, 100)))"
_Assert -Name 'relative spread of a single sample is zero' -Condition ((Get-RigRelativeSpread -Samples ([double[]] @(42))) -eq 0.0)

$logLines = @(
	"warn: Orleans.Lattice.BPlusTree.Grains.BPlusLeafGrain[0] Leaf projection for tree 'repo-context-vector-metadata' partition 0 is replaying beyond the configured budget (persistedCheckpoint 44900, MaxLeafReplayEntries 20000).",
	'info: Microsoft.Hosting.Lifetime[14] Now listening on: http://[::]:8080',
	"fail: Orleans.Lattice[0] Leaf projection for tree 'x' partition 1 cannot be recovered from the WAL alone",
	'warn: Orleans.Runtime[0] Dropping message because the target activation is invalid'
)
$counters = Measure-RigLogCounters -Lines $logLines
_Assert -Name 'the replay over-budget warning is counted' -Condition ($counters.ReplayOverBudgetWarnings -eq 1) -Detail ($counters | ConvertTo-Json -Compress)
_Assert -Name 'a stale-projection failure is counted' -Condition ($counters.ProjectionStaleFailures -eq 1) -Detail ($counters | ConvertTo-Json -Compress)
_Assert -Name 'a dropped message is counted' -Condition ($counters.DroppedMessages -eq 1) -Detail ($counters | ConvertTo-Json -Compress)
_Assert -Name 'warning and error lines are tallied separately' -Condition ($counters.WarningLines -eq 2 -and $counters.ErrorLines -eq 1) -Detail ($counters | ConvertTo-Json -Compress)
_Assert -Name 'a null log yields zeroed counters rather than an error' -Condition ((Measure-RigLogCounters -Lines $null).TotalLines -eq 0)

# ---------------------------------------------------------------------------
Write-Host 'Docker output parsing' -ForegroundColor Cyan
# ---------------------------------------------------------------------------

_Assert -Name 'a GiB memory reading is parsed' -Condition ((ConvertFrom-RigByteSize -Text '1.5GiB') -eq 1610612736) -Detail "got $(ConvertFrom-RigByteSize -Text '1.5GiB')"
_Assert -Name 'a MiB memory reading is parsed' -Condition ((ConvertFrom-RigByteSize -Text '512MiB') -eq 536870912)
_Assert -Name 'an unparsable memory reading yields null rather than an error' -Condition ($null -eq (ConvertFrom-RigByteSize -Text 'not-a-size'))

$statsCsv = Join-Path ([System.IO.Path]::GetTempPath()) ("rig-stats-{0}.csv" -f [guid]::NewGuid())
@(
	'lattice-coldstart-repocontext-1,12.50%,1.5GiB / 15GiB',
	'lattice-coldstart-repocontext-1,80.25%,2.5GiB / 15GiB',
	'lattice-coldstart-embedder-1,3.00%,256MiB / 15GiB'
) | Set-Content -LiteralPath $statsCsv -Encoding ascii
try {
	$summary = @(Measure-RigStatsCsv -CsvPath $statsCsv)
	$host1 = $summary | Where-Object { $_.Container -eq 'lattice-coldstart-repocontext-1' }
	_Assert -Name 'peak CPU per container is the maximum sample' -Condition ($host1.PeakCpuPercent -eq 80.25) -Detail ($summary | ConvertTo-Json -Compress)
	_Assert -Name 'peak memory per container is the maximum sample' -Condition ($host1.PeakMemoryBytes -eq 2684354560) -Detail ($summary | ConvertTo-Json -Compress)
	_Assert -Name 'each container is summarised separately' -Condition ($summary.Count -eq 2) -Detail ($summary | ConvertTo-Json -Compress)
}
finally { Remove-Item -LiteralPath $statsCsv -Force -ErrorAction SilentlyContinue }

$sse = "event: message`ndata: {`"jsonrpc`":`"2.0`",`"id`":1,`"result`":{`"content`":[{`"type`":`"text`",`"text`":`"{\`"mode\`":\`"semantic\`"}`"}]}}`n`n"
$decoded = ConvertFrom-RigMcpBody -Body $sse
_Assert -Name 'a one-event SSE response body is decoded' -Condition ($null -ne $decoded -and $decoded.id -eq 1) -Detail "$decoded"
_Assert -Name 'the first text content block is extracted' -Condition ((Get-RigMcpFirstText -Result $decoded.result) -eq '{"mode":"semantic"}') -Detail (Get-RigMcpFirstText -Result $decoded.result)
_Assert -Name 'a plain JSON response body is decoded' -Condition ((ConvertFrom-RigMcpBody -Body '{"jsonrpc":"2.0","id":7}').id -eq 7)
_Assert -Name 'an empty response body yields null rather than an error' -Condition ($null -eq (ConvertFrom-RigMcpBody -Body ''))

# ---------------------------------------------------------------------------
Write-Host 'Live image drift detection (Docker-backed: a REAL divergence)' -ForegroundColor Cyan
# ---------------------------------------------------------------------------

# The pure cases above prove the COMPARISON. They cannot prove that the
# detector reads the right two things out of Docker - and a drift check that
# never sees an actual divergence proves nothing at all. So this section builds
# one: two throwaway images with genuinely different ids, a container pinned to
# the first, and then the tag moved onto the second. That is precisely the shape
# a deploy leaves behind when it rebuilds and re-points a tag under a running
# container.
#
# Everything it creates lives in the RIG's own namespace (a `coldstart-` build
# tag and a `lattice-coldstart` container name) and is removed again, so the
# test cannot name, read or disturb anything belonging to the live deployment.

function _Docker {
	param([Parameter(Mandatory, ValueFromRemainingArguments)] [string[]] $DockerArgs)
	$output = & docker @DockerArgs 2>&1
	return [pscustomobject] @{ Ok = ($LASTEXITCODE -eq 0); Output = ($output | Out-String).Trim() }
}

$driftTag = 'repocontext-mcp:coldstart-drifttest'
$driftOtherTag = 'repocontext-mcp:coldstart-drifttest-other'
$driftContainer = 'lattice-coldstart-drifttest'

$dockerReady = $false
if (-not $SkipDockerTests) {
	try { $dockerReady = (_Docker version --format '{{.Server.Version}}').Ok } catch { $dockerReady = $false }
	if ($dockerReady -and -not (Get-Command tar -ErrorAction SilentlyContinue)) { $dockerReady = $false }
}

if (-not $dockerReady) {
	Write-Host '  SKIP  no reachable Docker daemon (or no tar); the real-divergence cases did not run.' -ForegroundColor Yellow
}
else {
	# The throwaway tags must themselves satisfy the rig's build-destination
	# guard: if this test needed a name the rig would refuse, it would be
	# proving the detector on artefacts the rig could never legitimately own.
	$fixtureAccepted = $true
	try { Assert-RigBuildImage -Config $baseline -Destination $driftTag | Out-Null }
	catch { $fixtureAccepted = $false }
	_Assert -Name "the drift fixture's own tag lives in the rig's build namespace" -Condition $fixtureAccepted

	$driftRoot = Join-Path ([System.IO.Path]::GetTempPath()) ("rig-drift-{0}" -f [guid]::NewGuid().ToString('N').Substring(0, 8))
	New-Item -ItemType Directory -Force -Path $driftRoot | Out-Null
	$imageIdA = ''
	$imageIdB = ''
	try {
		# Two single-file tarballs with different content import as two images
		# with different ids: no network, no build context, no base image.
		Set-Content -LiteralPath (Join-Path $driftRoot 'pinned.txt') -Value 'the code the container is running' -Encoding ascii
		Set-Content -LiteralPath (Join-Path $driftRoot 'moved.txt') -Value 'the code a restart would adopt instead' -Encoding ascii
		tar -cf (Join-Path $driftRoot 'pinned.tar') -C $driftRoot 'pinned.txt' | Out-Null
		tar -cf (Join-Path $driftRoot 'moved.tar') -C $driftRoot 'moved.txt' | Out-Null

		_Docker rm -f $driftContainer | Out-Null
		$imageIdA = (_Docker import (Join-Path $driftRoot 'pinned.tar') $driftTag).Output
		$imageIdB = (_Docker import (Join-Path $driftRoot 'moved.tar') $driftOtherTag).Output

		_Assert -Name 'the two fixture images really do have different ids' `
			-Condition ($imageIdA -like 'sha256:*' -and $imageIdB -like 'sha256:*' -and $imageIdA -ne $imageIdB) `
			-Detail "A='$imageIdA' B='$imageIdB'"

		# Created, never started: `.Image` is the pinned id either way, and a
		# stopped container is exactly the case where the swap happens on the
		# next start.
		$created = _Docker create --name $driftContainer $driftTag /noop
		_Assert -Name 'a container can be pinned to the first fixture image' -Condition $created.Ok -Detail $created.Output

		# NO-DRIFT case first: while the tag still resolves to the pinned image,
		# the detector must be quiet. A detector that shouted here would be
		# useless, because every run would shout.
		$observedClean = _Try { Get-RigLiveImagePin -Config $baseline -Container $driftContainer -ImageReference $driftTag }
		_Assert -Name 'an undisturbed container and tag are reported CLEAN' `
			-Condition ($null -ne $observedClean -and $observedClean.status -eq 'clean' -and -not $observedClean.drifted) `
			-Detail ($observedClean | ConvertTo-Json -Compress)

		# Now construct the hazard: move the tag onto the OTHER image, exactly
		# as a rebuild-and-re-point does. The container keeps running the image
		# it was pinned to; its next restart would not.
		$moved = _Docker tag $driftOtherTag $driftTag
		_Assert -Name 'the fixture tag can be moved onto the second image' -Condition $moved.Ok -Detail $moved.Output

		$observedDrift = _Try { Get-RigLiveImagePin -Config $baseline -Container $driftContainer -ImageReference $driftTag }
		_Assert -Name 'a REAL divergence between pin and tag is DETECTED' `
			-Condition ($null -ne $observedDrift -and $observedDrift.status -eq 'drift' -and $observedDrift.drifted) `
			-Detail ($observedDrift | ConvertTo-Json -Compress)
		_Assert -Name 'the detected drift reports the pinned id and the id the tag moved to' `
			-Condition ($null -ne $observedDrift -and $observedDrift.pinnedImageId -eq $imageIdA -and $observedDrift.tagImageId -eq $imageIdB) `
			-Detail ($observedDrift | ConvertTo-Json -Compress)

		# Restoring the tag must silence it again, so the check reflects the
		# host's state rather than latching once it has fired.
		_Docker tag $imageIdA $driftTag | Out-Null
		$observedRestored = _Try { Get-RigLiveImagePin -Config $baseline -Container $driftContainer -ImageReference $driftTag }
		_Assert -Name 're-pointing the tag at the pinned image clears the drift' `
			-Condition ($null -ne $observedRestored -and $observedRestored.status -eq 'clean') `
			-Detail ($observedRestored | ConvertTo-Json -Compress)

		# The two ways a real host disappoints the detector. Neither may throw,
		# and neither may be mistaken for drift.
		$observedNoContainer = _Try { Get-RigLiveImagePin -Config $baseline -Container 'lattice-coldstart-drifttest-absent' -ImageReference $driftTag }
		_Assert -Name 'an absent container is a quiet SKIP against a real daemon' `
			-Condition ($null -ne $observedNoContainer -and $observedNoContainer.status -eq 'skipped' -and -not $observedNoContainer.drifted) `
			-Detail ($observedNoContainer | ConvertTo-Json -Compress)

		$observedNoTag = _Try { Get-RigLiveImagePin -Config $baseline -Container $driftContainer -ImageReference 'repocontext-mcp:coldstart-drifttest-absent' }
		_Assert -Name 'an unresolvable image reference is a quiet SKIP against a real daemon' `
			-Condition ($null -ne $observedNoTag -and $observedNoTag.status -eq 'skipped' -and -not $observedNoTag.drifted) `
			-Detail ($observedNoTag | ConvertTo-Json -Compress)
	}
	finally {
		_Docker rm -f $driftContainer | Out-Null
		_Docker rmi -f $driftTag | Out-Null
		_Docker rmi -f $driftOtherTag | Out-Null
		foreach ($id in @($imageIdA, $imageIdB)) {
			if ($id -like 'sha256:*') { _Docker rmi -f $id | Out-Null }
		}
		Remove-Item -Recurse -Force -LiteralPath $driftRoot -ErrorAction SilentlyContinue
	}
}

# ---------------------------------------------------------------------------
Write-Host ''
Write-Host 'Build-source resolution (prepare-master and run-cohort must agree)' -ForegroundColor Cyan

$buildSourceRoot = Join-Path ([System.IO.Path]::GetTempPath()) ("rig-buildsrc-" + [guid]::NewGuid().ToString('n').Substring(0, 8))
New-Item -ItemType Directory -Force -Path (Join-Path $buildSourceRoot '.run\coldstart-rig') | Out-Null
	$buildSourceScriptRoot = Join-Path $buildSourceRoot 'pkg\scripts'
	New-Item -ItemType Directory -Force -Path $buildSourceScriptRoot | Out-Null
try {
	$recordPath = Join-Path $buildSourceRoot '.run\coldstart-rig\build-source.json'

	# No record at all: both callers must fall back to the configured source
	# rather than throwing, which is the state on a box that never built.
	$absent = Get-RigBuildSource -ScriptRoot $buildSourceScriptRoot
	_Assert -Name 'an absent build record resolves to null, so callers fall back to the configured source' `
		-Condition ($null -eq $absent) -Detail "got: $absent"

	# A record naming an image that does not exist must not be honoured: the
	# image it points at may have been pruned, and tagging from a missing
	# reference would fail confusingly rather than falling back.
	@{ image = 'repocontext-mcp:coldstart-buildsrctest-missing'; imageId = 'sha256:0'; gitRef = 'HEAD'; commitSha = 'deadbeef'; builtUtc = '2026-01-01T00:00:00Z' } |
		ConvertTo-Json | Set-Content -LiteralPath $recordPath -Encoding utf8
	$dangling = Get-RigBuildSource -ScriptRoot $buildSourceScriptRoot
	_Assert -Name 'a build record naming an absent image resolves to null' `
		-Condition ($null -eq $dangling) -Detail "got: $dangling"

	# A malformed record must degrade to the fallback rather than throwing, so a
	# truncated write cannot wedge every later rig invocation.
	'{ not json' | Set-Content -LiteralPath $recordPath -Encoding utf8
	$malformed = Get-RigBuildSource -ScriptRoot $buildSourceScriptRoot
	_Assert -Name 'a malformed build record resolves to null rather than throwing' `
		-Condition ($null -eq $malformed) -Detail "got: $malformed"
}
finally {
	Remove-Item -Recurse -Force -LiteralPath $buildSourceRoot -ErrorAction SilentlyContinue
}

# The load-bearing case, Docker-backed: a REAL divergence between the recorded
# build source and what the rig tag resolves to. This is the exact state
# prepare-master.ps1 used to leave behind, and the state in which a cohort
# measures one image while reporting another.
if ($dockerReady) {
	$bsRoot = Join-Path ([System.IO.Path]::GetTempPath()) ("rig-buildsrc-live-{0}" -f [guid]::NewGuid().ToString('N').Substring(0, 8))
	New-Item -ItemType Directory -Force -Path (Join-Path $bsRoot '.run\coldstart-rig') | Out-Null
	$bsScriptRoot = Join-Path $bsRoot 'pkg\scripts'
	New-Item -ItemType Directory -Force -Path $bsScriptRoot | Out-Null
	$bsRecord = Join-Path $bsRoot '.run\coldstart-rig\build-source.json'
	$bsBuilt = 'repocontext-mcp:coldstart-buildsrctest'
	$bsOther = 'repocontext-mcp:coldstart-buildsrctest-other'
	$bsIdA = ''
	$bsIdB = ''
	try {
		Set-Content -LiteralPath (Join-Path $bsRoot 'built.txt') -Value 'the image the operator built' -Encoding ascii
		Set-Content -LiteralPath (Join-Path $bsRoot 'other.txt') -Value 'the image a re-tag would substitute' -Encoding ascii
		tar -cf (Join-Path $bsRoot 'built.tar') -C $bsRoot 'built.txt' | Out-Null
		tar -cf (Join-Path $bsRoot 'other.tar') -C $bsRoot 'other.txt' | Out-Null

		$bsIdA = (_Docker import (Join-Path $bsRoot 'built.tar') $bsBuilt).Output
		$bsIdB = (_Docker import (Join-Path $bsRoot 'other.tar') $bsOther).Output

		_Assert -Name 'the two build-source fixture images really do have different ids' `
			-Condition ($bsIdA -like 'sha256:*' -and $bsIdB -like 'sha256:*' -and $bsIdA -ne $bsIdB) `
			-Detail "A=$bsIdA B=$bsIdB"

		@{ image = $bsBuilt; imageId = $bsIdA; gitRef = 'HEAD'; commitSha = 'abc123'; builtUtc = '2026-01-01T00:00:00Z' } |
			ConvertTo-Json | Set-Content -LiteralPath $bsRecord -Encoding utf8

		$resolved = Get-RigBuildSource -ScriptRoot $bsScriptRoot
		_Assert -Name 'a build record naming a REAL image is honoured' `
			-Condition ($null -ne $resolved -and "$($resolved.image)" -eq $bsBuilt) `
			-Detail ($resolved | ConvertTo-Json -Compress)

		_Assert -Name 'a recorded build matching the tested image is detected as matching' `
			-Condition ((Get-RigDockerImageId -Reference $bsBuilt) -eq "$($resolved.imageId)") `
			-Detail "tag=$(Get-RigDockerImageId -Reference $bsBuilt) record=$($resolved.imageId)"

		# Re-point the recorded tag at the OTHER image, exactly as a re-tag from
		# the configured default would. The recorded id and the tag now disagree,
		# which is what run-cohort.ps1 must refuse to measure.
		_Docker tag $bsIdB $bsBuilt | Out-Null
		$afterRetag = Get-RigDockerImageId -Reference $bsBuilt
		_Assert -Name 'a REAL divergence between the recorded id and the tag is DETECTED' `
			-Condition ($afterRetag -eq $bsIdB -and $afterRetag -ne "$($resolved.imageId)") `
			-Detail "tag now=$afterRetag record=$($resolved.imageId)"

		_Docker tag $bsIdA $bsBuilt | Out-Null
		_Assert -Name 're-pointing the tag at the recorded image clears the divergence' `
			-Condition ((Get-RigDockerImageId -Reference $bsBuilt) -eq "$($resolved.imageId)") `
			-Detail "tag=$(Get-RigDockerImageId -Reference $bsBuilt)"
	}
	finally {
		_Docker rmi -f $bsBuilt | Out-Null
		_Docker rmi -f $bsOther | Out-Null
		foreach ($id in @($bsIdA, $bsIdB)) {
			if ($id -like 'sha256:*') { _Docker rmi -f $id | Out-Null }
		}
		Remove-Item -Recurse -Force -LiteralPath $bsRoot -ErrorAction SilentlyContinue
	}
}

# ---------------------------------------------------------------------------
# The refusal MESSAGE, not just the detection that triggers it.
#
# The guard that raises this was originally written inline as
#   "...{0}..." + "..." + "..." -f $a, $b, $c
# and PowerShell binds -f TIGHTER than +, so the format applied only to the
# LAST fragment (which has no placeholders) and every {0}-{4} shipped
# unexpanded. The refusal fired correctly and named no image, no commit and no
# id - useless at exactly the moment an operator needs them. Detection was
# covered; the report was not. These tests close that.
Write-Host ''
Write-Host 'Image-provenance refusal message (the report, not just the decision)' -ForegroundColor Cyan

$refusal = Get-RigImageProvenanceRefusal -Provenance ([pscustomobject]@{
		mcpImage   = 'repocontext-mcp:coldstart-rig'
		mcpImageId = 'sha256:deadbeef'
		builtFrom  = [pscustomobject]@{
			image = 'repocontext-mcp:rig-build'; commitSha = 'abc1234'
			imageId = 'sha256:cafef00d'; matchesTestedImage = $false
		}
	})

# The regression test proper: a literal {0}-style placeholder means the format
# never applied. This is the assertion that fails against the inline form.
_Assert -Name 'the refusal expands every placeholder (no literal {N} survives)' `
	-Condition (-not ($refusal -match '\{\d\}')) `
	-Detail $refusal

# Paired positive assertions - each value must actually BE there. A "no
# placeholder" check alone would pass on an empty string, so these prove the
# message was built at all rather than merely lacking braces.
foreach ($pair in @(
		@{ what = 'the recorded image'; value = 'repocontext-mcp:rig-build' }
		@{ what = 'the recorded commit'; value = 'abc1234' }
		@{ what = 'the recorded image id'; value = 'sha256:cafef00d' }
		@{ what = 'the tag it would have measured'; value = 'repocontext-mcp:coldstart-rig' }
		@{ what = 'the id that tag resolves to'; value = 'sha256:deadbeef' })) {
	_Assert -Name "the refusal names $($pair.what)" `
		-Condition ($refusal.Contains($pair.value)) `
		-Detail "expected '$($pair.value)' in: $refusal"
}

_Assert -Name 'the refusal tells the operator how to recover' `
	-Condition ($refusal.Contains('./rig.ps1 tag')) `
	-Detail $refusal

# An unresolved tag id must degrade to a word, not to an empty gap.
$refusalUnresolved = Get-RigImageProvenanceRefusal -Provenance ([pscustomobject]@{
		mcpImage   = 'repocontext-mcp:coldstart-rig'
		mcpImageId = ''
		builtFrom  = [pscustomobject]@{
			image = 'repocontext-mcp:rig-build'; commitSha = 'abc1234'
			imageId = 'sha256:cafef00d'; matchesTestedImage = $false
		}
	})
_Assert -Name 'an unresolved tag id reads as "unresolved" rather than a blank' `
	-Condition ($refusalUnresolved.Contains('(unresolved)') -and -not ($refusalUnresolved -match '\{\d\}')) `
	-Detail $refusalUnresolved

# ---------------------------------------------------------------------------
Write-Host ''
Write-Host ("Passed: {0}  Failed: {1}  Total: {2}" -f $script:_PassCount, $script:_FailCount, ($script:_PassCount + $script:_FailCount)) `
	-ForegroundColor $(if ($script:_FailCount -eq 0) { 'Green' } else { 'Red' })

exit $script:_FailCount
