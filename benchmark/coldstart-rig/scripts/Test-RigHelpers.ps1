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
param()

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
Write-Host ''
Write-Host ("Passed: {0}  Failed: {1}  Total: {2}" -f $script:_PassCount, $script:_FailCount, ($script:_PassCount + $script:_FailCount)) `
	-ForegroundColor $(if ($script:_FailCount -eq 0) { 'Green' } else { 'Red' })

exit $script:_FailCount
