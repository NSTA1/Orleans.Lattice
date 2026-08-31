#!/usr/bin/env pwsh
<#
.SYNOPSIS
	Pure helper functions for the isolated cold-start and scale rig.

.DESCRIPTION
	Everything in this file is deliberately side-effect free (or file-read
	only) so the regression suite in Test-RigHelpers.ps1 can exercise it
	without Docker, without a restored volume, and without any dependence on
	wall-clock timing.

	Three groups live here:

	  1. Configuration resolution (Get-RigConfig).
	  2. The FAIL-CLOSED isolation guard (Assert-RigIsolation,
	     Assert-RigComposeIsolation). Decision D11 of epic #1830 requires the
	     rig to be structurally incapable of touching a live deployment, so
	     the guard refuses a configuration OR a resolved compose document
	     that names the live compose project, a live volume, a live image
	     tag, or the live host port.
	  3. Offline durable-state parsing (the file-WAL framing walk) and the
	     small deterministic statistics used by the measurement scripts.

	Dot-source it:  . (Join-Path $PSScriptRoot '_rig-helpers.ps1')
#>

Set-StrictMode -Version Latest

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

<#
.SYNOPSIS
	Loads the rig parameters, preferring the gitignored parameters.local.ps1
	over the committed parameters.ps1, and applies caller overrides.
#>
function Get-RigConfig {
	[CmdletBinding()]
	param(
		[string] $ParametersFile,
		[hashtable] $Override = @{},
		[string] $ScriptRoot
	)

	if (-not $ScriptRoot) { $ScriptRoot = $PSScriptRoot }

	if (-not $ParametersFile) {
		$local = Join-Path $ScriptRoot 'parameters.local.ps1'
		$default = Join-Path $ScriptRoot 'parameters.ps1'
		$ParametersFile = if (Test-Path -LiteralPath $local) { $local } else { $default }
	}

	if (-not (Test-Path -LiteralPath $ParametersFile)) {
		throw "Rig parameters file not found: $ParametersFile"
	}

	$config = & $ParametersFile
	if ($config -isnot [hashtable]) {
		throw "Rig parameters file '$ParametersFile' must evaluate to a hashtable; got '$($config.GetType().FullName)'."
	}

	foreach ($key in $Override.Keys) {
		$value = $Override[$key]
		# A switch/parameter left unbound arrives as $null or empty; never let
		# an unbound override erase a committed default.
		if ($null -ne $value -and "$value" -ne '') {
			$config[$key] = $value
		}
	}

	$config['ParametersFile'] = $ParametersFile
	return $config
}

# ---------------------------------------------------------------------------
# The isolation guard (decision D11)
# ---------------------------------------------------------------------------

# Keys the guard requires. A missing or empty key is a REFUSAL, not a default:
# the guard must never be able to pass because a value was absent.
$script:RigRequiredConfigKeys = @(
	'ProjectName',
	'MasterVolume',
	'ScaleMasterVolume',
	'WorkVolume',
	'HfCacheVolume',
	'HostPort',
	'McpImage',
	'EmbedderImage',
	'RequiredProjectPrefix',
	'RequiredVolumePrefix',
	'RequiredImageTag',
	'ForbiddenProjects',
	'ForbiddenVolumePrefixes',
	'ForbiddenVolumes',
	'ForbiddenImages',
	'ForbiddenPorts'
)

<#
.SYNOPSIS
	Normalises a Docker image reference so 'foo' and 'foo:latest' compare equal.
#>
function ConvertTo-RigNormalisedImage {
	[CmdletBinding()]
	param([string] $Image)

	if ([string]::IsNullOrWhiteSpace($Image)) { return '' }
	$trimmed = $Image.Trim()
	# A colon inside the final path segment is the tag; a colon before the last
	# slash is a registry port and does not count as a tag.
	$lastSlash = $trimmed.LastIndexOf('/')
	$lastColon = $trimmed.LastIndexOf(':')
	if ($lastColon -gt $lastSlash) { return $trimmed }
	return "$trimmed`:latest"
}

<#
.SYNOPSIS
	Returns the tag portion of a Docker image reference, or '' when untagged.
#>
function Get-RigImageTag {
	[CmdletBinding()]
	param([string] $Image)

	$normalised = ConvertTo-RigNormalisedImage -Image $Image
	if ($normalised -eq '') { return '' }
	$lastSlash = $normalised.LastIndexOf('/')
	$lastColon = $normalised.LastIndexOf(':')
	if ($lastColon -le $lastSlash) { return '' }
	return $normalised.Substring($lastColon + 1)
}

<#
.SYNOPSIS
	Refuses a rig configuration that could touch a live deployment.

.DESCRIPTION
	Fail-closed. The configuration must positively opt in to the rig's own
	naming (project prefix, volume prefix, image tag) AND must not name any
	forbidden project, volume, image or port. Any missing or empty required
	key is itself a refusal.

	Returns the validated configuration so a caller can pipe it.
#>
function Assert-RigIsolation {
	[CmdletBinding()]
	param(
		[Parameter(Mandatory)] [hashtable] $Config
	)

	$violations = [System.Collections.Generic.List[string]]::new()

	foreach ($key in $script:RigRequiredConfigKeys) {
		if (-not $Config.ContainsKey($key)) {
			$violations.Add("required key '$key' is missing")
			continue
		}
		$value = $Config[$key]
		if ($null -eq $value -or ($value -is [string] -and [string]::IsNullOrWhiteSpace($value))) {
			$violations.Add("required key '$key' is null or empty")
		}
	}

	if ($violations.Count -gt 0) {
		throw ("Rig isolation guard REFUSED to start: " + ($violations -join '; ') + '.')
	}

	$forbiddenProjects = @($Config.ForbiddenProjects)
	$forbiddenVolumePrefixes = @($Config.ForbiddenVolumePrefixes)
	$forbiddenVolumes = @($Config.ForbiddenVolumes)
	$forbiddenImages = @($Config.ForbiddenImages) | ForEach-Object { ConvertTo-RigNormalisedImage -Image $_ }
	$forbiddenPorts = @($Config.ForbiddenPorts) | ForEach-Object { [int] $_ }

	# --- Project ---
	$project = "$($Config.ProjectName)"
	foreach ($forbidden in $forbiddenProjects) {
		if ($project -ieq "$forbidden") {
			$violations.Add("compose project '$project' is the LIVE project")
		}
	}
	if (-not $project.StartsWith("$($Config.RequiredProjectPrefix)", [StringComparison]::Ordinal)) {
		$violations.Add("compose project '$project' does not start with the required rig prefix '$($Config.RequiredProjectPrefix)'")
	}

	# --- Volumes ---
	$volumeKeys = @('MasterVolume', 'ScaleMasterVolume', 'WorkVolume', 'HfCacheVolume')
	$seen = @{}
	foreach ($key in $volumeKeys) {
		$volume = "$($Config[$key])"
		$volumeViolations = Test-RigVolumeName -Volume $volume -Config $Config -Label $key
		if ($volumeViolations.Count -gt 0) { $violations.AddRange($volumeViolations) }
		if ($seen.ContainsKey($volume)) {
			$violations.Add("volume '$volume' is used for both $($seen[$volume]) and $key; the master and its working clone must be distinct")
		}
		else { $seen[$volume] = $key }
	}

	# --- Images ---
	foreach ($key in @('McpImage', 'EmbedderImage')) {
		$image = ConvertTo-RigNormalisedImage -Image "$($Config[$key])"
		if ($forbiddenImages -contains $image) {
			$violations.Add("$key '$image' is a LIVE image tag; the rig must only ever run its own additional tag")
		}
		$tag = Get-RigImageTag -Image $image
		if ($tag -ne "$($Config.RequiredImageTag)") {
			$violations.Add("$key '$image' does not carry the required rig tag '$($Config.RequiredImageTag)'")
		}
	}

	# The rig applies its tag to an already-built image; tagging an image onto
	# itself would mean the rig had been pointed at a live tag as a
	# destination.
	foreach ($pair in @(@('SourceMcpImage', 'McpImage'), @('SourceEmbedderImage', 'EmbedderImage'))) {
		if ($Config.ContainsKey($pair[0])) {
			$source = ConvertTo-RigNormalisedImage -Image "$($Config[$pair[0]])"
			$destination = ConvertTo-RigNormalisedImage -Image "$($Config[$pair[1]])"
			if ($source -eq $destination) {
				$violations.Add("$($pair[0]) '$source' is the same reference as $($pair[1]); the rig tag must be an ADDITIONAL tag, never the source tag itself")
			}
		}
	}

	# --- Port ---
	$port = 0
	if (-not [int]::TryParse("$($Config.HostPort)", [ref] $port)) {
		$violations.Add("HostPort '$($Config.HostPort)' is not an integer")
	}
	else {
		if ($port -lt 1 -or $port -gt 65535) {
			$violations.Add("HostPort $port is not a valid TCP port")
		}
		if ($forbiddenPorts -contains $port) {
			$violations.Add("HostPort $port is the LIVE host port")
		}
	}

	if ($violations.Count -gt 0) {
		throw ("Rig isolation guard REFUSED to start: " + ($violations -join '; ') + '.')
	}

	return $Config
}

<#
.SYNOPSIS
	Returns the isolation violations (if any) for a single volume name.
#>
function Test-RigVolumeName {
	[CmdletBinding()]
	param(
		[string] $Volume,
		[Parameter(Mandatory)] [hashtable] $Config,
		[string] $Label = 'volume'
	)

	$found = [System.Collections.Generic.List[string]]::new()

	if ([string]::IsNullOrWhiteSpace($Volume)) {
		$found.Add("$Label is null or empty")
		return , [string[]] $found.ToArray()
	}

	foreach ($forbidden in @($Config.ForbiddenVolumes)) {
		if ($Volume -ieq "$forbidden") {
			$found.Add("$Label '$Volume' is a LIVE volume")
		}
	}
	foreach ($prefix in @($Config.ForbiddenVolumePrefixes)) {
		if ($Volume.StartsWith("$prefix", [StringComparison]::OrdinalIgnoreCase)) {
			$found.Add("$Label '$Volume' carries the LIVE volume prefix '$prefix'")
		}
	}
	if (-not $Volume.StartsWith("$($Config.RequiredVolumePrefix)", [StringComparison]::Ordinal)) {
		$found.Add("$Label '$Volume' does not start with the required rig prefix '$($Config.RequiredVolumePrefix)'")
	}

	return , [string[]] $found.ToArray()
}

<#
.SYNOPSIS
	Refuses a RESOLVED compose document that could touch a live deployment.

.DESCRIPTION
	The second half of the guard. Assert-RigIsolation validates the operator's
	intent; this validates what Docker actually resolved, which is what will
	really be bound. Pass the object produced by
	`docker compose config --format json | ConvertFrom-Json`.

	Refuses when the document's project name is not the rig project, when a
	service declares a build (the rig must NEVER rebuild an image and so can
	never move a live tag), when a service runs an image outside the rig's own
	two tags, when a published port is forbidden, when a bind mount is
	writable, or when any named volume is outside the rig's own set.
#>
function Assert-RigComposeIsolation {
	[CmdletBinding()]
	param(
		[AllowNull()] $Document,
		[Parameter(Mandatory)] [hashtable] $Config
	)

	$violations = [System.Collections.Generic.List[string]]::new()

	if ($null -eq $Document) {
		throw 'Rig isolation guard REFUSED to start: the resolved compose document was empty.'
	}

	$allowedImages = @(
		(ConvertTo-RigNormalisedImage -Image "$($Config.McpImage)"),
		(ConvertTo-RigNormalisedImage -Image "$($Config.EmbedderImage)")
	)
	# The PRISTINE master is deliberately absent from this list. The running
	# stack must never be able to bind it, or a run would mutate the very
	# byte-identical baseline that makes two runs comparable. Only the
	# per-run working clone and the embedder's model cache are bindable.
	$allowedVolumes = @("$($Config.WorkVolume)", "$($Config.HfCacheVolume)")
	$forbiddenPorts = @($Config.ForbiddenPorts) | ForEach-Object { [int] $_ }

	$name = Get-RigMember -Object $Document -Name 'name'
	if ("$name" -ne "$($Config.ProjectName)") {
		$violations.Add("resolved compose project is '$name' but the rig project is '$($Config.ProjectName)'")
	}

	# A service mount reports its source as the compose-file volume KEY, while
	# the real Docker volume name lives on the top-level volume entry. Resolve
	# keys to names first, so the guard validates what would actually be bound
	# rather than a local alias that could point anywhere.
	$volumes = Get-RigMember -Object $Document -Name 'volumes'
	$resolvedNames = @{}
	if ($null -ne $volumes) {
		foreach ($volumeKey in (Get-RigMemberNames -Object $volumes)) {
			$volume = Get-RigMember -Object $volumes -Name $volumeKey
			$volumeName = "$(Get-RigMember -Object $volume -Name 'name')"
			if ([string]::IsNullOrWhiteSpace($volumeName)) { $volumeName = $volumeKey }
			$resolvedNames[$volumeKey] = $volumeName

			if ($allowedVolumes -notcontains $volumeName) {
				$violations.Add("declared volume '$volumeName' is not one of the rig's own volumes")
			}
			$declaredViolations = Test-RigVolumeName -Volume $volumeName -Config $Config -Label 'declared volume'
			if ($declaredViolations.Count -gt 0) { $violations.AddRange($declaredViolations) }
		}
	}

	$services = Get-RigMember -Object $Document -Name 'services'
	if ($null -eq $services) {
		$violations.Add('the resolved compose document declares no services')
	}
	else {
		foreach ($serviceName in (Get-RigMemberNames -Object $services)) {
			$service = Get-RigMember -Object $services -Name $serviceName

			if ($null -ne (Get-RigMember -Object $service -Name 'build')) {
				$violations.Add("service '$serviceName' declares a build; the rig must never build or re-tag an image")
			}

			$image = ConvertTo-RigNormalisedImage -Image "$(Get-RigMember -Object $service -Name 'image')"
			if ($allowedImages -notcontains $image) {
				$violations.Add("service '$serviceName' runs image '$image', which is not one of the rig's own tags")
			}

			foreach ($port in @(Get-RigMember -Object $service -Name 'ports')) {
				if ($null -eq $port) { continue }
				$published = "$(Get-RigMember -Object $port -Name 'published')"
				$parsed = 0
				if ([int]::TryParse($published, [ref] $parsed) -and $forbiddenPorts -contains $parsed) {
					$violations.Add("service '$serviceName' publishes the LIVE host port $parsed")
				}
			}

			foreach ($mount in @(Get-RigMember -Object $service -Name 'volumes')) {
				if ($null -eq $mount) { continue }
				$type = "$(Get-RigMember -Object $mount -Name 'type')"
				$source = "$(Get-RigMember -Object $mount -Name 'source')"
				if ($type -eq 'volume') {
					$bound = if ($resolvedNames.ContainsKey($source)) { $resolvedNames[$source] } else { $source }
					if ($allowedVolumes -notcontains $bound) {
						$violations.Add("service '$serviceName' binds volume '$bound', which is not one of the rig's own volumes")
					}
					$mountViolations = Test-RigVolumeName -Volume $bound -Config $Config -Label "service '$serviceName' volume"
					if ($mountViolations.Count -gt 0) { $violations.AddRange($mountViolations) }
				}
				elseif ($type -eq 'bind') {
					$readOnly = Get-RigMember -Object $mount -Name 'read_only'
					if ($readOnly -ne $true) {
						$violations.Add("service '$serviceName' binds host path '$source' writable; every rig bind mount must be read-only")
					}
				}
			}
		}
	}

	if ($violations.Count -gt 0) {
		throw ("Rig isolation guard REFUSED to start: " + (($violations | Select-Object -Unique) -join '; ') + '.')
	}

	return $true
}

<#
.SYNOPSIS
	Reads a member from a PSCustomObject or a hashtable, returning $null when absent.
#>
function Get-RigMember {
	[CmdletBinding()]
	param($Object, [Parameter(Mandatory)] [string] $Name)

	if ($null -eq $Object) { return $null }
	if ($Object -is [System.Collections.IDictionary]) {
		if ($Object.Contains($Name)) { return $Object[$Name] }
		return $null
	}
	$property = $Object.PSObject.Properties[$Name]
	if ($null -eq $property) { return $null }
	return $property.Value
}

<#
.SYNOPSIS
	Enumerates the member names of a PSCustomObject or a hashtable.
#>
function Get-RigMemberNames {
	[CmdletBinding()]
	param($Object)

	if ($null -eq $Object) { return @() }
	if ($Object -is [System.Collections.IDictionary]) { return @($Object.Keys) }
	return @($Object.PSObject.Properties.Name)
}

<#
.SYNOPSIS
	Decides whether an extracted staging copy still matches the backup tarball,
	so prepare-master.ps1 can skip re-extracting 1.8 GB.

.DESCRIPTION
	A staging directory carries a `.rig-manifest.json` recording the tarball's
	size and last-write time. The copy is current only when BOTH still match.

	FAILS SAFE ON AN UNRECOGNISED MANIFEST. A manifest written by an older
	revision of the rig can be missing a field this comparison needs, and the
	scripts run under `Set-StrictMode -Version Latest`, where reading an absent
	property is a TERMINATING ERROR rather than a null. Reading the manifest
	directly therefore turned an old staging directory into a crash
	("The property 'tarballLastWriteTicks' cannot be found on this object")
	instead of the cache miss it should have been. Every field is read through
	Get-RigMember, which yields $null for an absent one, and a missing or
	unparseable field reports NOT current - so the rig re-extracts, which is
	always safe, rather than trusting a manifest it cannot fully read.
#>
function Test-RigStagingManifestCurrent {
	[CmdletBinding()]
	param(
		$Manifest,
		[Parameter(Mandatory)] [long] $TarballSizeBytes,
		[Parameter(Mandatory)] [long] $TarballLastWriteTicks
	)

	if ($null -eq $Manifest) { return $false }

	$size = Get-RigMember -Object $Manifest -Name 'tarballSizeBytes'
	$ticks = Get-RigMember -Object $Manifest -Name 'tarballLastWriteTicks'
	if ($null -eq $size -or $null -eq $ticks) { return $false }

	$parsedSize = [long] 0
	$parsedTicks = [long] 0
	if (-not [long]::TryParse("$size", [ref] $parsedSize)) { return $false }
	if (-not [long]::TryParse("$ticks", [ref] $parsedTicks)) { return $false }

	return ($parsedSize -eq $TarballSizeBytes) -and ($parsedTicks -eq $TarballLastWriteTicks)
}

# ---------------------------------------------------------------------------
# Retrieval response readers
# ---------------------------------------------------------------------------

<#
.SYNOPSIS
	Reads the `mode` a retrieval response answered in ("semantic", "keyword" or
	"empty"), or $null when the body carried none.

.DESCRIPTION
	Shared by every script that asks the box a question - the cohort, the
	healing observer and the corpus verifier - because "which path answered"
	is the one field that distinguishes a working box from a silently degraded
	one, and three private copies of that reader would be three chances to
	disagree about it.

	Parses as JSON first and falls back to a textual probe, so a response
	wrapped in extra framing still yields its mode rather than being reported
	as no answer at all.
#>
function Get-RigRetrievalMode {
	[CmdletBinding()]
	param([AllowNull()] [string] $Text)

	if ([string]::IsNullOrWhiteSpace($Text)) { return $null }
	try {
		$parsed = $Text | ConvertFrom-Json
		$mode = $parsed.PSObject.Properties['mode']
		if ($null -ne $mode) { return "$($mode.Value)" }
	}
	catch {
		# Not JSON; fall through to the textual probe below.
	}
	if ($Text -match '"mode"\s*:\s*"(?<mode>[A-Za-z]+)"') { return $Matches['mode'] }
	return $null
}

<#
.SYNOPSIS
	Reads the `retrievalPath` a retrieval response declared alongside its
	unchanged `mode`, or 'none' when the body carried none.

.DESCRIPTION
	The vocabulary is S7's: semantic.exact, semantic.approximate,
	keyword.no_embedder, keyword.vector_plane_unavailable,
	keyword.index_degraded. `mode` says WHETHER the semantic plane answered;
	`retrievalPath` says WHICH path did, which is what makes an approximate
	answer distinguishable from an exact one instead of silently substituted.
#>
function Get-RigRetrievalPath {
	[CmdletBinding()]
	param([AllowNull()] [string] $Text)

	if ([string]::IsNullOrWhiteSpace($Text)) { return 'none' }
	$match = [regex]::Match($Text, '"retrievalPath"\s*:\s*"([^"]+)"')
	if ($match.Success) { return $match.Groups[1].Value }
	return 'none'
}

# ---------------------------------------------------------------------------
# Offline file-WAL framing walk
# ---------------------------------------------------------------------------

# Mirrors FileWalRecordFormat in src/lattice.storage.file. A record is
#   [type:1][bodyLen:4 LE][body:bodyLen][crc32:4 LE]
# with three types: Data (body = [offset:8 LE][payload]), Commit
# (body = [count:4 LE]) and Trim (body = [throughOffset:8 LE]).
$script:RigWalRecordTypeData = 1
$script:RigWalRecordTypeCommit = 2
$script:RigWalRecordTypeTrim = 3
$script:RigWalFramingOverhead = 9

<#
.SYNOPSIS
	Encodes one file-WAL record exactly as FileWalRecordFormat writes it,
	except that the trailing CRC-32 is written as the supplied placeholder.

.DESCRIPTION
	Used by the regression suite to build synthetic segment content, and it
	doubles as the executable specification of the framing the census walks.
	The census counts framing, not integrity, so it never reads the CRC; the
	placeholder therefore does not weaken the test.
#>
function New-RigWalRecordBytes {
	[CmdletBinding()]
	param(
		[Parameter(Mandatory)] [ValidateSet('Data', 'Commit', 'Trim')] [string] $Type,
		[long] $Offset = 0,
		[byte[]] $Payload = @(),
		[int] $Count = 1,
		[uint32] $Crc = 0
	)

	$body = switch ($Type) {
		'Data' { [byte[]] ([BitConverter]::GetBytes([long] $Offset) + $Payload) }
		'Commit' { [BitConverter]::GetBytes([int] $Count) }
		'Trim' { [BitConverter]::GetBytes([long] $Offset) }
	}

	$typeTag = switch ($Type) {
		'Data' { $script:RigWalRecordTypeData }
		'Commit' { $script:RigWalRecordTypeCommit }
		'Trim' { $script:RigWalRecordTypeTrim }
	}

	$bytes = [System.Collections.Generic.List[byte]]::new()
	$bytes.Add([byte] $typeTag)
	$bytes.AddRange([BitConverter]::GetBytes([int] $body.Length))
	$bytes.AddRange([byte[]] $body)
	$bytes.AddRange([BitConverter]::GetBytes([uint32] $Crc))
	return , $bytes.ToArray()
}

<#
.SYNOPSIS
	Walks the file-WAL framing over a stream and returns its census.

.DESCRIPTION
	The core parser. It is deliberately STREAMING and never materialises a
	segment: it reads each record's 9-byte frame header into one reused
	buffer, reads the 8 body bytes it actually needs (a Data record's offset,
	or a Trim record's through-offset), and SEEKS past the rest. The census
	counts records and bytes, so the payload itself is never wanted - reading
	it would cost 728 MB of I/O and a large-object allocation per segment on a
	real deployment, for nothing.

	A structurally impossible record (an unknown type tag, a negative or
	oversized body length, or a record that would run past the end) terminates
	the walk and is reported as TruncatedTailBytes rather than throwing, which
	is exactly how a torn tail from a crash presents.

	Integrity is NOT verified: the 4-byte CRC is skipped, so the census
	describes framing and record counts, not checksum validity.
#>
function Get-RigWalStreamCensus {
	[CmdletBinding()]
	param(
		[Parameter(Mandatory)] [System.IO.Stream] $Stream,
		[string] $Path = '<memory>'
	)

	if (-not [BitConverter]::IsLittleEndian) {
		throw 'The file-WAL framing is little-endian and this host is big-endian; the census cannot be trusted here.'
	}

	$length = $Stream.Length
	$position = 0L
	$data = 0L
	$commit = 0L
	$trim = 0L
	$payloadBytes = 0L
	$minOffset = [long]::MaxValue
	$maxOffset = [long]::MinValue
	$lastTrim = $null
	$malformed = $false

	# One buffer for the whole walk. Nine bytes covers the frame header
	# ([type:1][bodyLen:4][...]) and eight covers either body prefix we read,
	# so nothing else is ever allocated per record.
	$header = [byte[]]::new(9)

	while ($position + 9 -le $length) {
		if ($Stream.Position -ne $position) { [void] $Stream.Seek($position, [System.IO.SeekOrigin]::Begin) }
		if ((Read-RigExactly -Stream $Stream -Buffer $header -Count 5) -ne 5) { $malformed = $true; break }

		$type = $header[0]
		$bodyLength = [BitConverter]::ToInt32($header, 1)
		if ($bodyLength -lt 0) { $malformed = $true; break }

		$recordEnd = $position + 9 + $bodyLength
		if ($recordEnd -gt $length) { $malformed = $true; break }

		switch ($type) {
			$script:RigWalRecordTypeData {
				if ($bodyLength -lt 8) { $malformed = $true; break }
				if ((Read-RigExactly -Stream $Stream -Buffer $header -Count 8) -ne 8) { $malformed = $true; break }
				$offset = [BitConverter]::ToInt64($header, 0)
				$data++
				$payloadBytes += ($bodyLength - 8)
				if ($offset -lt $minOffset) { $minOffset = $offset }
				if ($offset -gt $maxOffset) { $maxOffset = $offset }
			}
			$script:RigWalRecordTypeCommit {
				if ($bodyLength -ne 4) { $malformed = $true; break }
				$commit++
			}
			$script:RigWalRecordTypeTrim {
				if ($bodyLength -ne 8) { $malformed = $true; break }
				if ((Read-RigExactly -Stream $Stream -Buffer $header -Count 8) -ne 8) { $malformed = $true; break }
				$through = [BitConverter]::ToInt64($header, 0)
				$trim++
				if ($null -eq $lastTrim -or $through -gt $lastTrim) { $lastTrim = $through }
			}
			default { $malformed = $true }
		}

		if ($malformed) { break }
		$position = $recordEnd
	}

	return [pscustomobject] @{
		Path                  = $Path
		SizeBytes             = [long] $length
		DataRecords           = $data
		CommitRecords         = $commit
		TrimRecords           = $trim
		PayloadBytes          = $payloadBytes
		MinDataOffset         = if ($data -gt 0) { $minOffset } else { $null }
		MaxDataOffset         = if ($data -gt 0) { $maxOffset } else { $null }
		LastTrimThroughOffset = $lastTrim
		TruncatedTailBytes    = [long] ($length - $position)
		Intact                = (-not $malformed) -and ($position -eq $length)
	}
}

<#
.SYNOPSIS
	Fills the first <paramref name="Count"/> bytes of a buffer from a stream,
	returning how many bytes were actually read.
#>
function Read-RigExactly {
	[CmdletBinding()]
	param(
		[Parameter(Mandatory)] [System.IO.Stream] $Stream,
		[Parameter(Mandatory)] [byte[]] $Buffer,
		[Parameter(Mandatory)] [int] $Count
	)

	$read = 0
	while ($read -lt $Count) {
		$chunk = $Stream.Read($Buffer, $read, $Count - $read)
		if ($chunk -le 0) { break }
		$read += $chunk
	}
	return $read
}

<#
.SYNOPSIS
	Walks the file-WAL framing over an in-memory segment and returns its census.

.DESCRIPTION
	A thin wrapper over Get-RigWalStreamCensus for callers that already hold
	the bytes (the regression suite builds synthetic segments this way).
#>
function Get-RigWalSegmentCensus {
	[CmdletBinding()]
	param(
		[Parameter(Mandatory)] [AllowEmptyCollection()] [byte[]] $Bytes,
		[string] $Path = '<memory>'
	)

	$stream = [System.IO.MemoryStream]::new($Bytes, $false)
	try { return Get-RigWalStreamCensus -Stream $stream -Path $Path }
	finally { $stream.Dispose() }
}

<#
.SYNOPSIS
	Walks the file-WAL framing of one wal.log segment file on disk.

.DESCRIPTION
	Streams the file rather than loading it: a real deployment's segments run
	to tens of megabytes each and hundreds of megabytes in total, and the
	census only ever needs 17 bytes per record.
#>
function Get-RigWalFileCensus {
	[CmdletBinding()]
	param([Parameter(Mandatory)] [string] $Path)

	$file = [System.IO.File]::Open(
		$Path,
		[System.IO.FileMode]::Open,
		[System.IO.FileAccess]::Read,
		[System.IO.FileShare]::ReadWrite)
	# Buffered so the many small reads do not each become a syscall. Note the
	# walk is interpreter-bound rather than I/O-bound at this scale (a full
	# 728 MB / 559k-record census takes ~100s either way), so this is about
	# keeping the cost bounded, not about speed. The deliberate trade is
	# BOUNDED MEMORY over wall clock: loading segments whole is faster today
	# but allocates a large object per segment, which is exactly the wrong
	# property for a rig whose purpose is to measure much larger trees.
	$stream = [System.IO.BufferedStream]::new($file, 1048576)
	try { return Get-RigWalStreamCensus -Stream $stream -Path $Path }
	finally { $stream.Dispose(); $file.Dispose() }
}

<#
.SYNOPSIS
	Walks every wal.log under a restored data root and aggregates the census
	per tree and per shard.

.DESCRIPTION
	The layout written by FileWalStorageProvider is
	<WalRoot>/<encodedTreeId>/shard-<n>/wal.log. Tree ids are percent-encoded
	on disk (every byte outside [A-Za-z0-9-._] becomes %XX), so the tree name
	is decoded back before it is reported.
#>
function Get-RigWalTreeCensus {
	[CmdletBinding()]
	param(
		[Parameter(Mandatory)] [string] $WalRoot
	)

	if (-not (Test-Path -LiteralPath $WalRoot)) {
		throw "WAL root not found: $WalRoot"
	}

	$shards = [System.Collections.Generic.List[object]]::new()

	foreach ($treeDirectory in (Get-ChildItem -LiteralPath $WalRoot -Directory | Sort-Object Name)) {
		$treeId = ConvertFrom-RigEncodedPathSegment -Segment $treeDirectory.Name
		foreach ($shardDirectory in (Get-ChildItem -LiteralPath $treeDirectory.FullName -Directory | Sort-Object Name)) {
			$log = Join-Path $shardDirectory.FullName 'wal.log'
			if (-not (Test-Path -LiteralPath $log)) { continue }

			$shardIndex = -1
			if ($shardDirectory.Name -match '^shard-(\d+)$') { $shardIndex = [int] $Matches[1] }

			$census = Get-RigWalFileCensus -Path $log
			$shards.Add([pscustomobject] @{
					TreeId                = $treeId
					ShardIndex            = $shardIndex
					SizeBytes             = $census.SizeBytes
					DataRecords           = $census.DataRecords
					CommitRecords         = $census.CommitRecords
					TrimRecords           = $census.TrimRecords
					PayloadBytes          = $census.PayloadBytes
					MinDataOffset         = $census.MinDataOffset
					MaxDataOffset         = $census.MaxDataOffset
					LastTrimThroughOffset = $census.LastTrimThroughOffset
					TruncatedTailBytes    = $census.TruncatedTailBytes
					Intact                = $census.Intact
				})
		}
	}

	$trees = foreach ($group in ($shards | Group-Object TreeId | Sort-Object Name)) {
		[pscustomobject] @{
			TreeId        = $group.Name
			Shards        = $group.Count
			SizeBytes     = ($group.Group | Measure-Object SizeBytes -Sum).Sum
			DataRecords   = ($group.Group | Measure-Object DataRecords -Sum).Sum
			CommitRecords = ($group.Group | Measure-Object CommitRecords -Sum).Sum
			TrimRecords   = ($group.Group | Measure-Object TrimRecords -Sum).Sum
			PayloadBytes  = ($group.Group | Measure-Object PayloadBytes -Sum).Sum
		}
	}

	return [pscustomobject] @{
		WalRoot            = (Resolve-Path -LiteralPath $WalRoot).Path
		Segments           = $shards.Count
		TotalSizeBytes     = ($shards | Measure-Object SizeBytes -Sum).Sum
		TotalDataRecords   = ($shards | Measure-Object DataRecords -Sum).Sum
		TotalCommitRecords = ($shards | Measure-Object CommitRecords -Sum).Sum
		TotalTrimRecords   = ($shards | Measure-Object TrimRecords -Sum).Sum
		TornSegments       = @($shards | Where-Object { -not $_.Intact }).Count
		Trees              = @($trees)
		Shards             = @($shards)
	}
}

<#
.SYNOPSIS
	Reverses the percent-encoding FileWalStorageProvider applies to a tree id
	before using it as a directory name.
#>
function ConvertFrom-RigEncodedPathSegment {
	[CmdletBinding()]
	param([Parameter(Mandatory)] [AllowEmptyString()] [string] $Segment)

	if ($Segment -notmatch '%') { return $Segment }

	$bytes = [System.Collections.Generic.List[byte]]::new()
	$index = 0
	while ($index -lt $Segment.Length) {
		if ($Segment[$index] -eq '%' -and $index + 2 -lt $Segment.Length) {
			$hex = $Segment.Substring($index + 1, 2)
			$value = 0
			if ([int]::TryParse($hex, [System.Globalization.NumberStyles]::HexNumber, [System.Globalization.CultureInfo]::InvariantCulture, [ref] $value)) {
				$bytes.Add([byte] $value)
				$index += 3
				continue
			}
		}
		$bytes.AddRange([System.Text.Encoding]::UTF8.GetBytes([string] $Segment[$index]))
		$index++
	}

	return [System.Text.Encoding]::UTF8.GetString($bytes.ToArray())
}

# ---------------------------------------------------------------------------
# Deterministic statistics and log counters
# ---------------------------------------------------------------------------

<#
.SYNOPSIS
	Nearest-rank percentile over a sample set. Deterministic and total: an
	empty sample returns $null rather than throwing.
#>
function Get-RigPercentile {
	[CmdletBinding()]
	param(
		[double[]] $Samples,
		[Parameter(Mandatory)] [ValidateRange(0, 100)] [double] $Percentile
	)

	if ($null -eq $Samples -or $Samples.Count -eq 0) { return $null }
	$sorted = [double[]] ($Samples | Sort-Object)
	if ($Percentile -le 0) { return $sorted[0] }
	$rank = [int] [Math]::Ceiling($Percentile / 100.0 * $sorted.Length)
	if ($rank -lt 1) { $rank = 1 }
	if ($rank -gt $sorted.Length) { $rank = $sorted.Length }
	return $sorted[$rank - 1]
}

<#
.SYNOPSIS
	Counts the container-log signals the epic cares about in a captured log.

.DESCRIPTION
	Returns replay over-budget warnings, stale-projection failures, dropped
	messages, and total warning/error lines. Matching is deliberately on
	stable message fragments emitted by the product rather than on log-level
	prefixes alone, so a change in logger formatting does not silently zero a
	counter without also failing the fragment.
#>
function Measure-RigLogCounters {
	[CmdletBinding()]
	param([AllowNull()] [string[]] $Lines)

	$counters = [ordered] @{
		TotalLines               = 0
		WarningLines             = 0
		ErrorLines               = 0
		ReplayOverBudgetWarnings = 0
		ProjectionStaleFailures  = 0
		DroppedMessages          = 0
		CursorPublishFailures    = 0
	}

	if ($null -eq $Lines) { return [pscustomobject] $counters }

	foreach ($line in $Lines) {
		if ($null -eq $line) { continue }
		$counters.TotalLines++
		if ($line -match '\bwarn\b|\bWarning\b') { $counters.WarningLines++ }
		if ($line -match '\bfail\b|\bError\b|\bcrit\b') { $counters.ErrorLines++ }
		if ($line -match 'replaying beyond') { $counters.ReplayOverBudgetWarnings++ }
		if ($line -match 'cannot be recovered') { $counters.ProjectionStaleFailures++ }
		if ($line -match 'dropp(ed|ing)') { $counters.DroppedMessages++ }
		if ($line -match 'cursor registration failed') { $counters.CursorPublishFailures++ }
	}

	return [pscustomobject] $counters
}

<#
.SYNOPSIS
	Relative spread of a set of samples, as a percentage of the mean.

.DESCRIPTION
	The run-to-run comparability figure the epic requires: two runs are
	comparable when the spread of their headline numbers is small relative to
	the effect a sub-issue is trying to attribute. Returns $null for an empty
	sample and 0 for a single sample.
#>
function Get-RigRelativeSpread {
	[CmdletBinding()]
	param([double[]] $Samples)

	if ($null -eq $Samples -or $Samples.Count -eq 0) { return $null }
	if ($Samples.Count -eq 1) { return 0.0 }
	$stats = $Samples | Measure-Object -Average -Minimum -Maximum
	if ($stats.Average -eq 0) { return 0.0 }
	return [Math]::Round((($stats.Maximum - $stats.Minimum) / $stats.Average) * 100.0, 2)
}
