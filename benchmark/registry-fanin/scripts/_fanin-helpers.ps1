#!/usr/bin/env pwsh
<#
.SYNOPSIS
	Pure helper functions for the isolated registry fan-in rig.

.DESCRIPTION
	Everything in this file is deliberately side-effect free (or file-read
	only) so Test-FanInHelpers.ps1 can exercise it without Docker, without a
	running silo, and without any dependence on wall-clock timing.

	Four groups live here:

	  1. Configuration resolution (Get-FanInConfig).
	  2. The FAIL-CLOSED isolation guard (Assert-FanInIsolation,
	     Assert-FanInContainerTarget). The rig runs its sidecar with
	     `--network container:<id>`, so a wrong id there would attach a load
	     generator to the PROTECTED deployment's network namespace and drive
	     synthetic writes straight at it. The guard therefore refuses a
	     configuration, and refuses any individual container id or name, that
	     names the live project, a live volume, a live image tag, the live
	     port, or a live container.
	  3. The measurement primitives the protocol mandates: timestamped log
	     bucketing (Measure-FanInTimeoutCensus), counter re-baselining
	     (New-FanInCounterBaseline / Compare-FanInCounterReading), and
	     restart-boundary detection (Test-FanInReadingSpansRestart).
	  4. Deterministic distribution statistics (Get-FanInPercentile,
	     Get-FanInDistribution, Get-FanInDispersion).

	Dot-source it:  . (Join-Path $PSScriptRoot '_fanin-helpers.ps1')
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
function Get-FanInConfig {
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
		throw "Fan-in rig parameters file not found: $ParametersFile"
	}

	$config = & $ParametersFile
	if ($config -isnot [hashtable]) {
		throw "Fan-in rig parameters file '$ParametersFile' must evaluate to a hashtable; got '$($config.GetType().FullName)'."
	}

	foreach ($key in $Override.Keys) {
		$value = $Override[$key]
		# A switch left unbound arrives as $null or empty; never let an unbound
		# override erase a committed default.
		if ($null -ne $value -and "$value" -ne '') {
			$config[$key] = $value
		}
	}

	$config['ParametersFile'] = $ParametersFile
	return $config
}

# ---------------------------------------------------------------------------
# The isolation guard
# ---------------------------------------------------------------------------

# Keys the guard requires. A missing or empty key is a REFUSAL, not a default:
# the guard must never be able to pass because a value was absent.
$script:FanInRequiredConfigKeys = @(
	'ProjectName',
	'WorkVolume',
	'HfCacheVolume',
	'HostPort',
	'McpImage',
	'EmbedderImage',
	'DriverImage',
	'RequiredProjectPrefix',
	'RequiredVolumePrefix',
	'RequiredImageTag',
	'ForbiddenProjects',
	'ForbiddenVolumePrefixes',
	'ForbiddenVolumes',
	'ForbiddenImages',
	'ForbiddenPorts',
	'ForbiddenContainerNames'
)

<#
.SYNOPSIS
	Normalises a Docker image reference so 'foo' and 'foo:latest' compare equal.
#>
function ConvertTo-FanInNormalisedImage {
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
function Get-FanInImageTag {
	[CmdletBinding()]
	param([string] $Image)

	$normalised = ConvertTo-FanInNormalisedImage -Image $Image
	if ($normalised -eq '') { return '' }
	$lastSlash = $normalised.LastIndexOf('/')
	$lastColon = $normalised.LastIndexOf(':')
	if ($lastColon -le $lastSlash) { return '' }
	return $normalised.Substring($lastColon + 1)
}

<#
.SYNOPSIS
	Returns the isolation violations (if any) for a single volume name.
#>
function Test-FanInVolumeName {
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
		if ($Volume -ieq "$forbidden") { $found.Add("$Label '$Volume' is a LIVE volume") }
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
	Refuses a configuration that could address the live deployment.
#>
function Assert-FanInIsolation {
	[CmdletBinding()]
	param(
		[Parameter(Mandatory)] [hashtable] $Config
	)

	$violations = [System.Collections.Generic.List[string]]::new()

	foreach ($key in $script:FanInRequiredConfigKeys) {
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
		throw ("Fan-in rig isolation guard REFUSED to start: " + ($violations -join '; ') + '.')
	}

	$forbiddenImages = @($Config.ForbiddenImages) | ForEach-Object { ConvertTo-FanInNormalisedImage -Image $_ }
	$forbiddenPorts = @($Config.ForbiddenPorts) | ForEach-Object { [int] $_ }

	# --- Project ---
	$project = "$($Config.ProjectName)"
	foreach ($forbidden in @($Config.ForbiddenProjects)) {
		if ($project -ieq "$forbidden") { $violations.Add("compose project '$project' is a FORBIDDEN project") }
	}
	if (-not $project.StartsWith("$($Config.RequiredProjectPrefix)", [StringComparison]::Ordinal)) {
		$violations.Add("compose project '$project' does not start with the required rig prefix '$($Config.RequiredProjectPrefix)'")
	}

	# --- Volumes ---
	$seen = @{}
	foreach ($key in @('WorkVolume', 'HfCacheVolume')) {
		$volume = "$($Config[$key])"
		$volumeViolations = Test-FanInVolumeName -Volume $volume -Config $Config -Label $key
		if ($volumeViolations.Count -gt 0) { $violations.AddRange($volumeViolations) }
		if ($seen.ContainsKey($volume)) {
			$violations.Add("volume '$volume' is used for both $($seen[$volume]) and $key; they must be distinct")
		}
		else { $seen[$volume] = $key }
	}

	# --- Images ---
	# The silo image and the embedder image both carry the rig tag. The DRIVER
	# image is exempt from the tag requirement and checked separately: it is
	# this rig's own artefact, built from a COPY-only Dockerfile, and it never
	# shares a repository with anything live - but it must still not BE a live
	# reference, which is what the forbidden-image check below enforces.
	foreach ($key in @('McpImage', 'EmbedderImage')) {
		$image = ConvertTo-FanInNormalisedImage -Image "$($Config[$key])"
		if ($forbiddenImages -contains $image) {
			$violations.Add("$key '$image' is a LIVE image tag; the rig must never run one")
		}
		$tag = Get-FanInImageTag -Image $image
		if ($tag -ne "$($Config.RequiredImageTag)") {
			$violations.Add("$key '$image' does not carry the required rig tag '$($Config.RequiredImageTag)'")
		}
	}

	$driver = ConvertTo-FanInNormalisedImage -Image "$($Config.DriverImage)"
	if ($forbiddenImages -contains $driver) {
		$violations.Add("DriverImage '$driver' is a LIVE image tag")
	}

	# Re-tagging an image onto itself would mean the rig had been pointed at a
	# live tag as a DESTINATION.
	if ($Config.ContainsKey('SourceEmbedderImage')) {
		$source = ConvertTo-FanInNormalisedImage -Image "$($Config.SourceEmbedderImage)"
		$destination = ConvertTo-FanInNormalisedImage -Image "$($Config.EmbedderImage)"
		if ($source -eq $destination) {
			$violations.Add("SourceEmbedderImage '$source' is the same reference as EmbedderImage; the rig tag must be an ADDITIONAL tag, never the source tag itself")
		}
	}

	# --- Port ---
	$port = 0
	if (-not [int]::TryParse("$($Config.HostPort)", [ref] $port)) {
		$violations.Add("HostPort '$($Config.HostPort)' is not an integer")
	}
	else {
		if ($port -lt 1 -or $port -gt 65535) { $violations.Add("HostPort $port is not a valid TCP port") }
		if ($forbiddenPorts -contains $port) { $violations.Add("HostPort $port is a FORBIDDEN host port") }
	}

	if ($violations.Count -gt 0) {
		throw ("Fan-in rig isolation guard REFUSED to start: " + ($violations -join '; ') + '.')
	}

	return $Config
}

<#
.SYNOPSIS
	Refuses a container id or name that is not a rig container.

.DESCRIPTION
	This is the guard that matters most in THIS rig, and it has no counterpart
	in the cold-start rig. The sidecar driver must share the silo's network
	namespace (`docker run --network container:<id>`), because the silo binds
	its Orleans gateway to loopback INSIDE its own container and no published
	port can reach it. A mistaken id there does not fail - it succeeds, against
	the wrong container, and drives synthetic tree creation and write load at
	the protected deployment.

	So every id the rig is about to hand to a docker verb passes through here
	first. The check is two-sided on purpose: a name must not be a forbidden
	one, AND its resolved compose project must be the rig's. Checking only the
	blocklist would pass any container not thought of in advance; checking only
	the project label would pass a container whose labels are absent.
#>
function Assert-FanInContainerTarget {
	[CmdletBinding()]
	param(
		[Parameter(Mandatory)] [hashtable] $Config,
		[string] $ContainerId,
		[string] $ContainerName,
		[AllowNull()] [string] $ComposeProject
	)

	$violations = [System.Collections.Generic.List[string]]::new()

	if ([string]::IsNullOrWhiteSpace($ContainerId) -and [string]::IsNullOrWhiteSpace($ContainerName)) {
		$violations.Add('no container id or name was supplied')
	}

	foreach ($candidate in @($ContainerId, $ContainerName)) {
		if ([string]::IsNullOrWhiteSpace($candidate)) { continue }
		foreach ($forbidden in @($Config.ForbiddenContainerNames)) {
			if ("$candidate" -ieq "$forbidden") {
				$violations.Add("container '$candidate' is a PROTECTED container")
			}
		}
		foreach ($prefix in @($Config.ForbiddenVolumePrefixes)) {
			# Compose derives container names from the project name, so the live
			# volume prefix ('repocontextcontainer_') and the live container name
			# prefix ('repocontextcontainer-') share a stem. Compare on the stem
			# so neither separator style slips through.
			$stem = "$prefix".TrimEnd('_', '-')
			if ($stem -ne '' -and "$candidate".StartsWith($stem, [StringComparison]::OrdinalIgnoreCase)) {
				$violations.Add("container '$candidate' carries the LIVE deployment's name stem '$stem'")
			}
		}
	}

	# The resolved compose project must be the rig's own. An ABSENT project is
	# a refusal, not a pass: a container with no compose label is not a rig
	# container, and treating "unknown" as "fine" is exactly the failure mode a
	# fail-closed guard exists to prevent.
	if ($null -eq $ComposeProject -or [string]::IsNullOrWhiteSpace($ComposeProject)) {
		$violations.Add('the container declares no compose project, so it cannot be confirmed as a rig container')
	}
	elseif ("$ComposeProject" -ne "$($Config.ProjectName)") {
		$violations.Add("container belongs to compose project '$ComposeProject', not the rig project '$($Config.ProjectName)'")
	}

	if ($violations.Count -gt 0) {
		throw ("Fan-in rig container guard REFUSED the target: " + ($violations -join '; ') + '.')
	}

	return $true
}

# ---------------------------------------------------------------------------
# Restart-boundary detection
# ---------------------------------------------------------------------------

<#
.SYNOPSIS
	Whether a counter reading taken at ReadingTimeUtc spans a container restart.

.DESCRIPTION
	RestartCount=0 does NOT prove no restart occurred, and that is the trap this
	function exists to close. A compose recreation produces a FRESH container:
	its RestartCount is 0 because Docker never restarted it - it replaced it -
	and its StartedAt is new. A reading tabled against a baseline taken before
	that recreation is comparing two different process lifetimes, and the
	difference reads as a plausible number rather than as an error.

	So the authority is the comparison of StartedAt against the baseline time,
	never RestartCount. RestartCount is still returned, because a non-zero value
	is independent positive evidence, but it can only ADD a restart, never rule
	one out.
#>
function Test-FanInReadingSpansRestart {
	[CmdletBinding()]
	param(
		[Parameter(Mandatory)] [datetime] $BaselineTimeUtc,
		[Parameter(Mandatory)] [datetime] $StartedAtUtc,
		[int] $RestartCount = 0
	)

	$recreated = $StartedAtUtc.ToUniversalTime() -gt $BaselineTimeUtc.ToUniversalTime()

	return [pscustomobject] @{
		SpansRestart    = ($recreated -or $RestartCount -gt 0)
		StartedAfterBaseline = $recreated
		RestartCount    = $RestartCount
		BaselineTimeUtc = $BaselineTimeUtc.ToUniversalTime()
		StartedAtUtc    = $StartedAtUtc.ToUniversalTime()
		Reason          = if ($recreated) {
			"container StartedAt $($StartedAtUtc.ToUniversalTime().ToString('o')) is AFTER the baseline taken at $($BaselineTimeUtc.ToUniversalTime().ToString('o')); the container was recreated and the baseline belongs to a dead process"
		}
		elseif ($RestartCount -gt 0) {
			"RestartCount is $RestartCount; the process restarted in place since the container was created"
		}
		else { '' }
	}
}

<#
.SYNOPSIS
	Records a counter baseline together with the process lifetime it belongs to.
#>
function New-FanInCounterBaseline {
	[CmdletBinding()]
	param(
		[Parameter(Mandatory)] [hashtable] $Counters,
		[Parameter(Mandatory)] [datetime] $StartedAtUtc,
		[datetime] $TakenAtUtc = [datetime]::UtcNow,
		[int] $RestartCount = 0,
		[string] $Label = ''
	)

	return [pscustomobject] @{
		Label        = $Label
		Counters     = $Counters.Clone()
		StartedAtUtc = $StartedAtUtc.ToUniversalTime()
		TakenAtUtc   = $TakenAtUtc.ToUniversalTime()
		RestartCount = $RestartCount
	}
}

<#
.SYNOPSIS
	Subtracts a baseline from a later reading, REFUSING to do so across a restart.

.DESCRIPTION
	Counter readings are scoped to a process lifetime. Tabling a delta across a
	restart boundary produces a number that looks like a measurement and is not
	one: the counter reset to zero, so the "delta" is really the post-restart
	absolute value, which understates or (when the reading is smaller than the
	baseline) goes negative and gets silently clamped by whoever reads it.

	This function refuses rather than clamping. A caller that wants a reading
	across a restart must re-baseline, which is the protocol's requirement.
#>
function Compare-FanInCounterReading {
	[CmdletBinding()]
	param(
		[Parameter(Mandatory)] $Baseline,
		[Parameter(Mandatory)] [hashtable] $Counters,
		[Parameter(Mandatory)] [datetime] $StartedAtUtc,
		[int] $RestartCount = 0
	)

	$boundary = Test-FanInReadingSpansRestart `
		-BaselineTimeUtc $Baseline.TakenAtUtc `
		-StartedAtUtc $StartedAtUtc `
		-RestartCount $RestartCount

	if ($boundary.SpansRestart -or $StartedAtUtc.ToUniversalTime() -ne $Baseline.StartedAtUtc) {
		throw ("Fan-in rig REFUSES to table a counter delta across a restart boundary: " +
			"baseline belongs to the process started $($Baseline.StartedAtUtc.ToString('o')), the reading to one started $($StartedAtUtc.ToUniversalTime().ToString('o')). " +
			"Re-baseline after every restart. $($boundary.Reason)")
	}

	$delta = @{}
	foreach ($key in $Counters.Keys) {
		$before = if ($Baseline.Counters.ContainsKey($key)) { [double] $Baseline.Counters[$key] } else { 0.0 }
		$after = [double] $Counters[$key]
		if ($after -lt $before) {
			throw ("Counter '$key' went BACKWARDS ($before -> $after) without a detected restart. " +
				'A monotonic counter cannot decrease within one process lifetime, so the two readings are not from the same process and must not be differenced.')
		}
		$delta[$key] = $after - $before
	}

	return [pscustomobject] @{
		Delta          = $delta
		WindowSeconds  = ([datetime]::UtcNow - $Baseline.TakenAtUtc).TotalSeconds
		BaselineLabel  = $Baseline.Label
	}
}

# ---------------------------------------------------------------------------
# Timestamped log census - the protocol's mandated measurement
# ---------------------------------------------------------------------------

# A host log record begins with an ISO-8601 timestamp, a level, and a category.
# Continuation lines are indented, so a record is a header plus everything up to
# the next header.
$script:FanInLogHeaderPattern = '^(?<ts>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z)\s+(?<level>\w+):\s+(?<category>[^\[\s]+)\[(?<eventId>\d+)\]\s*$'

<#
.SYNOPSIS
	Splits raw `docker logs -t`-style output into timestamped records.
#>
function Split-FanInLogRecords {
	[CmdletBinding()]
	param([string[]] $Lines)

	$records = [System.Collections.Generic.List[object]]::new()
	if ($null -eq $Lines) { return , $records.ToArray() }

	$current = $null
	$body = [System.Collections.Generic.List[string]]::new()

	foreach ($line in $Lines) {
		$text = "$line"
		$match = [regex]::Match($text, $script:FanInLogHeaderPattern)
		if ($match.Success) {
			if ($null -ne $current) {
				$current.Body = ($body -join "`n")
				$records.Add($current)
			}
			$body.Clear()
			$current = [pscustomobject] @{
				TimestampUtc = [datetime]::Parse($match.Groups['ts'].Value, [cultureinfo]::InvariantCulture, [System.Globalization.DateTimeStyles]::AdjustToUniversal -bor [System.Globalization.DateTimeStyles]::AssumeUniversal)
				Level        = $match.Groups['level'].Value
				Category     = $match.Groups['category'].Value
				EventId      = [int] $match.Groups['eventId'].Value
				Body         = ''
			}
		}
		elseif ($null -ne $current) {
			$body.Add($text)
		}
	}

	if ($null -ne $current) {
		$current.Body = ($body -join "`n")
		$records.Add($current)
	}

	return , $records.ToArray()
}

<#
.SYNOPSIS
	Extracts the CALLEE grain and interface member from an Orleans timeout body.

.DESCRIPTION
	Orleans renders a request descriptor as

		Request [<silo> <source-id>]->[<silo> <target-id>] <Member-descriptor> #<id>

	so the source and the target are structurally identical and are told apart
	ONLY by which side of the ']->[' separator they fall on. An earlier revision
	of this function matched the first '<type>/<key>' pair anywhere in the body,
	which is the SOURCE, and it therefore attributed every registry timeout to
	whichever caller happened to make it - reporting, for a storm that was
	entirely on 'latticeregistry/_lattice_trees', a spread across
	'sys.client/...', 'hotshardmonitor/...' and 'shardhealingorchestrator/...'.
	That is worse than an unparsed line, because it is a confident, plausible,
	wrong answer that inverts the very question the rig exists to settle. The
	separator is therefore matched explicitly and a body that does not carry one
	yields empty fields rather than a guess.

	The member is read from the text AFTER the target bracket, which Orleans
	renders in two shapes:

		Ns.IFace[(Ns.IFace)Ns.Impl].MemberAsync(...)   <- grain interface call
		Orleans.IRemindable.ReceiveReminder(...)       <- system interface call

	Note the second has no 'Async' suffix. A previous revision required one, so
	every reminder-tick timeout parsed as an empty member and vanished from the
	by-member census - which matters here because those ticks are the collateral
	damage of registry saturation and are the evidence that it cascades.
#>
function Get-FanInTimeoutTarget {
	[CmdletBinding()]
	param([string] $Body)

	$text = "$Body"

	$grain = ''
	$member = ''
	$iface = ''

	# The callee is the bracket to the RIGHT of the arrow. Its contents are
	# '<silo-address> <grain-id>', so the grain id is the last whitespace
	# separated token.
	$arrow = [regex]::Match($text, '\]\s*->\s*\[(?<target>[^\]]*)\]')
	if (-not $arrow.Success) {
		return [pscustomobject] @{ Grain = ''; Member = ''; Interface = '' }
	}

	$targetTokens = @(($arrow.Groups['target'].Value -split '\s+') | Where-Object { $_ })
	if ($targetTokens.Count -gt 0) { $grain = $targetTokens[-1] }

	$rest = $text.Substring($arrow.Index + $arrow.Length)

	# Prefer the ']. Member(' shape so the implementation type inside the
	# brackets can never be mistaken for the member, then fall back to the plain
	# 'IFace.Member(' shape used by system interfaces.
	$memberMatch = [regex]::Match($rest, '\]\.(?<member>[A-Za-z0-9_]+)\s*\(')
	if (-not $memberMatch.Success) {
		$memberMatch = [regex]::Match($rest, '\.(?<member>[A-Za-z0-9_]+)\s*\(')
	}
	if ($memberMatch.Success) { $member = $memberMatch.Groups['member'].Value }

	$ifaceMatch = [regex]::Match($rest, '(?<iface>I[A-Za-z0-9_]+)')
	if ($ifaceMatch.Success) { $iface = $ifaceMatch.Groups['iface'].Value }

	return [pscustomobject] @{
		Grain     = $grain
		Member    = $member
		Interface = $iface
	}
}

<#
.SYNOPSIS
	Buckets response-deadline timeouts by LOG TIMESTAMP.

.DESCRIPTION
	The measurement method is non-negotiable and this function is where it
	lives. A single scrape of a cumulative timeout counter reads as an ACTIVE
	fault, because it reports everything since process start as though it were
	happening now. A short delta of the same counter reads as HEALTHY, because
	a storm confined to a two-minute window five minutes after start contributes
	nothing to a window that does not contain it. Both single-shot readings are
	wrong, in OPPOSITE directions, which is worse than either being merely
	imprecise: whichever one an operator happens to take determines the
	conclusion.

	Only the timestamped distribution is truthful, so every timeout is bucketed
	by the timestamp ON ITS OWN LOG RECORD, relative to the container's ready
	moment, and the whole distribution is reported. The peak bucket is returned
	too, but as one field among many rather than as the headline - the protocol
	asks for the distribution precisely because the observed magnitude already
	varies 103 vs 18 across two runs, a factor of 5.7, and a peak cannot
	distinguish a real effect from that dispersion.
#>
function Measure-FanInTimeoutCensus {
	[CmdletBinding()]
	param(
		[string[]] $Lines,
		[Parameter(Mandatory)] [datetime] $ReadyAtUtc,
		[int] $BucketSeconds = 30,
		[int] $WindowSeconds = 900
	)

	$readyUtc = $ReadyAtUtc.ToUniversalTime()
	$records = Split-FanInLogRecords -Lines $Lines

	$events = [System.Collections.Generic.List[object]]::new()
	foreach ($record in $records) {
		if ($record.Body -notmatch 'TimeoutException') { continue }
		if ($record.Body -notmatch 'did not arrive on time') { continue }

		$offset = ($record.TimestampUtc - $readyUtc).TotalSeconds
		$target = Get-FanInTimeoutTarget -Body $record.Body

		# The status / diagnostics clause is the free discriminator between
		# 'never served' (absent: the target could not answer a status probe
		# because it was still activating) and 'served slowly' (present). Both
		# spellings are matched; Orleans 10.2.x renders 'Status:' and earlier
		# versions rendered 'Diagnostics:'.
		$carriesStatus = ($record.Body -match '(?:Status|Diagnostics):')

		$events.Add([pscustomobject] @{
			TimestampUtc  = $record.TimestampUtc
			OffsetSeconds = $offset
			Bucket        = [int] [math]::Floor($offset / $BucketSeconds)
			Category      = $record.Category
			Grain         = $target.Grain
			Member        = $target.Member
			CarriesStatus = $carriesStatus
		})
	}

	$inWindow = @($events | Where-Object { $_.OffsetSeconds -ge 0 -and $_.OffsetSeconds -lt $WindowSeconds })

	$buckets = @()
	if ($inWindow.Count -gt 0) {
		$buckets = @($inWindow | Group-Object -Property Bucket | Sort-Object { [int] $_.Name } | ForEach-Object {
			[pscustomobject] @{
				Bucket       = [int] $_.Name
				FromSeconds  = ([int] $_.Name) * $BucketSeconds
				ToSeconds    = (([int] $_.Name) + 1) * $BucketSeconds
				Count        = $_.Count
				NeverServed  = @($_.Group | Where-Object { -not $_.CarriesStatus }).Count
				ServedSlowly = @($_.Group | Where-Object { $_.CarriesStatus }).Count
			}
		})
	}

	$first = if ($inWindow.Count -gt 0) { ($inWindow | Sort-Object OffsetSeconds | Select-Object -First 1).OffsetSeconds } else { $null }
	$last = if ($inWindow.Count -gt 0) { ($inWindow | Sort-Object OffsetSeconds | Select-Object -Last 1).OffsetSeconds } else { $null }

	return [pscustomobject] @{
		ReadyAtUtc       = $readyUtc
		BucketSeconds    = $BucketSeconds
		WindowSeconds    = $WindowSeconds
		Total            = $inWindow.Count
		TotalOutsideWindow = $events.Count - $inWindow.Count
		NeverServed      = @($inWindow | Where-Object { -not $_.CarriesStatus }).Count
		ServedSlowly     = @($inWindow | Where-Object { $_.CarriesStatus }).Count
		FirstOffsetSeconds = $first
		LastOffsetSeconds  = $last
		Buckets          = $buckets
		ByGrain          = @($inWindow | Group-Object Grain | Sort-Object Count -Descending | ForEach-Object { [pscustomobject] @{ Grain = $_.Name; Count = $_.Count } })
		ByMember         = @($inWindow | Group-Object Member | Sort-Object Count -Descending | ForEach-Object { [pscustomobject] @{ Member = $_.Name; Count = $_.Count } })
		ByCaller         = @($inWindow | Group-Object Category | Sort-Object Count -Descending | ForEach-Object { [pscustomobject] @{ Category = $_.Name; Count = $_.Count } })
		Events           = $inWindow
	}
}

# ---------------------------------------------------------------------------
# Deterministic distribution statistics
# ---------------------------------------------------------------------------

<#
.SYNOPSIS
	Nearest-rank percentile over a numeric sample.
#>
function Get-FanInPercentile {
	[CmdletBinding()]
	param(
		[double[]] $Values,
		[Parameter(Mandatory)] [double] $Percentile
	)

	if ($null -eq $Values -or $Values.Count -eq 0) { return $null }
	if ($Percentile -le 0) { return ([double[]] ($Values | Sort-Object))[0] }

	$sorted = [double[]] ($Values | Sort-Object)
	$rank = [math]::Ceiling(($Percentile / 100.0) * $sorted.Count)
	if ($rank -lt 1) { $rank = 1 }
	if ($rank -gt $sorted.Count) { $rank = $sorted.Count }
	return $sorted[$rank - 1]
}

<#
.SYNOPSIS
	The distribution summary the protocol asks to be reported instead of a peak.
#>
function Get-FanInDistribution {
	[CmdletBinding()]
	param([double[]] $Values)

	if ($null -eq $Values -or $Values.Count -eq 0) {
		return [pscustomobject] @{ N = 0; Min = $null; P50 = $null; P95 = $null; P99 = $null; Max = $null; Mean = $null }
	}

	$sorted = [double[]] ($Values | Sort-Object)
	$sum = 0.0
	foreach ($v in $sorted) { $sum += $v }

	return [pscustomobject] @{
		N    = $sorted.Count
		Min  = $sorted[0]
		P50  = Get-FanInPercentile -Values $sorted -Percentile 50
		P95  = Get-FanInPercentile -Values $sorted -Percentile 95
		P99  = Get-FanInPercentile -Values $sorted -Percentile 99
		Max  = $sorted[$sorted.Count - 1]
		Mean = $sum / $sorted.Count
	}
}

<#
.SYNOPSIS
	Dispersion across the replicates of one protocol cell.

.DESCRIPTION
	Reported alongside every cell because the observed storm magnitude already
	varies 103 vs 18 across two runs - a factor of 5.7 - so a cell mean quoted
	without its spread cannot distinguish a real scaling effect from noise. The
	ratio max/min is reported as well as the standard deviation, because it is
	the statistic that makes the 5.7 directly comparable.
#>
function Get-FanInDispersion {
	[CmdletBinding()]
	param([double[]] $Values)

	if ($null -eq $Values -or $Values.Count -eq 0) {
		return [pscustomobject] @{ N = 0; Mean = $null; StdDev = $null; Min = $null; Max = $null; MaxOverMin = $null; RelativeSpread = $null }
	}

	$sorted = [double[]] ($Values | Sort-Object)
	$n = $sorted.Count
	$sum = 0.0
	foreach ($v in $sorted) { $sum += $v }
	$mean = $sum / $n

	$sq = 0.0
	foreach ($v in $sorted) { $sq += [math]::Pow($v - $mean, 2) }
	# Sample standard deviation; n=1 has no spread to report rather than zero.
	$sd = if ($n -gt 1) { [math]::Sqrt($sq / ($n - 1)) } else { $null }

	$min = $sorted[0]
	$max = $sorted[$n - 1]

	return [pscustomobject] @{
		N              = $n
		Mean           = $mean
		StdDev         = $sd
		Min            = $min
		Max            = $max
		MaxOverMin     = if ($min -gt 0) { $max / $min } else { $null }
		RelativeSpread = if ($mean -gt 0) { ($max - $min) / $mean } else { $null }
	}
}

<#
.SYNOPSIS
	Parses a Prometheus text-exposition scrape into a counter hashtable.
#>
function Get-FanInHostLoad {
	<#
	.SYNOPSIS
		A docker stats snapshot across every running container.

	.DESCRIPTION
		Captured at both ends of a measurement window so that co-tenancy becomes
		OBSERVED rather than assumed absent. This rig shares a host with other
		containers, and while their combined load is far too small to manufacture
		a 30-second timeout, it is a meaningful fraction of a sub-millisecond
		point-read baseline - which is precisely the figure being quoted.

		Two samples bracketing the window are enough to settle afterwards whether
		a spike was contention, instead of having to argue it without data.
	#>
	[CmdletBinding()]
	param()

	$raw = & docker stats --no-stream --format '{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.BlockIO}}' 2>$null
	if ($LASTEXITCODE -ne 0) { return @() }

	$rows = foreach ($line in @($raw)) {
		$parts = "$line" -split "`t"
		if ($parts.Count -lt 4) { continue }

		$cpu = 0.0
		$null = [double]::TryParse(($parts[1] -replace '%', ''), [System.Globalization.NumberStyles]::Float, [cultureinfo]::InvariantCulture, [ref] $cpu)

		[pscustomobject] @{
			Name       = $parts[0]
			CpuPercent = $cpu
			# CPUPerc is cores-normalised, so on a 16-CPU host 1600% is full
			# utilisation. Recording the derived core count avoids that being
			# misread as near-saturation.
			Cores      = [math]::Round($cpu / 100.0, 3)
			Memory     = $parts[2]
			BlockIO    = $parts[3]
		}
	}

	return @($rows)
}

function ConvertFrom-FanInPrometheusText {
	[CmdletBinding()]
	param(
		[string[]] $Lines,
		[string] $NameFilter = ''
	)

	$counters = @{}
	foreach ($line in @($Lines)) {
		$text = "$line".Trim()
		if ($text -eq '' -or $text.StartsWith('#')) { continue }

		$match = [regex]::Match($text, '^(?<series>[A-Za-z_:][A-Za-z0-9_:]*(?:\{[^}]*\})?)\s+(?<value>[-+0-9.eENaninf]+)\s*$')
		if (-not $match.Success) { continue }

		$series = $match.Groups['series'].Value
		if ($NameFilter -ne '' -and $series -notmatch $NameFilter) { continue }

		$value = 0.0
		if ([double]::TryParse($match.Groups['value'].Value, [System.Globalization.NumberStyles]::Float, [cultureinfo]::InvariantCulture, [ref] $value)) {
			$counters[$series] = $value
		}
	}

	return $counters
}

function Get-FanInNonInterleavedOperations {
	<#
	.SYNOPSIS
		Reads the non-interleaved registry arms from the source declaration.

	.DESCRIPTION
		Which ILatticeRegistry members carry [AlwaysInterleave] is a static
		property of the interface, so the collector must know it rather than
		infer it. It cannot be inferred: the in-flight counter this rig emits is
		GLOBAL across arms, so a non-interleaved member routinely reports a
		width above zero and any test of the form "width > 0 therefore it
		interleaved" fails in the permissive direction.

		The obvious alternative is to restate the list in the collector, and
		that is the option this function exists to avoid. A copy is correct the
		day it is written and silently wrong the first time a member gains or
		loses the attribute - and the wrongness never surfaces as a bad-looking
		reading. The collector would keep printing a confident Interleaved flag
		that no longer described the binary under test, and every attribution
		downstream would inherit the error while continuing to look reasonable.

		So the set is parsed from RegistryCallCensus.cs, and the parse is
		strict: an unresolved identifier, an empty result, or a missing file all
		throw. A collector that cannot establish which members interleave must
		fail rather than fall back to a default, because the fallback would be
		indistinguishable from a correct read in the output.
	#>
	[CmdletBinding()]
	param(
		[string] $CensusPath
	)

	if (-not $CensusPath) {
		$repoRoot = Split-Path -Parent (Split-Path -Parent (Split-Path -Parent $PSScriptRoot))
		$CensusPath = Join-Path $repoRoot 'src/lattice/BPlusTree/Grains/RegistryCallCensus.cs'
	}

	if (-not (Test-Path $CensusPath)) {
		throw "Cannot read the interleaving declaration: '$CensusPath' does not exist. The collector will not guess, because a guessed list produces confident output that silently misdescribes the binary."
	}

	$text = Get-Content -Raw $CensusPath

	# Map the arm constants first. The list is declared with C# identifiers, not
	# literals, so resolving it needs the constant table as well as the list.
	$constants = @{}
	foreach ($m in [regex]::Matches($text, 'const\s+string\s+(?<name>\w+)\s*=\s*"(?<value>[^"]+)"')) {
		$constants[$m.Groups['name'].Value] = $m.Groups['value'].Value
	}

	$listMatch = [regex]::Match(
		$text,
		'NonInterleavedOperations\s*=\s*\[(?<body>[^\]]*)\]',
		[System.Text.RegularExpressions.RegexOptions]::Singleline)

	if (-not $listMatch.Success) {
		throw "Could not find the NonInterleavedOperations declaration in '$CensusPath'. If it was renamed or reshaped, update this parser rather than restating the list in the collector."
	}

	$resolved = foreach ($token in ($listMatch.Groups['body'].Value -split ',')) {
		$name = "$token".Trim()
		if (-not $name) { continue }

		if ($name -match '^"(?<literal>[^"]+)"$') {
			$Matches['literal']
			continue
		}

		if (-not $constants.ContainsKey($name)) {
			throw "NonInterleavedOperations names '$name', which is not a const string in '$CensusPath'. Refusing to drop it silently: an arm missing from this set is reported as interleaved and its readings are then treated as attributable."
		}

		$constants[$name]
	}

	$resolved = @($resolved)
	if ($resolved.Count -eq 0) {
		throw "Parsed an EMPTY non-interleaved set from '$CensusPath'. That reads as 'every member interleaves', which would mark every arm attributable - the most permissive possible answer, reached by failure rather than by evidence."
	}

	, $resolved
}
