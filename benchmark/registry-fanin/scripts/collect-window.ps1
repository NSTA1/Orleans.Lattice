#!/usr/bin/env pwsh
<#
.SYNOPSIS
	Collects one measurement window: timestamped timeout census, registry
	instrument readings, and the estate depth they were taken at.

.DESCRIPTION
	This script is the measurement, and its method is not negotiable.

	It buckets timeouts by the TIMESTAMP carried in each log record, never by
	counter scrape. A single scrape of the timeout counter reads as an ACTIVE
	fault, because the counter is cumulative over the process lifetime and shows
	every timeout that has ever happened. A delta of the same counter across a
	90-second interval reads as HEALTHY, because the storm has already passed.
	Both single-shot readings are wrong, and they are wrong in OPPOSITE
	directions, so averaging or cross-checking them does not help either. Only
	the timestamped distribution says what actually happened and when.

	It re-baselines every counter against the container's StartedAt. A counter
	reading is scoped to one process lifetime and can never be tabled across a
	restart boundary, so Compare-FanInCounterReading REFUSES such a delta rather
	than clamping it to zero - a clamped delta is indistinguishable from a quiet
	window, which is the failure mode that makes a restart invisible.

	RestartCount = 0 does not prove no restart occurred: a compose recreation
	yields a brand-new container with RestartCount = 0 and a fresh StartedAt. So
	StartedAt, not RestartCount, is the authority, and every reading records it.

.PARAMETER ReadyAtUtc
	The moment the silo reported ready. The window is measured from here.

.PARAMETER WindowSeconds
	How long a window to collect. Defaults to the configured measure window.

.PARAMETER Label
	A label recorded with the result, e.g. 'K20-depth8-run1'.

.PARAMETER OutputPath
	Where to write the result JSON.
#>

[CmdletBinding()]
param(
	[Parameter(Mandatory)]
	[datetime] $ReadyAtUtc,

	[int] $WindowSeconds,
	[int] $BucketSeconds = 30,
	[string] $Label = '',
	[string] $OutputPath,
	[string] $ParametersFile
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

. (Join-Path $PSScriptRoot '_fanin-helpers.ps1')

$RigRoot = Split-Path -Parent $PSScriptRoot
$config = Get-FanInConfig -ParametersFile $ParametersFile -ScriptRoot $PSScriptRoot
$null = Assert-FanInIsolation -Config $config

# A [datetime] parameter binds with Kind = Unspecified, which later .ToUniversalTime()
# calls then treat as LOCAL time and shift by the offset. That silently moves the
# measurement window by whole hours, and the shifted window still looks entirely
# plausible - it simply reports zero timeouts because it is pointed at the wrong
# stretch of time. Pin the kind explicitly rather than trusting the binder.
$ReadyAtUtc = [datetime]::SpecifyKind(
	$(if ($ReadyAtUtc.Kind -eq [System.DateTimeKind]::Local) { $ReadyAtUtc.ToUniversalTime() } else { $ReadyAtUtc }),
	[System.DateTimeKind]::Utc)

if (-not $PSBoundParameters.ContainsKey('WindowSeconds')) {
	$WindowSeconds = [int] $config.MeasureWindowSec
}

# ---- Resolve the container, and guard it before any docker command sees it ----
$containerName = "$($config.ProjectName)-repocontext-1"
$inspectRaw = & docker inspect $containerName 2>$null
if ($LASTEXITCODE -ne 0) { throw "Container '$containerName' is not present." }

$inspect = ($inspectRaw | ConvertFrom-Json)[0]
Assert-FanInContainerTarget `
	-Config $config `
	-ContainerId $inspect.Id `
	-ContainerName $inspect.Name.TrimStart('/') `
	-ComposeProject $inspect.Config.Labels.'com.docker.compose.project'

$startedAtUtc = ([datetime] $inspect.State.StartedAt).ToUniversalTime()
$restartCount = [int] $inspect.RestartCount
$readingAtUtc = [datetime]::UtcNow

# ---- The timestamped timeout census: the measurement proper ----
# --since is bounded to the window so the log read stays cheap, but the census
# itself buckets on the timestamp INSIDE each record, not on the fetch bound.
$sinceIso = $ReadyAtUtc.ToString('o')
$logText = & docker logs $inspect.Id --since $sinceIso 2>&1
$logLines = @($logText | ForEach-Object { "$_" })

$census = Measure-FanInTimeoutCensus `
	-Lines $logLines `
	-ReadyAtUtc $ReadyAtUtc `
	-BucketSeconds $BucketSeconds `
	-WindowSeconds $WindowSeconds

# ---- The registry instrument: the primary (a)-vs-(b) discriminator ----
$metricsText = & curl.exe -s "http://127.0.0.1:$($config.HostPort)/metrics"
# curl's output is already line-split by PowerShell. Interpolating it into a
# single string ("$metricsText") joins the elements with SPACES, which silently
# destroys the line structure the parser depends on and yields an empty result
# that looks exactly like a silo emitting no metrics at all.
$metricLines = @($metricsText | ForEach-Object { "$_" })
$registry = ConvertFrom-FanInPrometheusText -Lines $metricLines -NameFilter 'orleans_lattice_registry_call'
$admission = ConvertFrom-FanInPrometheusText -Lines $metricLines -NameFilter 'orleans_lattice_registry_admission'
$storage = ConvertFrom-FanInPrometheusText -Lines $metricLines -NameFilter 'orleans_storage_read_latency'

# Derive per-operation service time and fan-in width from the sum/count pairs.
# A histogram's _sum and _count are BOTH cumulative over the process lifetime, so
# these are lifetime means, not window means - stated here because a lifetime
# mean quietly presented as a window figure is precisely the kind of reading this
# script exists to stop.
$arms = @{}
foreach ($key in $registry.Keys) {
	if ($key -notmatch '^orleans_lattice_registry_call_(duration|in_flight)_(sum|count)\{(?<tags>.*)\}$') { continue }
	$kind = $Matches[1]
	$part = $Matches[2]
	$op = if ($Matches['tags'] -match 'operation="(?<op>[^"]+)"') { $Matches['op'] } else { 'unknown' }

	if (-not $arms.ContainsKey($op)) {
		$arms[$op] = @{ DurationSum = 0.0; DurationCount = 0.0; InFlightSum = 0.0; InFlightCount = 0.0 }
	}

	$field = switch ("$kind/$part") {
		'duration/sum' { 'DurationSum' }
		'duration/count' { 'DurationCount' }
		'in_flight/sum' { 'InFlightSum' }
		'in_flight/count' { 'InFlightCount' }
	}

	$arms[$op][$field] = [double] $registry[$key]
}

# Resolved once, before any arm is judged, and deliberately NOT wrapped in a
# try/catch. If the interleaving set cannot be read the whole collection is
# void, because every attribution below depends on it and a failure here would
# otherwise surface as plausible-looking output rather than as an error.
$script:NonInterleavedOperations = Get-FanInNonInterleavedOperations

$armRows = foreach ($op in ($arms.Keys | Sort-Object)) {
	$a = $arms[$op]

	# Which members are non-interleaved is a property of ILatticeRegistry, not of
	# this run, so it is stated rather than inferred. The two GetAllTreeIdsAsync
	# overloads share one arm and are excluded from interleaving on CORRECTNESS
	# grounds - admitting a mutator mid-scan could reshape the tree under the
	# cursor and drop an already-registered id, which is a wrong answer rather
	# than a slow one. That exclusion is therefore not removable by tuning.
	#
	# The set is READ FROM THE SOURCE DECLARATION rather than restated here. A
	# copy would be correct on the day it was written and silently wrong the
	# first time a member gained or lost [AlwaysInterleave], and the failure
	# would be invisible: the collector would keep reporting a confident
	# Interleaved flag that no longer described the binary under test, and every
	# attribution downstream of it would inherit the error without any reading
	# looking wrong. Deriving it means a drifted list fails loudly at parse time
	# instead.
	$nonInterleaved = $op -in $script:NonInterleavedOperations

	$meanWidth = if ($a.InFlightCount -gt 0) { $a.InFlightSum / $a.InFlightCount } else { $null }

	# CAREFUL: the in-flight counter is GLOBAL across every registry arm, not
	# per-member. It answers "how many registry calls of any kind were in the
	# grain body when this one was admitted", which is the fan-in width worth
	# having - but it is NOT a measure of this member's own concurrency.
	#
	# So a non-interleaved member can and does report a width above zero: an
	# [AlwaysInterleave] read admitted earlier and now awaiting its downstream
	# hop is still counted as in flight, and Orleans can start a new turn once
	# the running turn yields at an await. Observed directly here - register is
	# non-interleaved yet reported a mean width of 1.44.
	#
	# An earlier revision of this script used width > 0 as evidence that a
	# member was genuinely interleaving, and that inference is simply wrong for
	# this counter. Interleaving is a property of the member's attribute, which
	# is known statically, so it is read from the declared list rather than
	# guessed from a reading that cannot carry it.
	$widthObserved = ($null -ne $meanWidth) -and ($meanWidth -gt 0)

	# The precondition. On a non-interleaved member a slow downstream hop yields
	# a SHORT per-call service time and a LOW admitted count - the same signature
	# admission failure produces - so the duration arm alone cannot separate
	# them, whatever the global width happened to be.
	$attributable = (-not $nonInterleaved) -and $widthObserved

	[pscustomobject] @{
		Operation            = $op
		Interleaved          = (-not $nonInterleaved)
		AdmittedCalls        = [long] $a.DurationCount
		MeanServiceMs        = if ($a.DurationCount -gt 0) { [math]::Round($a.DurationSum / $a.DurationCount, 4) } else { $null }
		MeanGlobalFanInWidth = if ($null -ne $meanWidth) { [math]::Round($meanWidth, 4) } else { $null }
		ConcurrencyObserved  = $widthObserved
		AttributionValid     = $attributable
		AttributionNote      = if ($attributable) {
			'interleaved member, and registry concurrency was observed, so the duration arm carries admission information'
		} elseif ($nonInterleaved) {
			'NOT ATTRIBUTABLE: member is not [AlwaysInterleave], so callers serialise behind the singleton turn token and a short service time cannot be distinguished from a call that was never admitted. The global width does not rescue this, because it does not measure THIS member concurrency.'
		} else {
			'NOT ATTRIBUTABLE: member is interleaved but no registry concurrency was observed at all, so the offered load did not exercise the seam and this is not a saturation measurement'
		}
		TotalServiceMs       = [math]::Round($a.DurationSum, 2)
	}
}

# ---- The ADMISSION GATE: the instruments that report on the bound itself ----
# These are not a second opinion on the registry arms above. They measure a
# different population: RegistryArms counts calls inside the registry singleton
# body summed over every caller in the cluster, including clients that never
# pass through a silo-side gate, whereas these count permits held by ONE silo's
# RegistryFanInGate. Only the second is bounded by GlobalMaxConcurrentReads.
#
# The original rig run reported a registry-side width of 1.9-3.2 against a bound
# of 16 and concluded the bound had room. That comparison is void: the two
# quantities were never the same number, and the gate had in fact not been
# entered at all. This block exists so the bound is read against its own
# instrument rather than against a neighbouring one.
function Get-AdmissionPair([string] $stem) {
	$sum = 0.0; $count = 0.0; $found = $false
	foreach ($key in $admission.Keys) {
		if ($key -match "^orleans_lattice_registry_admission_$([regex]::Escape($stem))_sum(\{.*\})?$") { $sum += [double] $admission[$key]; $found = $true }
		elseif ($key -match "^orleans_lattice_registry_admission_$([regex]::Escape($stem))_count(\{.*\})?$") { $count += [double] $admission[$key]; $found = $true }
	}
	return @{ Sum = $sum; Count = $count; Present = $found }
}

# Share of samples ABOVE a bucket boundary, read from the cumulative _bucket
# series. Returned as $null when no boundary at or below $le exists, rather than
# as zero: a missing boundary and a genuine zero share are different facts, and
# reporting the first as the second would recreate exactly the false-green this
# rig exists to stop.
function Get-AdmissionShareAbove([string] $stem, [double] $le, [double] $total) {
	if ($total -le 0) { return $null }
	$at = $null
	foreach ($key in $admission.Keys) {
		if ($key -notmatch "^orleans_lattice_registry_admission_$([regex]::Escape($stem))_bucket\{(?<tags>.*)\}$") { continue }
		if ($Matches['tags'] -notmatch 'le="(?<le>[^"]+)"') { continue }
		$bound = $Matches['le']
		if ($bound -eq '+Inf') { continue }
		$value = [double]::Parse($bound, [cultureinfo]::InvariantCulture)
		if ([math]::Abs($value - $le) -lt 1e-9) { $at = [double] $admission[$key] }
	}
	if ($null -eq $at) { return $null }
	return [math]::Round(($total - $at) / $total, 4)
}

$widthPair = Get-AdmissionPair 'in_flight'
$batchPair = Get-AdmissionPair 'batch_size'
$depthPair = Get-AdmissionPair 'queue_depth'
$waitPair = Get-AdmissionPair 'wait'

# Read from the source declaration for the same reason the interleaving set is:
# a restated constant is correct the day it is written and silently wrong the
# first time the bound is retuned, and the wrongness would surface as a
# confident-looking verdict rather than as an error.
$gateSource = Join-Path (Split-Path -Parent (Split-Path -Parent $RigRoot)) 'src/lattice/BPlusTree/RegistryFanInGate.cs'
if (-not (Test-Path $gateSource)) { throw "Cannot read the gate's constants: '$gateSource' is not present." }
$gateText = Get-Content -Raw -Path $gateSource
if ($gateText -notmatch 'GlobalMaxConcurrentReads\s*=\s*(?<n>\d+)') { throw "Cannot parse GlobalMaxConcurrentReads from '$gateSource'." }
$permits = [int] $Matches['n']
if ($gateText -notmatch 'MaxBatchSize\s*=\s*(?<n>\d+)') { throw "Cannot parse MaxBatchSize from '$gateSource'." }
$maxBatch = [int] $Matches['n']

$meanWidth = if ($widthPair.Count -gt 0) { [math]::Round($widthPair.Sum / $widthPair.Count, 4) } else { $null }
$meanBatch = if ($batchPair.Count -gt 0) { [math]::Round($batchPair.Sum / $batchPair.Count, 4) } else { $null }
$meanDepth = if ($depthPair.Count -gt 0) { [math]::Round($depthPair.Sum / $depthPair.Count, 4) } else { $null }
$meanWait = if ($waitPair.Count -gt 0) { [math]::Round($waitPair.Sum / $waitPair.Count, 4) } else { $null }

# The batched proportion, taken from the registry arms rather than from a
# histogram bucket. A dispatch of one id calls GetEntryAsync and a dispatch of
# two or more calls GetEntriesAsync, so the split between those two arms IS the
# batched proportion exactly, with no bucket-boundary approximation.
$singleDispatches = 0.0
$batchedDispatches = 0.0
foreach ($key in $registry.Keys) {
	if ($key -match '^orleans_lattice_registry_call_duration_count\{(?<tags>.*)\}$') {
		if ($Matches['tags'] -match 'operation="get_entry"') { $singleDispatches += [double] $registry[$key] }
		elseif ($Matches['tags'] -match 'operation="get_entries"') { $batchedDispatches += [double] $registry[$key] }
	}
}
$dispatchTotal = $singleDispatches + $batchedDispatches

# The gate is only entered by in-silo callers, so an absent instrument means the
# workload never reached it. That is reported as a distinct state from a low
# reading, because they license opposite conclusions: a low reading is evidence
# about the bound, an absent one is evidence about the rig.
$gateEntered = $depthPair.Present -and ($depthPair.Count -gt 0)
$offeredExceedsBound = $gateEntered -and ($null -ne $meanDepth) -and ($meanDepth -gt $permits)

$admissionGate = [pscustomobject] @{
	PermitCount            = $permits
	MaxBatchSize           = $maxBatch
	DownstreamKeyCeiling   = $permits * $maxBatch

	GateEntered            = $gateEntered
	Dispatches             = [long] $widthPair.Count
	Arrivals               = [long] $depthPair.Count

	MeanGateWidth          = $meanWidth
	ShareOfDispatchesAboveTen = Get-AdmissionShareAbove 'in_flight' 10 $widthPair.Count

	MeanOfferedQueueDepth  = $meanDepth
	ShareOfArrivalsAboveTen = Get-AdmissionShareAbove 'queue_depth' 10 $depthPair.Count

	MeanAdmissionWaitMs    = $meanWait
	MeanBatchSize          = $meanBatch

	SingleKeyDispatches    = [long] $singleDispatches
	BatchedDispatches      = [long] $batchedDispatches
	BatchedProportion      = if ($dispatchTotal -gt 0) { [math]::Round($batchedDispatches / $dispatchTotal, 4) } else { $null }

	# The verdict, and the only field here that should be read first.
	RegimeReached          = $offeredExceedsBound
	RegimeNote             = if (-not $gateEntered) {
		'NOT MEASURED: no arrival ever reached the silo-side gate, so this run says nothing whatever about the bound. The gate is entered only by in-silo callers (LatticeOptionsResolver and the per-tree background services); an Orleans client addressing ILatticeRegistry directly - which is what the probe arm does - passes it entirely. Use the fanout arm.'
	} elseif (-not $offeredExceedsBound) {
		"NOT MEASURED: offered fan-in (mean queue depth $meanDepth) did not exceed the bound of $permits, so the gate never queued. Every other admission figure here is therefore at its STRUCTURAL FLOOR, not at a comfortable level: admission dispatches synchronously on the arriving thread whenever a permit is free, so a sub-millisecond wait, a width near 1 and a batch near 1 are what ABSENT DEMAND looks like and are indistinguishable by eye from a bound with headroom. An instrument that cannot reach the failing regime yields no evidence, not weak evidence. Raise --fanout-width."
	} else {
		"MEASURED: offered fan-in (mean queue depth $meanDepth) exceeded the bound of $permits, so admission genuinely queued and the width, wait and batch-size readings above are evidence about the bound rather than about the offered load."
	}

	BucketNote             = 'Share-above figures are read from cumulative _bucket series, so they are only available at the exporter''s configured boundaries; a boundary that does not exist is reported as null rather than as zero, because "no such boundary" and "a zero share" license opposite conclusions. Means are over SAMPLES (dispatches and arrivals), not over time, so idle time contributes nothing to them - unlike the registry-side window means above.'
}

# ---- Result ----
$result = [pscustomobject] @{
	Label            = $Label
	ContainerId      = $inspect.Id
	ContainerName    = $inspect.Name.TrimStart('/')
	Image            = $inspect.Config.Image
	StartedAtUtc     = $startedAtUtc.ToString('o')
	ReadyAtUtc       = $ReadyAtUtc.ToString('o')
	ReadingAtUtc     = $readingAtUtc.ToString('o')
	RestartCount     = $restartCount
	WindowSeconds    = $WindowSeconds
	BucketSeconds    = $BucketSeconds

	# Every reading carries its own provenance. StartedAt is the authority for
	# the restart-boundary check; RestartCount is recorded but is NOT evidence,
	# because a compose recreation resets it to zero on a fresh container.
	ReadingSpansRestart = (Test-FanInReadingSpansRestart `
		-BaselineTimeUtc $ReadyAtUtc `
		-StartedAtUtc $startedAtUtc `
		-RestartCount $restartCount)

	TimeoutCensus    = $census
	RegistryArms     = @($armRows)
	AdmissionGate    = $admissionGate
	AdmissionSeries  = $admission
	StorageSeries    = $storage

	# Co-tenancy, observed rather than assumed. Taken at the end of the window;
	# run-cell takes the opening sample and passes it through, so the pair
	# brackets the window.
	HostLoadAtEnd    = @(Get-FanInHostLoad)

	# A scheduled compaction on a co-tenant rewrites several hundred MB of WAL
	# in a burst. A window straddling it will show a disk-I/O spike that does not
	# belong to this system, so windows are timestamped to let that be excluded
	# afterwards rather than contorting the schedule around a rough estimate.
	CoTenancyNote    = 'Windows are timestamped so a co-tenant compaction burst can be excluded afterwards. Treat an anomalous window against a materially different load profile as suspect until cleared, rather than averaging it in.'

	Method           = 'Timeouts bucketed by log timestamp, never by counter scrape. A single scrape of a cumulative counter reads as an active fault and a short delta of the same counter reads as healthy; both are wrong, in opposite directions.'
}

$json = $result | ConvertTo-Json -Depth 8
if ($OutputPath) {
	$dir = Split-Path -Parent $OutputPath
	if ($dir -and -not (Test-Path $dir)) { $null = New-Item -ItemType Directory -Path $dir -Force }
	Set-Content -Path $OutputPath -Value $json -Encoding utf8
	Write-Host "Window written to $OutputPath" -ForegroundColor Green
}

$json
