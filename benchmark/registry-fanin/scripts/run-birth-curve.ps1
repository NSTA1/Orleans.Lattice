<#
.SYNOPSIS
	Cold-start birth curve: when do per-tree background services appear, and
	does the registry saturate while they do?

.DESCRIPTION
	This script exists because of a specific measurement error, and the error is
	worth stating so it is not repeated.

	The per-tree background services are NOT started eagerly at silo boot. Each
	is birthed by an Orleans REMINDER that LatticeGrain registers the first time
	a tree is touched (HotShardMonitorGrain registers with dueTime 1 minute,
	period 1 minute). Reminders are persistent, so they survive into the next
	process lifetime and re-fire after a cold start without anything touching
	the tree.

	Two consequences follow, and both invalidate a shorter or warmer
	measurement:

	1. A window of a couple of minutes CANNOT see this. The reminder service has
	   to start, load the reminder table, and reach the first due tick. A
	   two-minute window measured against a K=20 estate reported the six
	   services at a count of 5 - the host's own trees - and zero timeouts, and
	   that reads exactly like "the rig cannot reproduce the storm". It was a
	   window-length artefact, not a rig defect.

	2. A steady-state probe cannot see it either, at any K. Once the birth burst
	   has passed, the reminders are spread across their period and the fan-in
	   that the burst produced is gone. Measuring a warm container therefore
	   samples the one regime that never failed.

	So the measurement has to be a cold start, over a window long enough to
	contain the burst, sampled as a CURVE rather than as a single reading at the
	end. A single end-of-window reading would show the services present and the
	registry healthy, which is true and tells you nothing about what happened at
	minute five.

	The volume is deliberately NOT reset between the estate build and the cold
	start. Resetting would wipe the reminder table, which is the very thing that
	carries the fan-in across the restart boundary - the restart would then
	measure an estate with no background services at all.
#>
[CmdletBinding()]
param(
	[int] $Trees = 40,
	[ValidateRange(60, 3600)]
	[int] $WindowSeconds = 900,
	[ValidateRange(10, 300)]
	[int] $SampleSeconds = 30,
	[string] $Label = '',
	[switch] $SkipBuild
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

. (Join-Path $PSScriptRoot '_fanin-helpers.ps1')

$config = Get-FanInConfig -ScriptRoot $PSScriptRoot
$null = Assert-FanInIsolation -Config $config

$rig = Join-Path $PSScriptRoot 'rig.ps1'
# Derived the same way collect-window derives it, so the two cannot disagree
# about which container a reading came from.
$script:ContainerName = "$($config.ProjectName)-repocontext-1"
if (-not $Label) { $Label = "birth-K$Trees" }
$resultsDir = Join-Path (Split-Path -Parent $PSScriptRoot) 'results'
$null = New-Item -ItemType Directory -Force -Path $resultsDir

# The six services the live estate shows at one row per tree. Named explicitly
# rather than discovered, so a service that STOPS appearing is visible as a
# missing row instead of silently dropping out of the table.
$services = @('hot-shard-monitor', 'shard-healing', 'tree-merge', 'tree-reshard', 'tree-resize', 'tree-snapshot')

function Get-RegistryArmCounts {
	$raw = @(& curl.exe -s "http://127.0.0.1:$($config.HostPort)/metrics" | ForEach-Object { "$_" })
	$series = ConvertFrom-FanInPrometheusText -Lines $raw -NameFilter 'orleans_lattice_registry_call_duration_count'

	$out = [ordered]@{}
	$total = 0
	foreach ($key in $series.Keys) {
		if ($key -match 'operation="([^"]+)"') {
			$out[$Matches[1]] = [int] $series[$key]
			$total += [int] $series[$key]
		}
	}
	$out['_total'] = $total
	$out
}

function Get-ServiceCounts {
	$raw = @(& curl.exe -s "http://127.0.0.1:$($config.HostPort)/metrics" | ForEach-Object { "$_" })
	$series = ConvertFrom-FanInPrometheusText -Lines $raw -NameFilter 'orleans_storage_read_latency_count'

	$out = [ordered]@{}
	foreach ($svc in $services) {
		$key = $series.Keys | Where-Object { $_ -like "*state_name=`"$svc`"*" } | Select-Object -First 1
		$out[$svc] = if ($key) { [int] $series[$key] } else { 0 }
	}
	$out
}

if (-not $SkipBuild) {
	Write-Host "Building estate: $Trees trees (shallow - fan-in is per-tree, not per-leaf)" -ForegroundColor Cyan
	& $rig reset | Out-Null
	& $rig up | Out-Null
	& $rig ready | Out-Null
	& $rig driver -DriverArgs "create --trees $Trees --leaves-per-tree 1 --keys-per-leaf 1"
}

Write-Host 'Pre-restart service counts (reminders now registered):' -ForegroundColor DarkGray
(Get-ServiceCounts).GetEnumerator() | ForEach-Object { Write-Host "  $($_.Key) = $($_.Value)" -ForegroundColor DarkGray }

# THE COLD START. down/up recreates the container but keeps the volume, so the
# reminder table survives and the birth burst lands in the new process.
Write-Host 'Cold start ...' -ForegroundColor Yellow
& $rig down | Out-Null
& $rig up | Out-Null
$ready = (& $rig ready | Out-String) | ConvertFrom-Json

$readyAt = [datetime]::SpecifyKind(
	[datetime]::Parse($ready.ReadyAtUtc, [cultureinfo]::InvariantCulture),
	[System.DateTimeKind]::Utc)

Write-Host "ready at $($readyAt.ToString('o')); sampling every ${SampleSeconds}s for ${WindowSeconds}s" -ForegroundColor Green

$samples = [System.Collections.Generic.List[object]]::new()
# Initialised before the loop rather than lazily, so the first sample records a
# delta of zero instead of failing under StrictMode. The first reading has no
# predecessor and its delta is meaningless either way.
$script:PrevArmTotal = $null
$script:FinalCensus = $null
$deadline = (Get-Date).ToUniversalTime().AddSeconds($WindowSeconds)

while ((Get-Date).ToUniversalTime() -lt $deadline) {
	$now = (Get-Date).ToUniversalTime()
	$counts = Get-ServiceCounts
	$arms = Get-RegistryArmCounts

	# Timeouts are counted from the LOG, bucketed by the timestamp each record
	# carries, never from a counter scrape. A cumulative counter read once looks
	# like an active fault and read as a short delta looks healthy; only the
	# timestamped records say when anything actually happened.
	#
	# --since is pinned to the cold start rather than to a relative window, so a
	# record cannot drift out of the census between samples.
	$logLines = @(docker logs $script:ContainerName --since $readyAt.ToString('o') 2>&1 | ForEach-Object { "$_" })
	$census = Measure-FanInTimeoutCensus -Lines $logLines -ReadyAtUtc $readyAt -WindowSeconds $WindowSeconds -BucketSeconds $SampleSeconds

	# Host CPU and memory, captured PER SAMPLE rather than once at the end.
	#
	# This is the variable that actually predicted the outcome, and capturing it
	# only at the end nearly cost the whole result. Across four K=80 cold starts
	# the offered registry load was constant to within 10% (1021-1132 calls per
	# 30 s) and exactly one run stormed - the one where a NEIGHBOURING container
	# was burning 5.96 CPU cores and sitting at 99% of its memory cap, while the
	# rig's own silo was idle at 0.27 cores. A rig that records only its own K
	# and its own load cannot see that, and would report a scaling law where the
	# real driver is contention for the host the silo is scheduled on.
	$hostLoad = Get-FanInHostLoad
	$hostTotalCores = 0.0
	$selfCores = 0.0
	foreach ($h in $hostLoad) {
		$hostTotalCores += [double] $h.Cores
		if ($h.Name -eq $script:ContainerName) { $selfCores = [double] $h.Cores }
	}

	$row = [ordered]@{
		OffsetSeconds  = [int] ($now - $readyAt).TotalSeconds
		AtUtc          = $now.ToString('o')
		HostCores      = [math]::Round($hostTotalCores, 2)
		SelfCores      = [math]::Round($selfCores, 2)
		NeighbourCores = [math]::Round($hostTotalCores - $selfCores, 2)
		TimeoutsTotal  = $census.Total
		NeverServed    = $census.NeverServed
		ServedSlowly   = $census.ServedSlowly
	}
	$script:FinalCensus = $census
	foreach ($svc in $services) { $row[$svc] = $counts[$svc] }

	# Registry call VOLUME per sample, recorded as a per-interval delta rather
	# than as the cumulative counter. This began as the discriminator between an
	# ACTIVATION WAVE (a single spike decaying to near zero, because grains
	# activate once and are then retained) and TICK-DRIVEN FAN-IN (a pulse every
	# 60 s, because the reminders carry a one-minute PERIOD and not a one-shot
	# due time).
	#
	# MEASURED: neither. The delta rises during birth and then holds a flat
	# plateau - ~550 per 30 s at K=45, ~1050 at K=85 - with no decay and no 60 s
	# cadence. Reminders are STAGGERED across their period rather than
	# synchronised, so tick-driven registry work arrives as constant background
	# load. Sampling at 30 s against a 60 s period would render a true pulse as
	# unmistakable alternation, and there is none.
	#
	# The delta is still recorded, for two reasons that outlived the original
	# question. It scales linearly in tree count (~0.41 calls/s/tree at both K),
	# which is what lets a tree count be converted into the registry load that
	# actually drives the fault. And it DIPS during the storm window and only
	# reaches the full plateau once the window closes, which is the signature of
	# genuine saturation rather than of extra offered load.
	$armTotal = [int] $arms['_total']
	$row['RegistryCallsTotal'] = $armTotal
	$row['RegistryCallsDelta'] = if ($null -eq $script:PrevArmTotal) { 0 } else { $armTotal - $script:PrevArmTotal }
	$script:PrevArmTotal = $armTotal
	foreach ($op in @('resolve', 'get_entry', 'get_shard_map', 'get_all_tree_ids', 'register')) {
		$row["arm_$op"] = if ($arms.Contains($op)) { [int] $arms[$op] } else { 0 }
	}

	$samples.Add([pscustomobject] $row)

	$svcSummary = ($services | ForEach-Object { $counts[$_] }) -join '/'
	Write-Host ("  t+{0,4}s  services {1}  regcalls +{2,-6} timeouts {3}" -f $row.OffsetSeconds, $svcSummary, $row.RegistryCallsDelta, $census.Total)

	Start-Sleep -Seconds $SampleSeconds
}

$outPath = Join-Path $resultsDir "$Label.json"
[pscustomobject]@{
	Label         = $Label
	Trees         = $Trees
	ReadyAtUtc    = $readyAt.ToString('o')
	WindowSeconds = $WindowSeconds
	Services      = $services
	Samples       = $samples
	# The full final census, not just the running scalars. ByGrain / ByMember
	# are what separate a registry storm from its collateral damage, and
	# NeverServed vs ServedSlowly is the Phase 2 mechanism discriminator, so
	# discarding them would leave the artefact unable to answer the question the
	# run was performed to answer.
	Census        = $script:FinalCensus
	HostLoadAtEnd = Get-FanInHostLoad
	Method        = 'Cold start with the reminder table preserved across the restart, sampled as a curve. Per-tree background services are reminder-birthed (dueTime 1 min), so a short window or a warm probe samples the regime that never failed.'
} | ConvertTo-Json -Depth 8 | Set-Content -Path $outPath

Write-Host ''
$samples | Format-Table -AutoSize
Write-Host "-> $outPath" -ForegroundColor Green
