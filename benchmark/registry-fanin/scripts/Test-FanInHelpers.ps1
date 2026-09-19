#!/usr/bin/env pwsh
<#
.SYNOPSIS
	Regression suite for _fanin-helpers.ps1.

.DESCRIPTION
	Deliberately a standalone script and NOT an NUnit fixture. The system under
	measurement is currently broken, and the task that produced this rig is a
	measurement task: an asserting fixture that encoded the current behaviour
	would land red in CI, and one that encoded the desired behaviour would land
	red too. Neither is useful until the fix exists.

	What IS worth guarding now is the rig's own logic, because the rig is what
	the conclusion will rest on. Every function exercised here is pure: no
	Docker, no running silo, no wall-clock dependence. Run it directly:

	    pwsh benchmark/registry-fanin/scripts/Test-FanInHelpers.ps1

	It exits non-zero on any failure so it can be wired into a pre-flight step
	without becoming a CI gate.
#>

[CmdletBinding()]
param()

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

. (Join-Path $PSScriptRoot '_fanin-helpers.ps1')

$script:Passed = 0
$script:Failed = 0

function Test-Case {
	param([string] $Name, [scriptblock] $Body)

	try {
		& $Body
		$script:Passed++
		Write-Host "  PASS  $Name" -ForegroundColor Green
	}
	catch {
		$script:Failed++
		Write-Host "  FAIL  $Name" -ForegroundColor Red
		Write-Host "        $($_.Exception.Message)" -ForegroundColor DarkRed
	}
}

function Assert-True {
	param([bool] $Condition, [string] $Message)
	if (-not $Condition) { throw $Message }
}

function Assert-Equal {
	param($Expected, $Actual, [string] $Message)
	if ("$Expected" -ne "$Actual") { throw "$Message (expected '$Expected', got '$Actual')" }
}

function Assert-Throws {
	param([scriptblock] $Body, [string] $Message)
	try { & $Body }
	catch { return }
	throw "$Message (no exception was thrown)"
}

function New-TestConfig {
	return @{
		ProjectName             = 'lattice-registry-fanin'
		WorkVolume              = 'lattice-registry-fanin-work'
		HfCacheVolume           = 'lattice-registry-fanin-hf'
		HostPort                = 18081
		McpImage                = 'repocontext-mcp:fanin-rig'
		EmbedderImage           = 'rc-embedder:fanin-rig'
		DriverImage             = 'repocontext-fanin-driver:rig'
		SourceEmbedderImage     = 'repocontextcontainer-embedder:latest'
		RequiredProjectPrefix   = 'lattice-registry-fanin'
		RequiredVolumePrefix    = 'lattice-registry-fanin'
		RequiredImageTag        = 'fanin-rig'
		ForbiddenProjects       = @('repocontextcontainer', 'lattice-coldstart')
		ForbiddenVolumePrefixes = @('repocontextcontainer_')
		ForbiddenVolumes        = @('repocontextcontainer_repocontext-data', 'repocontextcontainer_hf-cache')
		ForbiddenImages         = @('repocontext-mcp:local', 'repocontextcontainer-repocontext:latest', 'repocontextcontainer-embedder:latest')
		ForbiddenPorts          = @(8080, 18080)
		ForbiddenContainerNames = @('repocontextcontainer-repocontext-1', 'repocontextcontainer-repocontext', 'repocontextcontainer-embedder-1')
	}
}

Write-Host ''
Write-Host 'Image reference normalisation' -ForegroundColor Cyan

Test-Case 'untagged image gains :latest' {
	Assert-Equal 'foo:latest' (ConvertTo-FanInNormalisedImage -Image 'foo') 'untagged'
}

Test-Case 'registry port is not mistaken for a tag' {
	Assert-Equal 'host:5000/foo:latest' (ConvertTo-FanInNormalisedImage -Image 'host:5000/foo') 'registry port'
}

Test-Case 'tag extraction' {
	Assert-Equal 'fanin-rig' (Get-FanInImageTag -Image 'repocontext-mcp:fanin-rig') 'tag'
}

Write-Host ''
Write-Host 'Isolation guard' -ForegroundColor Cyan

Test-Case 'the committed configuration passes' {
	$config = Get-FanInConfig -ScriptRoot $PSScriptRoot -ParametersFile (Join-Path $PSScriptRoot 'parameters.ps1')
	$null = Assert-FanInIsolation -Config $config
}

Test-Case 'the live compose project is refused' {
	$config = New-TestConfig
	$config.ProjectName = 'repocontextcontainer'
	Assert-Throws { Assert-FanInIsolation -Config $config } 'live project'
}

Test-Case 'the live volume is refused' {
	$config = New-TestConfig
	$config.WorkVolume = 'repocontextcontainer_repocontext-data'
	Assert-Throws { Assert-FanInIsolation -Config $config } 'live volume'
}

Test-Case 'the live host port is refused' {
	$config = New-TestConfig
	$config.HostPort = 8080
	Assert-Throws { Assert-FanInIsolation -Config $config } 'live port'
}

Test-Case "the cold-start rig's port is refused too" {
	$config = New-TestConfig
	$config.HostPort = 18080
	Assert-Throws { Assert-FanInIsolation -Config $config } 'coldstart port'
}

Test-Case 'a live image tag is refused as the silo image' {
	$config = New-TestConfig
	$config.McpImage = 'repocontext-mcp:local'
	Assert-Throws { Assert-FanInIsolation -Config $config } 'live image'
}

Test-Case 'an image without the rig tag is refused' {
	$config = New-TestConfig
	$config.McpImage = 'repocontext-mcp:something-else'
	Assert-Throws { Assert-FanInIsolation -Config $config } 'untagged image'
}

Test-Case 'a missing required key is a refusal, not a default' {
	$config = New-TestConfig
	$config.Remove('ForbiddenContainerNames')
	Assert-Throws { Assert-FanInIsolation -Config $config } 'missing key'
}

Test-Case 'an empty required key is a refusal' {
	$config = New-TestConfig
	$config.WorkVolume = '   '
	Assert-Throws { Assert-FanInIsolation -Config $config } 'empty key'
}

Test-Case 'the work and cache volumes must be distinct' {
	$config = New-TestConfig
	$config.HfCacheVolume = $config.WorkVolume
	Assert-Throws { Assert-FanInIsolation -Config $config } 'duplicate volume'
}

Write-Host ''
Write-Host 'Container-target guard (the --network container: path)' -ForegroundColor Cyan

Test-Case 'a rig container is accepted' {
	$config = New-TestConfig
	$null = Assert-FanInContainerTarget -Config $config `
		-ContainerId 'abc123' -ContainerName 'lattice-registry-fanin-repocontext-1' `
		-ComposeProject 'lattice-registry-fanin'
}

Test-Case 'the protected container is refused by name' {
	$config = New-TestConfig
	Assert-Throws {
		Assert-FanInContainerTarget -Config $config `
			-ContainerId 'deadbeef' -ContainerName 'repocontextcontainer-repocontext-1' `
			-ComposeProject 'repocontextcontainer'
	} 'protected container'
}

Test-Case 'the live name stem is refused even for an unlisted container' {
	$config = New-TestConfig
	Assert-Throws {
		Assert-FanInContainerTarget -Config $config `
			-ContainerId 'deadbeef' -ContainerName 'repocontextcontainer-something-new-7' `
			-ComposeProject 'lattice-registry-fanin'
	} 'live stem'
}

Test-Case 'an unlabelled container is refused rather than assumed safe' {
	$config = New-TestConfig
	Assert-Throws {
		Assert-FanInContainerTarget -Config $config -ContainerId 'abc123' -ContainerName 'something' -ComposeProject ''
	} 'unlabelled container'
}

Test-Case 'a foreign compose project is refused' {
	$config = New-TestConfig
	Assert-Throws {
		Assert-FanInContainerTarget -Config $config -ContainerId 'abc123' -ContainerName 'other-1' -ComposeProject 'lattice-coldstart'
	} 'foreign project'
}

Test-Case 'no id and no name is refused' {
	$config = New-TestConfig
	Assert-Throws {
		Assert-FanInContainerTarget -Config $config -ContainerId '' -ContainerName '' -ComposeProject 'lattice-registry-fanin'
	} 'empty target'
}

Write-Host ''
Write-Host 'Restart-boundary detection' -ForegroundColor Cyan

Test-Case 'RestartCount=0 does NOT prove no restart when StartedAt moved' {
	$baseline = [datetime]::Parse('2026-09-19T10:00:00Z').ToUniversalTime()
	$started = [datetime]::Parse('2026-09-19T10:05:00Z').ToUniversalTime()
	$result = Test-FanInReadingSpansRestart -BaselineTimeUtc $baseline -StartedAtUtc $started -RestartCount 0
	Assert-True $result.SpansRestart 'a compose recreation must be detected from StartedAt alone'
	Assert-True $result.StartedAfterBaseline 'StartedAfterBaseline'
}

Test-Case 'a non-zero RestartCount is detected even when StartedAt is older' {
	$baseline = [datetime]::Parse('2026-09-19T10:00:00Z').ToUniversalTime()
	$started = [datetime]::Parse('2026-09-19T09:00:00Z').ToUniversalTime()
	$result = Test-FanInReadingSpansRestart -BaselineTimeUtc $baseline -StartedAtUtc $started -RestartCount 2
	Assert-True $result.SpansRestart 'in-place restart'
}

Test-Case 'a same-lifetime reading is clean' {
	$baseline = [datetime]::Parse('2026-09-19T10:00:00Z').ToUniversalTime()
	$started = [datetime]::Parse('2026-09-19T09:00:00Z').ToUniversalTime()
	$result = Test-FanInReadingSpansRestart -BaselineTimeUtc $baseline -StartedAtUtc $started -RestartCount 0
	Assert-True (-not $result.SpansRestart) 'clean reading'
}

Write-Host ''
Write-Host 'Counter re-baselining' -ForegroundColor Cyan

Test-Case 'a same-lifetime delta is computed' {
	$started = [datetime]::Parse('2026-09-19T09:00:00Z').ToUniversalTime()
	$baseline = New-FanInCounterBaseline -Counters @{ 'a' = 10 } -StartedAtUtc $started -TakenAtUtc ([datetime]::Parse('2026-09-19T10:00:00Z').ToUniversalTime())
	$delta = Compare-FanInCounterReading -Baseline $baseline -Counters @{ 'a' = 25 } -StartedAtUtc $started
	Assert-Equal 15 $delta.Delta['a'] 'delta'
}

Test-Case 'a delta across a restart boundary is REFUSED, not clamped' {
	$started = [datetime]::Parse('2026-09-19T09:00:00Z').ToUniversalTime()
	$baseline = New-FanInCounterBaseline -Counters @{ 'a' = 10 } -StartedAtUtc $started -TakenAtUtc ([datetime]::Parse('2026-09-19T10:00:00Z').ToUniversalTime())
	Assert-Throws {
		Compare-FanInCounterReading -Baseline $baseline -Counters @{ 'a' = 3 } -StartedAtUtc ([datetime]::Parse('2026-09-19T10:30:00Z').ToUniversalTime())
	} 'restart boundary'
}

Test-Case 'a counter going backwards without a detected restart is REFUSED' {
	$started = [datetime]::Parse('2026-09-19T09:00:00Z').ToUniversalTime()
	$baseline = New-FanInCounterBaseline -Counters @{ 'a' = 10 } -StartedAtUtc $started -TakenAtUtc ([datetime]::Parse('2026-09-19T10:00:00Z').ToUniversalTime())
	Assert-Throws {
		Compare-FanInCounterReading -Baseline $baseline -Counters @{ 'a' = 3 } -StartedAtUtc $started
	} 'backwards counter'
}

Write-Host ''
Write-Host 'Timestamped log bucketing' -ForegroundColor Cyan

$script:SampleLog = @(
	'2026-09-19T10:00:00.000Z info: Orleans.Lattice.Host[0]',
	'      ready',
	'2026-09-19T10:05:10.000Z fail: Orleans.Lattice.ShardHealingOrchestrator[0]',
	"      System.TimeoutException: Response did not arrive on time in '00:00:30' for message: 'Request latticeregistry/_lattice_trees ILatticeRegistry.ResolveAsync(...)'. About to break its promise.",
	'2026-09-19T10:05:20.000Z fail: Orleans.Lattice.HotShardMonitor[0]',
	"      System.TimeoutException: Response did not arrive on time in '00:00:30' for message: 'Request latticeregistry/_lattice_trees ILatticeRegistry.GetEntryAsync(...)'. Status: '[ok]'. About to break its promise.",
	'2026-09-19T10:06:05.000Z fail: Orleans.Lattice.ViewMaintainer[0]',
	"      System.TimeoutException: Response did not arrive on time in '00:00:30' for message: 'Request latticeregistry/_lattice_trees ILatticeRegistry.GetShardMapAsync(...)'. About to break its promise.",
	'2026-09-19T10:40:00.000Z info: Orleans.Lattice.Host[0]',
	'      still fine',
	'2026-09-19T11:00:00.000Z fail: Orleans.Lattice.Late[0]',
	"      System.TimeoutException: Response did not arrive on time in '00:00:30' for message: 'Request latticeregistry/_lattice_trees ILatticeRegistry.ResolveAsync(...)'. About to break its promise."
)

Test-Case 'records split on the header, continuation lines attach to the record' {
	$records = Split-FanInLogRecords -Lines $script:SampleLog
	Assert-Equal 6 $records.Count 'record count'
	Assert-Equal 'Orleans.Lattice.Host' $records[0].Category 'first category'
	Assert-True ($records[1].Body -match 'TimeoutException') 'body attached'
}

Test-Case 'timeouts bucket by their OWN timestamp, relative to ready' {
	$ready = [datetime]::Parse('2026-09-19T10:00:00Z').ToUniversalTime()
	$census = Measure-FanInTimeoutCensus -Lines $script:SampleLog -ReadyAtUtc $ready -BucketSeconds 60 -WindowSeconds 900
	Assert-Equal 3 $census.Total 'in-window total'
	Assert-Equal 1 $census.TotalOutsideWindow 'the 11:00 timeout falls outside a 15-minute window'
	Assert-Equal 310 $census.FirstOffsetSeconds 'first offset'
	# Two distinct minutes carry the three events: 10:05 has two, 10:06 has one.
	Assert-Equal 2 $census.Buckets.Count 'bucket count'
	Assert-Equal 2 $census.Buckets[0].Count 'first bucket count'
}

Test-Case 'the status clause discriminates never-served from served-slowly' {
	$ready = [datetime]::Parse('2026-09-19T10:00:00Z').ToUniversalTime()
	$census = Measure-FanInTimeoutCensus -Lines $script:SampleLog -ReadyAtUtc $ready -BucketSeconds 60 -WindowSeconds 900
	Assert-Equal 2 $census.NeverServed 'never served'
	Assert-Equal 1 $census.ServedSlowly 'served slowly'
}

Test-Case 'the target grain and member are extracted' {
	$ready = [datetime]::Parse('2026-09-19T10:00:00Z').ToUniversalTime()
	$census = Measure-FanInTimeoutCensus -Lines $script:SampleLog -ReadyAtUtc $ready -BucketSeconds 60 -WindowSeconds 900
	Assert-Equal 'latticeregistry/_lattice_trees' $census.ByGrain[0].Grain 'grain'
	Assert-True ($census.ByMember.Count -eq 3) 'three distinct members'
}

Test-Case 'the per-caller breakdown survives' {
	$ready = [datetime]::Parse('2026-09-19T10:00:00Z').ToUniversalTime()
	$census = Measure-FanInTimeoutCensus -Lines $script:SampleLog -ReadyAtUtc $ready -BucketSeconds 60 -WindowSeconds 900
	Assert-Equal 3 $census.ByCaller.Count 'three distinct callers'
}

Test-Case 'an empty log yields a well-formed empty census, not an error' {
	$ready = [datetime]::Parse('2026-09-19T10:00:00Z').ToUniversalTime()
	$census = Measure-FanInTimeoutCensus -Lines @() -ReadyAtUtc $ready
	Assert-Equal 0 $census.Total 'empty total'
	Assert-Equal 0 $census.Buckets.Count 'empty buckets'
}

Write-Host ''
Write-Host 'Distribution statistics' -ForegroundColor Cyan

Test-Case 'nearest-rank percentiles' {
	$values = [double[]] @(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
	Assert-Equal 5 (Get-FanInPercentile -Values $values -Percentile 50) 'p50'
	Assert-Equal 10 (Get-FanInPercentile -Values $values -Percentile 100) 'p100'
	Assert-Equal 1 (Get-FanInPercentile -Values $values -Percentile 1) 'p1'
}

Test-Case 'an empty sample yields nulls, not zeros' {
	$d = Get-FanInDistribution -Values @()
	Assert-Equal 0 $d.N 'n'
	Assert-True ($null -eq $d.P50) 'p50 is null rather than 0, so absent is distinguishable from zero'
}

Test-Case 'dispersion reports max/min, the statistic that makes 5.7 comparable' {
	$d = Get-FanInDispersion -Values ([double[]] @(18, 103))
	Assert-Equal 2 $d.N 'n'
	Assert-True ([math]::Abs($d.MaxOverMin - (103.0 / 18.0)) -lt 1e-9) 'max/min'
}

Test-Case 'a single replicate reports no standard deviation rather than zero' {
	$d = Get-FanInDispersion -Values ([double[]] @(42))
	Assert-True ($null -eq $d.StdDev) 'n=1 has no spread to report'
}

Write-Host ''
Write-Host 'Prometheus scrape parsing' -ForegroundColor Cyan

Test-Case 'labelled series are parsed and filtered' {
	$lines = @(
		'# HELP orleans_lattice_registry_call_duration_milliseconds_count help',
		'# TYPE orleans_lattice_registry_call_duration_milliseconds_count counter',
		'orleans_lattice_registry_call_duration_milliseconds_count{operation="resolve"} 42',
		'orleans_lattice_registry_call_duration_milliseconds_count{operation="get_entry"} 7',
		'some_other_metric 99'
	)
	$counters = ConvertFrom-FanInPrometheusText -Lines $lines -NameFilter 'orleans_lattice_registry'
	Assert-Equal 2 $counters.Count 'filtered count'
	Assert-Equal 42 $counters['orleans_lattice_registry_call_duration_milliseconds_count{operation="resolve"}'] 'value'
}

Write-Host ''
if ($script:Failed -gt 0) {
	Write-Host "FAILED: $($script:Failed) failed, $($script:Passed) passed." -ForegroundColor Red
	exit 1
}

Write-Host "OK: $($script:Passed) passed." -ForegroundColor Green
exit 0
