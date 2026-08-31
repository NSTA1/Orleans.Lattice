#!/usr/bin/env pwsh
<#
.SYNOPSIS
	Watches an ALREADY-RUNNING rig box heal itself while it serves traffic, and
	records what it saw as a timeseries.

.DESCRIPTION
	The cold-start cohort (run-cohort.ps1) answers "how long to the first
	semantic query". This script answers the other half of epic #1830's promise:
	does an upgraded box repair its own durable state, ON ITS OWN, WHILE STILL
	ANSWERING, and does it tell the truth about its state while it does?

	It never starts, stops or recreates anything. It attaches to the rig stack
	already brought up by `run-cohort.ps1 -KeepUp` (or `rig.ps1 up`) and, on a
	fixed cadence, records one sample of:

	  * liveOk / readyOk        - the two health probes. `/health/ready` is the
	                              CONJUNCTION of the lifecycle check and
	                              vector-plane retrieval readiness (S7/S12), so
	                              a box that cannot serve semantic retrieval must
	                              not report ready. A sample where readyOk is
	                              true while retrievalPath is a keyword value is
	                              a readiness LIE, and is counted as one.
	  * searchOk / searchMs / mode / retrievalPath
	                            - a real query on every sample. This is both the
	                              traffic the box is required to keep serving and
	                              the honesty check on how it describes itself.
	  * fileCount / vectorCount - from repocontext_list_repos, sampled
	                              throughout so data loss during a heal would
	                              show up as a step DOWN rather than having to be
	                              inferred from a before/after pair.
	  * healing / index / WAL log counters, tallied from the container's own log
	                              lines since the previous sample.

	WHY THE LOG, AND NOT A METRIC. The RepoContext container exposes no metrics
	endpoint, deliberately (epic decision D5: every mechanism is default-on and
	the compose file gains no knobs). Healing progress is therefore read from
	the orchestrator's own Information-level lines, and the durable outcome is
	read offline by inspect-state.ps1. Both are properties of what the box
	actually did, not of what an exporter happened to sample.

.PARAMETER DurationMinutes
	How long to observe. The script exits when the window elapses.

.PARAMETER IntervalSeconds
	Sampling cadence.

.PARAMETER OutputPath
	Where to write the timeseries JSON. Defaults to a stamped file under
	benchmark/.run/coldstart-rig/observations/.

.EXAMPLE
	./observe-healing.ps1 -DurationMinutes 120 -IntervalSeconds 30
#>
[CmdletBinding()]
param(
	[int] $DurationMinutes = 60,
	[int] $IntervalSeconds = 30,
	[string] $ParametersFile,
	[string] $RepoId,
	[string] $SemanticQuery,
	[string] $OutputPath,
	[string] $Label
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$here = Split-Path -Parent $MyInvocation.MyCommand.Path
. (Join-Path $here '_rig-docker.ps1')

$override = @{}
foreach ($key in @('RepoId', 'SemanticQuery')) {
	if ($PSBoundParameters.ContainsKey($key)) { $override[$key] = $PSBoundParameters[$key] }
}
$config = Get-RigConfig -ParametersFile $ParametersFile -ScriptRoot $here -Override $override

# The guard applies here exactly as it does to a cohort: this script talks to a
# container and reads its logs, so it must prove first that the container it is
# about to address is the rig's and not a live deployment's.
Assert-RigIsolation -Config $config | Out-Null
Assert-RigDockerIsolation -Config $config | Out-Null

$baseUri = "http://localhost:$($config.HostPort)/"
$liveUri = "http://localhost:$($config.HostPort)/health/live"
$readyUri = "http://localhost:$($config.HostPort)/health/ready"

$container = Get-RigContainerName -Config $config -Service 'repocontext'

$runRoot = Get-RigRunRoot -ScriptRoot $here
if (-not $OutputPath) {
	$directory = Join-Path $runRoot 'observations'
	New-Item -ItemType Directory -Force -Path $directory | Out-Null
	$stamp = [datetime]::UtcNow.ToString('yyyyMMddTHHmmssZ')
	$OutputPath = Join-Path $directory "observation-$stamp.json"
}
New-Item -ItemType Directory -Force -Path (Split-Path -Parent $OutputPath) | Out-Null

<#
.SYNOPSIS
	Returns the status code of a health probe, or 0 when it could not be reached
	at all (which is materially different from a 503 and must not be conflated).
#>
function Get-RigProbeStatus {
	[CmdletBinding()]
	param([Parameter(Mandatory)] [string] $Uri)

	try {
		$response = Invoke-WebRequest -Uri $Uri -Method Get -TimeoutSec 10 -SkipHttpErrorCheck -ErrorAction Stop
		return [int] $response.StatusCode
	}
	catch { return 0 }
}

<#
.SYNOPSIS
	Reads the first integer captured by a pattern over a response body, or null.
#>
function Get-RigFirstNumber {
	[CmdletBinding()]
	param([string] $Text, [Parameter(Mandatory)] [string] $Pattern)

	if ([string]::IsNullOrWhiteSpace($Text)) { return $null }
	$match = [regex]::Match($Text, $Pattern)
	if (-not $match.Success) { return $null }
	$parsed = [long] 0
	if ([long]::TryParse($match.Groups[1].Value, [ref] $parsed)) { return $parsed }
	return $null
}

# The log-line fragments each counter is tallied from. Substrings of the real
# message templates in src/, deliberately - a regex over a formatted line would
# break the moment an argument's rendering changed.
$logSignals = [ordered] @{
	healingAdmitted       = 'Admitted consolidation of donor shard'
	healingFinished       = 'finished (complete='
	consolidationStarted  = ': folding'
	annIndexOpened        = 'approximate index for'
	annIndexRestored      = 'restored from durable state: True'
	annIndexRebuilt       = 'restored from durable state: False'
	replayOverBudget      = 'over budget'
	errorLines            = '"LogLevel":"Error"'
}

$samples = [System.Collections.Generic.List[object]]::new()
$logCursor = [datetime]::UtcNow.AddSeconds(-5)
$started = [datetime]::UtcNow
$deadline = $started.AddMinutes($DurationMinutes)
$searchArguments = @{ repoId = "$($config.RepoId)"; query = "$($config.SemanticQuery)"; k = 5 }

Write-Host "Observing '$container' for $DurationMinutes minute(s) every $IntervalSeconds s." -ForegroundColor Cyan
Write-Host "  repo $($config.RepoId) | port $($config.HostPort) | output $OutputPath" -ForegroundColor DarkGray
Write-Host ''

while ([datetime]::UtcNow -lt $deadline) {
	$sampledAt = [datetime]::UtcNow

	$liveStatus = Get-RigProbeStatus -Uri $liveUri
	$readyStatus = Get-RigProbeStatus -Uri $readyUri

	$search = Invoke-RigMcpTool -BaseUri $baseUri -Name 'repocontext_search' -Arguments $searchArguments -TimeoutSec 120
	$mode = Get-RigRetrievalMode -Text $search.Text
	$retrievalPath = Get-RigRetrievalPath -Text $search.Text

	$repos = Invoke-RigMcpTool -BaseUri $baseUri -Name 'repocontext_list_repos' -Arguments @{} -TimeoutSec 120
	$fileCount = Get-RigFirstNumber -Text $repos.Text -Pattern '"fileCount"\s*:\s*(\d+)'
	$vectorCount = Get-RigFirstNumber -Text $repos.Text -Pattern '"embeddedVectorCount"\s*:\s*(\d+)'

	# Only the lines written since the previous sample, so each sample's
	# counters are a DELTA and the series can be summed or differenced.
	$sinceStamp = $logCursor.ToString('yyyy-MM-ddTHH:mm:ssZ')
	$logCursor = $sampledAt
	$logText = (Invoke-RigDocker -DockerArgs @('logs', '--since', $sinceStamp, $container) -AllowFailure | Out-String)

	$counters = [ordered] @{}
	foreach ($signal in $logSignals.Keys) {
		$needle = $logSignals[$signal]
		$counters[$signal] = ([regex]::Matches($logText, [regex]::Escape($needle))).Count
	}

	# A readiness claim is only honest if the box can actually serve the
	# retrieval it just claimed to be ready for. Recorded per sample rather than
	# asserted, so a violation is evidence rather than a crash.
	$readyButNotSemantic = ($readyStatus -eq 200) -and ($mode -ne 'semantic')

	$samples.Add([pscustomobject] @{
			sampledAtUtc        = $sampledAt.ToString('o')
			elapsedSeconds      = [Math]::Round(($sampledAt - $started).TotalSeconds, 1)
			liveStatus          = $liveStatus
			readyStatus         = $readyStatus
			searchOk            = $search.Ok
			searchMs            = $search.DurationMs
			mode                = $mode
			retrievalPath       = $retrievalPath
			readyButNotSemantic = $readyButNotSemantic
			fileCount           = $fileCount
			vectorCount         = $vectorCount
			log                 = [pscustomobject] $counters
		})

	Write-Host ("  {0,6}s live {1,3} ready {2,3}  search {3,6}ms {4,-8} {5,-22} files {6,-6} vectors {7,-7} folds+{8}/-{9}" -f `
			$samples[-1].elapsedSeconds, $liveStatus, $readyStatus, $search.DurationMs, $mode, $retrievalPath,
		$fileCount, $vectorCount, $counters.healingAdmitted, $counters.healingFinished)

	$remaining = ($deadline - [datetime]::UtcNow).TotalSeconds
	if ($remaining -le 0) { break }
	Start-Sleep -Seconds ([Math]::Min($IntervalSeconds, [int] [Math]::Ceiling($remaining)))
}

$searchSamples = @($samples | Where-Object { $_.searchOk })
$durations = [double[]] @($searchSamples | ForEach-Object { [double] $_.searchMs })

$summary = [ordered] @{
	samples             = $samples.Count
	searchOkCount       = $searchSamples.Count
	searchFailedCount   = ($samples.Count - $searchSamples.Count)
	liveOkCount         = @($samples | Where-Object { $_.liveStatus -eq 200 }).Count
	readyOkCount        = @($samples | Where-Object { $_.readyStatus -eq 200 }).Count
	readyButNotSemantic = @($samples | Where-Object { $_.readyButNotSemantic }).Count
	modes               = @($samples | ForEach-Object { $_.mode } | Sort-Object -Unique)
	retrievalPaths      = @($samples | ForEach-Object { $_.retrievalPath } | Sort-Object -Unique)
	searchMsP50         = Get-RigPercentile -Samples $durations -Percentile 50
	searchMsP95         = Get-RigPercentile -Samples $durations -Percentile 95
	searchMsMax         = Get-RigPercentile -Samples $durations -Percentile 100
	fileCountDistinct   = @($samples | Where-Object { $null -ne $_.fileCount } | ForEach-Object { $_.fileCount } | Sort-Object -Unique)
	vectorCountFirst    = @($samples | Where-Object { $null -ne $_.vectorCount } | ForEach-Object { $_.vectorCount })[0]
	vectorCountLast     = @($samples | Where-Object { $null -ne $_.vectorCount } | ForEach-Object { $_.vectorCount })[-1]
}
foreach ($signal in $logSignals.Keys) {
	$summary["total_$signal"] = ($samples | ForEach-Object { $_.log.$signal } | Measure-Object -Sum).Sum
}

$document = [ordered] @{
	schemaVersion = 1
	kind          = 'coldstart-rig/observation'
	label         = $(if ($Label) { $Label } else { 'observation' })
	generatedUtc  = [datetime]::UtcNow.ToString('o')
	container     = $container
	configuration = [ordered] @{
		projectName     = "$($config.ProjectName)"
		hostPort        = $config.HostPort
		mcpImage        = "$($config.McpImage)"
		workVolume      = "$($config.WorkVolume)"
		repoId          = "$($config.RepoId)"
		semanticQuery   = "$($config.SemanticQuery)"
		intervalSeconds = $IntervalSeconds
		durationMinutes = $DurationMinutes
	}
	summary       = [pscustomobject] $summary
	samples       = @($samples)
}

$document | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $OutputPath -Encoding utf8

Write-Host ''
Write-Host 'Observation summary' -ForegroundColor Cyan
Write-Host ("  samples {0}, search ok {1}, search failed {2}" -f $summary.samples, $summary.searchOkCount, $summary.searchFailedCount)
Write-Host ("  ready 200 on {0} of {1} samples; ready-but-not-semantic {2}" -f $summary.readyOkCount, $summary.samples, $summary.readyButNotSemantic)
Write-Host ("  modes {0} | retrieval paths {1}" -f ($summary.modes -join ','), ($summary.retrievalPaths -join ','))
Write-Host ("  search p50 {0}ms p95 {1}ms max {2}ms" -f $summary.searchMsP50, $summary.searchMsP95, $summary.searchMsMax)
Write-Host ("  folds admitted {0}, folds finished {1}" -f $summary.total_healingAdmitted, $summary.total_healingFinished)
Write-Host ("  files {0} | vectors {1} -> {2}" -f ($summary.fileCountDistinct -join ','), $summary.vectorCountFirst, $summary.vectorCountLast)
Write-Host ''
Write-Host "Observation written to $OutputPath" -ForegroundColor Green
