#!/usr/bin/env pwsh
<#
.SYNOPSIS
	Runs one cohort of the isolated cold-start rig and emits a machine-readable
	result set.

.DESCRIPTION
	The one-command entry point. From a prepared master volume it clones a
	fresh working volume, brings the stack up in isolation, and drives it
	through a stateless MCP client, recording per scenario:

	  * time to /health/live and time to /health/ready;
	  * TIME TO FIRST SUCCESSFUL SEMANTIC QUERY (the headline number);
	  * warm query latency (min / p50 / p95 / max / mean);
	  * the retrieval path that answered (semantic, keyword or empty);
	  * peak and mean CPU and memory per container;
	  * container-log counters for replay over-budget warnings, stale
	    projections and dropped messages.

	Three restart scenarios run in order, because they differ materially and a
	rig that tested only one would mislead:

	  first-boot        the very first activation on a freshly restored volume,
	                    with no snapshot captured by a clean shutdown;
	  graceful-restart  stop with SIGTERM and a drain window, then start, so
	                    the shutdown snapshot is available on the way back up;
	  sigkill-restart   SIGKILL with no drain, the container-recreate and
	                    out-of-memory analogue, where the snapshot is whatever
	                    the last periodic capture left behind.

	Every timing is measured from the container's own State.StartedAt as
	reported by the Docker daemon, not from the moment a compose CLI call
	returned, so compose overhead never lands in a headline number and all
	three scenarios share one comparable zero point.

	ISOLATION. The stack cannot touch a live deployment: run-cohort refuses to
	do anything until both halves of the guard have passed - the configuration
	(Assert-RigIsolation) and the compose document Docker actually resolved
	(Assert-RigComposeIsolation).

.PARAMETER Runs
	How many times to repeat the whole cohort, each from a freshly cloned
	master. Two or more runs give the run-to-run spread that says whether the
	rig itself is the noise source.

.PARAMETER Scenarios
	Which restart scenarios to run, in order. Defaults to all three.

.PARAMETER KeepUp
	Leave the stack running at the end (for poking at it by hand). The working
	volume is left in place either way; the next run recreates it.

.EXAMPLE
	./run-cohort.ps1

.EXAMPLE
	./run-cohort.ps1 -Runs 2 -CohortId baseline

.EXAMPLE
	./run-cohort.ps1 -Scenarios sigkill-restart -Runs 3
#>
[CmdletBinding()]
param(
	[int] $Runs = 1,
	[ValidateSet('first-boot', 'graceful-restart', 'sigkill-restart')]
	[string[]] $Scenarios = @('first-boot', 'graceful-restart', 'sigkill-restart'),
	[string] $CohortId,
	[string] $MasterVolume,
	[string] $RepoId,
	[string] $SemanticQuery,
	[int] $WarmQueryCount,
	[string] $ParametersFile,
	[switch] $SkipClone,
	[switch] $SkipWarmup,
	[switch] $QueryFromLive,
	[switch] $KeepUp
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$here = Split-Path -Parent $MyInvocation.MyCommand.Path
. (Join-Path $here '_rig-docker.ps1')

# Only pass overrides the caller actually supplied. An unbound [int] parameter
# binds as 0, which would otherwise silently override a configured value (and
# did: -WarmQueryCount defaulted the warm sample count to zero).
$override = @{}
foreach ($key in @('MasterVolume', 'RepoId', 'SemanticQuery', 'WarmQueryCount')) {
	if ($PSBoundParameters.ContainsKey($key)) { $override[$key] = $PSBoundParameters[$key] }
}
$config = Get-RigConfig -ParametersFile $ParametersFile -ScriptRoot $here -Override $override
# A switch is not a parameters-file setting, so it is projected onto the config
# after load: Measure-RigScenario reads only the config, and threading a second
# argument through it purely for this would make the scenario signature depend
# on how the cohort was invoked.
$config['QueryFromLive'] = [bool] $QueryFromLive

# --- Both halves of the fail-closed guard, before anything is started ----
Assert-RigIsolation -Config $config | Out-Null
Assert-RigDockerIsolation -Config $config | Out-Null
Write-Host "Isolation guard passed (configuration and resolved compose document)." -ForegroundColor Green
Write-Host "  project $($config.ProjectName) | port $($config.HostPort) | work volume $($config.WorkVolume)" -ForegroundColor DarkGray

if (-not (Test-RigVolumeExists -Name "$($config.MasterVolume)")) {
	throw "Master volume '$($config.MasterVolume)' does not exist. Run prepare-master.ps1 first."
}

if (-not $CohortId) { $CohortId = 'cohort-{0}' -f ([datetime]::UtcNow.ToString('yyyyMMddTHHmmssZ')) }

$runRoot = Get-RigRunRoot -ScriptRoot $here
$cohortDirectory = Join-Path (Join-Path $runRoot 'cohorts') $CohortId
New-Item -ItemType Directory -Force -Path $cohortDirectory | Out-Null

$baseUri = "http://localhost:$($config.HostPort)/"
$liveUri = "http://localhost:$($config.HostPort)/health/live"
$readyUri = "http://localhost:$($config.HostPort)/health/ready"

<#
.SYNOPSIS
	Waits for the rig's repocontext container to be running and returns its
	name and the instant the daemon says it started.
#>
function Wait-RigContainerRunning {
	param([hashtable] $Config, [int] $TimeoutSec = 300)

	$deadline = (Get-Date).AddSeconds($TimeoutSec)
	while ((Get-Date) -lt $deadline) {
		try {
			$name = Get-RigContainerName -Config $Config -Service 'repocontext'
			$state = (Invoke-RigDocker -DockerArgs @('inspect', '-f', '{{.State.Running}}', $name) | Out-String).Trim()
			if ($state -eq 'true') {
				return [pscustomobject] @{ Name = $name; StartedAtUtc = (Get-RigContainerStartedAtUtc -Container $name) }
			}
		}
		catch {
			# The container may not exist yet immediately after `up`.
		}
		Start-Sleep -Milliseconds 250
	}
	throw 'The rig repocontext container did not reach a running state.'
}

<#
.SYNOPSIS
	Measures one scenario against an already-started stack.
#>
function Measure-RigScenario {
	param(
		[hashtable] $Config,
		[string] $Scenario,
		[int] $RunIndex,
		[string] $OutputDirectory
	)

	$container = Wait-RigContainerRunning -Config $Config
	$zero = $container.StartedAtUtc
	$slug = "{0}-run{1}" -f $Scenario, $RunIndex

	$embedder = $null
	try { $embedder = Get-RigContainerName -Config $Config -Service 'embedder' } catch { $embedder = $null }
	$sampled = @($container.Name) + @($embedder | Where-Object { $_ })

	$statsPath = Join-Path $OutputDirectory "stats-$slug.csv"
	$sampler = Start-RigStatsSampler -Containers $sampled -CsvPath $statsPath

	try {
		$liveSeconds = Wait-RigHttpOk -Uri $liveUri -ZeroUtc $zero -TimeoutSec $Config.LiveTimeoutSec -IntervalMs $Config.ProbeIntervalMs

		# WHETHER THE FIRST QUERY WAITS FOR READINESS IS NOW A CHOICE, AND IT
		# MATTERS FOR ANY CROSS-IMAGE COMPARISON.
		#
		# `/health/ready` used to be the lifecycle check alone, and returned 200
		# within a few seconds. It is now the CONJUNCTION of the lifecycle check
		# and vector-plane retrieval readiness, which is a deliberate honesty
		# improvement: a box that cannot serve semantic retrieval must not claim
		# to be ready. But it means a rig that gates its first query on
		# readiness is measuring "readiness plus a query" on one image and
		# "a query" on the other, and would report the honesty improvement as a
		# cold-start REGRESSION.
		#
		# -QueryFromLive starts querying as soon as the process is listening and
		# discovers readiness by probing for it alongside, so the headline is
		# time-to-first-answer on both images and `readySeconds` is still
		# recorded. The default keeps the original gated behaviour so cohorts
		# taken before this option remain comparable with each other.
		$readySeconds = $null
		if (-not $Config.QueryFromLive) {
			$readySeconds = Wait-RigHttpOk -Uri $readyUri -ZeroUtc $zero -TimeoutSec $Config.ReadyTimeoutSec -IntervalMs $Config.ProbeIntervalMs
		}

		# Records the first instant readiness returned 200, when the first query
		# was not gated on it. Called from every polling loop below so readiness
		# is still observed even if it lands long after the first answer.
		$readyProbe = {
			if ($Config.QueryFromLive -and $null -eq $readySeconds) {
				try {
					$probe = Invoke-WebRequest -Uri $readyUri -Method Get -TimeoutSec 5 -SkipHttpErrorCheck -ErrorAction Stop
					if ($probe.StatusCode -eq 200) {
						$script:RigReadySeconds = [Math]::Round(([datetime]::UtcNow - $zero).TotalSeconds, 3)
					}
				}
				catch {
					# Not listening yet, or refused. Readiness stays unobserved.
				}
			}
		}
		$script:RigReadySeconds = $null

		# The headline: the FIRST tool call issued against this activation is
		# the semantic search, so nothing else has warmed the retrieval path.
		#
		# Two headline numbers, not one. repocontext_search answers with
		# `mode: keyword` when the semantic path throws - and on a cold tree
		# the exact-kNN prefix scan can exceed the Orleans response timeout and
		# do exactly that. A rig that recorded only "a query succeeded" would
		# report a fast number for a box that never answered semantically at
		# all, which is the precise failure this epic exists to stop. So:
		#   firstQuerySeconds         - first successful answer, ANY mode
		#   firstSemanticQuerySeconds - first answer with mode = semantic
		$arguments = @{ repoId = "$($Config.RepoId)"; query = "$($Config.SemanticQuery)"; k = 5 }
		$attempts = 0
		$first = $null
		$firstQuerySeconds = $null
		$firstQueryMode = $null
		$semanticSeconds = $null
		$warm = [System.Collections.Generic.List[double]]::new()
		$warmModes = [System.Collections.Generic.List[string]]::new()

		# Phase 1: retry until something answers.
		$deadline = (Get-Date).AddSeconds($Config.QueryTimeoutSec)
		while ((Get-Date) -lt $deadline) {
			$attempts++
			& $readyProbe
			$first = Invoke-RigMcpTool -BaseUri $baseUri -Name 'repocontext_search' -Arguments $arguments -TimeoutSec $Config.QueryTimeoutSec
			if ($first.Ok) {
				$firstQuerySeconds = [Math]::Round(([datetime]::UtcNow - $zero).TotalSeconds, 3)
				$firstQueryMode = Get-RigRetrievalMode -Text $first.Text
				if ($firstQueryMode -eq 'semantic') { $semanticSeconds = $firstQuerySeconds }
				break
			}
			Start-Sleep -Milliseconds $Config.ProbeIntervalMs
		}

		# Phase 2: warm samples on the same activation.
		if ($null -ne $first -and $first.Ok) {
			for ($i = 0; $i -lt [int] $Config.WarmQueryCount; $i++) {
				& $readyProbe
				$sample = Invoke-RigMcpTool -BaseUri $baseUri -Name 'repocontext_search' -Arguments $arguments -TimeoutSec $Config.QueryTimeoutSec
				if (-not $sample.Ok) { continue }
				$warm.Add([double] $sample.DurationMs)
				$mode = Get-RigRetrievalMode -Text $sample.Text
				$warmModes.Add("$mode")
				if ($null -eq $semanticSeconds -and $mode -eq 'semantic') {
					$semanticSeconds = [Math]::Round(([datetime]::UtcNow - $zero).TotalSeconds, 3)
				}
			}
		}

		# Phase 3: if nothing has answered semantically yet, keep asking within
		# a bounded budget so "never became semantic" is a recorded fact rather
		# than an artefact of having stopped asking too early.
		if ($null -ne $first -and $first.Ok -and $null -eq $semanticSeconds) {
			$semanticDeadline = (Get-Date).AddSeconds($Config.SemanticRetryBudgetSec)
			while ((Get-Date) -lt $semanticDeadline) {
				$attempts++
				& $readyProbe
				$sample = Invoke-RigMcpTool -BaseUri $baseUri -Name 'repocontext_search' -Arguments $arguments -TimeoutSec $Config.QueryTimeoutSec
				if ($sample.Ok -and (Get-RigRetrievalMode -Text $sample.Text) -eq 'semantic') {
					$semanticSeconds = [Math]::Round(([datetime]::UtcNow - $zero).TotalSeconds, 3)
					break
				}
				Start-Sleep -Milliseconds $Config.ProbeIntervalMs
			}
		}

		$retrievalMode = $firstQueryMode

		# QUIESCE. A scenario is chained: the graceful stop and the SIGKILL both
		# act on whatever state the previous scenario left behind. If one run's
		# warm samples were still competing with ongoing leaf activation while
		# another run's had settled, the two runs restart from materially
		# different states and the difference lands in the NEXT scenario's
		# headline. So each scenario ends by waiting for consecutive fast
		# answers, and hands over a comparable box.
		$quiesceSeconds = $null
		$quiesced = $false
		if ($null -ne $first -and $first.Ok) {
			$consecutive = 0
			$quiesceDeadline = (Get-Date).AddSeconds($Config.QuiesceTimeoutSec)
			while ((Get-Date) -lt $quiesceDeadline) {
				& $readyProbe
				$sample = Invoke-RigMcpTool -BaseUri $baseUri -Name 'repocontext_search' -Arguments $arguments -TimeoutSec $Config.QueryTimeoutSec
				if ($sample.Ok -and $sample.DurationMs -le [double] $Config.QuiesceThresholdMs) { $consecutive++ } else { $consecutive = 0 }
				if ($consecutive -ge [int] $Config.QuiesceSamples) { $quiesced = $true; break }
				Start-Sleep -Milliseconds 500
			}
			$quiesceSeconds = [Math]::Round(([datetime]::UtcNow - $zero).TotalSeconds, 3)
		}

		# One list_repos call for completeness. Deliberately AFTER the headline
		# so it cannot warm the retrieval path being measured.
		$listRepos = Invoke-RigMcpTool -BaseUri $baseUri -Name 'repocontext_list_repos' -Arguments @{} -TimeoutSec $Config.QueryTimeoutSec
		& $readyProbe
		if ($Config.QueryFromLive) { $readySeconds = $script:RigReadySeconds }
	}
	finally {
		$resources = Stop-RigStatsSampler -Process $sampler -CsvPath $statsPath
	}

	$logPath = Join-Path $OutputDirectory "log-$slug.txt"
	$logLines = @(Invoke-RigDocker -DockerArgs @('logs', '--since', $zero.ToString('o'), $container.Name) -AllowFailure | ForEach-Object { "$_" })
	Set-Content -LiteralPath $logPath -Value $logLines -Encoding ascii
	$counters = Measure-RigLogCounters -Lines $logLines

	$hostResource = @($resources | Where-Object { $_.Container -eq $container.Name })

	$warmSamples = [double[]] $warm.ToArray()
	return [pscustomobject] @{
		scenario                  = $Scenario
		runIndex                  = $RunIndex
		containerStartedAtUtc     = $zero.ToString('o')
		liveSeconds               = $liveSeconds
		readySeconds              = $readySeconds
		firstQuerySeconds         = $firstQuerySeconds
		firstSemanticQuerySeconds = $semanticSeconds
		semanticAchieved          = ($null -ne $semanticSeconds)
		firstQueryAttempts        = $attempts
		firstQueryDurationMs      = $(if ($null -ne $first) { $first.DurationMs } else { $null })
		firstQueryOk              = $(if ($null -ne $first) { $first.Ok } else { $false })
		firstQueryError           = $(if ($null -ne $first) { $first.Error } else { 'no attempt completed' })
		retrievalMode             = $retrievalMode
		warmQueryMs               = [ordered] @{
			count = $warmSamples.Length
			min   = Get-RigPercentile -Samples $warmSamples -Percentile 0
			p50   = Get-RigPercentile -Samples $warmSamples -Percentile 50
			p95   = Get-RigPercentile -Samples $warmSamples -Percentile 95
			max   = Get-RigPercentile -Samples $warmSamples -Percentile 100
			mean  = $(if ($warmSamples.Length -gt 0) { [Math]::Round((($warmSamples | Measure-Object -Average).Average), 1) } else { $null })
			modes = @($warmModes | Select-Object -Unique)
		}
		listReposMs               = $listRepos.DurationMs
		listReposOk               = $listRepos.Ok
		quiesced                  = $quiesced
		quiesceSeconds            = $quiesceSeconds
		peakCpuPercent            = $(if ($hostResource.Count -gt 0) { $hostResource[0].PeakCpuPercent } else { $null })
		peakMemoryBytes           = $(if ($hostResource.Count -gt 0) { $hostResource[0].PeakMemoryBytes } else { $null })
		resources                 = @($resources)
		logCounters               = $counters
		logPath                   = $logPath
		statsPath                 = $statsPath
	}
}

<#
.SYNOPSIS
	Captures the host conditions a cohort was taken under.

.DESCRIPTION
	Cold start here is CPU-bound and the measured window is tens of seconds, so
	other containers competing for cores widen the run-to-run spread materially.
	Recording the contention alongside the numbers means a noisy cohort is
	self-evidently noisy when someone reads it later, instead of quietly
	misleading them.
#>
function Get-RigHostContext {
	[CmdletBinding()]
	param([Parameter(Mandatory)] [hashtable] $Config)

	$running = @(Invoke-RigDocker -DockerArgs @('ps', '--format', '{{.Names}}') -AllowFailure | ForEach-Object { "$_".Trim() } | Where-Object { $_ })
	$foreign = @($running | Where-Object { -not $_.StartsWith("$($Config.ProjectName)", [StringComparison]::OrdinalIgnoreCase) })

	$cpus = $null
	$memoryBytes = $null
	try {
		$info = (Invoke-RigDocker -DockerArgs @('info', '--format', '{{.NCPU}}|{{.MemTotal}}') -AllowFailure | Out-String).Trim()
		$parts = $info -split '\|'
		if ($parts.Count -eq 2) {
			$parsed = 0
			if ([int]::TryParse($parts[0].Trim(), [ref] $parsed)) { $cpus = $parsed }
			$parsedLong = 0L
			if ([long]::TryParse($parts[1].Trim(), [ref] $parsedLong)) { $memoryBytes = $parsedLong }
		}
	}
	catch {
		# Best effort: a daemon that will not report info does not fail a cohort.
	}

	return [pscustomobject] @{
		dockerCpus               = $cpus
		dockerMemoryBytes        = $memoryBytes
		runningContainers        = $running.Count
		foreignContainers        = $foreign.Count
		foreignContainerNames    = @($foreign)
		# A cohort taken alongside unrelated containers is still valid, but its
		# spread is not the rig's floor - it is the host's.
		contended                = ($foreign.Count -gt 0)
	}
}

# --- Cohort --------------------------------------------------------------
$runResults = [System.Collections.Generic.List[object]]::new()
$hostContext = Get-RigHostContext -Config $config
if ($hostContext.contended) {
	Write-Host ("NOTE: {0} unrelated container(s) are running on this host. Cold start is CPU-bound, so the spread this cohort reports is the HOST's floor, not the rig's. Stop them before attributing a delta." -f $hostContext.foreignContainers) -ForegroundColor Yellow
}

# WARM-UP. The embedding companion loads its model into memory on first use,
# which on a cold container is a minute or more, and on a `first-boot` scenario
# that load lands INSIDE the measured window and dominates the run-to-run
# spread. So the cohort pays it once, up front, on a throwaway activation whose
# numbers are discarded - and from then on the embedder container is kept ALIVE
# across every run (only the repocontext container is recreated), so no later
# scenario ever pays it again. This is the single biggest thing that stops the
# rig from being its own noise source.
if (-not $SkipWarmup) {
	Write-Host 'Warm-up (discarded): loading the embedding model outside the measured window ...' -ForegroundColor DarkGray
	Invoke-RigCompose -Config $config -ComposeArgs @('down', '--remove-orphans') -AllowFailure | Out-Null
	New-RigVolume -Config $config -Name "$($config.HfCacheVolume)" | Out-Null
	# -SkipClone means "measure the durable state the working volume ALREADY
	# holds". Cloning here would silently destroy it before a single number was
	# taken, which is the opposite of what the flag promises - and is a real
	# footgun for a measurement taken after a long heal, where the working
	# volume is the whole point and cannot be regenerated in minutes. The
	# warm-up's job is to make the EMBEDDER resident; it does not need a fresh
	# volume to do that.
	if (-not $SkipClone) {
		Copy-RigVolume -Config $config -Source "$($config.MasterVolume)" -Destination "$($config.WorkVolume)" | Out-Null
	}
	elseif (-not (Test-RigVolumeExists -Name "$($config.WorkVolume)")) {
		throw "-SkipClone was given but working volume '$($config.WorkVolume)' does not exist. Run a cohort without -SkipClone first."
	}
	Invoke-RigCompose -Config $config -ComposeArgs @('up', '-d') | Out-Null

	$warmupContainer = Wait-RigContainerRunning -Config $config
	$warmupReady = Wait-RigHttpOk -Uri $readyUri -ZeroUtc $warmupContainer.StartedAtUtc -TimeoutSec $config.ReadyTimeoutSec -IntervalMs $config.ProbeIntervalMs
	if ($null -ne $warmupReady) {
		$warmupResult = Invoke-RigMcpTool -BaseUri $baseUri -Name 'repocontext_search' `
			-Arguments @{ repoId = "$($config.RepoId)"; query = "$($config.SemanticQuery)"; k = 5 } -TimeoutSec $config.QueryTimeoutSec
		Write-Host ("  warm-up query ok={0} in {1}ms; embedder now resident." -f $warmupResult.Ok, $warmupResult.DurationMs) -ForegroundColor DarkGray
	}
	else {
		Write-Host '  warm-up never became ready; continuing anyway.' -ForegroundColor Yellow
	}
	# Remove ONLY the host container. The embedder keeps running, and with it
	# the loaded model.
	Invoke-RigCompose -Config $config -ComposeArgs @('rm', '-sf', 'repocontext') -AllowFailure | Out-Null
}

for ($run = 1; $run -le $Runs; $run++) {
	Write-Host ''
	Write-Host ("=== run {0} of {1} ===" -f $run, $Runs) -ForegroundColor Cyan

	# Recreate the host container only; leave the embedder up and warm.
	Invoke-RigCompose -Config $config -ComposeArgs @('rm', '-sf', 'repocontext') -AllowFailure | Out-Null

	if (-not $SkipClone) {
		Write-Host "Cloning master '$($config.MasterVolume)' to working volume '$($config.WorkVolume)' ..." -ForegroundColor DarkGray
		$cloneWatch = [System.Diagnostics.Stopwatch]::StartNew()
		Copy-RigVolume -Config $config -Source "$($config.MasterVolume)" -Destination "$($config.WorkVolume)" | Out-Null
		$cloneWatch.Stop()
		Write-Host ("  cloned in {0:N1}s" -f $cloneWatch.Elapsed.TotalSeconds) -ForegroundColor DarkGray
	}

	$scenarioResults = [System.Collections.Generic.List[object]]::new()

	foreach ($scenario in $Scenarios) {
		Write-Host ("-- {0}" -f $scenario) -ForegroundColor Yellow

		switch ($scenario) {
			'first-boot' {
				Invoke-RigCompose -Config $config -ComposeArgs @('up', '-d') | Out-Null
			}
			'graceful-restart' {
				Invoke-RigCompose -Config $config -ComposeArgs @('stop', '-t', "$($config.GracefulStopTimeoutSec)", 'repocontext') | Out-Null
				Invoke-RigCompose -Config $config -ComposeArgs @('start', 'repocontext') | Out-Null
			}
			'sigkill-restart' {
				$container = Get-RigContainerName -Config $config -Service 'repocontext'
				Invoke-RigDocker -DockerArgs @('kill', '-s', 'KILL', $container) -AllowFailure | Out-Null
				Invoke-RigCompose -Config $config -ComposeArgs @('start', 'repocontext') | Out-Null
			}
		}

		Start-Sleep -Seconds ([int] $config.StartupSettleSec)
		$result = Measure-RigScenario -Config $config -Scenario $scenario -RunIndex $run -OutputDirectory $cohortDirectory
		$scenarioResults.Add($result)

		Write-Host ("   live {0,7}s  ready {1,7}s  FIRST QUERY {2,7}s ({3})  first SEMANTIC {4,7}s  warm p50 {5,7}ms" -f `
				$result.liveSeconds, $result.readySeconds, $result.firstQuerySeconds, $result.retrievalMode,
			$result.firstSemanticQuerySeconds, $result.warmQueryMs.p50) `
			-ForegroundColor Green
	}

	$runResults.Add([pscustomobject] @{ runIndex = $run; scenarios = @($scenarioResults) })
}

if (-not $KeepUp) {
	Invoke-RigCompose -Config $config -ComposeArgs @('down', '--remove-orphans') -AllowFailure | Out-Null
}

# --- Summary -------------------------------------------------------------
$allScenarios = @($runResults | ForEach-Object { $_.scenarios })
$summary = foreach ($group in ($allScenarios | Group-Object scenario)) {
	$headline = [double[]] @($group.Group | Where-Object { $null -ne $_.firstQuerySeconds } | ForEach-Object { [double] $_.firstQuerySeconds })
	$semantic = [double[]] @($group.Group | Where-Object { $null -ne $_.firstSemanticQuerySeconds } | ForEach-Object { [double] $_.firstSemanticQuerySeconds })
	$ready = [double[]] @($group.Group | Where-Object { $null -ne $_.readySeconds } | ForEach-Object { [double] $_.readySeconds })
	[pscustomobject] @{
		scenario                          = $group.Name
		samples                           = $group.Count
		firstQuerySeconds                 = $headline
		firstQuerySecondsMin              = Get-RigPercentile -Samples $headline -Percentile 0
		firstQuerySecondsMax              = Get-RigPercentile -Samples $headline -Percentile 100
		firstQuerySecondsMean             = $(if ($headline.Length -gt 0) { [Math]::Round((($headline | Measure-Object -Average).Average), 3) } else { $null })
		firstQueryRelativeSpreadPct       = Get-RigRelativeSpread -Samples $headline
		firstSemanticQuerySeconds         = $semantic
		firstSemanticQuerySecondsMean     = $(if ($semantic.Length -gt 0) { [Math]::Round((($semantic | Measure-Object -Average).Average), 3) } else { $null })
		firstSemanticRelativeSpreadPct    = Get-RigRelativeSpread -Samples $semantic
		semanticAchievedCount             = @($group.Group | Where-Object { $_.semanticAchieved }).Count
		readySecondsMean                  = $(if ($ready.Length -gt 0) { [Math]::Round((($ready | Measure-Object -Average).Average), 3) } else { $null })
		readyRelativeSpreadPct            = Get-RigRelativeSpread -Samples $ready
		retrievalModes                    = @($group.Group | ForEach-Object { $_.retrievalMode } | Select-Object -Unique)
	}
}

$cohort = [ordered] @{
	schemaVersion = 1
	kind          = 'coldstart-rig/cohort'
	cohortId      = $CohortId
	generatedUtc  = [datetime]::UtcNow.ToString('o')
	hostContext   = $hostContext
	configuration = [ordered] @{
		project        = "$($config.ProjectName)"
		hostPort       = [int] $config.HostPort
		mcpImage       = "$($config.McpImage)"
		embedderImage  = "$($config.EmbedderImage)"
		masterVolume   = "$($config.MasterVolume)"
		workVolume     = "$($config.WorkVolume)"
		repoId         = "$($config.RepoId)"
		semanticQuery  = "$($config.SemanticQuery)"
		warmQueryCount = [int] $config.WarmQueryCount
		scenarios      = @($Scenarios)
		runs           = $Runs
		queryFromLive  = [bool] $QueryFromLive
		skipClone      = [bool] $SkipClone
		skipWarmup     = [bool] $SkipWarmup
	}
	runs          = @($runResults)
	summary       = @($summary)
}

$cohortPath = Join-Path $cohortDirectory 'cohort.json'
$json = $cohort | ConvertTo-Json -Depth 12
Set-Content -LiteralPath $cohortPath -Value $json -Encoding ascii
Set-Content -LiteralPath (Join-Path (Join-Path $runRoot 'cohorts') 'cohort-latest.json') -Value $json -Encoding ascii

Write-Host ''
Write-Host 'Cohort summary' -ForegroundColor Cyan
foreach ($entry in $summary) {
	Write-Host ("  {0,-18} n={1}  first query min {2,7}s max {3,7}s mean {4,7}s spread {5,6}%  semantic {6}/{1} mean {7,7}s  modes {8}" -f `
			$entry.scenario, $entry.samples, $entry.firstQuerySecondsMin, $entry.firstQuerySecondsMax,
		$entry.firstQuerySecondsMean, $entry.firstQueryRelativeSpreadPct, $entry.semanticAchievedCount,
		$entry.firstSemanticQuerySecondsMean, ($entry.retrievalModes -join ','))
}
Write-Host ''
Write-Host "Cohort written to $cohortPath" -ForegroundColor Green
