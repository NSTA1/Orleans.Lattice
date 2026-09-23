<#
.SYNOPSIS
	Run one Layer 3 cohort: N silos, one Orleans-client producer, one log.

.DESCRIPTION
	A cohort is a single measurement cell. It scales the silo app to exactly
	N replicas, waits for the cluster to actually form, runs the producer job
	to completion against it, harvests the log the measurement is read from,
	and then scales the silos back to zero.

	The scale-to-zero is in a `finally`, so it happens on a failed or
	interrupted cohort as well as a clean one. A sweep abandoned at 3am with
	eight 4-vCPU replicas left running is the expensive failure mode here, and
	it is not one the operator would notice until the bill.

	Log harvest goes through Log Analytics rather than a live stream. The
	measurement lives in lines the engine prints during the run, so losing the
	log loses the cell; a workspace query can be retried until the terminal
	marker appears, whereas a dropped stream cannot be replayed.

.PARAMETER NamePrefix
	Run prefix identifying the provisioned rig (see deploy-aca.ps1).

.PARAMETER SiloCount
	Number of silos for this cohort. This is the independent variable.

.PARAMETER WorkloadMode
	BENCH_WORKLOAD_MODE value, e.g. set-many.

.PARAMETER DurationSec
	Producer run duration.

.PARAMETER TreeId
	Tree name. Each cohort gets a fresh tree so cells cannot contaminate one
	another through residual tree state.

.PARAMETER ShardCount
	Shard count the producer pins the tree to before load starts.
#>
[CmdletBinding()]
param(
	[Parameter(Mandatory)][string] $NamePrefix,
	[Parameter(Mandatory)][ValidateRange(1, 30)][int] $SiloCount,
	[string] $WorkloadMode = 'set-many',
	[int] $DurationSec = 45,
	# Per-SILO offered load, not per-cohort. The cohort multiplies by
	# SiloCount, so every cell offers the same load per silo and the curve
	# answers "does adding a silo add proportional capacity?". A fixed
	# cohort-wide rung would be absorbed by N=1 and every larger N would
	# look identical - a flat line, measuring the producer, not the cluster.
	#
	# 1200 vehicles x 5 Hz = 6,000 keys/s is the rung the Layer 2 `set-many`
	# row actually publishes ("1200:5:45" in $Layer2Rows), so the N=1 cell is
	# directly comparable with the single-silo baseline.
	#
	# Do NOT raise this to the script-wide '4000:5:45' default. That default
	# is the sweep-wide fallback, not the set-many rung, and Layer 2
	# deliberately overrides it downward because a batched flush decomposes
	# into many ~100-entity Azure Table transactions and the account tail-
	# spikes near its ceiling. Offering 20,000 keys/s per silo was measured
	# here and wedged the cluster completely: every flush slot stayed
	# occupied for the whole window, ops=0, ops/sec=0, failed=0. Because
	# nothing completes AND nothing fails, the cell reads as a silent zero
	# rather than as an error.
	[int] $VehiclesPerSilo = 1200,
	[int] $TickHz = 5,
	[int] $BatchSize = 4096,
	[int] $FlushMs = 50,
	# Also per-silo: one producer holding 8 flushes in flight cannot keep
	# N silos busy, so the in-flight budget scales with the cluster.
	[int] $FlushConcurrencyPerSilo = 8,
	# FIXED across the sweep - deliberately NOT per-silo. The tree's physical
	# shard count is a property of the tree, not of the cluster serving it,
	# and holding it constant is what makes the curve interpretable: at every
	# N the same `SetManyAsync` call fans out to the same 64 shard roots, so
	# the only variable across cells is how many hosts those roots are spread
	# over. 64 is the library default (see Silo/Program.cs, BENCH_SHARD_COUNT
	# docs) and therefore exactly what the Layer 2 single-silo row measures,
	# so the N=1 cell is directly comparable with the published baseline.
	#
	# This was measured the wrong way round first. With a per-silo count of
	# 32, N=1 ran 32 shards entirely in-process on one silo and sustained
	# ~6,000 ops/s cleanly, while N=2 ran 64 shards across two silos and
	# wedged completely: every `SetManyAsync` sat in its `fanout` stage until
	# it hit the 180s response timeout (measured p50 = 180,416 ms), ops=0 for
	# all 374 windows. Two things had changed at once - twice the offered
	# load AND twice the fan-out width with half of it now crossing the
	# network - so the collapse was 33x worse than the 2x the load alone
	# predicted, and the cell taught us nothing about scaling. Pinning the
	# shard count leaves offered load as the single moving part.
	[int] $ShardCount = 64,
	[int] $WalPartitions = 16,
	# Orleans clients the producer builds. One client pins to one gateway
	# (see BenchIngestEngine.DrainAsync), so reaching N silos needs several.
	# Over-provisioning is deliberate: gateway choice is a randomly-offset
	# round-robin per client, so 4x N makes full coverage overwhelmingly
	# likely rather than merely possible.
	[int] $ClientsPerSilo = 4,
	# The silo default of 30s turns a transient queue depth into a flood of
	# grain-rpc-deadline failures and reports collapse where Layer 2 would
	# have reported latency, so this tier raises it as Layer 2 does.
	#
	# It is raised *further* than Layer 2's 180s because this tier pays two
	# cold cluster-wide structural calls per cohort that Layer 2 does not:
	# ReshardAsync to 64 shards, and WarmUpAsync across all 64 roots, both
	# against a cold tree on a freshly-started replica. Both were observed
	# to exceed 180s while the grain was demonstrably still executing
	# (NumRunning=1, work items still retiring) - a slow call, not a wedged
	# one. Reshard aborts after a single attempt, so one timeout there
	# destroys the cohort outright; roughly a third of cohorts were lost
	# this way before the limit was raised.
	#
	# Raising it cannot distort the published numbers, because no
	# measurement-phase failure on this tier is timeout-driven: saturation
	# arrives as an explicit server-side rejection and the dominant failure
	# is an Azure Tables transaction conflict, both of which are reported
	# immediately regardless of this value. It only buys patience for the
	# setup calls that precede the measurement window.
	[int] $ResponseTimeoutSec = 420,
	# How long the engine may spend draining its in-flight flushes before
	# emitting FINAL. The engine default is 12s, sized to fit inside the
	# systemd TimeoutStopSec=30 window Layer 2's VM units run under; the
	# Layer 3 producer is a Container Apps job with no such window, so the
	# tail is allowed to actually finish.
	#
	# This is not cosmetic. The budget decides whether trailing work is
	# counted in `ops` or in `failed`, and the in-flight budget it has to
	# drain is FlushConcurrency x BatchSize, which scales with silo count.
	# At N=2 the default truncated the drain and reported failed=65,536 -
	# exactly 16 x 4096, the entire in-flight budget abandoned at the
	# deadline rather than any sustained error. Left unfixed, that artefact
	# grows with N and would show up on the published curve as "scaling
	# becomes less reliable", which is the one conclusion this benchmark
	# must not fabricate.
	[int] $InFlightTailBudgetSec = 120,
	# Replay admission queue depth per permit, handed to the silos. See the
	# BENCH_WAL_REPLAY_QUEUE_DEPTH entry in $siloEnv below for why the
	# library default refuses a cold 64-shard warm-up on a 4-vCPU host.
	# 64 admits one shard root per shard against a 4-permit ceiling, which
	# is the smallest value that cannot refuse this topology's cold start.
	[int] $WalReplayQueueDepth = 64,
	# Wall-clock ceiling on the producer's warm-up retry loop, in seconds.
	# Bounds the pathological case where every warm-up attempt burns the full
	# client response timeout; the attempt cap alone permits a half-hour hang
	# per cohort, which an unattended silo-count sweep multiplies by every
	# cell. A healthy warm-up on this topology completes in about a second.
	#
	# Sized to admit exactly one full-length attempt at the response timeout
	# above, and deliberately NOT two.
	#
	# The earlier 900s sizing was reasoned from "a budget that admits fewer
	# than two attempts is not a retry budget". That is sound when a retry
	# can plausibly succeed, and it is wrong here, because the two failure
	# modes a warm-up retry faces are not alike:
	#
	#   * A FAST transient (activation cancellation, placement not yet
	#     converged) fails in seconds. This budget still admits its retries -
	#     the loop only checks the budget before starting an attempt, so an
	#     attempt that failed at t=5s is followed immediately by another.
	#     Nothing about the fast path is lost here.
	#
	#   * A HUNG warm-up burns the entire response timeout and has never
	#     been observed to recover. Measured directly on set-many N=4:
	#     attempts at 0s, 421s and 842s each timed out at 7:00, with grain
	#     diagnostics showing Total Enqueued == Total processed,
	#     QueuedWorkItems=0 and NumRunning=1 - drained, then awaiting a
	#     fan-out that never returns. Worse, LatticeGrain is
	#     StatelessWorkerPlacement, so each retry spawns a NEW activation
	#     rather than replacing the stuck one, piling concurrent 64-root
	#     fan-outs onto an already struggling cluster.
	#
	# So on the only mode where a long budget changes the outcome, retrying
	# cannot help and actively makes it worse. The budget's real job is to
	# decide how long a doomed cohort bills N silos before being abandoned:
	# 900s spends about 21 minutes per wedge, this spends about 7.
	[int] $WarmUpBudgetSec = 400,
	[string] $TreeId,
	# Disambiguates the cohort log when the same (silos, workload) cell is
	# repeated N times. Without it every repeat overwrites the previous
	# cohort's log, which both destroys the per-cohort evidence the report
	# links to and makes the aggregation depend on parsing each log before
	# the next run clobbers it.
	[string] $CohortTag,
	# Per-cohort WAL table. Successive cohorts on one deployment otherwise
	# share the single default table and accumulate every prior cohort's
	# rows. Measured effect (#3348, 8-silo set-many): with the shared table,
	# arms produced bursts of 409 EntityAlreadyExists transaction failures
	# (0 / 95 / 153 / 0 across four successive cohorts) that correlated with
	# nothing under test and swamped the comparison; with a distinct table
	# per cohort the same four-arm sweep produced zero. Rotating BENCH_TREE_ID
	# keeps cohorts logically isolated but does not stop the table growing,
	# so set this per cohort whenever arms are to be compared.
	#
	# It does NOT fix the separate, unexplained decay in absolute throughput
	# across successive cohorts on one deployment (first cohort ~329 ops/s,
	# ninth ~17-45 regardless of table). Treat only the first cohort after a
	# deployment as a trustworthy absolute number.
	[string] $WalTable = "OrleansLatticeWal",
	# (#3348) The two saturation budgets the rig deliberately sets rather than
	# inheriting. Both default to Timeout.InfiniteTimeSpan in the library so
	# the bounds are opt-in on the released 9.x line (#3386, #3390), and both
	# are inert-to-harmful at that default for what this rig measures: an
	# unbounded fan-out IS the #3348 collapse, and an unbounded per-call gate
	# allowance is the retry multiplication behind it.
	#
	# They are parameters, not silo-binary defaults, because the rig now has
	# to run two arms that differ only in these values: an arm at the SHIPPED
	# defaults (pass 0 for both), which is the arm that says whether a
	# deployment setting nothing is fixed by the default-on per-partition gate
	# change, and an arm at the RECOMMENDED values, which says what an
	# operator following the docs gets. Passing them explicitly also puts the
	# values in the cohort's own env, so a log states the configuration that
	# ran instead of leaving it implicit in whichever image was built.
	#
	# 0 means infinite (inherit the library default).
	[int] $SetManyFanOutBudgetSec = 30,
	[int] $WalAdmissionCallBudgetSec = 15,
	# (#3396) WAL append coalescing threshold. Unlike the two budgets above,
	# 0 here is a MEANINGFUL value (coalescing disabled, the historical
	# unconditional final-entry flush kick) rather than "infinite", so it
	# cannot double as the inherit sentinel. -1 means "do not set the env
	# var at all", leaving the silo on the shipping library default.
	#
	# This is the knob the #3396 arms differ in: -1/4 is the shipped
	# behaviour, 0 is the control arm that reproduces pre-#3396 main.
	[int] $WalAppendCoalescingInFlightThreshold = -1,

	# Routes a one-entry bulk WAL append through the interleaving batched grain
	# method instead of the exclusive-turn singular overload (#3408). -1 leaves
	# the silo on its shipping default (off); 0 and 1 pin the control and fix
	# arms explicitly so a cohort's arm is never implicit.
	[int] $WalBatchedSingleEntryAppends = -1,
	# (#3402) Paced release of parked WAL-admission waiters on partition
	# recovery. 0 is MEANINGFUL here too - it is the pre-#3402 "release the
	# whole parked herd in one pass" behaviour, which is the control arm -
	# so -1 is the inherit sentinel meaning "do not set the env var".
	[int] $WalSaturationRecoveryReleaseBatch = -1,
	[int] $SettleSec = 30
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

. (Join-Path $PSScriptRoot 'aca-common.ps1')
# Format-CohortVerdictLogBlock: the verdict block Layer 3 appends must be
# byte-identical to Layer 2's, because the same Read-SiloLogStats parses both.
. (Join-Path $PSScriptRoot '_run-cohort-helpers.ps1')

$ctx = Read-AcaContext -NamePrefix $NamePrefix
if (-not $TreeId) { $TreeId = "l3-$WorkloadMode-n$SiloCount-$(Get-Date -Format 'yyyyMMdd-HHmmss')" }

# One ClusterId per cohort. Azure Table membership partitions by ClusterId and
# Orleans only reaps defunct rows after DefunctSiloExpiration (7 days by default),
# so a shared id accumulates every prior cohort's silo rows in one partition - and
# worse, a cohort that starts while the previous ACA revision is still retiring
# reads those already-killed silos as Active and must probe each to timeout before
# membership settles. That is what stalls warm-up past its budget and wedges a
# cohort. A fresh id hands each cohort an empty partition, so a retiring revision
# is simply invisible to it. Derived from TreeId so the cluster stays greppable
# back to its tree; both silo and producer must be given the same value.
$ClusterId = "azure-throughput-$TreeId"

$runRoot = Get-AcaRunRoot
$logStem = if ($CohortTag) { "$NamePrefix.n$SiloCount.$WorkloadMode.$CohortTag" } else { "$NamePrefix.n$SiloCount.$WorkloadMode" }
$logPath = Join-Path $runRoot "$logStem.log"

# Derive the per-cohort values from the per-silo rung.
$VehicleCount = $VehiclesPerSilo * $SiloCount
$FlushConcurrency = $FlushConcurrencyPerSilo * $SiloCount
$ClientCount = [Math]::Min(64, $ClientsPerSilo * $SiloCount)

Write-Host "[cohort] n=$SiloCount workload=$WorkloadMode duration=${DurationSec}s tree=$TreeId" -ForegroundColor Cyan
Write-Host "[cohort] offered vehicles=$VehicleCount tickHz=$TickHz (=$($VehicleCount * $TickHz) keys/s) shards=$ShardCount flushConcurrency=$FlushConcurrency clients=$ClientCount" -ForegroundColor DarkGray

# Per-cell silo configuration. BENCH_SHARD_COUNT stays 0 on every silo: in a
# multi-replica cluster all N would race the grow-only reshard, the first
# would win, and the rest would take the ArgumentOutOfRangeException as fatal
# and exit - turning a configuration detail into a replica crashloop. The
# producer performs the single reshard instead.
$siloEnv = @(
	"BENCH_TREE_ID=$TreeId",
	"BENCH_WAL_TABLE=$WalTable",
	"BENCH_WORKLOAD_MODE=$WorkloadMode",
	"BENCH_BATCH_SIZE=$BatchSize",
	"BENCH_FLUSH_MS=$FlushMs",
	"BENCH_FLUSH_CONCURRENCY=$FlushConcurrency",
	"BENCH_WAL_PARTITIONS=$WalPartitions",
	"BENCH_VEHICLE_COUNT=$VehicleCount",
	# Silo-side too: the inner hops (LatticeGrain -> shard -> leaf -> WAL)
	# are silo-to-silo in a multi-silo cluster, so leaving these at the 30s
	# default would time out the fan-out even when the client is patient.
	"BENCH_RESPONSE_TIMEOUT_SEC=$ResponseTimeoutSec",
	# The replay admission gate's bound is (depth x ceiling), and the ceiling
	# is derived from this silo's own CPU grant: 4 vCPU on ACA Consumption
	# gives 4 permits, so the default depth of 4 admits 16 concurrently
	# replaying activations. A cohort opens against a cold 64-shard tree and
	# the warm-up needs every shard root live at once, which exceeds that
	# bound on any N we measure and refuses the warm-up outright. The gate
	# reserves capacity for foreground readers queueing behind a background
	# walk; the bench has no foreground reader, so there is nothing to
	# protect. Raising it is the remediation the exception itself names.
	"BENCH_WAL_REPLAY_QUEUE_DEPTH=$WalReplayQueueDepth",
	# (#3348) Stated explicitly so the cohort's configuration is in its own
	# env rather than implicit in the silo binary's defaults. 0 = infinite.
	"BENCH_SET_MANY_FANOUT_BUDGET_SEC=$SetManyFanOutBudgetSec",
	"BENCH_WAL_ADMISSION_CALL_BUDGET_SEC=$WalAdmissionCallBudgetSec",
	"BENCH_CLUSTER_ID=$ClusterId",
	'BENCH_SHARD_COUNT=0',
	'BENCH_CLUSTERING=azuretable',
	'BENCH_INGEST_MODE=cluster',
	'BENCH_TOTAL_DURATION_SEC=0'
)

# (#3396) Only pinned when explicitly requested, so an ordinary sweep measures
# the shipping default rather than a value this script chose. The control arm
# passes 0 to reproduce pre-#3396 behaviour.
if ($WalAppendCoalescingInFlightThreshold -ge 0) {
	$siloEnv += "BENCH_WAL_APPEND_COALESCING_IN_FLIGHT_THRESHOLD=$WalAppendCoalescingInFlightThreshold"
}

if ($WalBatchedSingleEntryAppends -ge 0) {
	$siloEnv += "BENCH_WAL_BATCHED_SINGLE_ENTRY_APPENDS=$WalBatchedSingleEntryAppends"
}

# (#3402) Same treatment: only pinned when explicitly requested. 0 selects the
# pre-#3402 release-everything control arm.
if ($WalSaturationRecoveryReleaseBatch -ge 0) {
	$siloEnv += "BENCH_WAL_SATURATION_RECOVERY_RELEASE_BATCH=$WalSaturationRecoveryReleaseBatch"
}

$startedUtc = (Get-Date).ToUniversalTime()
$execName = $null

try {
	Write-Host '[cohort] applying silo configuration' -ForegroundColor DarkGray
	# Env vars and replica count go in ONE update. Two updates would mint two
	# revisions and, in Single revision mode, the second tears down the
	# cluster the first had just formed. See Set-AcaSiloCount.
	Set-AcaSiloCount -Context $ctx -Count $SiloCount -EnvVars $siloEnv | Out-Null

	# Capture the revision this cohort's silos run under. Superseded
	# revisions from earlier cohorts linger in Log Analytics forever and
	# emit identical line shapes, so pinning the silo harvest to this
	# revision is the only filter that positively identifies this cohort's
	# replica set. See Get-AcaActiveSiloRevision.
	$siloRevision = Get-AcaActiveSiloRevision -Context $ctx
	Write-Host "[cohort] silo revision=$siloRevision" -ForegroundColor DarkGray

	# Replicas reporting Running is necessary but not sufficient: a silo is
	# only useful once it has written itself into the clustering table and
	# joined. Starting the producer against a half-formed cluster would
	# measure fewer silos than the cell claims, which corrupts the very
	# relationship the sweep exists to plot.
	Write-Host "[cohort] settling ${SettleSec}s for cluster membership" -ForegroundColor DarkGray
	Start-Sleep -Seconds $SettleSec

	$prodEnv = @(
		'BENCH_PRODUCER_MODE=orleans-client',
		"BENCH_TREE_ID=$TreeId",
		"BENCH_WORKLOAD_MODE=$WorkloadMode",
		"BENCH_DURATION_SEC=$DurationSec",
		"BENCH_VEHICLE_COUNT=$VehicleCount",
		"BENCH_TICK_HZ=$TickHz",
		"BENCH_BATCH_SIZE=$BatchSize",
		"BENCH_FLUSH_MS=$FlushMs",
		"BENCH_FLUSH_CONCURRENCY=$FlushConcurrency",
		"BENCH_WAL_PARTITIONS=$WalPartitions",
		"BENCH_SHARD_COUNT=$ShardCount",
		"BENCH_CLIENT_COUNT=$ClientCount",
		"BENCH_INFLIGHT_TAIL_BUDGET_SEC=$InFlightTailBudgetSec",
		"BENCH_RESPONSE_TIMEOUT_SEC=$ResponseTimeoutSec",
		# Wall-clock ceiling on the producer's warm-up retry loop. The attempt
		# cap alone cannot bound it: a warm-up that keeps hitting the client
		# response timeout costs attempts x ResponseTimeoutSec, which is over
		# half an hour at the defaults, with billable replicas up the whole
		# time and nothing in the job status to distinguish it from progress.
		# A healthy warm-up here takes about a second, so this only ever fires
		# on the pathological case.
		"BENCH_WARMUP_BUDGET_SEC=$WarmUpBudgetSec",
		# Must match the silo's value exactly, or the client joins an empty
		# membership partition and finds no gateway.
		"BENCH_CLUSTER_ID=$ClusterId"
	)
	Invoke-Az (@(
		'containerapp', 'job', 'update',
		'--name', $ctx.producerJob, '--resource-group', $ctx.resourceGroup,
		'--set-env-vars') + $prodEnv + @('-o', 'none')) | Out-Null

	Write-Host '[cohort] starting producer job' -ForegroundColor Cyan
	$startJson = Invoke-Az @(
		'containerapp', 'job', 'start',
		'--name', $ctx.producerJob, '--resource-group', $ctx.resourceGroup,
		'-o', 'json'
	)
	$execName = (($startJson | ConvertFrom-Json).name)
	Write-Host "[cohort] execution=$execName" -ForegroundColor DarkGray

	# Derive the ceiling from the budgets the producer actually runs under
	# rather than a flat constant. A cohort legitimately spends, in series:
	# a reshard (up to one response timeout), a warm-up retry loop (up to
	# its wall-clock budget), the measurement window, and the in-flight
	# drain. A constant smaller than that sum stops a *healthy* job partway
	# and reports it as a failure, which is the worst possible outcome -
	# the cell is lost and the log says the producer died rather than that
	# the harness killed it. The slack absorbs container start and the
	# preseed pass.
	$executionCeilingSec = $DurationSec + $WarmUpBudgetSec + $ResponseTimeoutSec + $InFlightTailBudgetSec + 300
	$state = Wait-AcaJobExecution -Context $ctx -ExecutionName $execName -TimeoutSec $executionCeilingSec
	Write-Host "[cohort] execution finished: $state" -ForegroundColor DarkGray

	# A failed producer never prints its DONE marker, so the harvest would
	# poll to its full deadline waiting for a line that cannot arrive - with
	# N silos billing the whole time. Across an unattended sweep that is the
	# difference between a cell costing minutes and costing the harvest
	# timeout. Shorten the wait to a single ingestion-lag allowance, harvest
	# whatever diagnostics did land (the stack trace is the reason the cell
	# failed and is worth keeping), and move on.
	$harvestWait = if ($state -eq 'Succeeded') { 420 } else { 120 }
	if ($state -ne 'Succeeded') {
		Write-Warning "[cohort] execution state '$state' is not Succeeded; harvesting diagnostics only (no DONE marker will arrive)."
	}

	$lines = Get-AcaJobLog -Context $ctx -ExecutionName $execName -SinceUtc $startedUtc -TreeId $TreeId -MaxWaitSec $harvestWait
	$siloLines = Get-AcaSiloLog -Context $ctx -SinceUtc $startedUtc -UntilUtc ((Get-Date).ToUniversalTime()) -RevisionName $siloRevision

	# Sort the silo half by its `t=` timestamp. Read-SiloLogStats takes the
	# LAST productive [phaseA] window as the representative one, and Log
	# Analytics returns rows ordered by ingestion-stamped TimeGenerated
	# across N replicas - so without this the "last" window can belong to
	# whichever replica happened to flush its log last, not to the latest
	# point in the run.
	$siloLines = @($siloLines | Sort-Object -Property @{ Expression = {
		if ($_ -match 't=\s*([\d.]+)s') { [double]$Matches[1] } else { [double]::MaxValue }
	} })

	# One combined log per cohort, producer first. Read-SiloLogStats reads
	# throughput from the producer's `[silo] t=` lines and per-call quantiles
	# from the silos' `[phaseA]` lines, and it is the *same* function Layer 2
	# uses, so the two layers cannot drift apart on what a cell means.
	Set-Content -Path $logPath -Value (@($lines) + @($siloLines)) -Encoding utf8
	Write-Host "[cohort] harvested $($lines.Count) producer + $($siloLines.Count) silo line(s) -> $logPath" -ForegroundColor Green

	# Grade the cohort and append the verdict block the report parser reads.
	#
	# Layer 2's Resolve-CohortVerdict is deliberately NOT reused here, and
	# the reason is a real difference in what the two layers measure rather
	# than a convenience. Layer 2 hand-tunes each workload's rung to sit
	# *below* the single-account ceiling, so any failure there is genuinely
	# anomalous and Resolve-CohortVerdict rightly grades FailedFinal>0 as
	# FAILED. Layer 3 does the opposite on purpose: it offers rung x N in
	# order to find where the cluster stops converting extra silos into
	# extra throughput, so the interesting cells are the saturated ones.
	# Applying Layer 2's rule would exclude exactly the cells that locate
	# the knee, and the curve would terminate at the last unsaturated N.
	#
	# So Layer 3 grades on the one shape that is genuinely unusable - the
	# silent zero, where nothing completes and nothing fails because every
	# call is parked past the end of the measurement window. A cell that
	# delivered real throughput is HEALTHY and its failure count is carried
	# as data (Read-SiloLogStats already extracts it), not as grounds for
	# exclusion.
	$producerDone = @($lines | Where-Object { $_.Contains('[producer] DONE') }).Count -gt 0
	$productive = @($lines | Where-Object {
		$_.Contains('[silo] t=') -and ($_ -match 'ops/sec=\s*([\d,]+)') -and ([long]($Matches[1] -replace ',','') -gt 0)
	}).Count

	$verdictState = 'HEALTHY'
	$verdictDetail = ''
	if (-not $producerDone) {
		$verdictState = 'WEDGE'
		$verdictDetail = ' (producer emitted no DONE marker)'
	} elseif ($productive -eq 0) {
		$verdictState = 'WEDGE'
		$verdictDetail = ' (no productive window: every call outlived the measurement window)'
	}

	Add-Content -Path $logPath -Encoding utf8 -Value (Format-CohortVerdictLogBlock `
		-VerdictState $verdictState -VerdictDetail $verdictDetail -DrainTailSamples 0)
	$verdictColour = if ($verdictState -eq 'HEALTHY') { 'Green' } else { 'Red' }
	Write-Host "[cohort] verdict=$verdictState productiveWindows=$productive" -ForegroundColor $verdictColour

	[pscustomobject]@{
		NamePrefix    = $NamePrefix
		SiloCount     = $SiloCount
		WorkloadMode  = $WorkloadMode
		TreeId        = $TreeId
		DurationSec   = $DurationSec
		ExecutionName = $execName
		ExecutionState= $state
		LogPath       = $logPath
		LineCount     = $lines.Count + $siloLines.Count
	}
}
finally {
	# Unconditional. A cohort that threw still leaves N billed replicas up,
	# and the operator is asleep.
	try { Set-AcaSiloCount -Context $ctx -Count 0 -TimeoutSec 300 | Out-Null }
	catch { Write-Warning "[cohort] scale-to-zero FAILED: $_ - check: az containerapp show -n $($ctx.siloApp) -g $($ctx.resourceGroup)" }
}
