#!/usr/bin/env pwsh
#requires -Version 7.0
<#
.SYNOPSIS
	Regenerate every cell of docs/lattice/performance-single-silo.md against
	a freshly-provisioned VM in the operator's Azure subscription.

.DESCRIPTION
	End-to-end perf-report orchestration:
		1. Preflight (az login + parameters.local.ps1 + tools present).
		2. Generate a per-run prefix (does not collide with operator's
		   long-lived envs; <=9 cleaned chars so the bicep storage-account
		   name does not silently truncate uniqueString).
		3. Provision via benchmark/azure-throughput/scripts/deploy.ps1.
		4. Run Layer 1 cohorts (BDN microbench in-process on the VM).
		5. Run Layer 2 cohorts (silo + producer per workload mode).
		6. Aggregate per-row cells (median across N cohorts).
		7. Rewrite the perf-table:layer1 / perf-table:layer2 marker blocks
		   in docs/lattice/performance-single-silo.md.
		8. Teardown (az group delete --no-wait) unless -KeepVm.

	The script does not push results to the local VictoriaMetrics history
	stack (consistent with the existing real-Azure tier's policy in the
	perf-optimiser agent doc; this is for the user-facing reference numbers,
	not the per-cycle A/B trend).

.PARAMETER Layer
	Which layer(s) to regenerate. One of: all (default), 1, 2.
	-Layer all == both layers.

	Convenience: the -Layer1 / -Layer2 switches are equivalent to -Layer 1 /
	-Layer 2 (so '-Layer1' as a single token works). Setting both switches
	together is equivalent to -Layer all. Mixing -Layer with a disagreeing
	-LayerN switch is rejected.

.PARAMETER Layer1
	Convenience switch: equivalent to -Layer 1.

.PARAMETER Layer2
	Convenience switch: equivalent to -Layer 2.

.PARAMETER Workloads
	Comma-separated workload subset for the chosen layer. Defaults to all
	workloads.
		Layer 1 values: point-read, point-write, point-get-many,
						bulk-load, set-many-atomic, set-many-atomic-2,
						set-many-atomic-64, cross-tree-atomic-2,
						cross-tree-atomic-64
		Layer 2 values: set-many, set-many-atomic, set-many-atomic-2,
						cross-tree-atomic-2, cross-tree-atomic-64, set-point,
						set-point-mv, get-point, get-many
	Pass 'all' (or omit) to run every workload for the layer.

.PARAMETER N
	Per-workload cohort size. Default 3 (the n=3 baseline-cohort discipline
	from the perf-optimiser agent rules).

.PARAMETER VmSize
	Azure VM SKU. Default Standard_D4as_v5 (the empirically-derived
	sweet-spot for the 4k:5 rung; see benchmark/azure-throughput/throughput.md
	section 26.2 for the sizing rule). Override at your own risk: changing
	the SKU breaks comparability with the published baseline.

.PARAMETER Rung
	Layer 2 rung spec, 'vehicles:tickHz:durationSec'. Default '4000:5:45'.

.PARAMETER DryRun
	Run no Azure / SSH / dotnet commands. Read the most recent state.json
	from benchmark/.run/performance-report/<prefix>/state.json and rewrite
	the doc markers from it. Useful for tweaking the marker-rendering logic
	without paying for a fresh cohort.

.PARAMETER Diff
	When combined with -DryRun, show the planned doc diff and do NOT
	write the file. Standalone (without -DryRun) is reserved for future use
	and currently is a no-op.

.PARAMETER KeepVm
	Skip the teardown step. The resource group is preserved for manual
	post-mortem; remember to delete it via `az group delete --name rg-<prefix>`
	when done. The script prints the RG name in the final summary.

.PARAMETER ReuseVm
	Skip provisioning; reuse the named prefix's existing VM. Useful for
	incremental re-measurement after a hand-applied tweak on the VM.

.PARAMETER SkipDocUpdate
	Run cohorts and write state.json but do NOT touch the markdown.
	Useful for inspecting the raw cohort before publishing.

.PARAMETER Fidelity
	BDN fidelity for Layer 1 cohorts. One of:
	  dry   (default) - Job.Dry: 1 warmup + 1 measurement iter per [Benchmark]
						method. ~3 sec/method, ~75 sec/cohort across the doc's
						5 workloads. Per-cohort variance is wider than 'quick';
						the n=3 cohort discipline (median across N) provides the
						statistical guard. Right floor for a published-doc
						refresh whose precision is already 'approximate ceiling'.
	  quick           - Job.ShortRun: 1 launch + 3 warmup + 3 measurement iters.
						~30-40 sec/method when no glob expansion fires; on this
						bench the GlobFilter expands the 5 doc rows into ~25
						distinct methods (variant suffixes + parameterisations),
						so a 'quick' cohort takes ~10 min instead of ~3 min.
						Choose this when you want tighter per-cohort confidence
						intervals and are willing to pay the wall.
	  full            - Job.Default + forking toolchain. Gold-standard rigour;
						~30+ min per cohort. Reserved for the final re-verify
						pass when a 'dry' or 'quick' delta is borderline.

.PARAMETER NamePrefix
	Force a specific name prefix (cleaned to <=9 chars). Default is an
	auto-generated 'pr' + 7 random hex chars (= 9 cleaned chars). Use this
	when reproducing a specific cohort by prefix.

.PARAMETER ParametersFile
	Explicit path to the parameters .ps1 file. Defaults to
	benchmark/azure-throughput/scripts/parameters.local.ps1.

.EXAMPLE
	./benchmark/performance-report.ps1                                  # full sweep
.EXAMPLE
	./benchmark/performance-report.ps1 -Layer 2 -Workloads 'set-many'   # one row
.EXAMPLE
	./benchmark/performance-report.ps1 -DryRun                          # re-render doc only
.EXAMPLE
	./benchmark/performance-report.ps1 -DryRun -Diff                    # show planned diff
.EXAMPLE
	./benchmark/performance-report.ps1 -KeepVm -ReuseVm prdeadbee       # debug session
#>
[CmdletBinding()]
param(
	[ValidateSet('all','1','2','3')]
	[string] $Layer = 'all',

	[string] $Workloads,

	[ValidateRange(1, 20)]
	[int] $N = 3,

	[string] $VmSize = 'Standard_D4as_v5',

	[string] $Rung = '4000:5:45',

	# BENCH_BATCH_SIZE pinned into the Layer 2 silo env. Default 4096 matches
	# the silo's BENCH_BATCH_SIZE default; pinning it here keeps the meta
	# header auditable on state.json so the throughput cell can be interpreted
	# in context (the per-call read cells no longer depend on this value -
	# they are sourced directly from the get.duration / get_many.duration
	# caller-visible histograms on the lattice grain - but the throughput
	# cell's per-batch shape still does). Override at your own risk:
	# throughput results become a function of this number.
	[int] $BatchSize = 4096,

	[switch] $DryRun,
	[switch] $Diff,
	[switch] $KeepVm,
	[string] $ReuseVm,
	[switch] $SkipDocUpdate,

	# Diagnostic only: capture a per-cohort dotnet-counters trace (System.Runtime
	# + System.Net.Http) from the benchmark silo process while each Layer 2 cohort
	# runs. Off by default; pulls a counters-<cohort>.csv next to the silo log.
	# Use to investigate thread-pool / lock-contention / HTTP-pool bottlenecks.
	[switch] $CaptureCounters,

	[ValidateSet('dry','quick','full')]
	[string] $Fidelity = 'quick',

	# Convenience switches: equivalent to -Layer 1 / -Layer 2 / -Layer 3. Either
	# form works; mutually exclusive with -Layer except for the default (all).
	# These are here because operators reach for them naturally ('-Layer1') over
	# '-Layer 1'.
	[switch] $Layer1,
	[switch] $Layer2,
	[switch] $Layer3,

	# Layer 3 only: the silo counts to sweep. This list IS the x-axis of the
	# scaling curve, so it is the one Layer 3 knob an operator routinely
	# changes. Ascending order matters - the sweep checkpoints its state after
	# every cell, so an abandoned run still yields a usable prefix of the
	# curve rather than a scatter of disconnected points.
	[ValidateRange(1, 30)]
	[int[]] $SiloCounts = @(1, 2, 4, 6, 8),

	# Layer 3 only: reuse an already-provisioned ACA rig (see deploy-aca.ps1)
	# instead of provisioning and tearing one down. The name prefix of that
	# rig. Mirrors -ReuseVm for Layer 2.
	[string] $ReuseAca,

	# Layer 3 only: leave the ACA rig standing after the sweep. Mirrors -KeepVm.
	[switch] $KeepAca,

	# Layer 3 only: resume an interrupted sweep from the rig's state.json.
	# Every (workload, silo count) cell that already holds -N cohorts is kept
	# as-is and not re-run. A cell holding fewer is re-run in full, so it is
	# never a blend of two partial attempts. Without this, re-invoking a
	# 90-cohort sweep that died at cohort 60 starts from cohort 1 and pays
	# for the first 60 again.
	[switch] $Resume,

	# (#3348) Layer 3 only. The two saturation budgets the rig sets rather than
	# inheriting from the library, in seconds; 0 means infinite, i.e. run at
	# the SHIPPED default.
	#
	# The defaults here are the recommended finite values, so an ordinary sweep
	# measures the configuration the docs tell an operator to set. Pass 0 for
	# both to measure the out-of-the-box configuration instead. That second arm
	# is not optional when validating #3348: the per-partition WAL gate fix is
	# unconditional and default-on, whereas these two bounds are opt-in, so only
	# a zeroed arm can establish whether a deployment that configures nothing is
	# actually fixed. Running a single arm with both finite would leave any
	# recovery unattributable across three simultaneous changes.
	[int] $SetManyFanOutBudgetSec = 30,
	[int] $WalAdmissionCallBudgetSec = 15,
	# (#3396) Layer 3 only. WAL append coalescing in-flight threshold. -1
	# (the default) leaves the silo on the shipping library default; 0 is the
	# control arm that reproduces the pre-#3396 unconditional flush kick.
	# 0 cannot double as the inherit sentinel here because, unlike the two
	# budgets above, 0 is itself a meaningful value for this option.
	[int] $WalAppendCoalescingInFlightThreshold = -1,
	# (#3402) Layer 3 only. Paced release of parked WAL-admission waiters on
	# partition recovery. -1 inherits the shipping library default; 0 is the
	# control arm reproducing the pre-#3402 release-the-whole-herd behaviour.
	[int] $WalSaturationRecoveryReleaseBatch = -1,
	# Layer 3 only. Producer clients per silo (run-cohort-aca.ps1 -ClientsPerSilo,
	# capped at 64 in total). The generator runs inside those clients, so at
	# small silo counts the default of 4 can cap a fast read cell on the
	# producer rather than the cluster; the report then grades it
	# producer-bound. Raise it to measure such a cell. Recorded per cohort.
	[ValidateRange(1, 64)][int] $Layer3ClientsPerSilo = 4,
	# Layer 3 only. True-throughput escalation: a cell whose first cohort
	# completes at least this fraction of the offered load is re-run at double
	# the per-silo rung, at most -MaxRungEscalations times. 0 disables it and
	# publishes whatever each row's starting rung measured.
	[double] $SaturationRatio = 0.9,
	[int] $MaxRungEscalations = 3,
	[string] $ParametersFile
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

# Resolve the layer-switches into the single $Layer enum so the rest of the
# script branches on one value. Switch precedence:
#   - If no -LayerN switch is set, $Layer (default 'all') wins.
#   - If exactly one is set, it overrides $Layer (so '-Layer 2 -Layer1' is a
#     user mistake we report rather than silently picking one).
#   - If both -Layer1 and -Layer2 are set, that's 'all'.
#   - Mixing -Layer with -LayerN to disagreeing values is rejected.
#
# Layer 3 is deliberately NOT part of 'all'. Layers 1 and 2 share one Azure VM
# and one provisioning path; Layer 3 stands up a whole Container Apps
# environment and sweeps a silo count, on the order of an hour and tens of
# pounds. Folding it into the default would mean every unqualified run of this
# script silently bought a multi-silo sweep. It is opt-in only, via '-Layer 3'
# or '-Layer3', and it runs alone - it writes a different document from a
# different rig, so there is nothing to gain from interleaving it.
$switchPicks = @()
if ($Layer1) { $switchPicks += '1' }
if ($Layer2) { $switchPicks += '2' }
if ($Layer3) { $switchPicks += '3' }
if ($switchPicks.Count -gt 0) {
	if ($switchPicks -contains '3' -and $switchPicks.Count -gt 1) {
		throw "-Layer3 cannot be combined with -Layer1 / -Layer2: Layer 3 provisions its own Container Apps rig and writes a different document. Run it on its own."
	}
	$switchValue = if ($switchPicks.Count -eq 2) { 'all' } else { $switchPicks[0] }
	# If the caller passed both -Layer and a -LayerN switch with conflicting
	# values, surface a clear error instead of picking one silently.
	$layerExplicit = $PSBoundParameters.ContainsKey('Layer')
	if ($layerExplicit -and $Layer -ne $switchValue) {
		throw "Conflicting layer selection: -Layer '$Layer' vs the switch(es) that resolve to '$switchValue'. Use ONE of -Layer <all|1|2|3> OR -Layer1 / -Layer2 / -Layer3."
	}
	$Layer = $switchValue
}

# Resolve repo-relative paths once.
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot  = Resolve-Path (Join-Path $scriptDir '..')
$benchmarkRoot = $scriptDir   # benchmark/
$azureThroughputDir = Join-Path $benchmarkRoot 'azure-throughput'
$azScriptsDir = Join-Path $azureThroughputDir 'scripts'
$docPath = Join-Path $repoRoot 'docs/lattice/performance-single-silo.md'
$multiSiloDocPath = Join-Path $repoRoot 'docs/lattice/performance-multi-silo.md'
$runRoot = Join-Path $benchmarkRoot '.run/performance-report'
if (-not (Test-Path $runRoot)) { New-Item -ItemType Directory -Path $runRoot -Force | Out-Null }

# ────────────────────────────────────────────────────────────────────────────
# Row -> bench mapping. The doc tables have fixed labels; this table maps each
# labelled row to the underlying BDN method (Layer 1) or BENCH_WORKLOAD_MODE
# (Layer 2) plus the JSON metric keys (Layer 1) / log instrument tags (Layer 2)
# the aggregator reads. The label text below MUST match the row's first-column
# value in the doc verbatim (sans the surrounding pipes); the doc-update pass
# uses the label as the row key.
# ────────────────────────────────────────────────────────────────────────────

# Layer 1 rows. ExpectedBatchSize is recorded in the row label so the operator
# can verify the bench shape matches what the doc claims; a mismatch surfaces
# as a warning during aggregation (not an error - the operator may have
# intentionally changed the bench fixture size).
$Layer1Rows = @(
	@{
		Label = '`GetAsync` (point read)';
		WorkloadId = 'point-read';
		BdnMethod = 'PointRead';
		MetricSlug = 'point_read';
		ExpectedBatchSize = 1;
		CeilingUnit = 'keys/s';
	},
	@{
		Label = '`SetAsync` (point write)';
		WorkloadId = 'point-write';
		BdnMethod = 'PointWrite';
		MetricSlug = 'point_write';
		ExpectedBatchSize = 1;
		CeilingUnit = 'keys/s';
	},
	@{
		Label = '`GetManyAsync` (4 keys/call)';
		WorkloadId = 'point-get-many';
		BdnMethod = 'PointGetMany';
		MetricSlug = 'point_get_many';
		ExpectedBatchSize = 4;
		CeilingUnit = 'keys/s';
		# The PointGetMany bench builds a fixed 4-key batch
		# (LatticeMicroBenchmarks.cs GlobalSetup, getManyBatchSize = min(4,...)).
		# The label and ExpectedBatchSize are pinned to 4 to match the fixture;
		# the per-key ceiling cell scales by ExpectedBatchSize, so keeping these
		# aligned avoids silently inflating the displayed throughput.
	},
	@{
		Label = '`SetManyAsync` (1,000 keys/call)';
		WorkloadId = 'bulk-load';
		BdnMethod = 'BulkLoad';
		MetricSlug = 'bulk_load';
		ExpectedBatchSize = 1000;
		CeilingUnit = 'keys/s';
	},
	@{
		Label = '`SetManyAtomicAsync` (16 keys/saga)';
		WorkloadId = 'set-many-atomic';
		BdnMethod = 'SetManyAtomic';
		MetricSlug = 'set_many_atomic';
		ExpectedBatchSize = 16;
		CeilingUnit = 'keys/s';
	},
	@{
		Label = '`SetManyAtomicAsync` (2 keys/saga, single-tree)';
		WorkloadId = 'set-many-atomic-2';
		BdnMethod = 'SetManyAtomic_2Keys';
		MetricSlug = 'set_many_atomic_2_keys';
		ExpectedBatchSize = 2;
		CeilingUnit = 'keys/s';
	},
	@{
		Label = '`SetManyAtomicAsync` (64 keys/saga, single-tree)';
		WorkloadId = 'set-many-atomic-64';
		BdnMethod = 'SetManyAtomic_64Keys';
		MetricSlug = 'set_many_atomic_64_keys';
		ExpectedBatchSize = 64;
		CeilingUnit = 'keys/s';
	},
	@{
		Label = '`BeginAtomicWrite` cross-tree (2 keys/saga, 2 trees)';
		WorkloadId = 'cross-tree-atomic-2';
		BdnMethod = 'CrossTreeAtomic_2Keys';
		MetricSlug = 'cross_tree_atomic_2_keys';
		ExpectedBatchSize = 2;
		CeilingUnit = 'keys/s';
	},
	@{
		Label = '`BeginAtomicWrite` cross-tree (64 keys/saga, 2 trees)';
		WorkloadId = 'cross-tree-atomic-64';
		BdnMethod = 'CrossTreeAtomic_64Keys';
		MetricSlug = 'cross_tree_atomic_64_keys';
		ExpectedBatchSize = 64;
		CeilingUnit = 'keys/s';
	}
)

# Layer 2 rows. The doc rows are addressed by their public ILattice method name;
# the silo's BENCH_WORKLOAD_MODE env drives the matching code path.
$Layer2Rows = @(
	@{
		Label = '`GetAsync` (point read)';
		WorkloadId = 'get-point';
		WorkloadMode = 'get-point';
		ThroughputUnit = 'keys/s';
	},
	@{
		Label = '`SetAsync` (point write, 200 veh/5 Hz)';
		WorkloadId = 'set-point';
		WorkloadMode = 'set-point';
		ThroughputUnit = 'keys/s';
		# Per-workload offered rung. Point writes are the most account-bound
		# mode: each key is its own WAL append and its own Azure Table
		# operation (no batching of round-trips). The historical ~800-850
		# key-writes/s knee was an artefact of the phase-1 409
		# EntityAlreadyExists conflict storm (#824): a lost-response replay
		# re-drove divergent content onto durable offsets, collapsing the WAL
		# write path into a non-recovering wedge well below the real storage
		# ceiling. That storm was removed (commit a858602 / PR #838: O(1)
		# idempotent 409 replay proof + shard-side reconcile-before-resync +
		# a finite WAL network timeout). The real-Azure P=16 ladder now runs
		# clean across the whole 200->2000 offered-ops/s band (failed=0 at
		# every rung, transient single-account stalls self-heal within the
		# flush budget), with the single-account steady ceiling at ~5-8k
		# entries/s. Stochastic single-account brown-out wedges only reappear
		# above ~1500 offered ops/s. So the prior 100 veh (500 keys/s) rung
		# is now ~2x below the proven-reliable range. Holding the offered
		# rate to 200 veh keeps the cohort squarely inside the measured
		# failed=0 band with comfortable headroom below the ~1500 ops/s
		# stochastic-wedge onset, so the flush slots stay unsaturated and all
		# three cohorts complete with zero failures. The reported number is a
		# conservative *sustained* point-write rate at a sub-saturation
		# offered load, not the saturation ceiling (the account ceiling and
		# the multi-account fan-out remedy are discussed in
		# docs/lattice/throughput.md).
		# 200 veh x 5 Hz = 1,000 keys/s offered.
		Rung = '200:5:45';
	},
	@{
		Label = '`SetAsync` (point write + async materialised view, 200 veh/5 Hz)';
		WorkloadId = 'set-point-mv';
		WorkloadMode = 'set-point-mv';
		ThroughputUnit = 'keys/s';
		# A/B partner of the `set-point` row. The silo runs the identical
		# point-write workload but additionally attaches an asynchronous
		# materialised view derived from the same tree (a key-preserving
		# passthrough view; see the azure-throughput Silo's SetPointMv
		# workload mode). The view maintainer tails the WAL off the foreground
		# write hot path, so this cohort's throughput and per-call latency
		# should be statistically indistinguishable from the plain `set-point`
		# cohort - that equality is the evidence the materialised view is fully
		# asynchronous and does not tax the primary tree's write path. The
		# offered rung matches `set-point` exactly so the comparison is
		# like-for-like. 200 veh x 5 Hz = 1,000 keys/s offered.
		Rung = '200:5:45';
	},
	@{
		Label = '`GetManyAsync` (4,096 keys/call)';
		WorkloadId = 'get-many';
		WorkloadMode = 'get-many';
		ThroughputUnit = 'keys/s';
	},
	@{
		Label = '`SetManyAsync` (4,096 keys/call, 1200 veh/5 Hz)';
		WorkloadId = 'set-many';
		WorkloadMode = 'set-many';
		# Unit is keys/s (matches other rows) - one SetManyAsync entry = one
		# (key,value) pair = one key-write. The (4,096 keys/call) label
		# describes the per-call batch shape; the throughput cell is the
		# resulting key-write rate.
		ThroughputUnit = 'keys/s';
		# Per-workload offered rung. Batched writes amortise grain-RPC and
		# WAL cost across the call, so the saturation ceiling (~12.5k key-
		# writes/s) is much higher than point writes - but each flush still
		# decomposes into many ~100-entity Azure Table transactions, any one
		# of which can tail-spike past the 30s WAL dispatch timeout when the
		# single account is pushed near its ceiling. A single timed-out
		# transaction fails its whole 4,096-key flush (~11k failed keys),
		# leaving failed>0 -> FAILED; that struck ~1/3 of cohorts at the
		# ceiling rung. Holding the offered rate to roughly half the ceiling
		# keeps the per-transaction latency in the fast path so no flush
		# times out, every cohort finishes with zero failures, and the
		# reported number is a conservative *sustained* rate (not the
		# saturation ceiling - see docs/lattice/throughput.md).
		# 1200 veh x 5 Hz = 6,000 keys/s offered.
		Rung = '1200:5:45';
	},
	@{
		Label = '`SetManyAtomicAsync` (64 keys/saga, 100 veh/5 Hz)';
		WorkloadId = 'set-many-atomic';
		WorkloadMode = 'set-many-atomic';
		ThroughputUnit = 'keys/s';
		# Per-workload offered rung. Two constraints bound this value:
		#   1. FLOOR: each saga commits 64 *distinct* keys as one all-or-
		#      nothing batch, and the producer derives one key per vehicle,
		#      so the cohort must run at least 64 vehicles or the saga can
		#      only form <64 distinct keys and emits a duplicate-key batch
		#      that the atomic-write API (correctly) rejects with
		#      ArgumentException -> every flush fails (validated: 60 veh ->
		#      FINAL ops=0 failed=13262). A 64-key saga must be driven at
		#      >= 64 vehicles.
		#   2. CEILING: the saga holds one of the eight in-flight flush slots
		#      for its whole prepare+commit round-trip, so the slots peg
		#      (inFlight 8/8) at a sustained ceiling of only ~450-530 key-
		#      writes/s. Driving near that ceiling is not reproducible: with
		#      the slots pinned, an Azure tail-latency burst times out a saga
		#      flush (validated: 450 veh -> 2/3, one cohort FINAL failed).
		# 100 veh satisfies both: above the 64-key floor, and well below the
		# saturation point so the flush slots stay unsaturated (inFlight ~1-2,
		# matching the proven cross-tree-atomic-64 @ 100 shape) and the in-
		# flight sub-sagas quiesce to zero before stop. All cohorts then
		# report HEALTHY at a conservative *sustained* saga-commit rate (not
		# the ceiling - see docs/lattice/throughput.md).
		# 100 veh x 5 Hz = 500 keys/s offered.
		Rung = '100:5:45';
	},
	@{
		Label = '`SetManyAtomicAsync` (2 keys/saga, single-tree, 20 veh/5 Hz)';
		WorkloadId = 'set-many-atomic-2';
		WorkloadMode = 'set-many-atomic-2';
		ThroughputUnit = 'keys/s';
		# Per-workload offered rung. Small-batch atomic sagas are saga-rate
		# bound (each 2-key SetManyAtomicAsync is a full two-phase commit
		# whose Azure Table WAL writes saturate a single Tables account at a
		# few hundred sagas/s). The uniform high rung (4000 veh) offers ~150x
		# the sustainable saga rate, so the bounded ingest channel fills and
		# the post-load drain tail cannot clear inside the host stop window -
		# a non-representative overload wedge, not a throughput measurement.
		# The offered rate is also held comfortably below the saturation
		# ceiling so the in-flight flush slots are not pinned at the cap; that
		# keeps the post-producer drain tail short (few stragglers to finalize)
		# and the cohort reliably HEALTHY. 20 veh x 5 Hz ~= 100 keys/s offered.
		Rung = '20:5:45';
	},
	@{
		Label = '`BeginAtomicWrite` cross-tree (2 keys/saga, 2 trees, 8 veh/5 Hz)';
		WorkloadId = 'cross-tree-atomic-2';
		WorkloadMode = 'cross-tree-atomic-2';
		ThroughputUnit = 'keys/s';
		# Cross-tree 2-key sagas carry the highest per-op coordination cost
		# (coordinator grain + one sub-saga AtomicWriteGrain per tree), so
		# their sustainable rate is the lowest of all modes and their post-
		# load settle (quiescing the in-flight sub-sagas) is the most drain-
		# sensitive. Measured saturation ceiling on the D8as_v5 host is
		# ~105 entries/s (steady mean pins there with the 8 in-flight flush
		# slots fully saturated); at 15 veh the offered rate exceeds that
		# ceiling, so an ingest backlog accumulates over the load window that
		# the post-producer quiesce cannot reliably drain -> intermittent
		# drain-tail WEDGE. Holding the offered rate to ~80 entries/s (about
		# three quarters of the ceiling) keeps the flush slots unsaturated,
		# so no backlog builds, the in-flight sub-sagas quiesce to zero within
		# the pre-stop wait, and the cohort drains immediately and reports
		# HEALTHY. 8 veh x 5 Hz x 2 keys ~= 80 keys/s offered.
		Rung = '8:5:45';
	},
	@{
		Label = '`BeginAtomicWrite` cross-tree (64 keys/saga, 2 trees, 150 veh/5 Hz)';
		WorkloadId = 'cross-tree-atomic-64';
		WorkloadMode = 'cross-tree-atomic-64';
		ThroughputUnit = 'keys/s';
		# Per-workload offered rung. Cross-tree 64-key sagas pay the saga
		# coordination cost (coordinator grain + one sub-saga AtomicWriteGrain
		# per tree) on top of the WAL-flush ceiling, and commit WAL writes to
		# two trees per saga, so they push the single Tables account hardest
		# of all the write modes - the eight flush slots peg (inFlight 8/8) at
		# a saturated ceiling of ~900-930 key-writes/s. Driving near that
		# ceiling is not reproducible: an Azure tail-latency burst times out a
		# pinned cross-tree flush (validated: 200 veh -> intermittent DEGRADED
		# with exception lines as the slots brush 8/8; 250-450 veh pin 8/8).
		# After the saga-commit-path optimisation that dropped the redundant
		# pre-fan-out participant union, a sub-saturation re-probe on the
		# D8as_v5 host showed 150 veh is the highest rung that keeps the flush
		# slots unsaturated (inFlight median ~3, max <8) so no flush times out,
		# the in-flight sub-sagas quiesce to zero before stop, and all cohorts
		# report HEALTHY at a conservative *sustained* rate (not the ceiling -
		# see docs/lattice/throughput.md): N=3 at 150 veh -> 3/3 HEALTHY,
		# inFlight med 2-3, failed=0. 150 veh x 5 Hz = 750 keys/s offered.
		Rung = '150:5:45';
	}
)

# ────────────────────────────────────────────────────────────────────────────
# Layer 3 rows: the SAME nine workloads as $Layer2Rows, at the SAME per-silo
# rungs.
#
# Layer 2 asks "what does one silo sustain, per operation?"; Layer 3 asks
# "what happens to that number as silos are added?". An earlier revision
# carried only get-many / set-many / set-point to hold the sweep's cost down.
# That left the atomic, cross-tree and materialised-view paths with no
# multi-silo evidence at all, and those are the paths whose coordination
# (saga coordinator + per-tree sub-sagas, view maintainer tailing the WAL)
# most plausibly behaves differently once its grains are spread across hosts.
# The grid is |workloads| x |SiloCounts| x N cohorts; at 9 x 5 x 2 that is
# 90 cohorts, which is the cost of covering the whole public surface.
#
# RungPerSilo is the STARTING per-silo rung, not a fixed one. Layer 3
# publishes true throughput: a cell whose completed-work rate reaches
# -SaturationRatio (default 0.9) of the offered rate measured the offered
# load, not the system, so Invoke-Layer3Cohorts doubles the rung and re-runs
# the cell (up to -MaxRungEscalations times) until the cell plateaus below
# what was offered. The saturating rung a workload settles on at one silo
# count is carried forward as the starting rung for the next, larger count.
# Starting rungs are therefore set well above each workload's known per-silo
# capacity so escalation is the exception; they deliberately no longer match
# the Layer 2 rungs, which hold a single silo below saturation for a
# different purpose. The rung is PER SILO and multiplied by the silo count at
# cohort time, so per-silo demand never falls as the cluster grows.
#
# FlushConcurrencyPerSilo is the client's in-flight bound per silo. The
# atomic and cross-tree modes run their sagas sequentially inside each flush
# slot, and the point modes fan out at most that many calls per slot, so for
# those workloads this bound - not the offered rate - is what an
# undersized client would measure. The rig default of 8 was measured to cap
# them below the cluster's own ceiling; 64 lifts that cap clear of it (the
# 2-key sagas reach the transaction-registry refusal ceiling there). The
# point modes fan each of the FlushConcurrencyPerSilo x N slots out into
# FlushConcurrencyPerSilo calls (BENCH_POINT_FANOUT), so their in-flight
# call count is FlushConcurrencyPerSilo^2 x N: linear in N, with constant
# per-silo demand. get-point, whose calls complete in milliseconds, takes 16.
# (Before #3474 the per-slot fan-out was the cohort bound, making the count
# (FlushConcurrencyPerSilo x N)^2, so per-silo demand grew with N.)
#
# CohortsPerCell raises -N for one workload. get-point needs it: its per-cell
# result is bimodal across cohorts, so the median of 2 is a coin toss.
#
# ChartGroup places each workload on one of the absolute-throughput charts.
# The nine span four orders of magnitude (tens of keys/s for 2-key cross-tree
# sagas, ~100k keys/s for batched reads), so a single linear axis would
# flatten every write mode onto zero. The normalised speedup chart plots all
# nine together, because speedup is dimensionless.
#
# ThroughputUnit and WorkloadMode match $Layer2Rows exactly, because the whole
# design of Layer 3 is that a cell means the same thing in both documents.
# ────────────────────────────────────────────────────────────────────────────
$Layer3ChartGroups = @(
	@{ Id = 'reads';  Title = 'Reads: sustained throughput vs silo count' },
	@{ Id = 'writes'; Title = 'Point and batched writes: sustained throughput vs silo count' },
	@{ Id = 'atomic'; Title = 'Atomic and cross-tree sagas: sustained throughput vs silo count' }
)
$Layer3Rows = @(
	@{
		Label = '`GetAsync` (point read)';
		WorkloadId = 'get-point';
		WorkloadMode = 'get-point';
		ThroughputUnit = 'keys/s';
		ChartGroup = 'reads';
		RungPerSilo = '8000:5:45';
		FlushConcurrencyPerSilo = 16;
		CohortsPerCell = 3;
	},
	@{
		Label = '`GetManyAsync` (4,096 keys/call)';
		WorkloadId = 'get-many';
		WorkloadMode = 'get-many';
		ThroughputUnit = 'keys/s';
		ChartGroup = 'reads';
		RungPerSilo = '16000:5:45';
	},
	@{
		Label = '`SetAsync` (point write)';
		WorkloadId = 'set-point';
		WorkloadMode = 'set-point';
		ThroughputUnit = 'keys/s';
		ChartGroup = 'writes';
		RungPerSilo = '1200:5:45';
		FlushConcurrencyPerSilo = 64;
	},
	@{
		Label = '`SetAsync` (point write + async materialised view)';
		WorkloadId = 'set-point-mv';
		WorkloadMode = 'set-point-mv';
		ThroughputUnit = 'keys/s';
		ChartGroup = 'writes';
		RungPerSilo = '1200:5:45';
		FlushConcurrencyPerSilo = 64;
	},
	@{
		Label = '`SetManyAsync` (4,096 keys/call)';
		WorkloadId = 'set-many';
		WorkloadMode = 'set-many';
		ThroughputUnit = 'keys/s';
		ChartGroup = 'writes';
		RungPerSilo = '2400:5:45';
	},
	@{
		Label = '`SetManyAtomicAsync` (64 keys/saga)';
		WorkloadId = 'set-many-atomic';
		WorkloadMode = 'set-many-atomic';
		ThroughputUnit = 'keys/s';
		ChartGroup = 'atomic';
		RungPerSilo = '800:5:45';
		FlushConcurrencyPerSilo = 64;
	},
	@{
		Label = '`SetManyAtomicAsync` (2 keys/saga, single-tree)';
		WorkloadId = 'set-many-atomic-2';
		WorkloadMode = 'set-many-atomic-2';
		ThroughputUnit = 'keys/s';
		ChartGroup = 'atomic';
		RungPerSilo = '200:5:45';
		FlushConcurrencyPerSilo = 64;
	},
	@{
		Label = '`BeginAtomicWrite` cross-tree (2 keys/saga, 2 trees)';
		WorkloadId = 'cross-tree-atomic-2';
		WorkloadMode = 'cross-tree-atomic-2';
		ThroughputUnit = 'keys/s';
		ChartGroup = 'atomic';
		RungPerSilo = '100:5:45';
		FlushConcurrencyPerSilo = 64;
	},
	@{
		Label = '`BeginAtomicWrite` cross-tree (64 keys/saga, 2 trees)';
		WorkloadId = 'cross-tree-atomic-64';
		WorkloadMode = 'cross-tree-atomic-64';
		ThroughputUnit = 'keys/s';
		ChartGroup = 'atomic';
		RungPerSilo = '800:5:45';
		FlushConcurrencyPerSilo = 64;
	}
)

# ────────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────────

function New-RunPrefix {
	# 'pr' + 7 random hex chars = 9 cleaned chars. The bicep storage-account
	# name is take('st' + cleanedPrefix + uniqueString(rg.id), 24). uniqueString
	# is always 13 chars; 2 ('st') + 9 (cleaned prefix) + 13 = 24 exactly.
	# Going above 9 cleaned chars silently truncates uniqueString and weakens
	# cross-run isolation.
	$bytes = New-Object byte[] 4
	[System.Security.Cryptography.RandomNumberGenerator]::Fill($bytes)
	$hex = [System.BitConverter]::ToString($bytes).Replace('-','').ToLowerInvariant()
	# take 7 chars from 8-char hex (one digit of headroom in case of cryptic
	# all-zero / vanity-collision concerns).
	return 'pr' + $hex.Substring(0, 7)
}

function Get-MainSha {
	<#
	.SYNOPSIS
		Returns the short sha of the upstream main commit this branch is built
		on top of. That is the comparable anchor for any reader of the
		published doc - the per-branch HEAD sha changes on every commit, but
		the main sha records the silo binary's lineage and pairs naturally
		with PR / changelog history.
	#>
	# Prefer the merge-base with origin/main (works even when the branch is
	# rebased onto an arbitrary main commit). Fall back to local main, then to
	# HEAD if neither ref exists.
	$candidates = @('origin/main', 'main', 'HEAD')
	foreach ($ref in $candidates) {
		$mb = (& git merge-base HEAD $ref 2>$null)
		if ($mb) {
			$short = (& git rev-parse --short $mb.Trim() 2>$null)
			if ($short) { return $short.Trim() }
		}
	}
	return $null
}

function Get-StateOr {
	<#
	.SYNOPSIS
		Strict-mode-safe lookup of a state hashtable key with a default value.
	.DESCRIPTION
		Under `Set-StrictMode -Version Latest`, '$ht.missingKey' throws for
		any container shape (Hashtable, OrderedDictionary,
		OrderedHashtable from ConvertFrom-Json -AsHashtable). The traditional
		'$ht.key ?? default' pattern fails on the lookup before the ??
		operator can kick in. This helper does the ContainsKey guard once.
		Returns the value when present and non-null; the default otherwise.
		Used in meta-header overlays so state.json files that pre-date a key
		(legacy from earlier script versions) round-trip cleanly.
	#>
	[CmdletBinding()] param(
		[Parameter(Mandatory)] $State,
		[Parameter(Mandatory)][string] $Key,
		$Default = $null
	)
	if ($null -eq $State) { return $Default }
	# run-cohort-aca.ps1 returns a [pscustomobject], not a hashtable, so the
	# ContainsKey guard alone is not enough: a PSCustomObject has no such
	# method and the call throws rather than missing. Probe the property set
	# instead for that shape, and keep the ContainsKey path for every
	# dictionary shape the doc-string enumerates.
	$present = if ($State -is [System.Collections.IDictionary]) {
		$State.ContainsKey($Key)
	} elseif ($State -is [psobject]) {
		$null -ne $State.PSObject.Properties[$Key]
	} else {
		$false
	}
	if ($present) {
		$v = $State.$Key
		if ($null -ne $v) { return $v }
	}
	return $Default
}

function Test-PreflightOrThrow {
	[CmdletBinding()]
	param([Parameter(Mandatory)][string] $ParametersFilePath)

	# 1. az logged in?
	$null = & az account show 2>$null
	if ($LASTEXITCODE -ne 0) {
		throw "az CLI not logged in. Run 'az login' first."
	}

	# 2. parameters file exists?
	if (-not (Test-Path $ParametersFilePath)) {
		throw "$ParametersFilePath does not exist. Copy benchmark/azure-throughput/scripts/parameters.ps1 to parameters.local.ps1 and edit SubscriptionId (and Location if non-default)."
	}

	# 3. ssh + scp + gh available? (gh is not actually used by this script;
	# only ssh + scp matter, both used transitively by the azure-throughput
	# scripts. We probe ssh as a representative.)
	#
	# Test for ssh via Get-Command rather than invoking `ssh -V`: `ssh -V`
	# writes its version banner to stderr (exit code 0), and under Windows
	# PowerShell 5.1 with $ErrorActionPreference='Stop' a native command that
	# writes to stderr is raised as a terminating NativeCommandError - even
	# with the stderr redirected (`2>&1` or `2>$null`) - which aborts the whole
	# preflight with a misleading failure though ssh is installed and working.
	# Get-Command inspects PATH without executing ssh, so there is no banner to
	# trip the engine.
	if (-not (Get-Command ssh -ErrorAction SilentlyContinue)) {
		throw "ssh not found in PATH. The script needs ssh + scp (used by the azure-throughput scripts) to drive the VM."
	}

	# 4. dotnet sdk for local doc-update path? (the actual benchmark runs
	# happen on the VM; locally we only need to write JSON.)
	# Nothing to enforce here.

	return $true
}

function Resolve-Rung {
	[CmdletBinding()]
	param([Parameter(Mandatory)][string] $Spec)

	$parts = $Spec.Split(':')
	if ($parts.Count -ne 3) {
		throw "Invalid -Rung '$Spec'; expected 'vehicles:tickHz:durationSec' (e.g. '4000:5:45')."
	}
	return @{
		Vehicles    = [int] $parts[0]
		TickHz      = [int] $parts[1]
		DurationSec = [int] $parts[2]
	}
}

function Resolve-WorkloadIds {
	[CmdletBinding()]
	param(
		[Parameter(Mandatory)] $LayerRows,
		[string] $WorkloadsSpec
	)

	$allIds = @($LayerRows | ForEach-Object { $_.WorkloadId })
	if ([string]::IsNullOrWhiteSpace($WorkloadsSpec) -or $WorkloadsSpec -eq 'all') {
		return $allIds
	}
	$requested = $WorkloadsSpec.Split(',') | ForEach-Object { $_.Trim() } | Where-Object { $_ }
	$unknown = @($requested | Where-Object { $_ -notin $allIds })
	if ($unknown.Count -gt 0) {
		throw "Unknown -Workloads ids: $($unknown -join ', '). Valid for this layer: $($allIds -join ', ')."
	}
	# Preserve doc row order so partial sweeps don't reorder cells.
	return @($allIds | Where-Object { $_ -in $requested })
}

function Get-LatestStateFile {
	[CmdletBinding()] param()
	# Find the most recently modified state.json under any prefix directory.
	$candidates = @(Get-ChildItem -Path $runRoot -Recurse -Filter 'state.json' -ErrorAction SilentlyContinue)
	if ($candidates.Count -eq 0) { return $null }
	return ($candidates | Sort-Object LastWriteTime -Descending | Select-Object -First 1).FullName
}

function Read-StateFile {
	[CmdletBinding()] param([Parameter(Mandatory)][string] $Path)
	$raw = [System.IO.File]::ReadAllText($Path)
	return ($raw | ConvertFrom-Json -AsHashtable)
}

function Write-StateFile {
	[CmdletBinding()] param(
		[Parameter(Mandatory)][string] $Path,
		[Parameter(Mandatory)][hashtable] $State
	)
	$dir = Split-Path -Parent $Path
	if (-not (Test-Path $dir)) { New-Item -ItemType Directory -Path $dir -Force | Out-Null }
	$json = $State | ConvertTo-Json -Depth 12
	[System.IO.File]::WriteAllText($Path, $json, [System.Text.UTF8Encoding]::new($false))
}

function New-EmptyState {
	[CmdletBinding()] param(
		[Parameter(Mandatory)][string] $Prefix,
		[Parameter(Mandatory)][string] $VmSize,
		[Parameter(Mandatory)][string] $Region,
		[Parameter(Mandatory)][hashtable] $Rung,
		[int] $ResponseTimeoutSec = 180,
		[int] $WalPartitions = 8,
		[int] $WalMaxPendingBatches = 16,
		[int] $BatchSize = 4096,
		[string] $BdnFidelity = 'dry'
	)
	return @{
		schema             = 'v1'
		prefix             = $Prefix
		vmSize             = $VmSize
		region             = $Region
		dotnetVersion      = $null
		gitSha             = (& git rev-parse --short HEAD 2>$null).Trim()
		mainSha            = (Get-MainSha)
		rung               = $Rung
		responseTimeoutSec = $ResponseTimeoutSec
		walPartitions      = $WalPartitions
		walMaxPendingBatches = $WalMaxPendingBatches
		batchSize          = $BatchSize
		bdnFidelity        = $BdnFidelity
		startedUtc         = (Get-Date).ToUniversalTime().ToString('o')
		endedUtc           = $null
		layer1 = @{
			cohorts = @()
			rows    = @{}
		}
		layer2 = @{
			cohorts = @{}
			rows    = @{}
		}
		# Layer 3 is keyed two levels deep (mode -> silo count -> cohorts)
		# because the silo count is its independent variable. It also carries
		# its own siloCounts / acaPrefix rather than reusing the Layer 1/2
		# vmSize + region, since it runs on a different rig entirely and a
		# Layer 2 re-run must not repaint Layer 3's provenance.
		layer3 = @{
			cohorts    = @{}
			rows       = @{}
			siloCounts = @()
			acaPrefix  = $null
		}
	}
}

function Get-CleanedPrefix {
	[CmdletBinding()] param([Parameter(Mandatory)][string] $Prefix)
	$cleaned = ($Prefix -replace '-','').ToLowerInvariant()
	if ($cleaned.Length -gt 9) {
		throw "Prefix '$Prefix' cleans to $cleaned ($($cleaned.Length) chars); must be <=9 chars or the storage-account name silently truncates uniqueString."
	}
	return $cleaned
}

# ────────────────────────────────────────────────────────────────────────────
# Provision / teardown
# ────────────────────────────────────────────────────────────────────────────

function Invoke-Provision {
	[CmdletBinding()] param(
		[Parameter(Mandatory)][string] $Prefix,
		[Parameter(Mandatory)][string] $VmSize,
		[string] $ParametersFilePath
	)
	$deployScript = Join-Path $azScriptsDir 'deploy.ps1'
	if (-not (Test-Path $deployScript)) { throw "Missing $deployScript" }
	Write-Host "[provision] deploy.ps1 -NamePrefix $Prefix -VmSize $VmSize" -ForegroundColor Cyan
	# Use HASHTABLE splat (not array). Array splat against ScriptBlock /
	# ExternalScript via & does NOT recognise '-Name value' pairs as named
	# args - it passes them through as positional, which then trips
	# 'A positional parameter cannot be found' against scripts whose param
	# block uses [CmdletBinding()] with no Position=N attributes. Hashtable
	# splat binds by name correctly.
	$argMap = @{
		NamePrefix = $Prefix
		VmSize     = $VmSize
	}
	if ($ParametersFilePath) { $argMap['ParametersFile'] = $ParametersFilePath }
	& $deployScript @argMap
	if ($LASTEXITCODE -ne 0) { throw "deploy.ps1 failed (exit $LASTEXITCODE)" }
}

function Invoke-Teardown {
	[CmdletBinding()] param(
		[Parameter(Mandatory)][string] $Prefix
	)
	$rg = "rg-$Prefix"

	# Refuse to delete a group whose ownership tag disagrees with the prefix we
	# were asked to tear down. `az group delete` is irreversible and takes
	# whatever name it is handed, so a mis-set -Prefix (or a -ReuseVm pointing
	# at something shared) would otherwise silently destroy an unrelated group.
	# deploy.ps1 stamps latticeBenchRun=<prefix> on every group it creates.
	#
	# An ABSENT tag is permitted, and that asymmetry is deliberate. Groups
	# created before the tag existed carry none, and refusing those would leave
	# a 4-vCPU VM billing indefinitely - trading a recoverable cost for an
	# unrecoverable one in the wrong direction. Only a tag that is present and
	# WRONG is evidence the group belongs to someone else.
	$tag = & az group show --name $rg --query "tags.latticeBenchRun" -o tsv 2>$null
	$tag = "$tag".Trim()
	if ($tag -and $tag -ne $Prefix) {
		Write-Warning "[teardown] REFUSING to delete '$rg': it is tagged latticeBenchRun='$tag', not '$Prefix'. If it really is yours, delete it by hand: az group delete --name $rg --yes"
		return
	}

	Write-Host "[teardown] az group delete --name $rg --yes --no-wait" -ForegroundColor Cyan
	& az group delete --name $rg --yes --no-wait 2>&1 | Out-Null
	if ($LASTEXITCODE -ne 0) {
		Write-Warning "az group delete for $rg returned exit $LASTEXITCODE. Manually verify and run 'az group delete --name $rg --yes' if needed."
	}
}

function Get-VmDotnetVersion {
	<#
	.SYNOPSIS
		Probe `/usr/bin/dotnet --version` over ssh on the just-provisioned VM
		and return the resulting "X.Y.Z" string. Returns $null on any failure
		(network blip, ssh handshake, missing dotnet) so the caller can fall
		back to the legacy '10.0.x' placeholder rather than abort the run.
	.DESCRIPTION
		Both the Layer 1 and Layer 2 meta-headers stamp `dotnet=` into the
		per-table marker block. Before this helper existed both were rendered
		as the hard-coded placeholder '10.0.x' because $state.dotnetVersion
		was initialised to $null in New-EmptyState and never populated; the
		actual version (e.g. 10.0.108) was only known to update.ps1 as a
		local variable. Probing the VM directly is the most reliable source:
		it captures the SDK version the silo + microbench actually use, not
		whatever ambient SDK the operator's host happens to have installed.
	#>
	[CmdletBinding()] param(
		[Parameter(Mandatory)][string] $Prefix,
		[Parameter(Mandatory)][string] $ParametersFilePath
	)
	$p = & $ParametersFilePath
	if ($Prefix) { $p.NamePrefix = $Prefix; $p.ResourceGroup = "rg-$Prefix" }
	$pipName = "$($p.NamePrefix)-pip"
	$fqdn = (& az network public-ip show -g $p.ResourceGroup -n $pipName --query dnsSettings.fqdn -o tsv 2>$null).Trim()
	if (-not $fqdn) { return $null }
	$sshTarget = "$($p.AdminUsername)@$fqdn"
	$sshOpts = @(
		'-o','StrictHostKeyChecking=accept-new',
		'-o','ServerAliveInterval=15',
		'-o','ServerAliveCountMax=3',
		'-o','ConnectTimeout=10'
	)
	# Bracket with `timeout 10` so a wedged ssh handshake cannot hang the
	# whole run. `|| true` keeps the remote shell exiting 0 on any failure;
	# the empty stdout then trips the trim-empty guard below and we fall
	# back gracefully.
	$ver = (& ssh @sshOpts $sshTarget 'timeout 10 /usr/bin/dotnet --version 2>/dev/null || true').Trim()
	if (-not $ver) { return $null }
	# Defensive sanity check: dotnet --version is a SemVer-shaped X.Y.Z
	# string. Reject anything that doesn't match so a malformed remote
	# response (the wrong binary on $PATH, a banner line, etc.) doesn't
	# leak into the meta-header.
	if ($ver -notmatch '^\d+\.\d+\.\d+([\-+].*)?$') { return $null }
	return $ver
}

# ────────────────────────────────────────────────────────────────────────────
# Layer 1 cohorts (BDN microbench on the VM)
# ────────────────────────────────────────────────────────────────────────────

function Invoke-Layer1Cohorts {
	[CmdletBinding()] param(
		[Parameter(Mandatory)][string] $Prefix,
		[Parameter(Mandatory)][string[]] $WorkloadIds,
		[Parameter(Mandatory)][int] $N,
		[Parameter(Mandatory)][string] $ParametersFilePath,
		[ValidateSet('dry','quick','full')]
		[string] $Fidelity = 'dry'
	)
	$rows = @($Layer1Rows | Where-Object { $_.WorkloadId -in $WorkloadIds })
	if ($rows.Count -eq 0) {
		Write-Warning "[layer1] no workloads matched WorkloadIds=$($WorkloadIds -join ',')"
		return @()
	}

	# Build the BDN --filter spec (comma-separated *Method* globs; matches the
	# benchmark.ps1 convention).
	$filterPatterns = ($rows | ForEach-Object { '*.' + $_.BdnMethod }) -join ','
	Write-Host "[layer1] filter: $filterPatterns" -ForegroundColor DarkGray

	# SSH plumbing: resolve the VM host alias via parameters + az network public-ip.
	$p = & $ParametersFilePath
	if ($Prefix) { $p.NamePrefix = $Prefix; $p.ResourceGroup = "rg-$Prefix" }
	$pipName = "$($p.NamePrefix)-pip"
	$fqdn = (& az network public-ip show -g $p.ResourceGroup -n $pipName --query dnsSettings.fqdn -o tsv).Trim()
	$sshTarget = "$($p.AdminUsername)@$fqdn"
	$sshOpts = @(
		'-o','StrictHostKeyChecking=accept-new',
		'-o','ServerAliveInterval=15',
		'-o','ServerAliveCountMax=3',
		'-o','ConnectTimeout=10'
	)

	$prefixDir = Join-Path $runRoot $Prefix
	$mbDir = Join-Path $prefixDir 'microbench'
	if (-not (Test-Path $mbDir)) { New-Item -ItemType Directory -Path $mbDir -Force | Out-Null }

	# One-time build of the microbench project on the VM. update.ps1 only
	# publishes Silo + Producer; the microbench project is built on demand here
	# so the per-cohort `dotnet run --no-build` can find the assembly. We pay
	# the ~30s build cost once and amortise across all N cohorts (and across
	# all workloads within each cohort - BDN's filter just selects [Benchmark]
	# methods from the already-built assembly).
	Write-Host "[layer1] building Bench.Microbench on the VM (one-time, ~30s) ..." -ForegroundColor Cyan
	$buildCmd = "cd /opt/lattice/src && /usr/bin/dotnet build benchmark/host/Bench.Microbench/Orleans.Lattice.Benchmark.Microbench.csproj -c Release --nologo /clp:ErrorsOnly"
	# `| Out-Host` routes the remote build / ssh / scp stdout directly to the
	# console instead of letting it accumulate on this function's success
	# stream. PowerShell folds every uncaptured native-command stdout line
	# into the function's return value, so without the Out-Host pipe the
	# returned cohort array would be polluted with BDN progress text and
	# the caller's `foreach ($c in $Cohorts) { $c.metrics ... }` would
	# fail with 'The property metrics cannot be found on this object'
	# the first time it hit a stringified BDN log line. `$LASTEXITCODE` is
	# preserved through the pipe (it is set by the native command, not by
	# the pipeline).
	& ssh @sshOpts $sshTarget $buildCmd | Out-Host
	if ($LASTEXITCODE -ne 0) {
		throw "[layer1] microbench build on the VM failed (ssh exit $LASTEXITCODE). Cannot continue without the build output."
	}

	$cohorts = New-Object System.Collections.Generic.List[hashtable]
	for ($i = 1; $i -le $N; $i++) {
		$runId = (Get-Date).ToUniversalTime().ToString('yyyy-MM-ddTHH-mm-ssZ') + "-$i"
		$remoteResults = "/opt/lattice/.run/microbench/$runId/results.json"
		$localResults = Join-Path $mbDir "results-$runId.json"

		Write-Host "[layer1] cohort $i/$N runId=$runId ..." -ForegroundColor Cyan
		# Build the remote command. The microbench's Bench.Microbench Program.cs
		# accepts --results <path> and --filter <comma-globs>; we pin
		# BENCH_REGRESSION_GATE_ENABLED=false because this script's job is to
		# CREATE the new baseline, not gate against an old one.
		$remoteCmd = "cd /opt/lattice/src && BENCH_REGRESSION_GATE_ENABLED=false BENCH_MICROBENCH_FIDELITY=$Fidelity BENCH_MICROBENCH_WORKLOADS='$filterPatterns' BENCH_SCENARIO=performance-report-layer1 BENCH_RUN_ID=$runId /usr/bin/dotnet run --project benchmark/host/Bench.Microbench/Orleans.Lattice.Benchmark.Microbench.csproj -c Release --no-build -- --results $remoteResults"
		# See the Out-Host rationale on the build call above; the per-cohort
		# `dotnet run` produces hundreds of BDN progress + result lines and
		# is the dominant source of pollution if uncaptured.
		& ssh @sshOpts $sshTarget $remoteCmd | Out-Host
		if ($LASTEXITCODE -ne 0) {
			Write-Warning "[layer1] cohort $i/${N}: ssh microbench run exited $LASTEXITCODE; skipping pull"
			continue
		}
		# Pull results.json back.
		& scp @sshOpts "${sshTarget}:$remoteResults" $localResults | Out-Host
		if ($LASTEXITCODE -ne 0) {
			Write-Warning "[layer1] cohort $i/${N}: scp pull failed; results may be missing"
			continue
		}
		if (-not (Test-Path $localResults)) {
			Write-Warning "[layer1] cohort $i/${N}: results file did not land at $localResults"
			continue
		}

		$payload = (Get-Content $localResults -Raw | ConvertFrom-Json -AsHashtable)
		$cohorts.Add(@{
			runId   = $runId
			resultsPath = $localResults
			metrics = $payload.metrics
		})
	}
	# Return as a flat array. `,$x.ToArray()` wraps in a 1-element outer array
	# which the caller's foreach then iterates as a single (array-typed) cohort.
	# `@($x.ToArray())` always yields a flat array; empty input stays empty.
	#
	# `Where-Object { $_ -is [hashtable] }` type-narrows the explicit return
	# expression. It does NOT catch stray Write-Outputs leaked earlier in
	# the function body - PowerShell merges every uncaptured success-stream
	# write into the function's pipeline output, so the filter only operates
	# on what the `return` statement itself emits. The source-of-truth
	# protection is the `| Out-Host` pipes on every `& <native>` invocation
	# above; the filter just keeps the explicit return strictly hashtable-
	# typed so a future caller iterating $cohorts can rely on the shape.
	return @($cohorts.ToArray() | Where-Object { $_ -is [hashtable] })
}

# ────────────────────────────────────────────────────────────────────────────
# Layer 2 cohorts (silo + producer per workload mode)
# ────────────────────────────────────────────────────────────────────────────

function Invoke-Layer2Cohorts {
	[CmdletBinding()] param(
		[Parameter(Mandatory)][string] $Prefix,
		[Parameter(Mandatory)][string[]] $WorkloadIds,
		[Parameter(Mandatory)][hashtable] $Rung,
		[Parameter(Mandatory)][int] $N,
		[Parameter(Mandatory)][string] $ParametersFilePath,
		[int] $ResponseTimeoutSec = 180,
		[int] $WalPartitions = 8,
		[int] $WalMaxPendingBatches = 16,
		[int] $BatchSize = 4096,
		[switch] $CaptureCounters
	)
	$rows = @($Layer2Rows | Where-Object { $_.WorkloadId -in $WorkloadIds })
	if ($rows.Count -eq 0) {
		Write-Warning "[layer2] no workloads matched WorkloadIds=$($WorkloadIds -join ',')"
		return @{}
	}

	$runCohort = Join-Path $azScriptsDir 'run-cohort.ps1'
	if (-not (Test-Path $runCohort)) { throw "Missing $runCohort" }

	$prefixDir = Join-Path $runRoot $Prefix
	$l2Dir = Join-Path $prefixDir 'azure-throughput'
	if (-not (Test-Path $l2Dir)) { New-Item -ItemType Directory -Path $l2Dir -Force | Out-Null }

	# Source dir where run-cohort.ps1 lands its per-cohort artefacts.
	$cohortSourceDir = Join-Path $benchmarkRoot '.run/azure-throughput'

	$result = @{}
	foreach ($row in $rows) {
		$mode = $row.WorkloadMode
		# Per-workload offered rung: a row may override the sweep-wide $Rung
		# with its own 'vehicles:tickHz:durationSec' spec (see $Layer2Rows).
		# Small-batch atomic modes use a reduced rung so they are driven at
		# their sustainable saga rate instead of being over-driven into a
		# non-representative drain-tail wedge by the uniform high rung.
		$rowRung = if ($row.ContainsKey('Rung') -and $row.Rung) { Resolve-Rung -Spec $row.Rung } else { $Rung }
		Write-Host "[layer2] mode=$mode N=$N rung=$($rowRung.Vehicles)veh/$($rowRung.TickHz)Hz/$($rowRung.DurationSec)s" -ForegroundColor Cyan
		$cohortList = New-Object System.Collections.Generic.List[hashtable]

		for ($i = 1; $i -le $N; $i++) {
			$extraEnv = @{
				BENCH_RESPONSE_TIMEOUT_SEC    = "$ResponseTimeoutSec"
				BENCH_WORKLOAD_MODE           = $mode
				BENCH_WAL_PARTITIONS          = "$WalPartitions"
				BENCH_WAL_MAX_PENDING_BATCHES = "$WalMaxPendingBatches"
				BENCH_BATCH_SIZE              = "$BatchSize"
			}
			Write-Host "[layer2] cohort $i/$N mode=$mode ..." -ForegroundColor DarkGray
			# Capture the set of pre-existing log files so we can identify the
			# new one this run produced.
			$before = @(Get-ChildItem $cohortSourceDir -Filter "silo-*.log" -ErrorAction SilentlyContinue | ForEach-Object FullName)
			# `| Out-Host` routes the run-cohort.ps1 progress lines (silo +
			# producer ssh streams, az status pokes, etc.) to the console
			# instead of letting them accumulate on this function's success
			# stream. Without it the returned $result hashtable is polluted
			# with stringified progress lines that downstream consumers
			# treating $result as an enumerable would trip over; even the
			# narrow `foreach ($mode in $l2CohortsByMode.Keys)` consumer is
			# safer with the source-of-truth fix here than relying on key
			# enumeration to skip the noise. Mirrors the Out-Host pattern in
			# Invoke-Layer1Cohorts.
			& $runCohort `
				-Vehicles    $rowRung.Vehicles `
				-TickHz      $rowRung.TickHz `
				-DurationSec $rowRung.DurationSec `
				-NamePrefix  $Prefix `
				-ParametersFile $ParametersFilePath `
				-CaptureCounters:$CaptureCounters `
				-ExtraSiloEnv $extraEnv | Out-Host
			if ($LASTEXITCODE -ne 0) {
				Write-Warning "[layer2] cohort $i/$N (mode=$mode): run-cohort.ps1 exited $LASTEXITCODE; skipping"
				continue
			}
			$after = @(Get-ChildItem $cohortSourceDir -Filter "silo-*.log" -ErrorAction SilentlyContinue | ForEach-Object FullName)
			$newLogs = @($after | Where-Object { $_ -notin $before })
			if ($newLogs.Count -eq 0) {
				Write-Warning "[layer2] cohort $i/$N (mode=$mode): no new silo-*.log appeared in $cohortSourceDir"
				continue
			}
			$newest = ($newLogs | Sort-Object | Select-Object -Last 1)
			$cohortName = ([System.IO.Path]::GetFileNameWithoutExtension($newest)) -replace '^silo-', ''
			# Copy the silo log into the per-prefix dir so the prefix's artefacts
			# are self-contained.
			$siloDest = Join-Path $l2Dir "silo-$cohortName.log"
			Copy-Item $newest $siloDest -Force

			# Parse the steady-state mean and per-call p50/p75/p90/p99 from the silo log.
			$parsed = Read-SiloLogStats -SiloLogPath $siloDest -WorkloadMode $mode -BatchSize $BatchSize
			$cohortList.Add(@{
				cohortName    = $cohortName
				siloLog       = $siloDest
				steadyMean    = $parsed.SteadyMean
				perCallP50Ms  = $parsed.PerCallP50Ms
				perCallP75Ms  = $parsed.PerCallP75Ms
				perCallP90Ms  = $parsed.PerCallP90Ms
				perCallP99Ms  = $parsed.PerCallP99Ms
				inFlightMax   = $parsed.InFlightMax
				failed        = $parsed.Failed
				verdict       = $parsed.Verdict
				rungVehicles  = $rowRung.Vehicles
				rungTickHz    = $rowRung.TickHz
				rungDurationSec = $rowRung.DurationSec
			})
		}
		$result[$mode] = @($cohortList.ToArray())
	}
	return $result
}

# ────────────────────────────────────────────────────────────────────────────
# Layer 3 cohorts (multi-silo ACA cluster; one cell per silo count x workload)
# ────────────────────────────────────────────────────────────────────────────

function Invoke-Layer3Cohorts {
	<#
	.SYNOPSIS
		Sweep the silo count, running N cohorts per (silo count, workload)
		cell against an already-provisioned ACA rig.
	.DESCRIPTION
		The sweep is ordered silo-count-outermost and ascending, and it
		invokes $OnCellComplete after every cell. Both matter for an
		unattended run: a sweep that is abandoned (quota, spend cap, a
		wedged cohort, an operator Ctrl+C) then still yields a usable
		PREFIX of the scaling curve - 1,2,4 is a publishable curve, whereas
		an arbitrary scatter of 2,8,4 is not - and the checkpoint means the
		already-paid-for cells survive the abandonment.

		The offered load is scaled with the silo count: each row carries a
		per-silo rung, and the cohort is driven at (rung x N). Holding the
		per-silo demand constant as the cluster grows is what makes the
		result a capacity curve. Driving a FIXED total load across a
		growing cluster would instead measure latency under a thinning
		load, which would flatten into a meaningless plateau the moment the
		cluster outgrew the load, and would be read as a knee that is not
		there.
	#>
	[CmdletBinding()] param(
		[Parameter(Mandatory)][string] $AcaPrefix,
		[Parameter(Mandatory)][string[]] $WorkloadIds,
		[Parameter(Mandatory)][int[]] $SiloCounts,
		[Parameter(Mandatory)][int] $N,
		[int] $BatchSize = 4096,
		# ShardCount and WalPartitions are passed explicitly rather than left
		# to run-cohort-aca.ps1's own defaults. The published metadata has to
		# state the configuration that actually ran, and the only way to
		# guarantee that is for the same values to reach both the cohort
		# script and the metadata. When these were left implicit the doc
		# reported walPartitions=8 (the Layer 2 VM default) for a sweep that
		# ran at 16.
		[int] $ShardCount = 64,
		[int] $WalPartitions = 16,
		# Not a knob on this path: run-cohort-aca.ps1 takes no
		# -WalMaxPendingBatches, so this is the producer's own default and is
		# declared here only so the metadata has a single source for it. If
		# the producer default ever moves, this must move with it.
		[int] $WalMaxPendingBatches = 16,
		# (#3348) The rig's two deliberately-set saturation budgets. 0 means
		# infinite, i.e. inherit the shipped library default. See the matching
		# block in run-cohort-aca.ps1 for why these are parameters: the sweep
		# has to be runnable both at the shipped defaults and at the
		# recommended values, because only the former answers whether a
		# deployment that configures nothing is fixed.
		[int] $SetManyFanOutBudgetSec = 30,
		[int] $WalAdmissionCallBudgetSec = 15,
		[int] $WalAppendCoalescingInFlightThreshold = -1,
		[int] $WalSaturationRecoveryReleaseBatch = -1,
		[int] $ClientsPerSilo = 4,
		# True-throughput escalation. A cohort whose completed-work rate is
		# at least SaturationRatio x the offered rate measured the offered
		# load, not the cluster, so its rung is doubled and the cohort re-run,
		# at most MaxRungEscalations times per cell. 0 disables escalation.
		[double] $SaturationRatio = 0.9,
		[int] $MaxRungEscalations = 3,
		# Cells from an earlier run of this sweep (state.layer3.cohorts),
		# keyed [mode]["silos"]. A cell already holding $N cohorts is carried
		# over and skipped; see -Resume.
		[hashtable] $ExistingCells = @{},
		[scriptblock] $OnCellComplete
	)
	$rows = @($Layer3Rows | Where-Object { $_.WorkloadId -in $WorkloadIds })
	if ($rows.Count -eq 0) {
		Write-Warning "[layer3] no workloads matched WorkloadIds=$($WorkloadIds -join ',')"
		return @{}
	}

	$runCohort = Join-Path $azScriptsDir 'run-cohort-aca.ps1'
	if (-not (Test-Path $runCohort)) { throw "Missing $runCohort" }
	# Get-AcaRunRoot: the cohort log paths are derived, not captured from the
	# pipeline, so this function needs the same run-root resolver the cohort
	# script itself uses. Dot-sourcing here rather than at script scope keeps
	# the ACA module off the Layer 1/2 path entirely.
	. (Join-Path $azScriptsDir 'aca-common.ps1')

	# cells[mode][siloCount] = @(cohort, cohort, ...)
	$cells = @{}
	foreach ($row in $rows) { $cells[$row.WorkloadMode] = @{} }
	foreach ($mode in @($ExistingCells.Keys)) {
		if (-not $cells.ContainsKey($mode)) { $cells[$mode] = @{} }
		foreach ($k in @($ExistingCells[$mode].Keys)) { $cells[$mode]["$k"] = @($ExistingCells[$mode][$k]) }
	}

	# The saturating per-silo rung each workload settled on at the previous
	# (smaller) silo count. Per-silo capacity does not grow as silos are
	# added, so a rung that saturated N silos is the right place to start
	# N+1 and saves re-climbing the escalation ladder at every count. Seeded
	# from resumed cells so a resumed sweep does not restart low.
	$carryVehiclesPerSilo = @{}
	foreach ($mode in @($cells.Keys)) {
		foreach ($k in @($cells[$mode].Keys)) {
			foreach ($c in @($cells[$mode][$k])) {
				if (-not $c) { continue }
				$sc = [int](Get-StateOr $c 'siloCount' ([int]$k))
				if ($sc -le 0) { continue }
				$v = [int]([int](Get-StateOr $c 'rungVehicles' 0) / $sc)
				if (-not $carryVehiclesPerSilo.ContainsKey($mode) -or $v -gt $carryVehiclesPerSilo[$mode]) { $carryVehiclesPerSilo[$mode] = $v }
			}
		}
	}

	foreach ($silos in ($SiloCounts | Sort-Object)) {
		foreach ($row in $rows) {
			$mode = $row.WorkloadMode
			$cellN = [Math]::Max($N, [int](Get-StateOr $row 'CohortsPerCell' $N))
			$fcPerSilo = [int](Get-StateOr $row 'FlushConcurrencyPerSilo' 8)
			if ($cells[$mode].ContainsKey("$silos") -and @($cells[$mode]["$silos"]).Count -ge $cellN) {
				Write-Host "[layer3] silos=$silos mode=${mode}: $(@($cells[$mode]["$silos"]).Count) cohort(s) already recorded; skipping (resume)" -ForegroundColor DarkGray
				continue
			}
			$perSilo = Resolve-Rung -Spec $row.RungPerSilo
			$vehPerSilo = $perSilo.Vehicles
			if ($carryVehiclesPerSilo.ContainsKey($mode) -and $carryVehiclesPerSilo[$mode] -gt $vehPerSilo) { $vehPerSilo = $carryVehiclesPerSilo[$mode] }

			$cohortList = New-Object System.Collections.Generic.List[object]
			# A cell resumed with fewer than N cohorts is topped up rather
			# than re-run: its recorded cohorts are kept and numbering
			# continues after them, so raising -N on -Resume adds tie-break
			# cohorts to a cell without discarding what it already paid for.
			# The top-up runs at the recorded cohorts' rung so a cell never
			# mixes offered loads.
			$firstCohort = 1
			if ($cells[$mode].ContainsKey("$silos")) {
				foreach ($existing in @($cells[$mode]["$silos"])) { if ($existing) { $cohortList.Add($existing) } }
				$firstCohort = $cohortList.Count + 1
				if ($cohortList.Count -gt 0) { $vehPerSilo = [int]([int](Get-StateOr $cohortList[0] 'rungVehicles' ($vehPerSilo * $silos)) / $silos) }
			}
			Write-Host "[layer3] silos=$silos mode=$mode rung=$($vehPerSilo * $silos)veh/$($perSilo.TickHz)Hz/$($perSilo.DurationSec)s (= $vehPerSilo veh/silo) flushConcurrency=$fcPerSilo/silo cohorts=$cellN" -ForegroundColor Cyan

			for ($i = $firstCohort; $i -le $cellN; $i++) {
				# Probes run at a rung the cell went on to outgrow. They are
				# recorded on the accepted cohort as evidence for why the
				# published rung is what it is, never aggregated.
				$probes = New-Object System.Collections.Generic.List[object]
				$escalations = 0
				$accepted = $null
				while ($true) {
					$vehicles = $vehPerSilo * $silos
					$offered = $vehicles * $perSilo.TickHz
					Write-Host "[layer3] cohort $i/$cellN silos=$silos mode=$mode veh/silo=$vehPerSilo offered=$offered keys/s ..." -ForegroundColor DarkGray
					# `| Out-Host` for exactly the reason Invoke-Layer2Cohorts
					# gives: run-cohort-aca.ps1 emits a lot of az/cohort progress
					# chatter, and without this it lands on THIS function's
					# success stream and is returned alongside the cells hashtable.
					#
					# The cohort's own return object is deliberately not captured
					# from the pipeline. The log path is fully determined by
					# (prefix, silos, mode, tag), so derive it instead of fishing
					# for it.
					$cohortTag = "c$i"
					$expectedLog = Join-Path (Get-AcaRunRoot) "$AcaPrefix.n$silos.$mode.$cohortTag.log"
					if (Test-Path $expectedLog) { Remove-Item $expectedLog -Force }
					try {
						& $runCohort `
							-NamePrefix       $AcaPrefix `
							-SiloCount        $silos `
							-WorkloadMode     $mode `
							-DurationSec      $perSilo.DurationSec `
							-VehiclesPerSilo  $vehPerSilo `
							-TickHz           $perSilo.TickHz `
							-BatchSize        $BatchSize `
							-FlushConcurrencyPerSilo $fcPerSilo `
							-ShardCount       $ShardCount `
							-WalPartitions    $WalPartitions `
							-SetManyFanOutBudgetSec    $SetManyFanOutBudgetSec `
							-WalAdmissionCallBudgetSec $WalAdmissionCallBudgetSec `
							-WalAppendCoalescingInFlightThreshold $WalAppendCoalescingInFlightThreshold `
							-WalSaturationRecoveryReleaseBatch $WalSaturationRecoveryReleaseBatch `
							-ClientsPerSilo   $ClientsPerSilo `
							-CohortTag        $cohortTag | Out-Host
					} catch {
						Write-Warning "[layer3] cohort $i/$cellN (silos=$silos mode=$mode) threw: $($_.Exception.Message)"
						break
					}
					if (-not (Test-Path $expectedLog)) {
						Write-Warning "[layer3] cohort $i/$cellN (silos=$silos mode=$mode): no cohort log at $expectedLog; skipping"
						break
					}
					$parsed = Read-SiloLogStats -SiloLogPath $expectedLog -WorkloadMode $mode -BatchSize $BatchSize
					$entry = @{
						cohortName      = [System.IO.Path]::GetFileNameWithoutExtension($expectedLog)
						siloLog         = $expectedLog
						siloCount       = $silos
						steadyMean      = $parsed.SteadyMean
						finalOps        = $parsed.FinalOps
						finalActiveSec  = $parsed.FinalActiveSec
						finalThroughput = $parsed.FinalThroughput
						perCallP50Ms    = $parsed.PerCallP50Ms
						perCallP75Ms    = $parsed.PerCallP75Ms
						perCallP90Ms    = $parsed.PerCallP90Ms
						perCallP99Ms    = $parsed.PerCallP99Ms
						inFlightMax     = $parsed.InFlightMax
						failed          = $parsed.Failed
						verdict         = $parsed.Verdict
						rungVehicles    = $vehicles
						rungTickHz      = $perSilo.TickHz
						rungDurationSec = $perSilo.DurationSec
						flushConcurrencyPerSilo = $fcPerSilo
						clientsPerSilo  = $ClientsPerSilo
						offerBound      = $false
						executionState  = 'unknown'
					}
					Set-Layer3ProducerEvidence -Cohort $entry -LogPath $expectedLog
					Set-Layer3PreseedEvidence -Cohort $entry -LogPath $expectedLog -WorkloadMode $mode
					Write-Host ("[layer3]   -> {0} {1} {2} keys/s completed of {3} offered ({4} steady-mean, failed={5})" -f $parsed.Verdict, $mode, $parsed.FinalThroughput, $offered, $parsed.SteadyMean, $parsed.Failed) -ForegroundColor DarkGray

					# Only the first cohort of a cell escalates, so every cohort
					# the cell publishes ran at the same offered load. A cohort
					# that is not HEALTHY is not evidence of headroom either way,
					# so it never escalates.
					$reachedOffered = ($SaturationRatio -gt 0) -and ($parsed.Verdict -eq 'HEALTHY') -and ($null -ne $parsed.FinalThroughput) -and ([double]$parsed.FinalThroughput -ge ($SaturationRatio * $offered))
					if ($entry.producerBound) { $accepted = $entry; break }
					# An unseeded read cohort measured the miss path; a higher
					# offered load would not make it a read measurement.
					if ($entry.unseeded) { $accepted = $entry; break }
					if (-not $reachedOffered) { $accepted = $entry; break }
					if ($cohortList.Count -gt 0 -or $escalations -ge $MaxRungEscalations) {
						$entry.offerBound = $true
						Write-Warning "[layer3] cohort $i/$cellN (silos=$silos mode=$mode): completed $($parsed.FinalThroughput) of $offered offered keys/s, which is offer-bound; not escalating further ($escalations escalation(s) used). The cell is a lower bound."
						$accepted = $entry
						break
					}
					# Keep the probe's log under a name the next attempt will
					# not overwrite, then double the offered load.
					$probeLog = Join-Path (Get-AcaRunRoot) "$AcaPrefix.n$silos.$mode.$cohortTag.v$vehPerSilo.probe.log"
					if (Test-Path $probeLog) { Remove-Item $probeLog -Force }
					Move-Item $expectedLog $probeLog -Force
					$probes.Add(@{ vehiclesPerSilo = $vehPerSilo; offeredKeysPerSec = $offered; finalThroughput = $parsed.FinalThroughput; siloLog = $probeLog })
					Write-Host "[layer3]   offer-bound at $vehPerSilo veh/silo; escalating to $($vehPerSilo * 2) veh/silo" -ForegroundColor Yellow
					$vehPerSilo = $vehPerSilo * 2
					$escalations++
				}
				if ($null -eq $accepted) { continue }
				if ($probes.Count -gt 0) { $accepted['escalationProbes'] = @($probes.ToArray()) }
				$cohortList.Add($accepted)
			}
			$carryVehiclesPerSilo[$mode] = $vehPerSilo
			$cells[$mode]["$silos"] = @($cohortList.ToArray())

			# Checkpoint after EVERY cell, not at the end of the sweep. An
			# unattended multi-hour run that dies at silos=6 must not lose
			# the 1, 2 and 4 cells it already paid Azure for.
			if ($OnCellComplete) { & $OnCellComplete $cells }
		}
	}
	return $cells
}

function Aggregate-Layer3Cells {
	<#
	.SYNOPSIS
		Reduce the per-(mode, silo count) cohort lists to one published cell
		each, and derive the scaling-efficiency columns.
	.DESCRIPTION
		Uses exactly the Layer 2 reduction - median across HEALTHY cohorts -
		so a Layer 3 cell and a Layer 2 cell are computed identically and
		can be compared without an asterisk.

		Additionally derives, per cell, the speedup over the N=1 anchor and
		the per-silo efficiency. Those two columns are the whole point of
		the table: a raw throughput column alone does not tell a reader
		whether 8 silos are earning their keep, and the knee of the curve
		is far easier to see in the efficiency column than in the
		throughput one.
	#>
	[CmdletBinding()] param([Parameter(Mandatory)][hashtable] $Cells)
	$rows = @{}
	foreach ($row in $Layer3Rows) {
		$mode = $row.WorkloadMode
		if (-not $Cells.ContainsKey($mode)) { continue }
		$byCount = $Cells[$mode]
		$perCount = @{}
		foreach ($key in @($byCount.Keys)) {
			$cohorts = @($byCount[$key])
			if ($cohorts.Count -eq 0) { continue }
			foreach ($cohort in $cohorts) {
				$log = Get-StateOr $cohort 'siloLog' $null
				if ($log -and (Test-Path $log)) {
					Set-Layer3ProducerEvidence -Cohort $cohort -LogPath $log
					Set-Layer3PreseedEvidence -Cohort $cohort -LogPath $log -WorkloadMode $mode
				}
			}
			# A read cohort whose log carries no producer preseed line read an
			# empty tree, so it measured the miss path rather than the read
			# path (#3474). Refuse it before the HEALTHY reduction.
			$unseeded = @($cohorts | Where-Object { Get-StateOr $_ 'unseeded' $false })
			if ($unseeded.Count -gt 0) {
				Write-Warning "[aggregate-l3] mode=$mode silos=${key}: excluding $($unseeded.Count)/$($cohorts.Count) UNSEEDED cohort(s) (no '[producer] preseed' line; the read keyspace was empty)"
				$cohorts = @($cohorts | Where-Object { -not (Get-StateOr $_ 'unseeded' $false) })
			}
			# Same HEALTHY-only rule as Layer 2: a wedged cohort's reporter
			# often closes on a single sample, so its quantiles collapse to
			# one wildly inflated value and its steady mean is depressed by
			# drain-tail thrash. Either would distort the curve at exactly
			# the silo counts where the curve matters most.
			$healthy = @($cohorts | Where-Object { (Get-StateOr $_ 'verdict' '') -eq 'HEALTHY' })
			if ($healthy.Count -lt $cohorts.Count) {
				Write-Warning "[aggregate-l3] mode=$mode silos=${key}: excluding $($cohorts.Count - $healthy.Count)/$($cohorts.Count) non-HEALTHY cohort(s)"
			}
			if ($healthy.Count -eq 0) {
				Write-Warning "[aggregate-l3] mode=$mode silos=${key}: no HEALTHY cohorts; cell omitted"
				continue
			}
			# Layer 3 publishes completed-ops / active-elapsed, not the
			# rate>0 steady-state mean. See the rationale on FinalThroughput
			# in Read-SiloLogStats: client-side 4096-key batching makes the
			# per-second samples bimodal, so the rate>0 filter averages only
			# the spikes and reports more throughput than was offered. Fall
			# back to steadyMean only when a log predates the FINAL parsing,
			# and say so rather than silently mixing two definitions.
			$throughputs = @($healthy | ForEach-Object { $_.finalThroughput } | Where-Object { $_ -gt 0 })
			$throughputBasis = 'final'
			if ($throughputs.Count -eq 0) {
				$throughputs = @($healthy | ForEach-Object { $_.steadyMean } | Where-Object { $_ -gt 0 })
				$throughputBasis = 'steady-mean (fallback)'
				if ($throughputs.Count -gt 0) {
					Write-Warning "[aggregate-l3] mode=$mode silos=${key}: no FINAL throughput parsed; falling back to the steady-state mean, which overstates a batched write path."
				}
			}
			if ($throughputs.Count -eq 0) {
				Write-Warning "[aggregate-l3] mode=$mode silos=${key}: no positive throughput samples; cell omitted"
				continue
			}
			# A cohort can be graded HEALTHY - it produced productive
			# measurement windows - and still contribute no throughput
			# sample, because the FINAL line never landed in the harvest
			# window (the engine was observed still inside DrainAsync when
			# the harvest closed). Such a cohort is silently absent from the
			# median above. Publishing $healthy.Count as the cell's n would
			# then overstate how much evidence backs the number, which is
			# the same "announces more than it did" failure this harness has
			# been bitten by repeatedly. Count the samples actually used,
			# and say so when the two disagree.
			if ($throughputs.Count -lt $healthy.Count) {
				Write-Warning "[aggregate-l3] mode=$mode silos=${key}: $($healthy.Count - $throughputs.Count)/$($healthy.Count) HEALTHY cohort(s) produced no FINAL throughput line and are excluded from the median; publishing n=$($throughputs.Count)"
			}
			$p50s = @($healthy | ForEach-Object { $_.perCallP50Ms } | Where-Object { $null -ne $_ })
			$p99s = @($healthy | ForEach-Object { $_.perCallP99Ms } | Where-Object { $null -ne $_ })
			$perCount["$key"] = @{
				siloCount           = [int]$key
				sustainedThroughput = [int][math]::Round((Get-Median $throughputs), 0)
				throughputBasis     = $throughputBasis
				perCallP50Ms        = if ($p50s.Count -gt 0) { [math]::Round((Get-Median $p50s), 2) } else { $null }
				perCallP99Ms        = if ($p99s.Count -gt 0) { [math]::Round((Get-Median $p99s), 2) } else { $null }
				cohortN             = $throughputs.Count
				offeredKeysPerSec   = [int](($healthy | Select-Object -First 1).rungVehicles) * [int](($healthy | Select-Object -First 1).rungTickHz)
				flushConcurrencyPerSilo = (Get-StateOr ($healthy | Select-Object -First 1) 'flushConcurrencyPerSilo' 8)
				# Every published cohort still reached the offered load after
				# the escalation budget ran out: the cell is a lower bound on
				# the cluster's throughput, and the table says so.
				offerBound          = (@($healthy | Where-Object { -not (Get-StateOr $_ 'offerBound' $false) }).Count -eq 0)
				producerBound       = (@($healthy | Where-Object { Get-StateOr $_ 'producerBound' $false }).Count -gt 0)
			}
		}
		if ($perCount.Count -eq 0) { continue }

		# Speedup and efficiency are both relative to the N=1 anchor. Without
		# a measured N=1 cell there is no denominator, and inventing one (say,
		# the smallest silo count present) would silently redefine "1.00x" to
		# mean something else in a table that looks unchanged. Leave them
		# null instead and let the renderer say so.
		$anchor = if ($perCount.ContainsKey('1')) { $perCount['1'].sustainedThroughput } else { $null }
		foreach ($key in @($perCount.Keys)) {
			$cell = $perCount[$key]
			if ($anchor -and $anchor -gt 0 -and
				-not (Get-StateOr $perCount['1'] 'producerBound' $false) -and
				-not (Get-StateOr $cell 'producerBound' $false)) {
				$cell['speedup']    = [math]::Round($cell.sustainedThroughput / [double]$anchor, 2)
				$cell['efficiency'] = [math]::Round(($cell.sustainedThroughput / [double]$anchor) / [double]$cell.siloCount, 2)
			} else {
				$cell['speedup']    = $null
				$cell['efficiency'] = $null
			}
		}
		$rows[$row.Label] = $perCount
	}
	return $rows
}

function Set-Layer3ProducerEvidence {
	[CmdletBinding()] param(
		[Parameter(Mandatory)][hashtable] $Cohort,
		[Parameter(Mandatory)][string] $LogPath
	)
	$slip = $null
	$blocked = $null
	$generated = $null
	# Use paired full-run totals, never maxima drawn from different windows.
	# Slip alone (including legacy logs) cannot distinguish a slow generator
	# from one that fell behind because the cluster held its channel full.
	$line = Select-String -Path $LogPath -Pattern '^\[producer\] DONE ' | Select-Object -Last 1
	if ($line) {
		if ($line.Line -match 'slipMaxMs=\s*([\d.]+)') {
			$slip = [double]$Matches[1]
		}
		if ($line.Line -match 'genBlockedFrac=\s*([\d.]+)') {
			$blocked = [double]$Matches[1]
		}
		if ($line.Line -match '\savg=\s*([\d,.]+)\s*msg/s') {
			$generated = [double]($Matches[1] -replace ',', '')
		}
	}
	$offered = [double](Get-StateOr $Cohort 'rungVehicles' 0) * [double](Get-StateOr $Cohort 'rungTickHz' 0)
	$achieved = Get-StateOr $Cohort 'finalThroughput' $null
	# A slow generator only bounds the cell when the cluster kept pace with
	# what it did generate. The point-write modes buffer a whole flush batch
	# per slot, so the generator can run behind schedule without blocking
	# while the cluster retires well under half of what was generated and
	# drains the backlog long after the producer finishes; that cell is a
	# cluster ceiling, and grading it producer-bound would publish a real
	# measurement as a lower bound and drop its curve from every chart.
	$clusterKeptPace = ($null -eq $generated) -or ($generated -le 0) -or ($null -eq $achieved) -or
		([double]$achieved -ge (0.9 * $generated))
	$bound = ($null -ne $slip -and $slip -gt 1000) -and
		($null -ne $blocked -and $blocked -ge 0 -and $blocked -lt 0.2) -and
		$clusterKeptPace
	$Cohort['producerSlipMaxMs'] = $slip
	$Cohort.Remove('producerGenBlockedFracMax')
	$Cohort['producerGenBlockedFrac'] = $blocked
	$Cohort['producerGeneratedPerSec'] = $generated
	$Cohort['producerBound'] = $bound
	if ($bound) {
		Write-Warning "[layer3] PRODUCER-BOUND $LogPath : DONE slipMaxMs=$slip genBlockedFrac=$blocked generated=$generated achieved=$achieved offered=$offered keys/s. Generation is behind schedule with little channel back-pressure and the cluster kept pace with it; rendering a lower bound, not a cluster ceiling."
	}
}

function Set-Layer3PreseedEvidence {
	<#
	.SYNOPSIS
		Mark a read-mode Layer 3 cohort UNSEEDED when its log shows no
		producer preseed.
	.DESCRIPTION
		The Layer 3 silo returns early in cluster ingest mode and never runs
		the single-VM silo's read-mode preseed, so until #3474 every get-point
		and get-many cohort read an empty tree: each call was a miss, and a
		miss takes the serial shard-root turn instead of the optimistic read.
		The producer now seeds and logs '[producer] preseed ... entries=N'.
		A read cohort without that line (or with entries=0) is refused; the
		write modes are not applicable and are left unmarked.
	#>
	[CmdletBinding()] param(
		[Parameter(Mandatory)][hashtable] $Cohort,
		[Parameter(Mandatory)][string] $LogPath,
		[Parameter(Mandatory)][string] $WorkloadMode
	)
	if ($WorkloadMode -notin @('get-point', 'get-many')) {
		$Cohort.Remove('preseedEntries')
		$Cohort['unseeded'] = $false
		return
	}
	$entries = $null
	$line = Select-String -Path $LogPath -Pattern '^\[producer\] preseed treeId=\S+ entries=(\d+)' | Select-Object -Last 1
	if ($line) { $entries = [long]$line.Matches[0].Groups[1].Value }
	$Cohort['preseedEntries'] = $entries
	$Cohort['unseeded'] = -not ($null -ne $entries -and $entries -gt 0)
	if ($Cohort['unseeded']) {
		Write-Warning "[layer3] UNSEEDED $LogPath : $WorkloadMode cohort has no '[producer] preseed ... entries=N' line, so it read an empty keyspace (every call a miss). Refusing the cohort."
	}
}

function Read-SiloLogStats {
	[CmdletBinding()] param(
		[Parameter(Mandatory)][string] $SiloLogPath,
		[Parameter(Mandatory)][string] $WorkloadMode,
		[int] $BatchSize = 4096
	)
	# Reuses the same methodology run-cohort.ps1 emits: steady-state mean of
	# `[silo] t=` per-second rate samples over t in [15s, last-non-zero-rate].
	$samples = @()
	$failedSamples = 0
	foreach ($m in (Select-String -Path $SiloLogPath -Pattern '^\[silo\] t=')) {
		# Match both the new and legacy per-second formats so logs captured before
		# the silo rename ("Entries written per second" -> "ops/sec", "written="
		# -> "ops=") can still be parsed without re-provisioning. Drop the legacy
		# arm after the next round of cohorts retires those logs.
		if (-not ($m.Line -match 't=\s*([\d.]+)s\s+(?:ops|written)=\s*([\d,]+)\s+(?:ops/sec|Entries written per second)=\s*([\d,]+)\s+inFlight=\s*(\d+)')) {
			continue
		}
		$samples += [pscustomobject]@{
			t        = [double]$Matches[1]
			rate     = [long](($Matches[3]) -replace ',','')
			inFlight = [int]$Matches[4]
		}
		if ($m.Line -match 'failed=\s*([\d,]+)' -and [long](($Matches[1]) -replace ',','') -gt 0) {
			$failedSamples++
		}
	}
	$steady = $samples | Where-Object { $_.t -ge 15 -and $_.rate -gt 0 }
	$steadyMean = 0
	$inFlightMax = 0
	if (@($steady).Count -gt 0) {
		$steadyMean = [int](($steady | Measure-Object -Property rate -Sum).Sum / @($steady).Count)
		$inFlightMax = ($steady | Measure-Object -Property inFlight -Maximum).Maximum
	}

	# Per-call p50/p75/p90/p99. Pick the most representative instrument per
	# workload mode; fall back to lattice.op.duration_ms (the silo's ingest
	# envelope, present for every workload). Read modes now have their own
	# per-call lattice-grain instruments (get.duration / get_many.duration),
	# shipped alongside the existing write-side envelopes, so all five
	# workload modes use a real per-call duration histogram and the per-
	# batch-size divisor below has been retired.
	#
	# Each [phaseA] line shape:
	#   [phaseA] t=10.3s instrument=NAME tree=T shard=S phase=P status=... count=N sum=... min=... p50=... p75=... p90=... p99=... max=...
	# We anchor on 'instrument=NAME tree=' (with trailing 'tree=' as a hard
	# anchor) so substring matches like 'set.duration' vs 'set.stage.duration'
	# stay disjoint.
	$preferred = switch ($WorkloadMode) {
		'set-many'        { @('set_many.duration') }
		'set-many-atomic' { @('saga.broadcast.duration', 'set_many.duration') }
		'set-many-atomic-2' { @('saga.broadcast.duration', 'set_many.duration') }
		# Cross-tree commits drive the same per-tree sub-saga terminal-broadcast
		# path as a single-tree atomic write (one saga.broadcast.duration per
		# participating tree); the cross-tree coordinator's own
		# orleans.lattice.cross_tree.* counters are not in the PhaseA reporter's
		# instrument allowlist, so the per-call duration falls back to the
		# atomic family and then to lattice.op.duration_ms.
		'cross-tree-atomic-2'  { @('saga.broadcast.duration', 'set_many.duration') }
		'cross-tree-atomic-64' { @('saga.broadcast.duration', 'set_many.duration') }
		'set-point'       { @('set.duration') }
		'set-point-mv'    { @('set.duration') }
		'get-point'       { @('get.duration') }
		'get-many'        { @('get_many.duration') }
		default           { @() }
	}
	$candidates = @($preferred) + @('lattice.op.duration_ms')

	$p50 = $null; $p75 = $null; $p90 = $null; $p99 = $null; $instrumentUsed = $null
	foreach ($cand in $candidates) {
		# Anchor: 'instrument=<name> tree=' guarantees exact-name match.
		$pat = '^\[phaseA\][^\n]*instrument=' + [regex]::Escape($cand) + ' tree='
		$phaseAMatches = @(Select-String -Path $SiloLogPath -Pattern $pat)
		# Filter to the productive window (skip the first ~10s warm-up and the
		# trailing drain windows where count is low). The reporter emits at
		# ~10-second cadence; t>=15s catches the second window onward.
		$productive = @($phaseAMatches | Where-Object {
			# Exclude the asynchronous view-maintainer's internal apply rows
			# (tree=view-<name>). For set-point-mv the source tree and the
			# attached materialised-view tree BOTH emit set.duration, but the
			# view tree's rows are coalesced background apply batches - a single
			# recorded "set" can cover thousands of tailed WAL entries, so its
			# duration spans seconds and is NOT the caller-visible SetAsync
			# latency. Worse, the producer stops a few seconds before the silo,
			# so the final productive window often carries ONLY the view tree's
			# drain rows; without this filter the last-window pick lands on them
			# and publishes a multi-second cell for a workload whose real
			# per-call p50 matches plain set-point. Anchor the cell on the
			# source tree by dropping any tree=view-* row.
			($_.Line -notmatch ' tree=view-[^\s]*') -and
			# Exclude the library's own system trees (tree=_lattice_trees, the
			# tree registry, and any other _lattice_* tree). They emit the same
			# duration instruments for registry lookups, and after the producer
			# stops the final window can carry ONLY those rows, so the last-
			# window pick published a ~120 us registry read as the get-point
			# per-call latency (the bench tree's own p50 in the same cohort was
			# ~3 ms).
			($_.Line -notmatch ' tree=_lattice_[^\s]*') -and
			($_.Line -match '\[phaseA\] t=\s*([\d.]+)s') -and ([double]$Matches[1] -ge 15)
		})
		if ($productive.Count -eq 0) { continue }
		# Use the last full productive window (matches the pre-fix behaviour
		# for now; revisit to do count-weighted aggregation across windows).
		# Parse each token independently: silo logs from before the p75
		# reporter extension will only carry p50/p90/p99, in which case p75
		# stays null and the rendered cell falls through to 'not captured'.
		$last = $productive[-1].Line
		if ($last -match 'p50=([\d.]+)') { $p50 = [double]$Matches[1] }
		if ($last -match 'p75=([\d.]+)') { $p75 = [double]$Matches[1] }
		if ($last -match 'p90=([\d.]+)') { $p90 = [double]$Matches[1] }
		if ($last -match 'p99=([\d.]+)') { $p99 = [double]$Matches[1] }
		if ($null -ne $p50 -or $null -ne $p99) {
			$instrumentUsed = $cand
			break
		}
	}

	# Historic note: an earlier shape divided the get-point p50/p99 by
	# BENCH_BATCH_SIZE because the only available instrument was the silo's
	# per-batch ingest envelope (lattice.op.duration_ms). Now that the
	# lattice grain exposes a real per-call get.duration histogram, the
	# divisor is no longer needed and has been removed; all five workload
	# modes report the matching duration histogram's p50/p99 directly.

	# Verdict + FINAL failed.
	$verdict = ''
	$finalFailed = 0
	$verdictLine = (Select-String -Path $SiloLogPath -Pattern '^Verdict\s*:' | Select-Object -Last 1)
	if ($verdictLine) {
		if ($verdictLine.Line -match 'Verdict\s*:\s*([A-Z]+)') { $verdict = $Matches[1] }
	}
	$finalLine = (Select-String -Path $SiloLogPath -Pattern 'FINAL (ops|written)=' | Select-Object -First 1)
	if ($finalLine -and $finalLine.Line -match 'failed=([\d,]+)') {
		$finalFailed = [long]($Matches[1] -replace ',','')
	}

	# Completed-work throughput, as distinct from SteadyMean.
	#
	# SteadyMean averages the per-second rate samples that are non-zero
	# (t>=15s, rate>0). On the Layer 2 path that filter is benign, because
	# the producer and silo are co-located and work retires fairly smoothly.
	# On the Layer 3 path it is NOT: the client submits 4096-key batches, so
	# a whole batch retires in one sample and the intervening samples are
	# exactly zero. Filtering the zeros away then averages only the spikes.
	# Measured on a real N=1 cohort: the producer offered 5,935 keys/s and
	# SteadyMean reported 9,637 keys/s - a rate higher than the offered load,
	# which is impossible as a sustained figure.
	#
	# Worse for a scaling study, the size of that overstatement depends on
	# how bursty the run was, which itself varies with the silo count, so the
	# artefact would bend the very curve the tier exists to measure.
	#
	# FinalThroughput is instead total completed ops divided by the active
	# elapsed time the engine reports at FINAL. It counts only work that
	# actually succeeded, over the wall-clock it actually took, so it cannot
	# exceed the offered load and carries no windowing bias. Layer 3 publishes
	# this; SteadyMean is still returned so both remain inspectable and so
	# the Layer 2 path is completely unchanged.
	$finalOps = $null
	$finalActiveSec = $null
	$finalThroughput = $null
	if ($finalLine) {
		if ($finalLine.Line -match 'FINAL (?:ops|written)=([\d,]+)') {
			$finalOps = [long]($Matches[1] -replace ',','')
		}
		# Prefer 'active=' (elapsed minus idle) and fall back to 'elapsed='.
		if ($finalLine.Line -match 'active=([\d.]+)s') {
			$finalActiveSec = [double]$Matches[1]
		} elseif ($finalLine.Line -match 'elapsed=([\d.]+)s') {
			$finalActiveSec = [double]$Matches[1]
		}
		if ($null -ne $finalOps -and $null -ne $finalActiveSec -and $finalActiveSec -gt 0) {
			$finalThroughput = [math]::Round($finalOps / $finalActiveSec, 0)
		}
	}

	return @{
		SteadyMean      = $steadyMean
		FinalOps        = $finalOps
		FinalActiveSec  = $finalActiveSec
		FinalThroughput = $finalThroughput
		PerCallP50Ms    = $p50
		PerCallP75Ms    = $p75
		PerCallP90Ms    = $p90
		PerCallP99Ms    = $p99
		InstrumentUsed  = $instrumentUsed
		InFlightMax     = $inFlightMax
		Failed          = ($finalFailed + $failedSamples)
		Verdict         = $verdict
	}
}

# ────────────────────────────────────────────────────────────────────────────
# Aggregation
# ────────────────────────────────────────────────────────────────────────────

function Aggregate-Layer1Cells {
	[CmdletBinding()] param([Parameter(Mandatory)] $Cohorts)
	# Cohorts are an array of hashtables with .metrics dictionary
	# (microbench_<slug>_p50_ns, _alloc_b, etc). Normalise to a flat array
	# defensively - upstream may wrap a single cohort as a scalar or as a
	# 1-element array; @($x) handles both shapes plus the empty case.
	$Cohorts = @($Cohorts)
	if ($Cohorts.Count -eq 0) { return @{} }
	$rows = @{}
	foreach ($row in $Layer1Rows) {
		$slug = $row.MetricSlug
		$p50Key   = "microbench_${slug}_p50_ns"
		$p75Key   = "microbench_${slug}_p75_ns"
		$p90Key   = "microbench_${slug}_p90_ns"
		$p99Key   = "microbench_${slug}_p99_ns"
		$allocKey = "microbench_${slug}_alloc_b"
		# A cohort can occasionally report alloc_b=0 for a row that clearly
		# allocates. This is a BenchmarkDotNet MemoryDiagnoser measurement
		# artefact, not a real allocation-free path: the per-op allocated-bytes
		# delta (workload minus overhead) is computed from GC counters, and under
		# a Server / background (concurrent) GC a collection landing inside the
		# measurement window perturbs that delta enough to floor the cheapest
		# paths (~200-300 B/op point reads) to a reported 0. The microbench host
		# now runs Workstation, non-concurrent GC (see the .csproj) which removes
		# the contamination at the source. As defence in depth the aggregator
		# still drops alloc_b=0 cohorts for batched rows (ExpectedBatchSize>1),
		# whose larger per-call allocation makes a true 0 impossible. Point rows
		# (ExpectedBatchSize 1 or unset) are NOT dropped: some of them - e.g.
		# ExistsAsync returning a cached Task<bool> singleton - genuinely allocate
		# ~0, so a measured 0 there is correct and must be preserved.
		$batchSize = if ($row.ContainsKey('ExpectedBatchSize')) { [int]$row.ExpectedBatchSize } else { 1 }
		$dropZeroAllocCohorts = $batchSize -gt 1
		$p50s = @(); $p75s = @(); $p90s = @(); $p99s = @(); $allocs = @()
		$allocZeroDropped = 0
		foreach ($c in $Cohorts) {
			if ($c.metrics.ContainsKey($p50Key)   -and $null -ne $c.metrics[$p50Key])   { $p50s   += [double]$c.metrics[$p50Key] }
			if ($c.metrics.ContainsKey($p75Key)   -and $null -ne $c.metrics[$p75Key])   { $p75s   += [double]$c.metrics[$p75Key] }
			if ($c.metrics.ContainsKey($p90Key)   -and $null -ne $c.metrics[$p90Key])   { $p90s   += [double]$c.metrics[$p90Key] }
			if ($c.metrics.ContainsKey($p99Key)   -and $null -ne $c.metrics[$p99Key])   { $p99s   += [double]$c.metrics[$p99Key] }
			if ($c.metrics.ContainsKey($allocKey) -and $null -ne $c.metrics[$allocKey]) {
				$allocValue = [double]$c.metrics[$allocKey]
				if ($dropZeroAllocCohorts -and $allocValue -eq 0.0) {
					$allocZeroDropped++
					continue
				}
				$allocs += $allocValue
			}
		}
		if ($p50s.Count -eq 0) {
			Write-Warning "[aggregate-l1] no p50 samples for row '$($row.Label)' (slug=$slug); cohort N=$($Cohorts.Count)"
			continue
		}
		if ($allocZeroDropped -gt 0) {
			Write-Warning "[aggregate-l1] row '$($row.Label)' (slug=$slug, batchSize=$batchSize): dropped $allocZeroDropped of $($Cohorts.Count) per-cohort alloc_b=0 samples as BDN MemoryDiagnoser async-thread-loss artefacts. $($allocs.Count) usable allocation cohort(s) remain."
		}
		$p50Median = (Get-Median $p50s)
		$p75Median = if ($p75s.Count -gt 0) { (Get-Median $p75s) } else { $null }
		$p90Median = if ($p90s.Count -gt 0) { (Get-Median $p90s) } else { $null }
		$p99Median = if ($p99s.Count -gt 0) { (Get-Median $p99s) } else { $null }
		# Explicit null check, NOT truthy: a real measurement of '0 bytes
		# allocated' (e.g. ExistsAsync returning a cached Task<bool> singleton)
		# is falsy under `if ($allocMedian)` and was previously coerced to $null,
		# rendering as 'n/a' in the doc table even though zero is the correct
		# answer. The batched-row zero-drop above prevents a stray GC-perturbed
		# alloc_b=0 from clobbering a batched row's median - if every cohort for a
		# batched row dropped, $allocs is empty and the cell renders as 'n/a'
		# (correctly: we have no usable sample).
		$allocMedian = if ($allocs.Count -gt 0) { (Get-Median $allocs) } else { $null }
		# Per-key throughput ceiling: (1 / p50_seconds) * batchSize.
		# Batched calls return per-key keys/s rather than per-call calls/s
		# so the column is comparable across rows; the (N keys/call) label
		# part keeps the per-call batch shape visible to the reader.
		$ceiling = if ($p50Median -gt 0) { [int]((1e9 / $p50Median) * $batchSize) } else { $null }
		$rows[$row.Label] = @{
			perCallP50Ns        = [int][math]::Round($p50Median, 0)
			perCallP75Ns        = if ($null -ne $p75Median) { [int][math]::Round($p75Median, 0) } else { $null }
			perCallP90Ns        = if ($null -ne $p90Median) { [int][math]::Round($p90Median, 0) } else { $null }
			perCallP99Ns        = if ($null -ne $p99Median) { [int][math]::Round($p99Median, 0) } else { $null }
			allocB              = if ($null -ne $allocMedian) { [int][math]::Round($allocMedian, 0) } else { $null }
			singleThreadCeiling = $ceiling
			cohortN             = $p50s.Count
		}
	}
	# A partial miss is a warning above: one row losing its samples is a real
	# but survivable measurement gap, and the rest of the table still publishes.
	# A TOTAL miss is categorically different and is never legitimate - cohorts
	# ran, yet not one of the Layer 1 rows resolved a single p50. That is the
	# signature of the whole microbench class having aborted (a [GlobalSetup]
	# throw is reported by BenchmarkDotNet as 'ExitCode != 0 and no results
	# reported' and does not fail the run), and the consequence is that the
	# perf-table:layer1 block of docs/lattice/performance-single-silo.md
	# regenerates EMPTY while this script exits 0. Issue #3126 is that exact
	# false green. Fail loudly instead: an empty published table must cost a red
	# run, not a warning nobody reads.
	if ($rows.Count -eq 0) {
		throw "[aggregate-l1] $($Cohorts.Count) cohort(s) ran but not one of the $(@($Layer1Rows).Count) Layer 1 rows resolved a p50 sample. This is a total Layer 1 miss, which is never legitimate: it means the microbench class produced no results at all (most often a [GlobalSetup] throw, which BenchmarkDotNet reports as 'ExitCode != 0 and no results reported' without failing the run). Publishing would blank the perf-table:layer1 block of docs/lattice/performance-single-silo.md. Inspect the BenchmarkDotNet output above for the underlying failure."
	}
	return $rows
}

function Aggregate-Layer2Cells {
	[CmdletBinding()] param([Parameter(Mandatory)][hashtable] $CohortsByMode)
	$rows = @{}
	foreach ($row in $Layer2Rows) {
		$mode = $row.WorkloadMode
		if (-not $CohortsByMode.ContainsKey($mode)) { continue }
		$cohorts = @($CohortsByMode[$mode])
		if ($cohorts.Count -eq 0) {
			Write-Warning "[aggregate-l2] no cohorts for mode=$mode (row '$($row.Label)')"
			continue
		}
		# Only aggregate cohorts the harness graded HEALTHY. A WEDGE/FAILED
		# cohort's silo reporter often closes on a single-sample window, so
		# its per-call p50/p75/p90/p99 collapse to one identical (and wildly
		# inflated) value; letting that into the cross-cohort median poisons
		# the published per-call cells even when the other cohorts are clean.
		# Its steady-state mean is equally unrepresentative (drain-tail
		# thrash depresses it). Dropping non-HEALTHY cohorts keeps the doc a
		# measurement of the system's healthy behaviour; if a workload has no
		# HEALTHY cohort the row is left to its preserved/_pending_ state and
		# a warning is surfaced for the operator.
		$healthy = @($cohorts | Where-Object { (Get-StateOr $_ 'verdict' '') -eq 'HEALTHY' })
		if ($healthy.Count -lt $cohorts.Count) {
			Write-Warning "[aggregate-l2] mode=${mode}: excluding $($cohorts.Count - $healthy.Count)/$($cohorts.Count) non-HEALTHY cohort(s) from aggregation"
		}
		if ($healthy.Count -eq 0) {
			Write-Warning "[aggregate-l2] mode=${mode}: no HEALTHY cohorts in $($cohorts.Count) cohort(s); row '$($row.Label)' not updated"
			continue
		}
		$cohorts = $healthy
		$means = @($cohorts | ForEach-Object { $_.steadyMean } | Where-Object { $_ -gt 0 })
		$p50s  = @($cohorts | ForEach-Object { $_.perCallP50Ms } | Where-Object { $null -ne $_ })
		$p75s  = @($cohorts | ForEach-Object { $_.perCallP75Ms } | Where-Object { $null -ne $_ })
		$p90s  = @($cohorts | ForEach-Object { $_.perCallP90Ms } | Where-Object { $null -ne $_ })
		$p99s  = @($cohorts | ForEach-Object { $_.perCallP99Ms } | Where-Object { $null -ne $_ })
		if ($means.Count -eq 0) {
			Write-Warning "[aggregate-l2] mode=${mode}: no positive steady-state means in $($cohorts.Count) cohorts"
			continue
		}
		$rows[$row.Label] = @{
			sustainedThroughput = [int][math]::Round((Get-Median $means), 0)
			throughputUnit      = $row.ThroughputUnit
			perCallP50Ms        = if ($p50s.Count -gt 0) { [math]::Round((Get-Median $p50s), 2) } else { $null }
			perCallP75Ms        = if ($p75s.Count -gt 0) { [math]::Round((Get-Median $p75s), 2) } else { $null }
			perCallP90Ms        = if ($p90s.Count -gt 0) { [math]::Round((Get-Median $p90s), 2) } else { $null }
			perCallP99Ms        = if ($p99s.Count -gt 0) { [math]::Round((Get-Median $p99s), 2) } else { $null }
			cohortN             = $cohorts.Count
		}
	}
	return $rows
}

function Get-Median {
	[CmdletBinding()] param([Parameter(Mandatory)][double[]] $Values)
	if ($Values.Count -eq 0) { return 0.0 }
	$sorted = @($Values | Sort-Object)
	$mid = [int]([math]::Floor($sorted.Count / 2))
	if ($sorted.Count % 2 -eq 1) { return $sorted[$mid] }
	return ($sorted[$mid - 1] + $sorted[$mid]) / 2.0
}

# ────────────────────────────────────────────────────────────────────────────
# Doc rewrite
# ────────────────────────────────────────────────────────────────────────────

function Format-Layer1Row {
	[CmdletBinding()] param(
		[Parameter(Mandatory)][string] $Label,
		[Parameter(Mandatory)][hashtable] $Cell,
		[string] $CeilingUnit = 'op/s'
	)
	# Use Get-StateOr for the post-p75 percentile fields so legacy state.json
	# files (written before the four-percentile columns were introduced) that
	# only carry perCallP50Ns / allocB / singleThreadCeiling still render
	# instead of throwing under Set-StrictMode -Version Latest.
	$p50 = Format-Duration (Get-StateOr $Cell 'perCallP50Ns')
	$p75 = Format-Duration (Get-StateOr $Cell 'perCallP75Ns')
	$p90 = Format-Duration (Get-StateOr $Cell 'perCallP90Ns')
	$p99 = Format-Duration (Get-StateOr $Cell 'perCallP99Ns')
	$alloc = Format-Bytes (Get-StateOr $Cell 'allocB')
	$ceiling = Format-Throughput (Get-StateOr $Cell 'singleThreadCeiling') $CeilingUnit
	return ('| {0} | **{1}** | {2} | {3} | {4} | {5} | **{6}** |' -f $Label, $p50, $p75, $p90, $p99, $alloc, $ceiling)
}

function Format-Layer2Row {
	[CmdletBinding()] param(
		[Parameter(Mandatory)][string] $Label,
		[Parameter(Mandatory)][hashtable] $Cell
	)
	# Same strict-mode-safe accessor pattern as Format-Layer1Row: state.json
	# files written before the p75/p90 columns landed only carry the p50/p99
	# fields; Get-StateOr returns $null on the missing keys and the Format-
	# Layer2Latency helper degrades to 'not captured'.
	$unit = Get-StateOr $Cell 'throughputUnit' 'op/s'
	$thr = Format-Throughput (Get-StateOr $Cell 'sustainedThroughput') $unit
	$p50 = Format-Layer2Latency (Get-StateOr $Cell 'perCallP50Ms')
	$p75 = Format-Layer2Latency (Get-StateOr $Cell 'perCallP75Ms')
	$p90 = Format-Layer2Latency (Get-StateOr $Cell 'perCallP90Ms')
	$p99 = Format-Layer2Latency (Get-StateOr $Cell 'perCallP99Ms')
	return ('| {0} | **{1}** | {2} | {3} | {4} | {5} |' -f $Label, $thr, $p50, $p75, $p90, $p99)
}

function Format-Layer2Latency {
	<#
	.SYNOPSIS
		Render a millisecond cell with a sensible unit prefix. Values >= 1 ms
		render as "~X.XX ms"; smaller values render in microseconds so the
		read-mode per-key cells (derived as ingest-envelope / BatchSize,
		typically sub-microsecond to single-digit microseconds) read honestly
		rather than as "~0 ms".
	#>
	[CmdletBinding()] param($Ms)
	if ($null -eq $Ms) { return 'not captured' }
	$v = [double]$Ms
	if ($v -ge 1.0) {
		return ('~{0} ms' -f [math]::Round($v, 2))
	}
	$us = $v * 1000.0
	if ($us -ge 1.0) {
		return ('~{0} us' -f [math]::Round($us, 2))
	}
	# Sub-microsecond: report in ns.
	$ns = $us * 1000.0
	return ('~{0} ns' -f [math]::Round($ns, 0))
}

function Format-Duration {
	[CmdletBinding()] param($Ns)
	# `$Ns` may be $null when the upstream BDN cohort did not emit this
	# percentile (e.g. older state.json files written before the p75/p90
	# columns were added). Mirror Format-Bytes' graceful 'n/a' rendering
	# instead of throwing a parameter-binding error.
	if ($null -eq $Ns) { return 'n/a' }
	$asInt = [int]$Ns
	if ($asInt -lt 1000) { return "$asInt ns" }
	if ($asInt -lt 1000000) {
		$us = [math]::Round($asInt / 1000.0, 2)
		return "$us us"
	}
	$ms = [math]::Round($asInt / 1000000.0, 2)
	return "$ms ms"
}

function Format-Bytes {
	[CmdletBinding()] param($Value)
	if ($null -eq $Value) { return 'n/a' }
	$b = [int]$Value
	if ($b -lt 1024) { return "$b B" }
	if ($b -lt 1024 * 1024) {
		return ('{0:N0} KB' -f ($b / 1024.0))
	}
	return ('{0:N0} KB' -f ($b / 1024.0))
}

function Format-Throughput {
	[CmdletBinding()] param([Parameter(Mandatory)] $Value, [string] $Unit = 'op/s')
	if ($null -eq $Value) { return "n/a $Unit" }
	$v = [double]$Value
	if ($v -ge 1e6) {
		$m = [math]::Round($v / 1e6, 2)
		return "~$m M $Unit"
	}
	if ($v -ge 1e3) {
		$k = [math]::Round($v / 1e3, 1)
		return "~$k k $Unit"
	}
	return "$([int]$v) $Unit"
}

function New-MetaHeaderForLayer1 {
	[CmdletBinding()] param(
		[Parameter(Mandatory)][hashtable] $State,
		[Parameter(Mandatory)][hashtable] $RowsAgg,
		[hashtable] $Existing = @{}
	)
	$cohortN = if ($RowsAgg.Count -gt 0) {
		($RowsAgg.Values | ForEach-Object { $_.cohortN } | Sort-Object -Unique) -join '/'
	} elseif ($Existing.ContainsKey('cohortN')) {
		$Existing['cohortN'] # preserved
	} else { '0' }
	$rowsDate = if ($RowsAgg.Count -gt 0) {
		(Get-Date).ToUniversalTime().ToString('yyyy-MM-dd')
	} elseif ($Existing.ContainsKey('rowsMeasured')) {
		$Existing['rowsMeasured']
	} else { (Get-Date).ToUniversalTime().ToString('yyyy-MM-dd') }

	# Start from the existing meta (so unknown keys like provenanceNote ride
	# through) and overlay the keys this script owns - but ONLY when we have
	# fresh rows. When every cohort failed (RowsAgg empty), preserve the
	# existing meta header verbatim so the doc doesn't claim to be VM-grounded
	# when in fact the cells fell back to the previous values.
	$meta = @{}
	foreach ($k in $Existing.Keys) { $meta[$k] = $Existing[$k] }
	if (-not $meta.ContainsKey('schema')) { $meta['schema'] = 'v1' }
	if ($RowsAgg.Count -gt 0) {
		$meta['schema']        = 'v1'
		$meta['host']          = $State.vmSize
		$meta['dotnet']        = (Get-StateOr $State 'dotnetVersion' '10.0.x')
		$meta['bdnFidelity']   = (Get-StateOr $State 'bdnFidelity' 'dry')
		$meta['bdnToolchain']  = 'InProcessEmitToolchain'
		$meta['cohortN']       = $cohortN
		$meta['rowsMeasured']  = $rowsDate
		$meta['gitSha']        = (Get-StateOr $State 'mainSha' (Get-StateOr $State 'gitSha' 'unknown'))
		$meta['methodology']   = 'Per-call p50/p75/p90/p99 and allocations reported directly by BenchmarkDotNet (linear-interpolation quantiles over the workload sample). Per-thread call rate = round(1 / p50) * batchSize, reported in keys/s so batched calls (GetMany, SetMany, SetManyAtomic) are directly comparable to single-key calls (Get, Set). Cells are the median across N cohorts of each per-cohort BDN quantile.'
	}
	return $meta
}

function New-MetaHeaderForLayer2 {
	[CmdletBinding()] param(
		[Parameter(Mandatory)][hashtable] $State,
		[Parameter(Mandatory)][hashtable] $RowsAgg,
		[hashtable] $Existing = @{}
	)
	$cohortN = if ($RowsAgg.Count -gt 0) {
		($RowsAgg.Values | ForEach-Object { $_.cohortN } | Sort-Object -Unique) -join '/'
	} elseif ($Existing.ContainsKey('cohortN')) {
		$Existing['cohortN'] # preserved
	} else { '0' }
	$rowsDate = if ($RowsAgg.Count -gt 0) {
		(Get-Date).ToUniversalTime().ToString('yyyy-MM-dd')
	} elseif ($Existing.ContainsKey('rowsMeasured')) {
		$Existing['rowsMeasured']
	} else { (Get-Date).ToUniversalTime().ToString('yyyy-MM-dd') }
	$rung = ('{0} vehicles / {1} Hz / {2}s' -f $State.rung.Vehicles, $State.rung.TickHz, $State.rung.DurationSec)

	$meta = @{}
	foreach ($k in $Existing.Keys) { $meta[$k] = $Existing[$k] }
	if (-not $meta.ContainsKey('schema')) { $meta['schema'] = 'v1' }
	if ($RowsAgg.Count -gt 0) {
		$meta['schema']             = 'v1'
		$meta['host']               = $State.vmSize
		$meta['region']             = $State.region
		$meta['dotnet']             = (Get-StateOr $State 'dotnetVersion' '10.0.x')
		$meta['walPartitions']      = $State.walPartitions
		$meta['walMaxPendingBatches'] = $State.walMaxPendingBatches
		$meta['batchSize']          = (Get-StateOr $State 'batchSize' 4096)
		$meta['rung']               = $rung
		$meta['responseTimeoutSec'] = $State.responseTimeoutSec
		$meta['cohortN']            = $cohortN
		$meta['rowsMeasured']       = $rowsDate
		$meta['gitSha']             = (Get-StateOr $State 'mainSha' (Get-StateOr $State 'gitSha' 'unknown'))
		$meta['methodology']        = 'Throughput cell = median across N HEALTHY cohorts of the steady-state mean (silo per-second rate samples, t>=15s, rate>0; see benchmark/azure-throughput/throughput.md section 27.1). Per-call p50/p75/p90/p99 cells = median across N HEALTHY cohorts of the per-mode preferred [phaseA] duration instrument (set.duration for set-point and set-point-mv, set_many.duration for set-many, saga.broadcast.duration for set-many-atomic, get.duration for get-point, get_many.duration for get-many). Cohorts the harness graded WEDGE/FAILED are excluded from aggregation so a non-representative overload tail cannot poison a cell. The rung shown above is the read-workload offered load; every write workload is driven at a reduced per-row offered load (annotated in its operation label) chosen to keep the single Azure Tables account below saturation, so each cohort reports a sustained, reproducible key-write rate rather than an overload tail. Each per-cohort quantile is computed inside the silo''s 10-second reporter window from a 4096-sample reservoir; the cell is the median of those per-cohort quantiles. All five workload modes report the matching caller-visible duration histogram directly; no per-batch-size divisor is applied.'
	}
	return $meta
}

function Render-MetaHeader {
	[CmdletBinding()] param(
		[Parameter(Mandatory)][string] $Layer,
		[Parameter(Mandatory)][hashtable] $Meta
	)
	$nl = "`r`n"
	$sb = [System.Text.StringBuilder]::new()
	[void]$sb.Append("<!-- perf-table:${Layer}:start").Append($nl)
	# Stable ordering: schema first, methodology last (it's the longest), the
	# rest alphabetical so a diff is local.
	$keys = @($Meta.Keys | Where-Object { $_ -ne 'schema' -and $_ -ne 'methodology' } | Sort-Object)
	[void]$sb.Append("  schema=$($Meta['schema'])").Append($nl)
	foreach ($k in $keys) {
		$v = $Meta[$k]
		[void]$sb.Append("  $k=$v").Append($nl)
	}
	[void]$sb.Append("  methodology=$($Meta['methodology'])").Append($nl)
	[void]$sb.Append("  DO-NOT-HAND-EDIT-BETWEEN-MARKERS").Append($nl)
	[void]$sb.Append("-->")
	return $sb.ToString()
}

function Render-Layer1Table {
	[CmdletBinding()] param([Parameter(Mandatory)][hashtable] $RowsAgg, [Parameter(Mandatory)][hashtable] $ExistingRows)
	$nl = "`r`n"
	$sb = [System.Text.StringBuilder]::new()
	[void]$sb.Append('| Operation                                | Per-call p50 | Per-call p75 | Per-call p90 | Per-call p99 | Allocations | Per-thread call rate (1 / p50) |').Append($nl)
	[void]$sb.Append('|------------------------------------------|-------------:|-------------:|-------------:|-------------:|------------:|-------------------------------:|').Append($nl)
	foreach ($row in $Layer1Rows) {
		if ($RowsAgg.ContainsKey($row.Label)) {
			[void]$sb.Append((Format-Layer1Row -Label $row.Label -Cell $RowsAgg[$row.Label] -CeilingUnit $row.CeilingUnit)).Append($nl)
		} elseif ($ExistingRows -and $ExistingRows.ContainsKey($row.Label)) {
			# Preserve prior cell content if this layer / row wasn't re-run.
			[void]$sb.Append($ExistingRows[$row.Label]).Append($nl)
		} else {
			[void]$sb.Append('| ' + $row.Label.PadRight(40) + ' | _pending_    | _pending_    | _pending_    | _pending_    | _pending_   | _pending_             |').Append($nl)
		}
	}
	return $sb.ToString().TrimEnd("`r","`n")
}

function Render-Layer2Table {
	[CmdletBinding()] param([Parameter(Mandatory)][hashtable] $RowsAgg, [Parameter(Mandatory)][hashtable] $ExistingRows)
	$nl = "`r`n"
	$sb = [System.Text.StringBuilder]::new()
	[void]$sb.Append('| Operation                                | Sustained throughput | Per-call p50  | Per-call p75  | Per-call p90  | Per-call p99  |').Append($nl)
	[void]$sb.Append('|------------------------------------------|---------------------:|--------------:|--------------:|--------------:|--------------:|').Append($nl)
	foreach ($row in $Layer2Rows) {
		if ($RowsAgg.ContainsKey($row.Label)) {
			[void]$sb.Append((Format-Layer2Row -Label $row.Label -Cell $RowsAgg[$row.Label])).Append($nl)
		} elseif ($ExistingRows -and $ExistingRows.ContainsKey($row.Label)) {
			[void]$sb.Append($ExistingRows[$row.Label]).Append($nl)
		} else {
			[void]$sb.Append('| ' + $row.Label.PadRight(40) + ' | _pending_            | _pending_     | _pending_     | _pending_     | _pending_     |').Append($nl)
		}
	}
	return $sb.ToString().TrimEnd("`r","`n")
}

function Render-Layer3Table {
	<#
	.SYNOPSIS
		Render the multi-silo grid: one row per (workload, silo count), with
		the scaling columns that make the curve legible in text.
	.DESCRIPTION
		The grid is grouped workload-major and ordered by ascending silo
		count, so each workload's curve reads top-to-bottom as a block. The
		speedup and efficiency columns are derived against the N=1 anchor by
		Aggregate-Layer3Cells; where no N=1 cell was measured they render as
		'n/a' rather than silently re-basing on some other silo count.

		This table is the authoritative companion to the Mermaid chart.
		Mermaid's xychart-beta is still experimental and its multi-series
		rendering is not guaranteed on every GitHub surface, so the numbers
		must be readable without it - the chart is an aid, never the record.
	#>
	[CmdletBinding()] param([Parameter(Mandatory)][hashtable] $RowsAgg)
	$nl = "`r`n"
	$sb = [System.Text.StringBuilder]::new()
	[void]$sb.Append('| Operation | Silos | Offered | Sustained throughput | Speedup vs 1 silo | Per-silo efficiency | Per-call p50 | Per-call p99 |').Append($nl)
	[void]$sb.Append('|-----------|------:|--------:|---------------------:|------------------:|--------------------:|-------------:|-------------:|').Append($nl)
	foreach ($row in $Layer3Rows) {
		if (-not $RowsAgg.ContainsKey($row.Label)) {
			[void]$sb.Append(('| {0} | _pending_ | _pending_ | _pending_ | _pending_ | _pending_ | _pending_ | _pending_ |' -f $row.Label)).Append($nl)
			continue
		}
		$byCount = $RowsAgg[$row.Label]
		$keys = @($byCount.Keys | Sort-Object { [int]$_ })
		foreach ($k in $keys) {
			$cell = $byCount[$k]
			$thr = Format-Throughput (Get-StateOr $cell 'sustainedThroughput') (Get-StateOr $row 'ThroughputUnit' 'keys/s')
			if ((Get-StateOr $cell 'offerBound' $false) -or (Get-StateOr $cell 'producerBound' $false)) { $thr = '>= ' + $thr }
			$off = Format-Throughput (Get-StateOr $cell 'offeredKeysPerSec') (Get-StateOr $row 'ThroughputUnit' 'keys/s')
			$sp  = Get-StateOr $cell 'speedup'
			$ef  = Get-StateOr $cell 'efficiency'
			$spS = if ($null -ne $sp) { ('{0}x' -f $sp) } else { 'n/a' }
			$efS = if ($null -ne $ef) { ('{0}%' -f [int][math]::Round($ef * 100, 0)) } else { 'n/a' }
			$p50 = Format-Layer2Latency (Get-StateOr $cell 'perCallP50Ms')
			$p99 = Format-Layer2Latency (Get-StateOr $cell 'perCallP99Ms')
			[void]$sb.Append(('| {0} | {1} | {2} | **{3}** | {4} | {5} | {6} | {7} |' -f $row.Label, $cell.siloCount, $off, $thr, $spS, $efS, $p50, $p99)).Append($nl)
		}
	}
	if (@($RowsAgg.Values | ForEach-Object { $_.Values } | Where-Object { Get-StateOr $_ 'producerBound' $false }).Count -gt 0) {
		[void]$sb.Append($nl).Append('Producer-bound cells are lower bounds (>=), not cluster ceilings. Speedup and efficiency are unavailable when the cell or its 1-silo anchor is producer-bound.')
	}
	return $sb.ToString().TrimEnd("`r","`n")
}

function Render-Layer3Chart {
	<#
	.SYNOPSIS
		Render the scaling curves as Mermaid xychart-beta blocks: one
		normalised speedup chart carrying every workload, then one
		absolute-throughput chart per $Layer3ChartGroups entry.
	.DESCRIPTION
		Mermaid's xychart-beta takes a single shared x-axis, so every series
		in a chart must be sampled at the SAME silo counts. A workload
		missing a cell at some silo count cannot simply be given a gap -
		xychart has no null - so it is dropped from that chart rather than
		plotted against a shifted axis, which would draw a confident, wrong
		curve. The table always carries every measured cell, so nothing is
		lost by that exclusion.

		The speedup chart comes first because it is the one chart on which
		all nine workloads are comparable: throughput spans four orders of
		magnitude across them, but speedup over the workload's own 1-silo
		cell is dimensionless. It carries an ideal-linear reference series
		(y = N) as its FIRST line, so the gap between each workload and
		linear scaling is visible without arithmetic. A workload with no
		measured 1-silo cell has no speedup and is left off that chart.

		Absolute charts use thousands of keys/s, except a group whose peak
		is below 10k keys/s, which is plotted in plain keys/s so its tick
		labels are not all fractional.
	#>
	[CmdletBinding()] param([Parameter(Mandatory)][hashtable] $RowsAgg)
	$nl = "`r`n"

	$renderChart = {
		param([string] $Title, [string] $YLabel, [int[]] $Axis, [object[]] $Series, [double] $YMax)
		$sb = [System.Text.StringBuilder]::new()
		[void]$sb.Append('```mermaid').Append($nl)
		[void]$sb.Append('xychart-beta').Append($nl)
		[void]$sb.Append(('    title "{0}"' -f $Title)).Append($nl)
		[void]$sb.Append(('    x-axis "Silos" [{0}]' -f (($Axis | ForEach-Object { '"' + $_ + '"' }) -join ', '))).Append($nl)
		[void]$sb.Append(('    y-axis "{0}" 0 --> {1}' -f $YLabel, $YMax)).Append($nl)
		foreach ($s in $Series) {
			[void]$sb.Append(('    line [{0}]' -f ($s.Values -join ', '))).Append($nl)
		}
		[void]$sb.Append('```').Append($nl).Append($nl)
		# xychart-beta has no legend, so the series order has to be stated in
		# prose or the chart is unreadable.
		[void]$sb.Append(('Series order (xychart-beta renders no legend): {0}.' -f (($Series | ForEach-Object { $_.Name }) -join ', then ')))
		return $sb.ToString()
	}
	$sharedAxis = {
		param([object[]] $Rows)
		$axis = $null
		foreach ($r in $Rows) {
			$counts = @($RowsAgg[$r.Label].Keys | ForEach-Object { [int]$_ } | Sort-Object)
			$axis = if ($null -eq $axis) { $counts } else { @($axis | Where-Object { $_ -in $counts }) }
		}
		return ,@($axis)
	}

	$available = @($Layer3Rows | Where-Object { $RowsAgg.ContainsKey($_.Label) })
	if ($available.Count -eq 0) { return '_No multi-silo cells measured yet._' }
	$measured = @($available | Where-Object {
		@($RowsAgg[$_.Label].Values | Where-Object { Get-StateOr $_ 'producerBound' $false }).Count -eq 0
	})
	if ($measured.Count -eq 0) { return '_No cluster-ceiling curves available; producer-bound lower bounds remain in the table._' }

	$blocks = New-Object System.Collections.Generic.List[string]
	if ($measured.Count -lt $available.Count) {
		$blocks.Add('Producer-bound workload curves are omitted; their lower bounds remain in the table.')
	}

	# 1. Normalised speedup, every workload that has an N=1 anchor.
	$anchored = @($measured | Where-Object { $RowsAgg[$_.Label].ContainsKey('1') })
	if ($anchored.Count -gt 0) {
		$axis = & $sharedAxis $anchored
		if ($axis.Count -ge 2) {
			$series = @(@{ Name = 'ideal linear scaling (y = silos)'; Values = @($axis | ForEach-Object { $_ }) })
			$maxVal = [double]($axis | Measure-Object -Maximum).Maximum
			foreach ($r in $anchored) {
				$vals = @($axis | ForEach-Object { [double](Get-StateOr $RowsAgg[$r.Label]["$_"] 'speedup' 0) })
				foreach ($v in $vals) { if ($v -gt $maxVal) { $maxVal = $v } }
				$series += ,@{ Name = $r.Label; Values = $vals }
			}
			$blocks.Add((& $renderChart 'Speedup over 1 silo vs silo count' 'Speedup (x)' $axis $series ([math]::Ceiling($maxVal * 1.1))))
		}
	}

	# 2. Absolute throughput, one chart per group.
	foreach ($g in $Layer3ChartGroups) {
		$rows = @($measured | Where-Object { (Get-StateOr $_ 'ChartGroup' 'writes') -eq $g.Id })
		if ($rows.Count -eq 0) { continue }
		$axis = & $sharedAxis $rows
		if ($axis.Count -lt 2) {
			$blocks.Add(('_{0}: needs at least two silo counts measured for every workload in the group; see the table below._' -f $g.Title))
			continue
		}
		$peak = 0.0
		foreach ($r in $rows) { foreach ($c in $axis) { $v = [double]$RowsAgg[$r.Label]["$c"].sustainedThroughput; if ($v -gt $peak) { $peak = $v } } }
		$div = if ($peak -ge 10000) { 1000.0 } else { 1.0 }
		$yLabel = if ($div -gt 1) { 'Thousand keys/s' } else { 'keys/s' }
		$series = @()
		foreach ($r in $rows) {
			$series += ,@{ Name = $r.Label; Values = @($axis | ForEach-Object { [math]::Round($RowsAgg[$r.Label]["$_"].sustainedThroughput / $div, 2) }) }
		}
		$blocks.Add((& $renderChart $g.Title $yLabel $axis $series ([math]::Ceiling(($peak / $div) * 1.1))))
	}

	return ($blocks -join ($nl + $nl))
}

function New-MetaHeaderForLayer3 {
	[CmdletBinding()] param(
		[Parameter(Mandatory)][hashtable] $State,
		[Parameter(Mandatory)][hashtable] $RowsAgg,
		[hashtable] $Existing = @{}
	)
	$meta = @{}
	foreach ($k in $Existing.Keys) { $meta[$k] = $Existing[$k] }
	if (-not $meta.ContainsKey('schema')) { $meta['schema'] = 'v1' }
	if ($RowsAgg.Count -eq 0) { return $meta }

	$counts = @()
	foreach ($label in $RowsAgg.Keys) { $counts += @($RowsAgg[$label].Keys | ForEach-Object { [int]$_ }) }
	$counts = @($counts | Sort-Object -Unique)
	$cohortN = @()
	foreach ($label in $RowsAgg.Keys) { $cohortN += @($RowsAgg[$label].Values | ForEach-Object { $_.cohortN }) }

	$meta['schema']        = 'v1'
	$meta['host']          = 'Azure Container Apps (Consumption)'
	# siloSize is separate from host on purpose: the whole point of Layer 3
	# is that N of these are running, so a reader has to be able to see the
	# per-replica size without parsing it back out of a host string.
	$meta['siloSize']      = '4 vCPU / 8 GiB'
	$meta['region']        = (Get-StateOr $State 'region' 'unknown')
	$meta['dotnet']        = (Get-StateOr $State 'dotnetVersion' '10.0.x')
	$meta['siloCounts']    = ($counts -join ',')
	# Pinned for every silo count, and recorded because without it a reader
	# cannot tell a compute knee from a shard-count knee: at a fixed 64
	# shards, N=8 leaves only 8 shard roots per silo.
	$meta['shardCount']            = (Get-StateOr $State 'layer3ShardCount' 64)
	# Fallbacks are the ACA cohort path's real defaults, NOT Layer 2's VM
	# defaults. A stale state file that predates these keys must still
	# describe the run that happened.
	$meta['walPartitions']         = (Get-StateOr $State 'layer3WalPartitions' 16)
	$meta['walMaxPendingBatches']  = (Get-StateOr $State 'layer3WalMaxPendingBatches' 16)
	$meta['responseTimeoutSec']    = (Get-StateOr $State 'responseTimeoutSec' 180)
	# The per-silo STARTING rung and client in-flight bound. Offered load for
	# a cell is the rung x silo count, doubled while the cell stays
	# offer-bound (see saturationRatio); the offered load each cell actually
	# ran at is the table's Offered column.
	$rungList = @()
	foreach ($row in $Layer3Rows) {
		$parts = ([string]$row.RungPerSilo).Split(':')
		$rungList += ('{0}={1} veh/silo @ {2} Hz / {3}s, flushConcurrency {4}/silo' -f $row.WorkloadId, $parts[0], $parts[1], $parts[2], (Get-StateOr $row 'FlushConcurrencyPerSilo' 8))
	}
	$meta['rungPerSilo']   = ($rungList -join '; ')
	$meta['saturationRatio']    = (Get-StateOr $State 'layer3SaturationRatio' 0.9)
	$meta['maxRungEscalations'] = (Get-StateOr $State 'layer3MaxRungEscalations' 3)
	$meta['batchSize']     = (Get-StateOr $State 'batchSize' 4096)
	$meta['cohortN']       = ((@($cohortN | Sort-Object -Unique)) -join '/')
	$meta['rowsMeasured']  = (Get-Date).ToUniversalTime().ToString('yyyy-MM-dd')
	$meta['gitSha']        = (Get-StateOr $State 'mainSha' (Get-StateOr $State 'gitSha' 'unknown'))
	$meta['walAccounts']   = 1
	$meta['methodology']   = 'Each cell is the median across N HEALTHY cohorts of completed-work throughput: total successfully-completed keys at FINAL divided by the engine''s active elapsed time. Layer 3 deliberately does NOT reuse Layer 2''s rate>0 steady-state mean. On this path the client submits 4096-key batches, so a whole batch retires inside one per-second sample and the samples between retirements are exactly zero; filtering the zeros away averages only the spikes and reports more throughput than was offered (measured: 9,637 keys/s reported against 5,935 keys/s actually offered). The overstatement also varies with burstiness, which varies with silo count, so it would bend the scaling curve itself. Completed-ops / active-elapsed counts only work that succeeded over the wall-clock it took, so it cannot exceed the offered load and carries no windowing bias. Per-call p50/p99 come from the [phaseA] duration histogram of ONE representative silo, not an aggregate across silos. Every cell is a true-throughput measurement, not an offered-load one: offered load is scaled with the silo count (a per-silo rung driven at rung x silo count, so per-silo demand never falls as the cluster grows), and a cell whose first cohort completes at least saturationRatio of what was offered is re-run at double the rung, up to maxRungEscalations times, so the published figure is where the cluster stopped keeping up rather than what the client asked for. A cell still offer-bound after the last escalation is published as a lower bound (>=). Speedup and per-silo efficiency are derived against the measured 1-silo cell. Every cohort starts on empty storage: with the silos parked, the harness deletes every table in the storage account except the clustering table and points the silos at a freshly named WAL table and grain-state table, so no cohort inherits trees, registry rows, or WAL backlog from an earlier one. All silo counts share ONE Azure Storage account for the WAL; see the caveats section for what its own metrics showed.'
	return $meta
}

function Get-ExistingTableRows {
	[CmdletBinding()] param([Parameter(Mandatory)][string] $Content, [Parameter(Mandatory)][string] $Layer)
	# Pull each existing data row inside the perf-table:<layer> block as a
	# label -> line dictionary so a partial regen preserves them.
	$pattern = '<!-- perf-table:' + [regex]::Escape($Layer) + ':start.*?-->(?<body>.*?)<!-- perf-table:' + [regex]::Escape($Layer) + ':end -->'
	$m = [regex]::Match($Content, $pattern, [System.Text.RegularExpressions.RegexOptions]::Singleline)
	$dict = @{}
	if (-not $m.Success) { return $dict }
	$body = $m.Groups['body'].Value
	foreach ($line in $body.Split("`n")) {
		$t = $line.TrimEnd("`r")
		if ($t -notmatch '^\|') { continue }
		# Skip header (---) separator lines.
		if ($t -match '^\|[\s:|-]+\|\s*$') { continue }
		# Skip the actual header row (which is part of the table the script
		# regenerates).
		if ($t -match '^\|\s*Operation\s*\|') { continue }
		# Extract the first column (the label) for matching.
		$cells = $t.TrimStart('|').Split('|')
		if ($cells.Count -lt 1) { continue }
		$label = $cells[0].Trim()
		if ($label) { $dict[$label] = $t }
	}
	return $dict
}

function Get-ExistingMetaHeader {
	[CmdletBinding()] param([Parameter(Mandatory)][string] $Content, [Parameter(Mandatory)][string] $Layer)
	# Pull the existing meta-header keys for the named layer's block so a
	# rewrite can preserve any key the script does not actively own (e.g.
	# operator-curated provenanceNote).
	$pattern = '<!-- perf-table:' + [regex]::Escape($Layer) + ':start\s*\r?\n(?<body>.*?)\r?\n-->'
	$m = [regex]::Match($Content, $pattern, [System.Text.RegularExpressions.RegexOptions]::Singleline)
	$dict = @{}
	if (-not $m.Success) { return $dict }
	foreach ($line in $m.Groups['body'].Value.Split("`n")) {
		$t = $line.Trim().TrimEnd("`r")
		if ([string]::IsNullOrEmpty($t)) { continue }
		if ($t.StartsWith('DO-NOT-')) { continue }
		$idx = $t.IndexOf('=')
		if ($idx -lt 0) { continue }
		$k = $t.Substring(0, $idx).Trim()
		$v = $t.Substring($idx + 1).Trim()
		if ($k) { $dict[$k] = $v }
	}
	return $dict
}

# ────────────────────────────────────────────────────────────────────────────
# Provenance note rendering (the "> Measured ..." blockquote that follows
# each :end marker). Owned by the script the same way the marker block is,
# but lives OUTSIDE the marker pair so the line is visible to readers
# scanning the doc without expanding any HTML comments.
# ────────────────────────────────────────────────────────────────────────────

function Render-ProvenanceNote {
	[CmdletBinding()] param(
		[Parameter(Mandatory)][string] $Layer,
		[Parameter(Mandatory)][hashtable] $Meta
	)
	$date    = if ($Meta.ContainsKey('rowsMeasured'))  { $Meta['rowsMeasured'] }  else { 'unknown' }
	$hostSku = if ($Meta.ContainsKey('host'))          { $Meta['host'] }          else { 'unknown' }
	$dot     = if ($Meta.ContainsKey('dotnet'))        { $Meta['dotnet'] }        else { 'unknown' }
	$sha     = if ($Meta.ContainsKey('gitSha') -and $Meta['gitSha']) { $Meta['gitSha'] } else { 'unknown' }
	$cohN    = if ($Meta.ContainsKey('cohortN'))       { $Meta['cohortN'] }       else { 'unknown' }
	switch ($Layer) {
		'layer1' {
			$fid = if ($Meta.ContainsKey('bdnFidelity')) { $Meta['bdnFidelity'] } else { 'unknown' }
			return "> Measured ${date} on ${hostSku} (.NET ${dot}) at git sha ${sha}, n=${cohN} cohorts (BDN ${fid})."
		}
		'layer2' {
			$region = if ($Meta.ContainsKey('region')) { $Meta['region'] }  else { 'unknown' }
			$rung   = if ($Meta.ContainsKey('rung'))   { $Meta['rung'] }    else { 'unknown' }
			return "> Measured ${date} on ${hostSku} in ${region} (.NET ${dot}) at git sha ${sha}, n=${cohN} cohorts. Read workloads were driven at ${rung}; each write workload was driven at a reduced per-row offered load (annotated in its operation label) to hold the single Azure Tables account below saturation."
		}
		'layer3' {
			$region = if ($Meta.ContainsKey('region'))     { $Meta['region'] }     else { 'unknown' }
			$counts = if ($Meta.ContainsKey('siloCounts')) { $Meta['siloCounts'] } else { 'unknown' }
			return "> Measured ${date} on ${hostSku} in ${region} (.NET ${dot}) at git sha ${sha}, n=${cohN} cohorts per cell, silo counts ${counts}. Offered load scales with the silo count and is raised until each cell plateaus below it, so every figure is true throughput rather than offered load; every cohort starts on freshly emptied storage. All silo counts share one Azure Storage account for the WAL - see the caveats below for what its metrics showed."
		}
		default { throw "Unknown layer '$Layer' for Render-ProvenanceNote" }
	}
}

function Set-ProvenanceNote {
	<#
	.SYNOPSIS
		Inserts or rewrites the "> Measured ..." blockquote that immediately
		follows the named layer's :end marker. Match by anchored prefix so a
		hand-written blockquote elsewhere (e.g. an unrelated quote in the
		surrounding prose) is never touched.
	#>
	[CmdletBinding()] param(
		[Parameter(Mandatory)][string] $Content,
		[Parameter(Mandatory)][string] $Layer,
		[Parameter(Mandatory)][string] $Note
	)
	# Match the :end marker, then any whitespace (including a single blank
	# line), then optionally an existing '> Measured ...' line. Substitute
	# with marker + blank + our note + blank.
	$endTag = "<!-- perf-table:${Layer}:end -->"
	$pattern = '(?m)' + [regex]::Escape($endTag) + "\r?\n(\r?\n)?(?:> Measured [^\r\n]*\r?\n(\r?\n)?)?"
	$replacement = $endTag + "`r`n`r`n" + $Note + "`r`n`r`n"
	# Replace at most once - if for some reason two :end markers share a
	# layer name (which the hygiene test forbids), at least we don't fan out.
	return [regex]::Replace($Content, $pattern, [System.Text.RegularExpressions.MatchEvaluator] { param($m) $replacement }, 1)
}

function Update-DocMarkers {
	[CmdletBinding()] param(
		[Parameter(Mandatory)][string] $DocPath,
		[Parameter(Mandatory)][hashtable] $State,
		[switch] $WhatIf
	)
	$content = [System.IO.File]::ReadAllText($DocPath)

	$nl = "`r`n"

	$existingL1 = Get-ExistingTableRows -Content $content -Layer 'layer1'
	$existingL2 = Get-ExistingTableRows -Content $content -Layer 'layer2'
	$existingMetaL1 = Get-ExistingMetaHeader -Content $content -Layer 'layer1'
	$existingMetaL2 = Get-ExistingMetaHeader -Content $content -Layer 'layer2'

	# Rewrite a layer's block only when we have at least one fresh aggregated
	# row. If every cohort failed for a layer (RowsAgg empty), leave the
	# existing block byte-identical - rewriting it would re-sort meta keys and
	# re-shape the table header, producing noisy "diff for no reason" output
	# and (worse) painting a stale meta header (host=current-VM-SKU,
	# cohortN=preserved) onto rows that fell back from the previous
	# measurement.
	if ($State.layer1.rows.Count -gt 0) {
		$meta1 = New-MetaHeaderForLayer1 -State $State -RowsAgg $State.layer1.rows -Existing $existingMetaL1
		$header1 = Render-MetaHeader -Layer 'layer1' -Meta $meta1
		$table1 = Render-Layer1Table -RowsAgg $State.layer1.rows -ExistingRows $existingL1
		$newBlock1 = $header1 + $nl + $nl + $table1 + $nl + $nl + '<!-- perf-table:layer1:end -->'

		$pattern1 = '(?s)<!-- perf-table:layer1:start.*?<!-- perf-table:layer1:end -->'
		$content = [regex]::Replace($content, $pattern1, [System.Text.RegularExpressions.MatchEvaluator] { param($m) $newBlock1 })
		$content = Set-ProvenanceNote -Content $content -Layer 'layer1' -Note (Render-ProvenanceNote -Layer 'layer1' -Meta $meta1)
	} elseif ($existingL1.Count -eq 0) {
		# Edge case: marker block exists but contains no rows (e.g. operator
		# committed an empty marker pair as a seed). Render placeholders so the
		# next run has something to update.
		$meta1 = New-MetaHeaderForLayer1 -State $State -RowsAgg @{} -Existing $existingMetaL1
		$header1 = Render-MetaHeader -Layer 'layer1' -Meta $meta1
		$table1 = Render-Layer1Table -RowsAgg @{} -ExistingRows @{}
		$newBlock1 = $header1 + $nl + $nl + $table1 + $nl + $nl + '<!-- perf-table:layer1:end -->'
		$pattern1 = '(?s)<!-- perf-table:layer1:start.*?<!-- perf-table:layer1:end -->'
		$content = [regex]::Replace($content, $pattern1, [System.Text.RegularExpressions.MatchEvaluator] { param($m) $newBlock1 })
	}

	if ($State.layer2.rows.Count -gt 0) {
		$meta2 = New-MetaHeaderForLayer2 -State $State -RowsAgg $State.layer2.rows -Existing $existingMetaL2
		$header2 = Render-MetaHeader -Layer 'layer2' -Meta $meta2
		$table2 = Render-Layer2Table -RowsAgg $State.layer2.rows -ExistingRows $existingL2
		$newBlock2 = $header2 + $nl + $nl + $table2 + $nl + $nl + '<!-- perf-table:layer2:end -->'

		$pattern2 = '(?s)<!-- perf-table:layer2:start.*?<!-- perf-table:layer2:end -->'
		$content = [regex]::Replace($content, $pattern2, [System.Text.RegularExpressions.MatchEvaluator] { param($m) $newBlock2 })
		$content = Set-ProvenanceNote -Content $content -Layer 'layer2' -Note (Render-ProvenanceNote -Layer 'layer2' -Meta $meta2)
	} elseif ($existingL2.Count -eq 0) {
		$meta2 = New-MetaHeaderForLayer2 -State $State -RowsAgg @{} -Existing $existingMetaL2
		$header2 = Render-MetaHeader -Layer 'layer2' -Meta $meta2
		$table2 = Render-Layer2Table -RowsAgg @{} -ExistingRows @{}
		$newBlock2 = $header2 + $nl + $nl + $table2 + $nl + $nl + '<!-- perf-table:layer2:end -->'
		$pattern2 = '(?s)<!-- perf-table:layer2:start.*?<!-- perf-table:layer2:end -->'
		$content = [regex]::Replace($content, $pattern2, [System.Text.RegularExpressions.MatchEvaluator] { param($m) $newBlock2 })
	}

	if ($WhatIf) {
		# Print to stdout; do not write.
		Write-Host '--- planned doc (markers only) ---' -ForegroundColor Cyan
		# Just show the marker regions, not the whole file.
		$matchesL1 = [regex]::Matches($content, '(?s)<!-- perf-table:layer1:start.*?<!-- perf-table:layer1:end -->')
		$matchesL2 = [regex]::Matches($content, '(?s)<!-- perf-table:layer2:start.*?<!-- perf-table:layer2:end -->')
		foreach ($m in @($matchesL1) + @($matchesL2)) { Write-Host $m.Value }
		return
	}

	[System.IO.File]::WriteAllText($DocPath, $content)
}

function Update-MultiSiloDocMarkers {
	<#
	.SYNOPSIS
		Rewrite the two marker regions in performance-multi-silo.md: the
		scaling chart and the multi-silo grid.
	.DESCRIPTION
		Two regions rather than one because they fail independently. The
		chart needs every plotted workload to share an x-axis, so a partial
		sweep can legitimately produce a full table and no chart; collapsing
		them into one region would mean a missing chart also blanked the
		numbers, which are the authoritative record.

		Unlike Update-DocMarkers, this does NOT preserve prior rows on a
		miss. A Layer 2 table is a set of independent per-operation rows, so
		preserving an un-rerun row is honest. A scaling curve is not: mixing
		cells from two different sweeps produces a curve whose shape is an
		artefact of when each point was measured. If a sweep yields no
		aggregated cells the block is left byte-identical instead.
	#>
	[CmdletBinding()] param(
		[Parameter(Mandatory)][string] $DocPath,
		[Parameter(Mandatory)][hashtable] $State,
		[switch] $WhatIf
	)
	if (-not (Test-Path $DocPath)) { throw "Multi-silo doc not found at $DocPath" }
	$content = [System.IO.File]::ReadAllText($DocPath)
	$nl = "`r`n"

	$rowsAgg = if ($State.ContainsKey('layer3')) { $State.layer3.rows } else { @{} }
	if ($rowsAgg.Count -eq 0) {
		Write-Warning "[layer3] no aggregated cells; leaving $DocPath unchanged. A scaling curve must not blend cells from different sweeps."
		return $false
	}

	$existingMeta = Get-ExistingMetaHeader -Content $content -Layer 'layer3'
	$meta = New-MetaHeaderForLayer3 -State $State -RowsAgg $rowsAgg -Existing $existingMeta

	$header = Render-MetaHeader -Layer 'layer3' -Meta $meta
	$table  = Render-Layer3Table -RowsAgg $rowsAgg
	$block  = $header + $nl + $nl + $table + $nl + $nl + '<!-- perf-table:layer3:end -->'
	$pattern = '(?s)<!-- perf-table:layer3:start.*?<!-- perf-table:layer3:end -->'
	if (-not [regex]::IsMatch($content, $pattern)) {
		throw "Missing <!-- perf-table:layer3:start --> ... <!-- perf-table:layer3:end --> marker pair in $DocPath"
	}
	$content = [regex]::Replace($content, $pattern, [System.Text.RegularExpressions.MatchEvaluator] { param($m) $block })
	$content = Set-ProvenanceNote -Content $content -Layer 'layer3' -Note (Render-ProvenanceNote -Layer 'layer3' -Meta $meta)

	$chart = Render-Layer3Chart -RowsAgg $rowsAgg
	# The chart block carries 'schema' and the do-not-edit notice - the two keys
	# that mark a block as mechanically managed - but deliberately not the full
	# provenance key set. It is rendered from the same $rowsAgg as the table in
	# this one atomic update, so it cannot disagree with the table beside it,
	# and duplicating 14 keys (including the multi-paragraph 'methodology'
	# value) directly above the figure would double the doc's marker bulk for
	# no reader benefit. PerformanceReportMarkerHygieneTestsBase encodes the
	# same split.
	$chartBlock = '<!-- perf-chart:layer3:start' + $nl + '  schema=v1' + $nl + '  DO-NOT-HAND-EDIT-BETWEEN-MARKERS' + $nl + '-->' + $nl + $nl + $chart + $nl + $nl + '<!-- perf-chart:layer3:end -->'
	$chartPattern = '(?s)<!-- perf-chart:layer3:start.*?<!-- perf-chart:layer3:end -->'
	if (-not [regex]::IsMatch($content, $chartPattern)) {
		throw "Missing <!-- perf-chart:layer3:start --> ... <!-- perf-chart:layer3:end --> marker pair in $DocPath"
	}
	$content = [regex]::Replace($content, $chartPattern, [System.Text.RegularExpressions.MatchEvaluator] { param($m) $chartBlock })

	if ($WhatIf) {
		Write-Host '--- planned multi-silo doc (markers only) ---' -ForegroundColor Cyan
		foreach ($m in @([regex]::Matches($content, $chartPattern)) + @([regex]::Matches($content, $pattern))) { Write-Host $m.Value }
		return $true
	}
	[System.IO.File]::WriteAllText($DocPath, $content)
	return $true
}

# ────────────────────────────────────────────────────────────────────────────
# Main
# ────────────────────────────────────────────────────────────────────────────

function Main {
	# Resolve parameters file.
	$paramFile = if ($ParametersFile) { $ParametersFile } else { Join-Path $azScriptsDir 'parameters.local.ps1' }

	# DryRun path: read latest state.json and re-render the doc; no Azure.
	if ($DryRun) {
		$stateFile = Get-LatestStateFile
		if (-not $stateFile) {
			throw "DryRun: no state.json found under $runRoot. Run a non-DryRun pass first to populate state.json."
		}
		Write-Host "[dry-run] state file: $stateFile" -ForegroundColor Cyan
		$state = Read-StateFile -Path $stateFile

		# Layer 3 replays the multi-silo doc and returns. Without this the
		# switch combination silently did the wrong thing: -Layer3 -DryRun fell
		# through to the Layer 1/2 replay below, rewrote performance-single-silo.md
		# from a state file that has no layer1/layer2 section, reported "doc
		# rewritten", and left the multi-silo doc untouched. A no-op that
		# announces success is worse than a throw, because the operator's next
		# move is to conclude the render bug they were chasing is fixed.
		if ($Layer3) {
			if (-not $state.ContainsKey('layer3')) {
				throw "DryRun -Layer3: $stateFile has no 'layer3' section. Run a Layer 3 pass first, or drop -Layer3 to replay the single-silo doc."
			}
			# Re-aggregate from the raw cells for the same reason Layer 2 does:
			# replaying is how an aggregator fix is validated without paying for
			# a fresh sweep.
			if ($state.layer3.ContainsKey('cohorts') -and $state.layer3.cohorts -is [System.Collections.IDictionary] -and $state.layer3.cohorts.Count -gt 0) {
				# Re-derive the per-call quantiles (only) from each retained
				# cohort log, so a fix to the [phaseA] instrument/tree selection
				# reaches the published latency columns without a fresh sweep.
				# Throughput stays frozen: it is FINAL-line derived and a
				# re-parse would reproduce it exactly.
				$l3BatchSize = [int](Get-StateOr $state 'batchSize' 4096)
				foreach ($mode in @($state.layer3.cohorts.Keys)) {
					foreach ($siloKey in @($state.layer3.cohorts[$mode].Keys)) {
						foreach ($cohort in @($state.layer3.cohorts[$mode][$siloKey])) {
							$cohortLog = Get-StateOr $cohort 'siloLog' $null
							if ($cohortLog -and (Test-Path $cohortLog)) {
								$reparsed = Read-SiloLogStats -SiloLogPath $cohortLog -WorkloadMode $mode -BatchSize $l3BatchSize
								$cohort.perCallP50Ms = $reparsed.PerCallP50Ms
								$cohort.perCallP75Ms = $reparsed.PerCallP75Ms
								$cohort.perCallP90Ms = $reparsed.PerCallP90Ms
								$cohort.perCallP99Ms = $reparsed.PerCallP99Ms
							}
						}
					}
				}
				$state.layer3.rows = Aggregate-Layer3Cells -Cells $state.layer3.cohorts
			}
			$l3Replayed = Update-MultiSiloDocMarkers -DocPath $multiSiloDocPath -State $state -WhatIf:$Diff
			if (-not $Diff -and -not $SkipDocUpdate) {
				if ($l3Replayed) {
					Write-Host "[dry-run] multi-silo doc rewritten from state.json" -ForegroundColor Green
				} else {
					Write-Host "[dry-run] multi-silo doc left unchanged (no aggregated cells in $stateFile)" -ForegroundColor Yellow
				}
			}
			return
		}
		# Re-aggregate from the raw per-cohort metrics whenever the state file
		# carries them. The pre-baked $state.layer1.rows / $state.layer2.rows
		# are kept as a fallback for state files written before the cohorts
		# arrays were populated, but when both are present the cohorts win -
		# re-aggregating is the whole point of a dry-run replay (fix an
		# aggregator bug locally, replay against the on-disk run logs, see
		# the corrected doc without paying for a fresh Azure VM).
		if ($state.ContainsKey('layer1') -and $state.layer1.ContainsKey('cohorts') -and @($state.layer1.cohorts).Count -gt 0) {
			$state.layer1.rows = Aggregate-Layer1Cells -Cohorts $state.layer1.cohorts
		}
		# Re-derive each Layer 2 cohort's steady-state mean and per-call
		# quantiles from its retained on-disk silo log before re-aggregating.
		# Read-SiloLogStats runs once at cohort time and freezes its result into
		# state.json, so without this re-parse a fix to the per-call instrument
		# selection (e.g. excluding the async view-maintainer's tree=view-* rows)
		# would never reach a -DryRun replay - Aggregate-Layer2Cells only
		# re-medians the already-baked per-cohort values. Re-parsing the logs the
		# run already pulled back is the whole point of a dry-run replay: fix an
		# aggregator/parse bug locally, replay, see the corrected doc without
		# paying for a fresh Azure VM. Cohorts whose silo log is no longer on
		# disk keep their stored values.
		if ($state.ContainsKey('layer2') -and $state.layer2.ContainsKey('cohorts') -and $state.layer2.cohorts -is [hashtable]) {
			$dryRunBatchSize = [int](Get-StateOr $state 'batchSize' 4096)
			foreach ($mode in @($state.layer2.cohorts.Keys)) {
				foreach ($cohort in @($state.layer2.cohorts[$mode])) {
					$cohortLog = Get-StateOr $cohort 'siloLog' $null
					if ($cohortLog -and (Test-Path $cohortLog)) {
						$reparsed = Read-SiloLogStats -SiloLogPath $cohortLog -WorkloadMode $mode -BatchSize $dryRunBatchSize
						$cohort.steadyMean   = $reparsed.SteadyMean
						$cohort.perCallP50Ms = $reparsed.PerCallP50Ms
						$cohort.perCallP75Ms = $reparsed.PerCallP75Ms
						$cohort.perCallP90Ms = $reparsed.PerCallP90Ms
						$cohort.perCallP99Ms = $reparsed.PerCallP99Ms
						$cohort.inFlightMax  = $reparsed.InFlightMax
						$cohort.failed       = $reparsed.Failed
						$cohort.verdict      = $reparsed.Verdict
					}
				}
			}
		}
		if ($state.ContainsKey('layer2') -and $state.layer2.ContainsKey('cohorts') -and $state.layer2.cohorts -is [hashtable] -and $state.layer2.cohorts.Count -gt 0) {
			$state.layer2.rows = Aggregate-Layer2Cells -CohortsByMode $state.layer2.cohorts
		}
		Update-DocMarkers -DocPath $docPath -State $state -WhatIf:$Diff
		if (-not $Diff -and -not $SkipDocUpdate) {
			Write-Host "[dry-run] doc rewritten from state.json" -ForegroundColor Green
		}
		return
	}

	# ── Layer 3 ──────────────────────────────────────────────────────────
	# Returns early rather than joining the Layer 1/2 flow. Layer 3 runs on
	# a Container Apps rig, not the Azure VM, so every step of the shared
	# path below - Test-PreflightOrThrow (checks the VM parameters file),
	# Invoke-Provision (creates the VM), Get-VmDotnetVersion (SSHes to it),
	# Invoke-Teardown (deletes rg-<prefix>) - is either irrelevant or
	# actively wrong here. In particular the shared teardown would delete a
	# resource group this layer never created.
	if ($Layer -eq '3') {
		$acaScript = Join-Path $azScriptsDir 'deploy-aca.ps1'
		if (-not (Test-Path $acaScript)) { throw "Missing $acaScript" }
		# Dot-sourced here as well as inside Invoke-Layer3Cohorts: a function
		# that dot-sources gets the definitions in ITS scope only, so Main
		# cannot see Get-AcaRunRoot unless it sources the module itself.
		. (Join-Path $azScriptsDir 'aca-common.ps1')

		$acaPrefix = if ($ReuseAca) { Get-CleanedPrefix -Prefix $ReuseAca } elseif ($NamePrefix) { Get-CleanedPrefix -Prefix $NamePrefix } else { New-RunPrefix }
		Write-Host "[main] Layer 3 aca prefix=$acaPrefix silos=$($SiloCounts -join ',')" -ForegroundColor Cyan

		$l3Dir = Join-Path $runRoot $acaPrefix
		if (-not (Test-Path $l3Dir)) { New-Item -ItemType Directory -Path $l3Dir -Force | Out-Null }
		$l3StateFile = Join-Path $l3Dir 'state.json'
		# The region has to come from the ACA context, not from the Layer 1/2
		# -Region default: this layer's resources live wherever deploy-aca.ps1
		# put them, and the meta-header's region key is provenance a reader
		# uses to reproduce the run. On a fresh sweep the context does not
		# exist yet, so fall back to deploy-aca.ps1's own default and correct
		# it from the context once provisioning has written one (below).
		$l3Region = 'westus3'
		$l3ContextPath = Join-Path (Get-AcaRunRoot) "$acaPrefix.context.json"
		if (Test-Path $l3ContextPath) {
			try {
				$ctxLoc = (Get-Content $l3ContextPath -Raw | ConvertFrom-Json).location
				if ($ctxLoc) { $l3Region = [string]$ctxLoc }
			} catch {
				Write-Warning "[layer3] could not read region from $l3ContextPath; using $l3Region"
			}
		}
		$l3State = if (Test-Path $l3StateFile) {
			Read-StateFile -Path $l3StateFile
		} else {
			New-EmptyState -Prefix $acaPrefix -VmSize $VmSize -Region $l3Region -Rung (Resolve-Rung -Spec $Rung) -BatchSize $BatchSize -BdnFidelity $Fidelity
		}
		$l3State['region'] = $l3Region
		if (-not $l3State.ContainsKey('layer3')) {
			$l3State['layer3'] = @{ cohorts = @{}; rows = @{}; siloCounts = @(); acaPrefix = $null }
		}
		$l3State.layer3.acaPrefix  = $acaPrefix
		$l3State.layer3.siloCounts = @($SiloCounts)
		$l3State['batchSize']      = $BatchSize
		$l3State.startedUtc        = (Get-Date).ToUniversalTime().ToString('o')

		$acaProvisioned = $false
		try {
			if (-not $ReuseAca) {
				# Same reasoning as the Layer 1/2 provisioning gate: flag
				# teardown-needed BEFORE the call, because deploy-aca.ps1
				# creates the resource group early and can throw later (an
				# image build failure, a quota refusal) with billable
				# resources already standing.
				$acaProvisioned = $true
				& $acaScript -NamePrefix $acaPrefix | Out-Host
				if ($LASTEXITCODE -ne 0) { throw "deploy-aca.ps1 exited $LASTEXITCODE" }
				# Provisioning has now written the context, so the region the
				# resources actually landed in is knowable. Correct the state
				# rather than publishing the pre-provisioning guess.
				if (Test-Path $l3ContextPath) {
					try {
						$ctxLoc = (Get-Content $l3ContextPath -Raw | ConvertFrom-Json).location
						if ($ctxLoc) { $l3State['region'] = [string]$ctxLoc }
					} catch {
						Write-Warning "[layer3] could not read region from $l3ContextPath after provisioning"
					}
				}
			} else {
				Write-Host "[main] -ReuseAca ${ReuseAca}: skipping ACA provisioning" -ForegroundColor Yellow
			}

			$l3Ids = Resolve-WorkloadIds -LayerRows $Layer3Rows -WorkloadsSpec $Workloads
			Write-Host "[main] Layer 3 workloads: $($l3Ids -join ',')" -ForegroundColor Cyan

			# Checkpoint after every cell. An unattended sweep that dies at
			# silos=6 must keep the 1/2/4 cells it already paid for.
			$checkpoint = {
				param($cells)
				$l3State.layer3.cohorts = $cells
				$l3State.layer3.rows    = Aggregate-Layer3Cells -Cells $cells
				Write-StateFile -Path $l3StateFile -State $l3State
			}

			# One declaration of the Layer 3 cohort configuration, used for
			# BOTH the cohort invocation and the published metadata. Keeping
			# them in one place is what stops the doc describing a
			# configuration that never ran - the failure mode that published
			# walPartitions=8 for a sweep that ran at 16.
			$l3ShardCount           = 64
			$l3WalPartitions        = 16
			$l3WalMaxPendingBatches = 16
			$l3State.layer3ShardCount           = $l3ShardCount
			$l3State.layer3WalPartitions        = $l3WalPartitions
			$l3State.layer3WalMaxPendingBatches = $l3WalMaxPendingBatches
			# run-cohort-aca.ps1's own -ResponseTimeoutSec default, which this
			# driver does not override. Recorded so the header states the value
			# that ran rather than New-EmptyState's Layer 2 default of 180.
			$l3State['responseTimeoutSec']      = 420
			$l3State.layer3SaturationRatio      = $SaturationRatio
			$l3State.layer3MaxRungEscalations   = $MaxRungEscalations

			$l3Cells = Invoke-Layer3Cohorts `
				-AcaPrefix   $acaPrefix `
				-WorkloadIds $l3Ids `
				-SiloCounts  $SiloCounts `
				-N           $N `
				-BatchSize   $BatchSize `
				-ShardCount           $l3ShardCount `
				-WalPartitions        $l3WalPartitions `
				-WalMaxPendingBatches $l3WalMaxPendingBatches `
				-SetManyFanOutBudgetSec    $SetManyFanOutBudgetSec `
				-WalAdmissionCallBudgetSec $WalAdmissionCallBudgetSec `
				-WalAppendCoalescingInFlightThreshold $WalAppendCoalescingInFlightThreshold `
				-WalSaturationRecoveryReleaseBatch $WalSaturationRecoveryReleaseBatch `
				-ClientsPerSilo     $Layer3ClientsPerSilo `
				-SaturationRatio    $SaturationRatio `
				-MaxRungEscalations $MaxRungEscalations `
				-ExistingCells $(if ($Resume -and $l3State.layer3.cohorts -is [System.Collections.IDictionary]) { $l3State.layer3.cohorts } else { @{} }) `
				-OnCellComplete $checkpoint

			$l3State.layer3.cohorts = $l3Cells
			$l3State.layer3.rows    = Aggregate-Layer3Cells -Cells $l3Cells
			$l3State.endedUtc       = (Get-Date).ToUniversalTime().ToString('o')
			Write-StateFile -Path $l3StateFile -State $l3State
			Write-Host "[main] state.json: $l3StateFile" -ForegroundColor Green

			if (-not $SkipDocUpdate) {
				$l3Written = Update-MultiSiloDocMarkers -DocPath $multiSiloDocPath -State $l3State -WhatIf:$Diff
				if ($l3Written) {
					Write-Host "[main] doc updated: $multiSiloDocPath" -ForegroundColor Green
				} else {
					# Reporting "doc updated" after the renderer declined to
					# write is how an unattended sweep that produced no usable
					# cell gets mistaken for a successful one. Fail loudly
					# instead: an empty sweep is never the intended outcome.
					throw "[main] Layer 3 sweep produced no aggregated cells; $multiSiloDocPath was left unchanged. Check the per-cohort warnings above."
				}
			} else {
				Write-Host "[main] -SkipDocUpdate: not rewriting $multiSiloDocPath" -ForegroundColor Yellow
			}
		} finally {
			# Park the silos unconditionally, even when the rig is kept.
			# "No containers left running longer than needed" has to hold on
			# the failure path too, and a sweep that throws mid-cell would
			# otherwise leave N replicas billing indefinitely.
			try {
				. (Join-Path $azScriptsDir 'aca-common.ps1')
				$parkCtx = Read-AcaContext -NamePrefix $acaPrefix
				Set-AcaSiloCount -Context $parkCtx -Count 0 | Out-Null
			} catch {
				Write-Warning "[main] could not park silos for ${acaPrefix}: $($_.Exception.Message)"
			}

			if ($acaProvisioned -and -not $KeepAca) {
				Invoke-AcaTeardown -NamePrefix $acaPrefix
				Assert-NoStrayBenchContainers -NamePrefix $acaPrefix
			} elseif ($KeepAca) {
				Write-Host ''
				Write-Host "[main] -KeepAca: resource group 'rg-$acaPrefix' preserved (silos parked at zero)." -ForegroundColor Yellow
				Write-Host "       Manual cleanup: az group delete --name rg-$acaPrefix --yes" -ForegroundColor Yellow
			} elseif ($ReuseAca) {
				Write-Host ''
				Write-Host "[main] -ReuseAca: resource group 'rg-$acaPrefix' preserved (was not provisioned by this run)." -ForegroundColor Yellow
			}
		}

		Write-Host ''
		Write-Host '=== performance-report.ps1 (Layer 3) complete ===' -ForegroundColor Green
		Write-Host ("Prefix     : {0}" -f $acaPrefix)
		Write-Host ("State file : {0}" -f $l3StateFile)
		Write-Host ("Doc        : {0}" -f $multiSiloDocPath)
		return
	}

	# Preflight.
	Test-PreflightOrThrow -ParametersFilePath $paramFile | Out-Null
	# Resolve prefix.
	$prefix = if ($NamePrefix) { Get-CleanedPrefix -Prefix $NamePrefix } elseif ($ReuseVm) { Get-CleanedPrefix -Prefix $ReuseVm } else { New-RunPrefix }
	Write-Host "[main] prefix=$prefix" -ForegroundColor Cyan

	# Resolve rung + region.
	$rungHt = Resolve-Rung -Spec $Rung
	$p = & $paramFile
	$region = $p.Location

	# Initialise state (or merge with prior if -ReuseVm).
	$prefixDir = Join-Path $runRoot $prefix
	if (-not (Test-Path $prefixDir)) { New-Item -ItemType Directory -Path $prefixDir -Force | Out-Null }
	$stateFile = Join-Path $prefixDir 'state.json'
	$state = if (Test-Path $stateFile) {
		Read-StateFile -Path $stateFile
	} else {
		New-EmptyState -Prefix $prefix -VmSize $VmSize -Region $region -Rung $rungHt -BatchSize $BatchSize -BdnFidelity $Fidelity
	}
	$state.startedUtc = (Get-Date).ToUniversalTime().ToString('o')
	# Stamp the resolved CLI -Fidelity / -BatchSize onto state ONLY for layers
	# that are actually running this invocation. bdnFidelity is a Layer 1
	# concept (BDN); batchSize is a Layer 2 concept (BENCH_BATCH_SIZE). A
	# -Layer 2 run that overwrites bdnFidelity (or a -Layer 1 run that
	# overwrites batchSize) would cause the OTHER layer's meta-header to
	# re-render with a stale value sourced from this run's CLI defaults
	# rather than from the layer's actual measurement. Gating preserves
	# the cross-layer invariant: each meta-key reflects the most recent run
	# that actually measured the layer it belongs to.
	if ($Layer -in 'all','1') { $state['bdnFidelity'] = $Fidelity }
	if ($Layer -in 'all','2') { $state['batchSize']   = $BatchSize }

	$provisioned = $false
	try {
		# Provision (unless -ReuseVm).
		if (-not $ReuseVm) {
			# Flag teardown-needed BEFORE calling Invoke-Provision, not
			# after. Invoke-Provision wraps deploy.ps1, which internally
			# (a) creates the resource group + VM + storage account, and
			# only then (b) chains into update.ps1 to publish the silo +
			# producer binaries on the VM. Step (b) is where the remote
			# `dotnet publish` runs - any compile error there throws
			# *after* the Azure resources from step (a) already exist
			# and are billing. The teardown gate must therefore reflect
			# "did we ask Azure for a resource group?" (i.e., always on
			# the path that called Invoke-Provision), not "did
			# Invoke-Provision return successfully?". Otherwise a remote
			# build failure orphans the resource group and the VM burns
			# paid compute until the deploy script's auto-shutdown
			# timer (default 1900 UTC) fires - hours later, at the
			# operator's expense.
			$provisioned = $true
			Invoke-Provision -Prefix $prefix -VmSize $VmSize -ParametersFilePath $paramFile
		} else {
			Write-Host "[main] -ReuseVm ${ReuseVm}: skipping provisioning" -ForegroundColor Yellow
		}

		# Probe the VM's actual `dotnet --version` and persist it on $state.
		# Stamps the meta-header's `dotnet=` cell with the real SDK the silo
		# + microbench run under, replacing the legacy '10.0.x' placeholder.
		# Failures are non-fatal (the placeholder is the documented fallback).
		$probed = Get-VmDotnetVersion -Prefix $prefix -ParametersFilePath $paramFile
		if ($probed) {
			$state['dotnetVersion'] = $probed
			Write-Host "[main] vm dotnet: $probed" -ForegroundColor DarkGray
		} else {
			Write-Warning "[main] could not probe /usr/bin/dotnet --version on the VM; meta-header will retain the previous value (or the '10.0.x' placeholder)."
		}

		# Layer 1.
		if ($Layer -in 'all','1') {
			$l1Ids = Resolve-WorkloadIds -LayerRows $Layer1Rows -WorkloadsSpec $Workloads
			Write-Host "[main] Layer 1 workloads: $($l1Ids -join ',')" -ForegroundColor Cyan
			$l1Cohorts = Invoke-Layer1Cohorts -Prefix $prefix -WorkloadIds $l1Ids -N $N -ParametersFilePath $paramFile -Fidelity $Fidelity
			# Merge cohorts (rather than replace) so partial re-runs don't lose
			# earlier rows' data; the aggregator will use the latest cohorts.
			$state.layer1.cohorts = @($state.layer1.cohorts) + @($l1Cohorts)
			$l1Rows = Aggregate-Layer1Cells -Cohorts $l1Cohorts
			foreach ($k in $l1Rows.Keys) { $state.layer1.rows[$k] = $l1Rows[$k] }
		}

		# Layer 2.
		if ($Layer -in 'all','2') {
			$l2Ids = Resolve-WorkloadIds -LayerRows $Layer2Rows -WorkloadsSpec $Workloads
			Write-Host "[main] Layer 2 workloads: $($l2Ids -join ',')" -ForegroundColor Cyan
			$l2CohortsByMode = Invoke-Layer2Cohorts -Prefix $prefix -WorkloadIds $l2Ids -Rung $rungHt -N $N -ParametersFilePath $paramFile -ResponseTimeoutSec $state.responseTimeoutSec -WalPartitions $state.walPartitions -WalMaxPendingBatches $state.walMaxPendingBatches -BatchSize $BatchSize -CaptureCounters:$CaptureCounters
			foreach ($mode in $l2CohortsByMode.Keys) { $state.layer2.cohorts[$mode] = $l2CohortsByMode[$mode] }
			$l2Rows = Aggregate-Layer2Cells -CohortsByMode $l2CohortsByMode
			foreach ($k in $l2Rows.Keys) { $state.layer2.rows[$k] = $l2Rows[$k] }
		}

		$state.endedUtc = (Get-Date).ToUniversalTime().ToString('o')
		Write-StateFile -Path $stateFile -State $state
		Write-Host "[main] state.json: $stateFile" -ForegroundColor Green

		if (-not $SkipDocUpdate) {
			Update-DocMarkers -DocPath $docPath -State $state
			Write-Host "[main] doc updated: $docPath" -ForegroundColor Green
		} else {
			Write-Host "[main] -SkipDocUpdate: not rewriting $docPath" -ForegroundColor Yellow
		}

	} finally {
		if ($provisioned -and -not $KeepVm) {
			Invoke-Teardown -Prefix $prefix
		} elseif ($KeepVm) {
			Write-Host ''
			Write-Host "[main] -KeepVm: resource group 'rg-$prefix' preserved." -ForegroundColor Yellow
			Write-Host "       Manual cleanup: az group delete --name rg-$prefix --yes" -ForegroundColor Yellow
		} elseif ($ReuseVm) {
			Write-Host ''
			Write-Host "[main] -ReuseVm: resource group 'rg-$prefix' preserved (was not provisioned by this run)." -ForegroundColor Yellow
		}
	}

	Write-Host ''
	Write-Host '=== performance-report.ps1 complete ===' -ForegroundColor Green
	Write-Host ("Prefix     : {0}" -f $prefix)
	Write-Host ("State file : {0}" -f $stateFile)
	Write-Host ("Doc        : {0}" -f $docPath)
}

# Trap SIGINT so the finally block runs even on Ctrl+C.
try { Main } catch {
	Write-Host ''
	Write-Host "[main] FAILED: $($_.Exception.Message)" -ForegroundColor Red
	throw
}
