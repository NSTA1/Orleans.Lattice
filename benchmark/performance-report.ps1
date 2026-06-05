#!/usr/bin/env pwsh
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
						bulk-load, set-many-atomic
		Layer 2 values: set-many, set-many-atomic, set-point, get-point,
						get-many
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
	[ValidateSet('all','1','2')]
	[string] $Layer = 'all',

	[string] $Workloads,

	[ValidateRange(1, 20)]
	[int] $N = 3,

	[string] $VmSize = 'Standard_D4as_v5',

	[string] $Rung = '4000:5:45',

	[switch] $DryRun,
	[switch] $Diff,
	[switch] $KeepVm,
	[string] $ReuseVm,
	[switch] $SkipDocUpdate,

	[ValidateSet('dry','quick','full')]
	[string] $Fidelity = 'dry',

	# Convenience switches: equivalent to -Layer 1 / -Layer 2. Either form works;
	# mutually exclusive with -Layer except for the default (all). These are here
	# because operators reach for them naturally ('-Layer1') over '-Layer 1'.
	[switch] $Layer1,
	[switch] $Layer2,

	[string] $NamePrefix,
	[string] $ParametersFile
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

# Resolve the layer-switches into the single $Layer enum so the rest of the
# script branches on one value. Switch precedence:
#   - If neither -Layer1 nor -Layer2 is set, $Layer (default 'all') wins.
#   - If exactly one is set, it overrides $Layer (so '-Layer 2 -Layer1' is a
#     user mistake we report rather than silently picking one).
#   - If both are set, that's 'all'.
#   - Mixing -Layer with -Layer1 / -Layer2 to disagreeing values is rejected.
$switchPicks = @()
if ($Layer1) { $switchPicks += '1' }
if ($Layer2) { $switchPicks += '2' }
if ($switchPicks.Count -gt 0) {
	$switchValue = if ($switchPicks.Count -eq 2) { 'all' } else { $switchPicks[0] }
	# If the caller passed both -Layer and a -LayerN switch with conflicting
	# values, surface a clear error instead of picking one silently.
	$layerExplicit = $PSBoundParameters.ContainsKey('Layer')
	if ($layerExplicit -and $Layer -ne $switchValue) {
		throw "Conflicting layer selection: -Layer '$Layer' vs the switch(es) that resolve to '$switchValue'. Use ONE of -Layer <all|1|2> OR -Layer1 / -Layer2."
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
		CeilingUnit = 'op/s';
	},
	@{
		Label = '`SetAsync` (point write)';
		WorkloadId = 'point-write';
		BdnMethod = 'PointWrite';
		MetricSlug = 'point_write';
		ExpectedBatchSize = 1;
		CeilingUnit = 'op/s';
	},
	@{
		Label = '`GetManyAsync` (16 keys/call)';
		WorkloadId = 'point-get-many';
		BdnMethod = 'PointGetMany';
		MetricSlug = 'point_get_many';
		ExpectedBatchSize = 16;
		CeilingUnit = 'calls/s';
		# Note: the current bench builds a 4-key batch (LatticeMicroBenchmarks.cs
		# line ~430). When this script first runs against the real bench, the
		# row label '(16 keys/call)' will not match. The aggregator emits a
		# warning; consider adding a BENCH_MICROBENCH_GETMANY_BATCH env to the
		# fixture and pinning it to 16 to align bench with doc.
	},
	@{
		Label = '`SetManyAsync` (1,000 entries/call)';
		WorkloadId = 'bulk-load';
		BdnMethod = 'BulkLoad';
		MetricSlug = 'bulk_load';
		ExpectedBatchSize = 1000;
		CeilingUnit = 'calls/s';
	},
	@{
		Label = '`SetManyAtomicAsync` (16 keys/saga)';
		WorkloadId = 'set-many-atomic';
		BdnMethod = 'SetManyAtomic';
		MetricSlug = 'set_many_atomic';
		ExpectedBatchSize = 16;
		CeilingUnit = 'sagas/s';
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
		Label = '`SetAsync` (point write)';
		WorkloadId = 'set-point';
		WorkloadMode = 'set-point';
		ThroughputUnit = 'keys/s';
	},
	@{
		Label = '`GetManyAsync` (4,096 keys/call)';
		WorkloadId = 'get-many';
		WorkloadMode = 'get-many';
		ThroughputUnit = 'keys/s';
	},
	@{
		Label = '`SetManyAsync` (4,096 entries/call)';
		WorkloadId = 'set-many';
		WorkloadMode = 'set-many';
		ThroughputUnit = 'entries/s';
	},
	@{
		Label = '`SetManyAtomicAsync` (64 keys/saga)';
		WorkloadId = 'set-many-atomic';
		WorkloadMode = 'set-many-atomic';
		ThroughputUnit = 'keys/s';
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
	$null = & ssh -V 2>&1
	if ($LASTEXITCODE -ne 0) {
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
		[string] $BdnFidelity = 'dry'
	)
	return @{
		schema             = 'v1'
		prefix             = $Prefix
		vmSize             = $VmSize
		region             = $Region
		dotnetVersion      = $null
		gitSha             = (& git rev-parse --short HEAD 2>$null).Trim()
		rung               = $Rung
		responseTimeoutSec = $ResponseTimeoutSec
		walPartitions      = $WalPartitions
		walMaxPendingBatches = $WalMaxPendingBatches
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
	Write-Host "[teardown] az group delete --name $rg --yes --no-wait" -ForegroundColor Cyan
	& az group delete --name $rg --yes --no-wait 2>&1 | Out-Null
	if ($LASTEXITCODE -ne 0) {
		Write-Warning "az group delete for $rg returned exit $LASTEXITCODE. Manually verify and run 'az group delete --name $rg --yes' if needed."
	}
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
	& ssh @sshOpts $sshTarget $buildCmd
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
		& ssh @sshOpts $sshTarget $remoteCmd
		if ($LASTEXITCODE -ne 0) {
			Write-Warning "[layer1] cohort $i/${N}: ssh microbench run exited $LASTEXITCODE; skipping pull"
			continue
		}
		# Pull results.json back.
		& scp @sshOpts "${sshTarget}:$remoteResults" $localResults
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
	return ,$cohorts.ToArray()
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
		[int] $WalMaxPendingBatches = 16
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
		Write-Host "[layer2] mode=$mode N=$N" -ForegroundColor Cyan
		$cohortList = New-Object System.Collections.Generic.List[hashtable]

		for ($i = 1; $i -le $N; $i++) {
			$extraEnv = @{
				BENCH_RESPONSE_TIMEOUT_SEC    = "$ResponseTimeoutSec"
				BENCH_WORKLOAD_MODE           = $mode
				BENCH_WAL_PARTITIONS          = "$WalPartitions"
				BENCH_WAL_MAX_PENDING_BATCHES = "$WalMaxPendingBatches"
			}
			Write-Host "[layer2] cohort $i/$N mode=$mode ..." -ForegroundColor DarkGray
			# Capture the set of pre-existing log files so we can identify the
			# new one this run produced.
			$before = @(Get-ChildItem $cohortSourceDir -Filter "silo-*.log" -ErrorAction SilentlyContinue | ForEach-Object FullName)
			& $runCohort `
				-Vehicles    $Rung.Vehicles `
				-TickHz      $Rung.TickHz `
				-DurationSec $Rung.DurationSec `
				-NamePrefix  $Prefix `
				-ParametersFile $ParametersFilePath `
				-ExtraSiloEnv $extraEnv
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

			# Parse the steady-state mean and per-call p50/p99 from the silo log.
			$parsed = Read-SiloLogStats -SiloLogPath $siloDest -WorkloadMode $mode
			$cohortList.Add(@{
				cohortName    = $cohortName
				siloLog       = $siloDest
				steadyMean    = $parsed.SteadyMean
				perCallP50Ms  = $parsed.PerCallP50Ms
				perCallP99Ms  = $parsed.PerCallP99Ms
				inFlightMax   = $parsed.InFlightMax
				failed        = $parsed.Failed
				verdict       = $parsed.Verdict
			})
		}
		$result[$mode] = ,$cohortList.ToArray()
	}
	return $result
}

function Read-SiloLogStats {
	[CmdletBinding()] param(
		[Parameter(Mandatory)][string] $SiloLogPath,
		[Parameter(Mandatory)][string] $WorkloadMode
	)
	# Reuses the same methodology run-cohort.ps1 emits: steady-state mean of
	# `[silo] t=` per-second rate samples over t in [15s, last-non-zero-rate].
	$samples = @()
	$failedSamples = 0
	foreach ($m in (Select-String -Path $SiloLogPath -Pattern '^\[silo\] t=')) {
		if ($m.Line -match 't=\s*([\d.]+)s\s+written=\s*([\d,]+)\s+Entries written per second=\s*([\d,]+)\s+inFlight=\s*(\d+)') {
			$samples += [pscustomobject]@{
				t        = [double]$Matches[1]
				rate     = [long](($Matches[3]) -replace ',','')
				inFlight = [int]$Matches[4]
			}
			if ($m.Line -match 'failed=\s*([\d,]+)' -and [long](($Matches[1]) -replace ',','') -gt 0) {
				$failedSamples++
			}
		}
	}
	$steady = $samples | Where-Object { $_.t -ge 15 -and $_.rate -gt 0 }
	$steadyMean = 0
	$inFlightMax = 0
	if (@($steady).Count -gt 0) {
		$steadyMean = [int](($steady | Measure-Object -Property rate -Sum).Sum / @($steady).Count)
		$inFlightMax = ($steady | Measure-Object -Property inFlight -Maximum).Maximum
	}

	# Per-call p50/p99. We look in the last full [phaseA] reporter window for
	# the matching duration histogram. The workload-mode -> instrument mapping:
	#   set-many / set-many-atomic -> set_many.duration   (envelope)
	#   set-point                  -> set.duration
	#   get-point                  -> get.duration
	#   get-many                   -> get_many.duration
	$instrumentName = switch ($WorkloadMode) {
		'set-many'        { 'set_many.duration' }
		'set-many-atomic' { 'set_many.duration' }
		'set-point'       { 'set.duration' }
		'get-point'       { 'get.duration' }
		'get-many'        { 'get_many.duration' }
		default           { 'set_many.duration' }
	}

	# [phaseA] lines look like:
	#   [phaseA] <instrument>{tag=val,...} p50=N.NN p95=N.NN p99=N.NN ...
	# Find the last block of [phaseA] lines containing $instrumentName and pull
	# its p50 / p99.
	$phaseAMatches = @(Select-String -Path $SiloLogPath -Pattern ('^\[phaseA\].*' + [regex]::Escape($instrumentName)))
	$p50 = $null; $p99 = $null
	if ($phaseAMatches.Count -gt 0) {
		$last = $phaseAMatches[-1].Line
		if ($last -match 'p50=([\d.]+)') { $p50 = [double]$Matches[1] }
		if ($last -match 'p99=([\d.]+)') { $p99 = [double]$Matches[1] }
	}

	# Verdict + FINAL failed.
	$verdict = ''
	$finalFailed = 0
	$verdictLine = (Select-String -Path $SiloLogPath -Pattern '^Verdict\s*:' | Select-Object -Last 1)
	if ($verdictLine) {
		if ($verdictLine.Line -match 'Verdict\s*:\s*([A-Z]+)') { $verdict = $Matches[1] }
	}
	$finalLine = (Select-String -Path $SiloLogPath -Pattern 'FINAL written=' | Select-Object -First 1)
	if ($finalLine -and $finalLine.Line -match 'failed=([\d,]+)') {
		$finalFailed = [long]($Matches[1] -replace ',','')
	}

	return @{
		SteadyMean    = $steadyMean
		PerCallP50Ms  = $p50
		PerCallP99Ms  = $p99
		InFlightMax   = $inFlightMax
		Failed        = ($finalFailed + $failedSamples)
		Verdict       = $verdict
	}
}

# ────────────────────────────────────────────────────────────────────────────
# Aggregation
# ────────────────────────────────────────────────────────────────────────────

function Aggregate-Layer1Cells {
	[CmdletBinding()] param([Parameter(Mandatory)] $Cohorts)
	# Cohorts are an array of hashtables with .metrics dictionary
	# (microbench_<slug>_p50_ns, _alloc_b, etc).
	if ($Cohorts.Count -eq 0) { return @{} }
	$rows = @{}
	foreach ($row in $Layer1Rows) {
		$slug = $row.MetricSlug
		$p50Key = "microbench_${slug}_p50_ns"
		$allocKey = "microbench_${slug}_alloc_b"
		$p50s = @()
		$allocs = @()
		foreach ($c in $Cohorts) {
			if ($c.metrics.ContainsKey($p50Key) -and $null -ne $c.metrics[$p50Key]) {
				$p50s += [double]$c.metrics[$p50Key]
			}
			if ($c.metrics.ContainsKey($allocKey) -and $null -ne $c.metrics[$allocKey]) {
				$allocs += [double]$c.metrics[$allocKey]
			}
		}
		if ($p50s.Count -eq 0) {
			Write-Warning "[aggregate-l1] no p50 samples for row '$($row.Label)' (slug=$slug); cohort N=$($Cohorts.Count)"
			continue
		}
		$p50Median = (Get-Median $p50s)
		$allocMedian = if ($allocs.Count -gt 0) { (Get-Median $allocs) } else { $null }
		$ceiling = if ($p50Median -gt 0) { [int](1e9 / $p50Median) } else { $null }
		$rows[$row.Label] = @{
			perCallP50Ns        = [int][math]::Round($p50Median, 0)
			allocB              = if ($allocMedian) { [int][math]::Round($allocMedian, 0) } else { $null }
			singleThreadCeiling = $ceiling
			cohortN             = $p50s.Count
		}
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
		$means = @($cohorts | ForEach-Object { $_.steadyMean } | Where-Object { $_ -gt 0 })
		$p50s  = @($cohorts | ForEach-Object { $_.perCallP50Ms } | Where-Object { $null -ne $_ })
		$p99s  = @($cohorts | ForEach-Object { $_.perCallP99Ms } | Where-Object { $null -ne $_ })
		if ($means.Count -eq 0) {
			Write-Warning "[aggregate-l2] mode=${mode}: no positive steady-state means in $($cohorts.Count) cohorts"
			continue
		}
		$rows[$row.Label] = @{
			sustainedThroughput = [int][math]::Round((Get-Median $means), 0)
			throughputUnit      = $row.ThroughputUnit
			perCallP50Ms        = if ($p50s.Count -gt 0) { [math]::Round((Get-Median $p50s), 2) } else { $null }
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
	$p50 = Format-Duration $Cell.perCallP50Ns
	$alloc = Format-Bytes $Cell.allocB
	$ceiling = Format-Throughput $Cell.singleThreadCeiling $CeilingUnit
	return ('| {0} | **{1}** | {2} | **{3}** |' -f $Label, $p50, $alloc, $ceiling)
}

function Format-Layer2Row {
	[CmdletBinding()] param(
		[Parameter(Mandatory)][string] $Label,
		[Parameter(Mandatory)][hashtable] $Cell
	)
	$unit = $Cell.throughputUnit
	$thr = Format-Throughput $Cell.sustainedThroughput $unit
	$p50 = if ($null -eq $Cell.perCallP50Ms) { 'not captured' } else { ('~{0} ms' -f $Cell.perCallP50Ms) }
	$p99 = if ($null -eq $Cell.perCallP99Ms) { 'not captured' } else { ('~{0} ms' -f $Cell.perCallP99Ms) }
	return ('| {0} | **{1}** | {2} | {3} |' -f $Label, $thr, $p50, $p99)
}

function Format-Duration {
	[CmdletBinding()] param([Parameter(Mandatory)][int] $Ns)
	if ($Ns -lt 1000) { return "$Ns ns" }
	if ($Ns -lt 1000000) {
		$us = [math]::Round($Ns / 1000.0, 2)
		return "$us us"
	}
	$ms = [math]::Round($Ns / 1000000.0, 2)
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
		$meta['dotnet']        = ($State.dotnetVersion ?? '10.0.x')
		$meta['bdnFidelity']   = ($State.bdnFidelity ?? 'dry')
		$meta['bdnToolchain']  = 'InProcessEmitToolchain'
		$meta['cohortN']       = $cohortN
		$meta['rowsMeasured']  = $rowsDate
		$meta['methodology']   = 'Per-call p50 and allocations reported directly by BenchmarkDotNet. Single-thread ceiling = round(1 / p50). Cells are the median of N cohorts.'
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
	$rung = ('{0}vehicles/{1}Hz/{2}s' -f $State.rung.Vehicles, $State.rung.TickHz, $State.rung.DurationSec)

	$meta = @{}
	foreach ($k in $Existing.Keys) { $meta[$k] = $Existing[$k] }
	if (-not $meta.ContainsKey('schema')) { $meta['schema'] = 'v1' }
	if ($RowsAgg.Count -gt 0) {
		$meta['schema']             = 'v1'
		$meta['host']               = $State.vmSize
		$meta['region']             = $State.region
		$meta['dotnet']             = ($State.dotnetVersion ?? '10.0.x')
		$meta['walPartitions']      = $State.walPartitions
		$meta['walMaxPendingBatches'] = $State.walMaxPendingBatches
		$meta['rung']               = $rung
		$meta['responseTimeoutSec'] = $State.responseTimeoutSec
		$meta['cohortN']            = $cohortN
		$meta['rowsMeasured']       = $rowsDate
		$meta['methodology']        = 'Throughput cell = median across N cohorts of the steady-state mean (silo per-second rate samples, t>=15s, rate>0; see benchmark/azure-throughput/throughput.md section 27.1). Per-call p50/p99 cells = median across N cohorts of the matching duration histogram p50/p99 from the last full [phaseA] reporter window.'
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
	[void]$sb.Append('| Operation                                | Per-call p50 | Allocations | Single-thread ceiling |').Append($nl)
	[void]$sb.Append('|------------------------------------------|-------------:|------------:|----------------------:|').Append($nl)
	foreach ($row in $Layer1Rows) {
		if ($RowsAgg.ContainsKey($row.Label)) {
			[void]$sb.Append((Format-Layer1Row -Label $row.Label -Cell $RowsAgg[$row.Label] -CeilingUnit $row.CeilingUnit)).Append($nl)
		} elseif ($ExistingRows -and $ExistingRows.ContainsKey($row.Label)) {
			# Preserve prior cell content if this layer / row wasn't re-run.
			[void]$sb.Append($ExistingRows[$row.Label]).Append($nl)
		} else {
			[void]$sb.Append('| ' + $row.Label.PadRight(40) + ' | _pending_    | _pending_   | _pending_             |').Append($nl)
		}
	}
	return $sb.ToString().TrimEnd("`r","`n")
}

function Render-Layer2Table {
	[CmdletBinding()] param([Parameter(Mandatory)][hashtable] $RowsAgg, [Parameter(Mandatory)][hashtable] $ExistingRows)
	$nl = "`r`n"
	$sb = [System.Text.StringBuilder]::new()
	[void]$sb.Append('| Operation                                | Sustained throughput | Per-call p50  | Per-call p99  |').Append($nl)
	[void]$sb.Append('|------------------------------------------|---------------------:|--------------:|--------------:|').Append($nl)
	foreach ($row in $Layer2Rows) {
		if ($RowsAgg.ContainsKey($row.Label)) {
			[void]$sb.Append((Format-Layer2Row -Label $row.Label -Cell $RowsAgg[$row.Label])).Append($nl)
		} elseif ($ExistingRows -and $ExistingRows.ContainsKey($row.Label)) {
			[void]$sb.Append($ExistingRows[$row.Label]).Append($nl)
		} else {
			[void]$sb.Append('| ' + $row.Label.PadRight(40) + ' | _pending_            | _pending_     | _pending_     |').Append($nl)
		}
	}
	return $sb.ToString().TrimEnd("`r","`n")
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
		Update-DocMarkers -DocPath $docPath -State $state -WhatIf:$Diff
		if (-not $Diff -and -not $SkipDocUpdate) {
			Write-Host "[dry-run] doc rewritten from state.json" -ForegroundColor Green
		}
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
		New-EmptyState -Prefix $prefix -VmSize $VmSize -Region $region -Rung $rungHt -BdnFidelity $Fidelity
	}
	$state.startedUtc = (Get-Date).ToUniversalTime().ToString('o')
	# Stamp the resolved CLI fidelity onto state so the meta-header records
	# what was actually used; tolerates state.json files predating this slot.
	$state['bdnFidelity'] = $Fidelity

	$provisioned = $false
	try {
		# Provision (unless -ReuseVm).
		if (-not $ReuseVm) {
			Invoke-Provision -Prefix $prefix -VmSize $VmSize -ParametersFilePath $paramFile
			$provisioned = $true
		} else {
			Write-Host "[main] -ReuseVm ${ReuseVm}: skipping provisioning" -ForegroundColor Yellow
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
			$l2CohortsByMode = Invoke-Layer2Cohorts -Prefix $prefix -WorkloadIds $l2Ids -Rung $rungHt -N $N -ParametersFilePath $paramFile -ResponseTimeoutSec $state.responseTimeoutSec -WalPartitions $state.walPartitions -WalMaxPendingBatches $state.walMaxPendingBatches
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
