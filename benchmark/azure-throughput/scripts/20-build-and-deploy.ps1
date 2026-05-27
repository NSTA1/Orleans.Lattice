#requires -Version 7
<#
.SYNOPSIS
    Builds the producer and silo container images via `az acr build` and deploys them as
    a single Azure Container Instances container group.
.DESCRIPTION
    Both containers share the container group's loopback network namespace; the producer
    connects to the silo on 127.0.0.1:7000.

    Knobs (params win over env vars, env vars win over defaults):
      -VehicleCount     number of synthetic vehicles (default 1000)
      -TickHz           per-vehicle samples per second (default 5)
      -DurationSec      producer run duration in seconds (default 120)
      -TotalDurationSec hard wall-clock ceiling for the whole run, after which the
                        container group is force-stopped and the silo log is captured
                        (default 120; >= DurationSec + a small grace).
      -Tag              image tag (default 'latest')
      -SkipBuild        reuse the existing image; only redeploy the container group.
      -LocalBuild       build images locally via `docker build` and `docker push` instead
                        of `az acr build`. Requires Docker Desktop (or equivalent) on a
                        linux/amd64 host. Trades remote build time (~1m45s for a clean
                        ACI build with full source upload) for local incremental
                        `docker build` + `docker push` (~15s when only Program.cs
                        changed), which is the dominant speedup on code-change
                        iteration. The remote `az acr build` path remains the default
                        and the supported fallback for clean environments without
                        Docker.
      -NoWait           submit the deployment and return immediately (legacy behaviour;
                        the script will not capture logs or stop the group).

    Env-var equivalents (used as fallbacks):
      BENCH_VEHICLE_COUNT, BENCH_TICK_HZ, BENCH_DURATION_SEC, BENCH_TOTAL_DURATION_SEC,
      BENCH_IMAGE_TAG
#>

[CmdletBinding()]
param(
    [int]    $VehicleCount,
    [int]    $TickHz,
    [int]    $DurationSec,
    [int]    $TotalDurationSec,
    [string] $Tag,
    [string] $TreeId,
    [switch] $SkipBuild,
    [switch] $LocalBuild,
    [switch] $NoWait
)

$ErrorActionPreference = 'Stop'

$ctxPath = Join-Path $PSScriptRoot '.context.json'
if (-not (Test-Path $ctxPath)) {
    throw "Run 10-provision.ps1 first; missing $ctxPath."
}
$ctx = Get-Content $ctxPath | ConvertFrom-Json

# Resolve each knob: explicit param -> env var -> default.
$vehicleCount = if ($PSBoundParameters.ContainsKey('VehicleCount')) { $VehicleCount.ToString() }
                elseif ($env:BENCH_VEHICLE_COUNT) { $env:BENCH_VEHICLE_COUNT }
                else { '1000' }
$tickHz       = if ($PSBoundParameters.ContainsKey('TickHz'))       { $TickHz.ToString() }
                elseif ($env:BENCH_TICK_HZ) { $env:BENCH_TICK_HZ }
                else { '5' }
$duration     = if ($PSBoundParameters.ContainsKey('DurationSec'))  { $DurationSec.ToString() }
                elseif ($env:BENCH_DURATION_SEC) { $env:BENCH_DURATION_SEC }
                else { '120' }
# Hard wall-clock cap for the whole orchestrated run. Defaults to 120s so an interactive
# run (or an automated agent) is bounded predictably and the container group is always
# stopped at the end rather than left charging until manual teardown.
$totalDuration = if ($PSBoundParameters.ContainsKey('TotalDurationSec')) { $TotalDurationSec.ToString() }
                  elseif ($env:BENCH_TOTAL_DURATION_SEC) { $env:BENCH_TOTAL_DURATION_SEC }
                  else { '120' }
# Tree id is per-run by default. Every silo activation against a stale tree id pays a
# manifest-replay cost proportional to the prior runs' entry count, which biases the
# first ~10s of throughput numbers. Default to a fresh UTC-stamped id so every run
# starts against an empty manifest partition. The operator can pin BENCH_TREE_ID to
# opt out (e.g. to deliberately measure recovery cost against a populated WAL).
$treeId        = if ($PSBoundParameters.ContainsKey('TreeId') -and $TreeId) { $TreeId }
                  elseif ($env:BENCH_TREE_ID) { $env:BENCH_TREE_ID }
                  else { "azure-throughput-$((Get-Date).ToUniversalTime().ToString('yyyyMMdd-HHmmss'))" }
$tag          = if ($PSBoundParameters.ContainsKey('Tag'))          { $Tag }
                elseif ($env:BENCH_IMAGE_TAG) { $env:BENCH_IMAGE_TAG }
                else { 'latest' }
# WAL phase-0 candidate-row elision. Off by default to match the library's
# wire-compat default; set BENCH_WAL_ELIMINATE_CANDIDATE_ROW=true to A/B
# the optimisation against a real Azure Tables account.
$walElimCRow  = if ($env:BENCH_WAL_ELIMINATE_CANDIDATE_ROW) { $env:BENCH_WAL_ELIMINATE_CANDIDATE_ROW }
                else { 'false' }
# WAL partition count per tree. Default 8 (matches Silo/Program.cs default).
# Operator override path is honoured so a P=1 vs P=8 A/B can be driven from the
# host env without editing the inline YAML below.
$walPartitions = if ($env:BENCH_WAL_PARTITIONS) { $env:BENCH_WAL_PARTITIONS }
                 else { '8' }
# Per-WalShardGrain pipeline depth. Default 8 (matches Silo/Program.cs default).
$walMaxPending = if ($env:BENCH_WAL_MAX_PENDING_BATCHES) { $env:BENCH_WAL_MAX_PENDING_BATCHES }
                 else { '8' }
# PhaseTwoWorker coalescing window in ms. Default 0 (drain-on-first-signal, matches
# the library default and Silo/Program.cs default). Surfaced as an env-var override so
# a U9c sweep (e.g. 0 vs 5 vs 10 ms) can be driven from the host without editing YAML.
# Default 5: at the c2-iii durable Azure Tables baseline, P2=0 regresses -57%
# vs P2=5 (scaling.md U9p step 8c-c-iv-c2-iv probe-D). All c2-iii / c2-vi /
# c2-vi-followup milestones used P2=5, so the deploy default tracks the
# operating-point baseline rather than the library default of 0.
$phase2CoalescingMs = if ($env:BENCH_WAL_PHASE2_COALESCING_WINDOW_MS) { $env:BENCH_WAL_PHASE2_COALESCING_WINDOW_MS }
                      else { '5' }
# In-silo SetManyAsync flush concurrency cap (drainer semaphore size). Default 8
# (matches Silo/Program.cs default). Surfaced as an env-var override so a U1b
# A/B (e.g. 8 vs 16 vs 32) can be driven from the host without editing YAML.
$flushConcurrency = if ($env:BENCH_FLUSH_CONCURRENCY) { $env:BENCH_FLUSH_CONCURRENCY }
                    else { '8' }
# In-silo TcpIngestService flush-window cadence in milliseconds. Default 50
# (matches Silo/Program.cs default). Surfaced as an env-var override so a
# U9l sweep (e.g. 50 vs 100 vs 200 vs 400 ms) over the flush-window vs
# producer-inter-tick interaction can be driven from the host without
# editing YAML. The producer's inter-tick at TickHz=5 is 200 ms, so this
# probe brackets it on both sides.
$flushMs = if ($env:BENCH_FLUSH_MS) { $env:BENCH_FLUSH_MS }
           else { '50' }
# Optional ShardRoot fan-out override. The benchmark default is 32, set by
# U9p step 8c-c-iv-c2-ii: at the durable Azure Tables baseline, raising the
# shard count from the library default lifts SteadyAvg +166% (2,817 -> 7,501/s
# at 10000:5). s=64 regresses (-14%), so s=32 is the measured sweet spot.
# Set BENCH_SHARD_COUNT=0 to fall back to the library default; any other
# positive value triggers the silo's startup ReshardAsync(N) call.
$shardCount = if ($env:BENCH_SHARD_COUNT) { $env:BENCH_SHARD_COUNT }
              else { '32' }
# Producer batch size (entries per SetManyAsync). Default 4096 (matches
# Silo/Program.cs default and sizes the 64-way shard fan-out so each shard
# sees ~64 entries per batch). Surfaced as an env-var override so a U7 A/B
# (e.g. 1024 vs 4096) can be driven from the host without editing YAML.
$batchSize = if ($env:BENCH_BATCH_SIZE) { $env:BENCH_BATCH_SIZE }
             else { '4096' }
# Phase A diagnostic reporter cadence in the silo. Forward whatever the
# operator (or 40-ladder.ps1) put on the host env; default to 10s so a
# 60s rung captures ~5 windows of attribution data without burying the
# main throughput log in noise. Set 0 to disable.
$phaseAReportSec = if ($env:BENCH_PHASEA_REPORT_SEC) { $env:BENCH_PHASEA_REPORT_SEC }
                   else { '10' }
# Orleans Silo+Client ResponseTimeout in seconds. Default 180. U9p step
# 8c-b-i probe lever: lifts the caller-side timeout on ILattice.SetManyAsync
# so a slow worst-partition WAL flush does not surface as an Orleans
# TimeoutException + producer reconnect storm. The Orleans library default
# is 30s, but at the c2-iii durable Azure Tables baseline the WAL p99 is
# already ~2-3s and tail outliers push past 30s under load, so 30s
# reproducibly collapses the producer at higher rungs (c2-iv-redux session
# observed: WP=16 / 25000:5 with 30s timeout -> producer Broken pipe at
# t=110s, FinalAvgRate=38/s). c2-iii-ship and all subsequent c2-* probes
# used 180s. Forwarded into both SiloMessagingOptions.ResponseTimeout and
# ClientMessagingOptions.ResponseTimeout in Silo/Program.cs.
$responseTimeoutSec = if ($env:BENCH_RESPONSE_TIMEOUT_SEC) { $env:BENCH_RESPONSE_TIMEOUT_SEC }
                      else { '180' }
# Diagnostic gate (c2-vi etag-race probe). When '1' the silo emits one
# stdout line per leaf/internal grain PersistAsync call with
# state.RecordExists, state.Etag, and a caller tag. Default empty so
# normal runs pay zero cost.
$tracePersist = if ($env:LATTICE_BENCH_TRACE_PERSIST) { $env:LATTICE_BENCH_TRACE_PERSIST }
                else { '' }
# Leaf/internal/atomic grain checkpoint storage. BENCH_LEAF_STORAGE_KIND selects
# the IGrainStorage backing the lattice's leaf/internal/atomic state:
#   "azure" (default) - production-shape Azure Table grain storage. Reuses the
#                       same storage account + credential as the WAL provider
#                       and writes to the table named by BENCH_LEAF_STORAGE_TABLE
#                       (default "OrleansLatticeGrainState"). This is the
#                       production-viable baseline.
#   "memory"          - Orleans.Persistence.Memory. Diagnostic-only A/B lever
#                       (step 8c-c-i exposed its NumStorageGrains=10 default
#                       as a chokepoint once the WAL pipeline was uncorked).
#   "null"            - benchmark-only NullGrainStorage. Removes persistence
#                       from the measurement window to expose the WAL ceiling;
#                       NOT production-shape.
# BENCH_LEAF_STORAGE_TABLE selects the Azure Table name used when
# leafStorageKind=azure (default "OrleansLatticeGrainState").
# BENCH_LEAF_STORAGE_NUM_GRAINS overrides MemoryGrainStorageOptions
# .NumStorageGrains when leafStorageKind=memory (0 = keep the Orleans
# default of 10).
$leafStorageKind = if ($env:BENCH_LEAF_STORAGE_KIND) { $env:BENCH_LEAF_STORAGE_KIND }
                   else { 'azure' }
$leafStorageTable = if ($env:BENCH_LEAF_STORAGE_TABLE) { $env:BENCH_LEAF_STORAGE_TABLE }
                    else { 'OrleansLatticeGrainState' }
$leafStorageNumGrains = if ($env:BENCH_LEAF_STORAGE_NUM_GRAINS) { $env:BENCH_LEAF_STORAGE_NUM_GRAINS }
                        else { '0' }
# Throughput-capture (throughput-capture-plan.md step 6): selects which
# ILattice op the silo dispatches per producer batch. Default `set-many`
# preserves the legacy bench shape (one ILattice.SetManyAsync per batch).
# Other accepted values: `set-many-atomic`, `set-point`, `get-point`,
# `get-many`. See Silo/Program.cs::BenchWorkloadMode for the dispatch
# table.
$workloadMode = if ($env:BENCH_WORKLOAD_MODE) { $env:BENCH_WORKLOAD_MODE }
                else { 'set-many' }
# Per-saga key count used only when workloadMode == set-many-atomic.
# Default 64 - a realistic atomic-write shape. (BENCH_BATCH_SIZE=4096
# would not be a meaningful atomic-saga size; that's the SetMany batch
# shape, not the atomic-saga shape.)
$atomicBatchSize = if ($env:BENCH_ATOMIC_BATCH_SIZE) { $env:BENCH_ATOMIC_BATCH_SIZE }
                   else { '64' }

Write-Host "[deploy] knobs: vehicles=$vehicleCount tickHz=$tickHz duration=${duration}s totalDuration=${totalDuration}s tag=$tag treeId=$treeId walPartitions=$walPartitions walMaxPending=$walMaxPending phase2CoalescingMs=$phase2CoalescingMs flushConcurrency=$flushConcurrency flushMs=$flushMs shardCount=$shardCount batchSize=$batchSize walElimCRow=$walElimCRow phaseAReportSec=$phaseAReportSec responseTimeoutSec=$responseTimeoutSec leafStorageKind=$leafStorageKind leafStorageTable=$leafStorageTable leafStorageNumGrains=$leafStorageNumGrains workloadMode=$workloadMode atomicBatchSize=$atomicBatchSize skipBuild=$SkipBuild localBuild=$LocalBuild noWait=$NoWait" -ForegroundColor Cyan

# Repo root is three levels up from this script (benchmark/azure-throughput/scripts).
$repoRoot = Resolve-Path (Join-Path $PSScriptRoot '..' '..' '..')
Write-Host "[deploy] repoRoot=$repoRoot prefix=$($ctx.Prefix)" -ForegroundColor Cyan

# Stage a minimal build context (just the paths the Dockerfiles COPY from) into a temp
# directory and point `az acr build` at that. Two reasons:
#   1. Using the repo root as the context uploads `.vs/`, `bin/`, `obj/`, `.git/` and
#      every other sample tree on every run - and `.vs/*.vsidx` is mmap'd by Visual
#      Studio, so the tar packer hits "Permission denied" if the IDE is open.
#   2. The minimal context is ~a few MB instead of hundreds, so the upload step is fast.
$stage = Join-Path ([IO.Path]::GetTempPath()) "lattice-acr-stage-$(Get-Random)"
Write-Host "[deploy] staging build context at $stage" -ForegroundColor DarkGray
New-Item -ItemType Directory -Force -Path $stage | Out-Null

# Robocopy is used purely so we can exclude bin/obj/.vs without depending on
# .dockerignore (which is the tracked file and shouldn't be edited from a temp script).
$rcExclude = @('/XD', 'bin', 'obj', '.vs', '.git', 'TestResults', '.azurite', 'node_modules', '/NFL', '/NDL', '/NJH', '/NJS', '/NC', '/NS', '/NP')

function Stage-Path {
    param([string] $RelPath)
    $src = Join-Path $repoRoot.Path $RelPath
    if (-not (Test-Path $src)) { throw "staging: source path missing: $src" }
    $dst = Join-Path $stage $RelPath
    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $dst) | Out-Null
    robocopy $src $dst /E @rcExclude | Out-Null
    # robocopy uses exit codes 0-7 for success; 8+ is a real failure.
    if ($LASTEXITCODE -ge 8) { throw "robocopy $src -> $dst failed (exit=$LASTEXITCODE)." }
}

Stage-Path 'src/lattice'
Stage-Path 'src/lattice.replication'
Stage-Path 'src/lattice.replication.grpc'
Stage-Path 'src/lattice.storage.azuretable'
Stage-Path 'samples/VehicleFleetSimulator/src/VehicleFleetSimulator.Abstractions'
Stage-Path 'benchmark/azure-throughput/Producer'
Stage-Path 'benchmark/azure-throughput/Silo'

# The .dockerignore at the repo root is referenced by both Dockerfiles when a .dockerignore
# sits next to the build context root; copy it so the in-image COPY rules continue to match.
$rootDockerIgnore = Join-Path $repoRoot.Path '.dockerignore'
if (Test-Path $rootDockerIgnore) {
    Copy-Item $rootDockerIgnore (Join-Path $stage '.dockerignore') -Force
}

Write-Host "[deploy] staged size:" (
    '{0:N1} MB' -f ((Get-ChildItem $stage -Recurse -File | Measure-Object Length -Sum).Sum / 1MB)
) -ForegroundColor DarkGray

$producerImage = "$($ctx.AcrLoginServer)/azure-throughput-producer:$tag"
$siloImage     = "$($ctx.AcrLoginServer)/azure-throughput-silo:$tag"

if ($SkipBuild) {
    Write-Host "[deploy] -SkipBuild: reusing existing images in $($ctx.AcrLoginServer)" -ForegroundColor Yellow
    Remove-Item $stage -Recurse -Force -ErrorAction SilentlyContinue
} elseif ($LocalBuild) {
    # Local docker build + push. Faster than `az acr build` for code-change
    # iteration because:
    #   (a) source upload to ACR is skipped (gigabytes of layers stay on the
    #       local builder cache; only the changed layers are pushed),
    #   (b) builds reuse the host's Docker layer cache across runs, so the
    #       restore/publish steps don't re-execute when only Program.cs
    #       changed.
    # Requires Docker Desktop (or equivalent) on a linux/amd64 host so the
    # produced image matches what ACI expects. The remote `az acr build`
    # path remains the default and the supported fallback for clean
    # environments without Docker.
    Write-Host "[deploy] -LocalBuild: building images locally via 'docker build' ..." -ForegroundColor Cyan
    Push-Location $stage
    try {
        Write-Host "[deploy] az acr login -n $($ctx.Acr) ..." -ForegroundColor DarkGray
        & az acr login --name $ctx.Acr | Out-Host
        if ($LASTEXITCODE -ne 0) { throw "az acr login failed with exit code $LASTEXITCODE." }

        Write-Host "[deploy] docker build (producer) -> $producerImage ..." -ForegroundColor Cyan
        & docker build `
            --platform linux/amd64 `
            --file "benchmark/azure-throughput/Producer/Dockerfile" `
            --tag $producerImage `
            . | Out-Host
        if ($LASTEXITCODE -ne 0) { throw "docker build (producer) failed with exit code $LASTEXITCODE." }

        Write-Host "[deploy] docker build (silo) -> $siloImage ..." -ForegroundColor Cyan
        & docker build `
            --platform linux/amd64 `
            --file "benchmark/azure-throughput/Silo/Dockerfile" `
            --tag $siloImage `
            . | Out-Host
        if ($LASTEXITCODE -ne 0) { throw "docker build (silo) failed with exit code $LASTEXITCODE." }

        Write-Host "[deploy] docker push $producerImage ..." -ForegroundColor Cyan
        & docker push $producerImage | Out-Host
        if ($LASTEXITCODE -ne 0) { throw "docker push (producer) failed with exit code $LASTEXITCODE." }

        Write-Host "[deploy] docker push $siloImage ..." -ForegroundColor Cyan
        & docker push $siloImage | Out-Host
        if ($LASTEXITCODE -ne 0) { throw "docker push (silo) failed with exit code $LASTEXITCODE." }
    } finally {
        Pop-Location
        Remove-Item $stage -Recurse -Force -ErrorAction SilentlyContinue
    }
} else {
    Write-Host "[deploy] building producer image via 'az acr build' ..." -ForegroundColor Cyan
    # `az acr build --file` is resolved relative to the CURRENT working directory. Push-Location
    # into the staging tree so both the --file path and the source context path are unambiguous.
    Push-Location $stage
    try {
        & az acr build --registry $ctx.Acr `
            --image "azure-throughput-producer:$tag" `
            --file "benchmark/azure-throughput/Producer/Dockerfile" `
            --platform linux `
            . | Out-Host
        if ($LASTEXITCODE -ne 0) { throw "az acr build (producer) failed with exit code $LASTEXITCODE." }

        Write-Host "[deploy] building silo image via 'az acr build' ..." -ForegroundColor Cyan
        & az acr build --registry $ctx.Acr `
            --image "azure-throughput-silo:$tag" `
            --file "benchmark/azure-throughput/Silo/Dockerfile" `
            --platform linux `
            . | Out-Host
        if ($LASTEXITCODE -ne 0) { throw "az acr build (silo) failed with exit code $LASTEXITCODE." }
    } finally {
        Pop-Location
        Remove-Item $stage -Recurse -Force -ErrorAction SilentlyContinue
    }
}

# ACR admin credentials so ACI can pull. (Managed identity for storage stays the silo's auth.)
$acrCreds = az acr credential show --name $ctx.Acr --output json | ConvertFrom-Json
$acrUser = $acrCreds.username
$acrPass = $acrCreds.passwords[0].value

$containerGroup = "$($ctx.Prefix)-bench"

# Delete any existing container group with this name so we always get a fresh run.
$existing = az container show --resource-group $ctx.ResourceGroup --name $containerGroup --query name --output tsv 2>$null
if ($existing) {
    Write-Host "[deploy] existing container group found; deleting ..." -ForegroundColor Yellow
    az container delete --resource-group $ctx.ResourceGroup --name $containerGroup --yes --output none
}

# YAML deployment - the only ACI shape that supports multiple containers in one group with
# user-assigned managed identity attached.
$yamlPath = Join-Path $PSScriptRoot '.aci-deploy.yaml'
$yaml = @"
apiVersion: '2021-10-01'
location: $($ctx.Location)
name: $containerGroup
identity:
  type: UserAssigned
  userAssignedIdentities:
    $($ctx.IdentityResourceId): {}
properties:
  osType: Linux
  restartPolicy: Never
  imageRegistryCredentials:
    - server: $($ctx.AcrLoginServer)
      username: $acrUser
      password: $acrPass
  containers:
    - name: silo
      properties:
        image: $siloImage
        resources:
          requests:
            cpu: 2.0
            memoryInGB: 4.0
        ports:
          - port: 7000
            protocol: TCP
        environmentVariables:
          - name: BENCH_STORAGE_URI
            value: $($ctx.StorageUri)
          - name: BENCH_WAL_TABLE
            value: OrleansLatticeWal
          - name: BENCH_TREE_ID
            value: $treeId
          - name: BENCH_TCP_PORT
            value: '7000'
          - name: BENCH_BATCH_SIZE
            value: '$batchSize'
          - name: BENCH_FLUSH_MS
            value: '$flushMs'
          - name: BENCH_FLUSH_CONCURRENCY
            value: '$flushConcurrency'
          - name: BENCH_SHARD_COUNT
            value: '$shardCount'
          - name: BENCH_WAL_PARTITIONS
            value: '$walPartitions'
          - name: BENCH_WAL_MAX_PENDING_BATCHES
            value: '$walMaxPending'
          - name: BENCH_WAL_PHASE2_COALESCING_WINDOW_MS
            value: '$phase2CoalescingMs'
          - name: BENCH_PIPELINE_PHASE2
            value: '1'
          - name: BENCH_WAL_ELIMINATE_CANDIDATE_ROW
            value: '$walElimCRow'
          - name: BENCH_REPORT_SEC
            value: '1'
          - name: BENCH_PHASEA_REPORT_SEC
            value: '$phaseAReportSec'
          - name: BENCH_TOTAL_DURATION_SEC
            value: '$totalDuration'
          - name: LATTICE_BENCH_TRACE_PERSIST
            value: '$tracePersist'
          - name: BENCH_RESPONSE_TIMEOUT_SEC
            value: '$responseTimeoutSec'
          - name: BENCH_LEAF_STORAGE_KIND
            value: '$leafStorageKind'
          - name: BENCH_LEAF_STORAGE_TABLE
            value: '$leafStorageTable'
          - name: BENCH_LEAF_STORAGE_NUM_GRAINS
            value: '$leafStorageNumGrains'
          - name: BENCH_WORKLOAD_MODE
            value: '$workloadMode'
          - name: BENCH_ATOMIC_BATCH_SIZE
            value: '$atomicBatchSize'
          - name: BENCH_VEHICLE_COUNT
            value: '$vehicleCount'
          - name: AZURE_CLIENT_ID
            value: $((az identity show --name $ctx.Identity --resource-group $ctx.ResourceGroup --query clientId --output tsv))
    - name: producer
      properties:
        image: $producerImage
        resources:
          requests:
            cpu: 1.0
            memoryInGB: 1.5
        environmentVariables:
          - name: BENCH_VEHICLE_COUNT
            value: '$vehicleCount'
          - name: BENCH_TICK_HZ
            value: '$tickHz'
          - name: BENCH_SILO_HOST
            value: 127.0.0.1
          - name: BENCH_SILO_PORT
            value: '7000'
          - name: BENCH_DURATION_SEC
            value: '$duration'
"@
Set-Content -Path $yamlPath -Value $yaml -Encoding utf8

Write-Host "[deploy] creating container group $containerGroup ..." -ForegroundColor Cyan
az container create --resource-group $ctx.ResourceGroup --file $yamlPath --output none
if ($LASTEXITCODE -ne 0) { throw "az container create failed with exit code $LASTEXITCODE." }

if ($NoWait) {
    Write-Host "[deploy] -NoWait: deployment submitted; not waiting. Tail logs with:" -ForegroundColor Green
    Write-Host "  ./30-tail-logs.ps1"
    return
}

# Bounded wait loop. The producer container is configured with BENCH_DURATION_SEC so it
# will close the socket and the silo will emit its [silo] FINAL line and exit. The wall-
# clock cap below is a hard ceiling on top of that, so a wedged producer / silo cannot
# keep the container group running indefinitely.
$runStarted = Get-Date
$deadline   = $runStarted.AddSeconds([int]$totalDuration)
Write-Host "[deploy] waiting up to ${totalDuration}s for run to complete (deadline=$($deadline.ToString('HH:mm:ss')))" -ForegroundColor Cyan

# Pre-stage the results directory and the log file paths so we can start
# streaming each container's stdout into .run/ from the moment the
# container group is created. A post-stop `az container logs` fetch only
# returns the last ~4 KiB the ACI driver still has buffered, which loses
# every per-second progress line and (more importantly) any pre-stop
# exception stack. `az container logs --follow` keeps the stream open
# for the lifetime of the container and writes everything to disk as it
# arrives, so a force-stop at the deadline still leaves us with the full
# in-run history.
$resultsDir = Join-Path $PSScriptRoot '..' '.run'
New-Item -ItemType Directory -Force -Path $resultsDir | Out-Null
$runId = (Get-Date).ToUniversalTime().ToString('yyyyMMdd-HHmmssZ')
$siloLogPath     = Join-Path $resultsDir "silo-$runId.log"
$producerLogPath = Join-Path $resultsDir "producer-$runId.log"

Write-Host "[deploy] streaming silo log     -> $siloLogPath" -ForegroundColor Cyan
Write-Host "[deploy] streaming producer log -> $producerLogPath" -ForegroundColor DarkGray
$siloStream = Start-Process -FilePath 'az' `
    -ArgumentList @('container','logs','--resource-group',$ctx.ResourceGroup,'--name',$containerGroup,'--container-name','silo','--follow') `
    -RedirectStandardOutput $siloLogPath -RedirectStandardError (Join-Path $resultsDir "silo-$runId.err.log") `
    -NoNewWindow -PassThru
$producerStream = Start-Process -FilePath 'az' `
    -ArgumentList @('container','logs','--resource-group',$ctx.ResourceGroup,'--name',$containerGroup,'--container-name','producer','--follow') `
    -RedirectStandardOutput $producerLogPath -RedirectStandardError (Join-Path $resultsDir "producer-$runId.err.log") `
    -NoNewWindow -PassThru

$producerState = ''
$siloState     = ''
while ((Get-Date) -lt $deadline) {
    $producerState = (& az container show --resource-group $ctx.ResourceGroup --name $containerGroup `
        --query "containers[?name=='producer'].instanceView.currentState.state" --output tsv 2>$null) -as [string]
    $siloState = (& az container show --resource-group $ctx.ResourceGroup --name $containerGroup `
        --query "containers[?name=='silo'].instanceView.currentState.state" --output tsv 2>$null) -as [string]
    if ($producerState) { $producerState = $producerState.Trim() }
    if ($siloState)     { $siloState     = $siloState.Trim() }
    if ($producerState -eq 'Terminated' -and $siloState -eq 'Terminated') { break }
    Start-Sleep -Seconds 5
}

$elapsed = [int]((Get-Date) - $runStarted).TotalSeconds
if ($producerState -ne 'Terminated' -or $siloState -ne 'Terminated') {
    Write-Warning "[deploy] wall-clock deadline reached after ${elapsed}s (producer='$producerState' silo='$siloState'); force-stopping container group."
    az container stop --resource-group $ctx.ResourceGroup --name $containerGroup --output none 2>$null
} else {
    Write-Host "[deploy] run completed after ${elapsed}s; stopping container group to release compute." -ForegroundColor Green
    az container stop --resource-group $ctx.ResourceGroup --name $containerGroup --output none 2>$null
}

# Give the streaming `az container logs --follow` processes a moment to
# drain the last buffered lines after the container group transitions to
# Stopped, then terminate them. They will not exit on their own because
# `az container logs --follow` keeps polling.
Start-Sleep -Seconds 3
foreach ($proc in @($siloStream, $producerStream)) {
    if ($proc -and -not $proc.HasExited) {
        try { $proc.Kill() } catch { }
    }
}

# Print the FINAL line (the headline scalar) to stdout so an interactive operator sees
# the result without opening the saved file, and an automated runner can grep $siloLog
# for the same line.
$final = (Get-Content $siloLogPath -ErrorAction SilentlyContinue) | Select-String -Pattern '^\[silo\] FINAL' | Select-Object -Last 1
if ($final) {
    Write-Host ""
    Write-Host $final.Line -ForegroundColor Green
} else {
    Write-Warning "[deploy] no FINAL line found in silo log; run may have been force-stopped before drain completed."
    Write-Host "[deploy] last 10 silo log lines:" -ForegroundColor DarkGray
    (Get-Content $siloLogPath -ErrorAction SilentlyContinue) | Select-Object -Last 10 | ForEach-Object { Write-Host "  $_" }
}

Write-Host ""
Write-Host "[deploy] done. results:" -ForegroundColor Green
Write-Host "  silo log     : $siloLogPath"
Write-Host "  producer log : $producerLogPath"
