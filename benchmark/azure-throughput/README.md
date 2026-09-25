# Azure throughput benchmark (real Azure Storage)

A two-process Linux VM deployment that measures **operations per second** (entries
written per second on the default `set-many` workload) when
a single-silo Orleans.Lattice host backed by a real Azure Storage account is fed a
sustained stream of synthetic vehicle telemetry. The tree is configured with
`AzureTableWalStorageProvider` so every commit produces real WAL traffic against
Azure Tables.

This is the only benchmark in the suite that runs against **real Azure Storage**
rather than Azurite or in-memory storage. The local docker-compose scenarios
(`benchmark.ps1 <scenario>`) are reproducible but Azurite collapses network RTT and
does not model Azure Tables partition-server behaviour or throttling. Use this
harness when a throughput claim needs to be backed by real-Azure numbers.

## Topology

```mermaid
flowchart LR
	subgraph VM["Linux VM (single host)"]
		direction LR
		P[lattice-producer.service<br/>synthetic fleet emitter]
		S[lattice-silo.service<br/>BENCH_WORKLOAD_MODE operation<br/>(default ILattice.SetManyAsync)]
		P -- loopback TCP<br/>127.0.0.1:7000 --> S
	end
	S -- AzureTableWalStorage<br/>managed identity --> AZ[(Azure Storage Account<br/>Tables)]
```

Both processes run as systemd units on the same Linux VM and share a loopback hop
to `127.0.0.1:7000`. The silo authenticates to Azure Tables via the VM's
system-assigned managed identity (no keys, no connection strings).

## Why a single VM (not ACI)

The harness previously ran as two ACI containers. That topology produced a series
of investigation artefacts that turned out to be ACI-induced rather than Lattice
bugs (60s `az container logs` tail truncation, multi-pipe stdout scraping
duplication, cold-start variance, no live attach for `dotnet-dump` /
`dotnet-counters`). See `throughput.md` section 0 in this folder for the
full rationale. The single-VM topology gives deterministic CPU, deterministic
NIC (accelerated networking), the full `dotnet-*` diagnostic surface, and
journald-backed log capture with no scraper indirection.

## Files

| Path | Purpose |
|------|---------|
| `Producer/Program.cs` | Generates `VehicleTelemetryEvent` records and writes JSON lines over TCP, or (Layer 3, `BENCH_PRODUCER_MODE=orleans-client`) drives `ILattice` directly as an Orleans client. |
| `Silo/Program.cs` | Lattice silo host; TCP listener (or, on Layer 3, cluster ingest) -> the `BENCH_WORKLOAD_MODE` operation (default `ILattice.SetManyAsync`). |
| `Engine/` | Shared ingest engine both the silo and the Orleans-client producer run: batching, workload dispatch, and the per-second and `FINAL` report lines. |
| `Producer/Dockerfile`, `Silo/Dockerfile` | Container images for the Layer 3 (Azure Container Apps) rig. |
| `infra/main.bicep` | VM + NIC (accelerated networking) + NSG + storage account + role assignments. |
| `infra/cloud-init.yaml` | First-boot bootstrap (`.NET 10 SDK`, dotnet diagnostic tools, `/opt/lattice` tree). |
| `infra/bootstrap.sh` | Manual / fallback bootstrap path; idempotent. |
| `infra/lattice-silo.service` | systemd unit template for the silo (placeholders filled in by `update.ps1`). |
| `infra/lattice-producer.service` | systemd unit template for the co-located producer. |
| `scripts/parameters.ps1` | Default parameters (subscription, region, prefix, VM size). |
| `scripts/parameters.local.ps1` | **Gitignored** operator overrides. Created by `deploy.ps1` if missing. |
| `scripts/deploy.ps1` | End-to-end provision: key gen, `~/.ssh/config`, Bicep deploy, cloud-init wait, bootstrap fallback, chained `update.ps1`. |
| `scripts/update.ps1` | Inner loop: `git ls-files \| tar \| ssh` -> `dotnet publish` silo+producer on the VM -> `systemctl restart`. |
| `scripts/run-cohort.ps1` | Single cohort: applies env drop-ins, restarts silo, starts producer, waits for FINAL, extracts journals, prints summary. |
| `scripts/ladder.ps1` | Thin loop over `run-cohort.ps1` for rung sweeps; writes `.ladder-results.csv`. |
| `scripts/vm.ps1` | Day-to-day helper: `start` / `stop` / `status` / `ssh` / `logs` / `refresh-ip`. |
| `scripts/_run-cohort-helpers.ps1` | Verdict-computation helpers `run-cohort.ps1` dot-sources. |
| `scripts/Test-CohortVerdict.ps1` | Regression tests for those helpers against literal log fixtures (pure pwsh, no Azure). |
| `scripts/deploy-aca.ps1` | Layer 3: provisions the multi-silo Azure Container Apps rig and builds its images remotely with `az acr build`. |
| `scripts/run-cohort-aca.ps1` | Layer 3: one cohort - by default empties the rig's storage first (`-ResetStorage`), pins the silos to exactly N, runs the producer job, parks the silos at zero, then harvests the producer and silo logs from Log Analytics into one cohort log. |
| `scripts/aca-common.ps1` | Shared Layer 3 helpers, also dot-sourced by `performance-report.ps1`: resource naming, the Azure CLI convention, the run-context file, silo scaling, the per-cohort storage reset, job waits and log harvest, and teardown with its ownership guard. |

## One-time setup

1. Sign in to Azure (`az login`).
2. Copy the parameters template (or just let `deploy.ps1` do it for you):
   ```powershell
   Copy-Item benchmark/azure-throughput/scripts/parameters.ps1 `
			 benchmark/azure-throughput/scripts/parameters.local.ps1
   # edit: SubscriptionId, Location (match your Tables-account region),
   # SshPublicKeyPath (deploy.ps1 will generate the key if missing).
   ```
3. Deploy:
   ```powershell
   ./benchmark/azure-throughput/scripts/deploy.ps1
   ```
   `deploy.ps1` provisions infra, waits for cloud-init, then chains to
   `update.ps1` which publishes the silo + producer and starts the silo.

To spin up a second environment side-by-side (e.g. an experimental SKU):

```powershell
./benchmark/azure-throughput/scripts/deploy.ps1 -NamePrefix lat-exp -VmSize Standard_F8as_v6
```

The resource group becomes `rg-lat-exp` and the `~/.ssh/config` host alias becomes
`lat-exp` so the day-to-day scripts all accept `-NamePrefix lat-exp`.

## Daily workflow

```powershell
./benchmark/azure-throughput/scripts/vm.ps1 start                # ~30s
./benchmark/azure-throughput/scripts/update.ps1                  # sync source, publish, restart silo
./benchmark/azure-throughput/scripts/run-cohort.ps1 -Vehicles 4000 -TickHz 5 -DurationSec 30
./benchmark/azure-throughput/scripts/ladder.ps1 -Rungs '4000:5','6000:5','8000:5'
./benchmark/azure-throughput/scripts/vm.ps1 logs                 # journalctl -fu lattice-silo
./benchmark/azure-throughput/scripts/vm.ps1 stop                 # deallocate; no compute charges
```

`update.ps1` flags:
- `-NoBuild` -- just bounce the silo (no rsync, no publish).
- `-NoRestart` -- sync + publish, leave the service alone (inspect first).
- `-Clean` -- wipe `/opt/lattice/publish*` before publishing (force full rebuild).
- `-SkipUnitSync` -- skip re-rendering the systemd units when only source changed.

`run-cohort.ps1` flags:
- `-Vehicles <N>` -- synthetic fleet size (default 4000).
- `-TickHz <N>` -- per-vehicle samples per second (default 5).
- `-DurationSec <N>` -- producer run time (default 45).
- `-ExtraSiloEnv @{ BENCH_FOO='bar' }` -- arbitrary env overrides for the silo unit.
- `-NamePrefix lat-exp` -- target a non-default environment.
- `-ParametersFile <path>` -- explicit parameters file instead of auto-discovery.
- `-QuiesceTimeoutSec <N>` -- max seconds to wait for the silo's in-flight gauge to
  drain after the producer stops, before the silo is stopped (default 60; `0` skips).
- `-CaptureCounters` -- attach `dotnet-counters` to the silo for the cohort.

Every cohort writes three artefacts under `benchmark/.run/azure-throughput/`:
- `silo-<cohort>.log` -- silo journal (between cohort start and silo stop)
- `producer-<cohort>.log` -- producer journal
- `sampler-<cohort>.csv` -- per-second CPU% / RSS samples from the VM

plus `counters-<cohort>.csv` when `-CaptureCounters` is set.

## Workloads

`BENCH_WORKLOAD_MODE` (pass it through `-ExtraSiloEnv`) selects the `ILattice`
operation the silo dispatches per producer batch; unset or unknown means `set-many`.

| Mode | Operation |
|------|-----------|
| `set-many` | One `SetManyAsync` per producer batch (`BENCH_BATCH_SIZE` entries). |
| `set-many-atomic` | `SetManyAtomicAsync` sagas of `BENCH_ATOMIC_BATCH_SIZE` keys (default 64). |
| `set-many-atomic-2` | Single-tree `SetManyAtomicAsync` sagas of 2 keys. |
| `cross-tree-atomic-2` | `BeginAtomicWrite(...).CommitAsync()` across `{treeId}` and `{treeId}-b`, 1 key per tree. |
| `cross-tree-atomic-64` | The same cross-tree saga with 64 keys (32 per tree). |
| `set-point` | One `SetAsync` per key. |
| `set-point-mv` | `set-point` with an asynchronous materialised view attached to the tree - the A/B partner that shows whether maintaining a view perturbs the source write path. |
| `get-point` | One `GetAsync` per key, over a keyspace the silo pre-seeds at startup with one `SetManyAsync` of `BENCH_VEHICLE_COUNT` keys (the silo reads the producer's variable; 0, its default there, skips the pre-seed). In cluster ingest mode (Layer 3) the silo skips this step and the producer seeds the same keys after warm-up instead, logging `[producer] preseed ... entries=N`. |
| `get-many` | `GetManyAsync` over the same pre-seeded keyspace. |

## Parallel Layer 3 producer

The Orleans-client producer uses `BENCH_GENERATOR_PARALLELISM` workers (default
`Environment.ProcessorCount`; `0` also means automatic), capped at the vehicle
count. `run-cohort-aca.ps1 -GeneratorParallelism K` pins it for a cohort; omission
resets any previous pin to automatic. Workers own disjoint vehicle slices and
retain the per-vehicle tick pacing: total offered load is still vehicles x Hz,
not K times that rate. An in-progress tick finishes at the duration boundary.

Keys are formatted once before measurement. `get-point` and `get-many` pass
empty values directly to the ingest engine, which reads only their keys; write
modes still serialize the same telemetry JSON with a fresh tick timestamp.
The TCP producer and its JSON wire protocol are unchanged. The client generator
transfers up to 1,024 entries per channel item (64 bounded items), avoiding a
shared channel lock per key. The engine still receives individual entries and
retains its own batching, flush concurrency, and single pre-seed pass. In
addition to the channel, each worker and the reader can hold one chunk.

Periodic and `DONE` lines carry `genBlockedFrac` and `slipMaxMs`:

- `genBlockedFrac` is generator-seconds spent waiting for channel capacity,
  divided by elapsed seconds x worker count. It excludes CPU/scheduling lateness:
  a high value is consumer/cluster back-pressure, not a slow generator. Periodic
  values cover the reporting interval; `DONE` covers the full run. Live blocked
  writes remain observable.
- `slipMaxMs` is the run-wide maximum schedule slip across workers, including
  overrun of an unfinished tick, not just the last completed tick.

`performance-report.ps1` warns and renders `>= X` only when the same `DONE` line
reports slip above 1,000 ms AND `genBlockedFrac` below 0.2. High slip with high
channel-wait time is consistent with a saturated cluster and is NOT marked
producer-bound, even when achieved throughput is below offered load. Slip still
includes lateness accumulated during channel waits; the wait fraction is what
distinguishes that case from generation falling behind without back-pressure.
The rule uses paired full-run totals, never maxima from different windows.
Legacy logs without both fields, and logs without `DONE`, cannot establish a
producer bottleneck and are not flagged. Re-run known producer-limited legacy
cells with the new producer before making a cluster-ceiling claim.

Scaling ratios are omitted when the cell or its 1-silo anchor is producer-bound,
and charts omit affected workload curves rather than plot a misleading plateau.
An omission note appears only when a curve was actually excluded. Resume and
dry-run aggregation re-read retained logs; paired evidence is retained in cohort
state as `producerSlipMaxMs` and `producerGenBlockedFrac`.

For a local generator-only measurement, build the Producer project in Release,
then run its DLL with `--dry-run`. This bypasses TCP, Orleans, Azure credentials,
and pre-seeding, but drains the same channel adapter into a no-op sink:

```powershell
$env:BENCH_WORKLOAD_MODE = 'get-many'
$env:BENCH_VEHICLE_COUNT = '1000000'
$env:BENCH_TICK_HZ = '10'
$env:BENCH_DURATION_SEC = '10'
$env:BENCH_GENERATOR_PARALLELISM = '4' # Repeat with 1, keeping load unchanged.
dotnet benchmark\azure-throughput\Producer\bin\Release\net10.0\VehicleFleetSimulator.AzureThroughput.Producer.dll --dry-run
```

Compare `DONE avg` and verify `dry-run drained` equals `DONE total`. This measures
local generation/queue capacity, not cluster throughput or a guaranteed parallel
speedup. No producer replicas or sliced pre-seeding are required by this path.

## Auto-shutdown safety net

The Bicep deploys a DevTestLab `shutdown-computevm-<vm>` schedule that fires at
**19:00 UTC daily** (configurable via `AutoShutdownTime` / `AutoShutdownTimeZone`
in `parameters.local.ps1`). If you forget `vm.ps1 stop`, the VM deallocates
automatically.

## Reading the results

`run-cohort.ps1` prints a self-contained summary block (values are placeholders):

```
=== Cohort complete ===
Host         : <vCPU> vCPU / <MiB> MiB / <kernel>
Cohort       : v<vehicles>-h<tickHz>-<durationSec>s-<utc>
Producer     : <systemd state of lattice-producer>
Silo FINAL   : [silo] FINAL ops=<n> failed=<n> discarded=<n> elapsed=<s>s active=<s>s ...
Steady mean  : <n> e/s (n=<samples> samples, t>=15s, rate>0) inFlight med/max=<n>/<n>
FINAL active : <n> entries in <s>s active = <n>/s
Drain tail   : <n> trailing rate=0 sample(s) post-producer
Silo CPU     : avg <pct>% / peak <pct>% (of one vCPU)
System CPU   : avg <pct>% / peak <pct>%
Silo RSS peak: <GiB> GiB (of <GiB> GiB)
Diagnostics  : stall-watchdog=<n>  wal-slot=<n>  wal-append=<n>  exceptions=<n>  failed-samples=<n>
Verdict      : HEALTHY | DEGRADED | WEDGE | FAILED
Logs         : <silo log>
             : <producer log>
             : <sampler csv>
```

`Steady mean` is the **primary** throughput number: the mean of the silo's
per-second rate samples taken at `t >= 15s` with a non-zero rate, which trims both
the warm-up ramp and the post-producer drain. `FINAL active` -- entries / active
window -- is a secondary diagnostic; the drain tail inflates its denominator when
the silo wedges, so it is flagged `(drain-inflated; ignore)` on a `WEDGE` verdict.

`Diagnostics` counts `[stall-watchdog] WEDGE DETECTED` bursts, the `[wal-slot*`
and `[wal-append*` lifecycle token families, exceptions attributable to the cohort,
and per-second samples that reported failures. A non-zero watchdog, wal-slot, or
wal-append count indicates a real wedge-shape; combined with a non-zero `failed`
count it's the evidence triad for re-opening `wedge-plan.md` (see section 23 of
that file for the policy).

`ladder.ps1` produces a CSV with one row per rung covering written, failed,
active-avg, CPU peak, RSS, verdict, and a UTC timestamp. Default location:
`scripts/.ladder-results.csv` (gitignored).

## Saturation knobs

`wedge-plan.md` section 23.3 (in this folder) catalogues what each knob bounds and
when to turn it, as of 2026-06-04; its defaults are a snapshot from that date. The
current short version, in the order an investigator reaches for them:

| Knob | Default | What it does |
|------|---------|--------------|
| `BENCH_VEHICLE_COUNT`, `BENCH_TICK_HZ` | 4000, 5 | Offered rate. |
| `BENCH_RESPONSE_TIMEOUT_SEC` | 30 | Silo grain-RPC deadline. **Raise to 180 when saturating** or you'll see `[silo] grain-rpc-deadline` failures that look like wedges but aren't. The `ladder.ps1` script pins this to 180 by default for exactly this reason. |
| `BENCH_BATCH_SIZE` | 4096 | Entries per `SetManyAsync`. |
| `BENCH_FLUSH_CONCURRENCY` | 8 | Parallel in-flight flushes from `TcpIngestService`. |
| `BENCH_POINT_FANOUT` | `BENCH_FLUSH_CONCURRENCY` | Concurrent calls per flush slot in the point modes (`set-point`, `set-point-mv`, `get-point`). The ACA cohort script sets it to the per-silo flush bound so point-mode in-flight scales linearly with silo count. |
| `BENCH_WAL_PARTITIONS` | `LatticeOptions.DefaultWalPartitions` (currently 8) | WAL grain count per tree. Pairs with `BENCH_FLUSH_CONCURRENCY`. Inherited from the shipping default so the bench tracks the library; override explicitly to A/B against a non-default fan-out. |
| `BENCH_WAL_MAX_PENDING_BATCHES` | `LatticeOptions.DefaultWalMaxPendingBatches` (currently 16) | Per-WalShardGrain pipeline depth. Inherited from the shipping default so the bench tracks the library; see [WAL Tuning](../../docs/lattice/wal-tuning.md) for the storage-account-throughput envelope above which raising this further stops helping. |
| `BENCH_SET_MANY_FANOUT_BUDGET_SEC` | `30` | Seconds `LatticeGrain.SetManyAsync` may spend awaiting its per-shard fan-out before refusing the call with `LatticeSaturatedException` (`SetManyFanOut`). **One of two knobs here that deliberately do not inherit the library default**, which is `Timeout.InfiniteTimeSpan` so that the bound is opt-in on the released line ([#3386](https://github.com/NSTA1/Orleans.Lattice/issues/3386)). An unbounded fan-out is the [#3348](https://github.com/NSTA1/Orleans.Lattice/issues/3348) collapse itself, so the rig opts in to the recommended finite budget and measures the corrected configuration. Set `0` for infinite to reproduce the pre-fix shape. |
| `BENCH_WAL_ADMISSION_CALL_BUDGET_SEC` | `15` | Seconds **one top-level call** may spend waiting at the WAL admission saturation gate, summed across every append and every retry layer (`LatticeOptions.WalAdmissionSaturationCallBudget`). The second knob that deliberately does not inherit the library default, which is `Timeout.InfiniteTimeSpan` ([#3390](https://github.com/NSTA1/Orleans.Lattice/issues/3390)). Left infinite, only the per-append `WalAdmissionSaturationWaitBudget` applies and the three nested retry layers each open a fresh one - the multiplication [#3348](https://github.com/NSTA1/Orleans.Lattice/issues/3348) names as remedy 3, recorded in its cohort logs as `10488ms of that was saturation back-off` against a 5 s per-append budget. Set `0` for infinite. |
| `BENCH_TREE_ID` | rotates per cohort | Pin to re-use an existing WAL partition; otherwise every cohort starts on an empty manifest. |

All of these can be passed via `-ExtraSiloEnv @{ BENCH_FOO = 'bar' }` to
`run-cohort.ps1`.

## A/B-ing a WAL optimisation

```powershell
# Baseline arm.
./scripts/run-cohort.ps1 -Vehicles 4000 -TickHz 5 -DurationSec 60 `
	-ExtraSiloEnv @{ BENCH_WAL_ELIMINATE_CANDIDATE_ROW = 'false'; BENCH_TREE_ID = 'ab-baseline' }

# Candidate arm.
./scripts/run-cohort.ps1 -Vehicles 4000 -TickHz 5 -DurationSec 60 `
	-ExtraSiloEnv @{ BENCH_WAL_ELIMINATE_CANDIDATE_ROW = 'true'; BENCH_TREE_ID = 'ab-candidate' }
```

Keep `Vehicles`, `TickHz`, `DurationSec` identical between arms so the only
changed variable is the option under test. Pinning `BENCH_TREE_ID` per arm
tags the WAL partition keys but is not required for cohort correctness; the
default per-cohort rotation keeps each measurement starting from an empty
manifest.

## Tearing down

```powershell
# Stops billing for compute (storage + PIP still bill at ~$14/month idle).
./benchmark/azure-throughput/scripts/vm.ps1 stop

# Full teardown.
az group delete --name rg-lat --yes --no-wait
```

## Caveats

- The harness measures **end-to-end commit throughput** with a single silo and
  a single lattice tree. It is not a proxy for the partitioned-WAL benchmark
  (`benchmark/host/Bench.WalAzureTable`), which is a structural correctness probe.
- Managed identity role propagation can take up to 60s after `deploy.ps1`
  completes. `deploy.ps1` waits for cloud-init to finish before chaining to
  `update.ps1`, which is usually enough; if the silo's first WAL write fails
  with a 403, wait a minute and run `update.ps1` again to bounce the silo.
- The tree's keys are `Guid.ToString("N")` so the workload is uniform-random
  across shards. To skew distribution, edit `Producer/Program.cs`.

## Historical context

- `wedge-plan.md` -- residual WAL wedge investigation, closed after section 23
  (the F8-on-VM re-verification cohorts).
- `throughput.md` -- performance follow-up. Section 25 is the 2026-06-04
  `Standard_F8as_v6` sweep; the later `Standard_D4as_v5` baselines are in sections
  27, 30, 31 and 34.5. The saturation-knobs catalogue is in `wedge-plan.md`
  section 23.3.

## Layer 3 (multi-silo, Azure Container Apps)

`performance-report.ps1 -Layer 3` (or `-Layer3`) measures the same engine and all nine
workloads against N silos. It provisions a rig with `scripts/deploy-aca.ps1` (or reuses
one with `-ReuseAca <prefix>`), runs `-N` cohorts (default 3) of every workload at each
count in `-SiloCounts` (default `1, 2, 4, 6, 8`) through `scripts/run-cohort-aca.ps1`,
and deletes the resource group afterwards unless `-KeepAca` is set or the rig was
reused. `-Resume` continues an interrupted sweep from its saved state. Both scripts can
also be run by hand.

`scripts/deploy-aca.ps1` provisions resource group `rg-<prefix>`, tagged with the prefix
so teardown can prove it owns the group: a container registry (images built remotely
with `az acr build`, so no local Docker), one storage account for the WAL, Orleans
clustering and grain state, Log Analytics, a Container Apps environment, the silo app,
and the producer as a manually triggered ACA Job in Orleans-client mode. It does not size
the silo app for a measurement: each cohort pins its own replica count.

| Parameter | Default | Meaning |
|-----------|---------|---------|
| `-NamePrefix` | (required) | Run prefix every resource name derives from. |
| `-Location` | `westus3` | Azure region. |
| `-SiloCpu`, `-SiloMemoryGi` | `4`, `8` | vCPU and GiB per silo replica, also used for the producer job (ranges 1-4 and 1-8). |
| `-SiloCount` | `2` | Recorded in the run context only; `run-cohort-aca.ps1 -SiloCount` sets the real replica count. |
| `-ReuseRg` | off | Provision into an existing `rg-<prefix>` from an earlier run (it must carry the rig's tag). Without it, an existing group is refused. |
| `-SkipImageBuild` | off | Reuse the images already in the registry, for a `-ReuseRg` redeploy. |

It writes the run context to `benchmark/.run/aca/<prefix>.context.json`, which is
gitignored. The file holds the registry and storage credentials, so do not share it.

`scripts/run-cohort-aca.ps1 -NamePrefix <prefix> -SiloCount <N>` runs one cohort. With
`-ResetStorage` on (the default, #3458) it first parks the silos and deletes every table
in the rig's storage account except the clustering table (`OrleansSiloInstances`), then
points the cohort at freshly named WAL and grain-state tables; pass
`-ResetStorage:$false` to keep the older accumulate-across-cohorts behaviour. It then
pins the silo app to exactly N replicas under a per-cohort tree id (and an Orleans
cluster id derived from it), runs the producer job, and parks the silos at zero again -
in a `finally` as well, so a failed cohort does not leave replicas billing. Last, it
harvests the producer and silo logs from Log Analytics into
`benchmark/.run/aca/<prefix>.n<N>.<workload>[.<CohortTag>].log` and appends the same
verdict block Layer 2 writes: `HEALTHY`, or `WEDGE` when the producer printed no DONE
marker or no window was productive.

| Parameter | Default | Meaning |
|-----------|---------|---------|
| `-NamePrefix`, `-SiloCount` | (required) | The rig (read from its run context) and the silo count, 1-30. |
| `-WorkloadMode` | `set-many` | `BENCH_WORKLOAD_MODE` (see [Workloads](#workloads)). |
| `-DurationSec` | `45` | Producer run time. |
| `-VehiclesPerSilo`, `-TickHz` | `1200`, `5` | Offered load **per silo**: the cohort offers `VehiclesPerSilo x SiloCount` vehicles, so every cell offers the same load per silo. |
| `-BatchSize`, `-FlushMs`, `-FlushConcurrencyPerSilo` | `4096`, `50`, `8` | Ingest batch size, flush interval, and per-silo in-flight flushes (the cohort runs `FlushConcurrencyPerSilo x SiloCount`). |
| `-ShardCount`, `-WalPartitions` | `64`, `16` | Tree shape, not scaled by the silo count; the producer reshards the tree to `-ShardCount` before load starts. |
| `-ClientsPerSilo` | `4` | Orleans clients the producer opens per silo (64 at most in total). |
| `-ResponseTimeoutSec`, `-WarmUpBudgetSec`, `-InFlightTailBudgetSec` | `420`, `400`, `120` | Grain-call deadline, wall-clock ceiling on the producer's warm-up retries, and the drain allowed before `FINAL`. |
| `-WalReplayQueueDepth` | `64` | `BENCH_WAL_REPLAY_QUEUE_DEPTH` for the silos. |
| `-SetManyFanOutBudgetSec`, `-WalAdmissionCallBudgetSec` | `30`, `15` | The two saturation budgets from [Saturation knobs](#saturation-knobs); `0` means infinite, the library default. |
| `-WalAppendCoalescingInFlightThreshold`, `-WalBatchedSingleEntryAppends`, `-WalSaturationRecoveryReleaseBatch`, `-WalSaturationAcuteOnly` | `-1` | A/B arms for WAL behaviours. `-1` sets nothing, so the silo keeps its own default; any value from `0` up is passed to the silo as the matching `BENCH_WAL_*` variable. |
| `-WalMaterialiserPinBuckets` | `-1` | Floor on the durable pin buckets per pin shard (#3576). `-1` sets nothing, so the silo keeps the library default; any value from `1` up is passed as `BENCH_WAL_MATERIALISER_PIN_BUCKETS`. |
| `-ResetStorage` | `$true` | Empty the rig's storage before the cohort (above). |
| `-WalTable`, `-GrainStateTable` | `OrleansLatticeWal`, `OrleansLatticeGrainState` | Table names. Under `-ResetStorage` they are replaced by fresh `Wal<stamp>` / `Gs<stamp>` names unless passed explicitly. |
| `-TreeId`, `-CohortTag` | generated, none | Tree name (default `l3-<workload>-n<N>-<stamp>`), and a tag that keeps repeated cohorts' logs apart. |
| `-ExtraSiloEnv` | none | Extra `NAME=value` silo environment variables, applied last. |
| `-SettleSec` | `30` | Wait for cluster membership before starting the producer. |

Neither script deletes the rig. `performance-report.ps1` tears down the rigs it
provisions; for a rig deployed by hand, delete `rg-<prefix>` yourself, or dot-source
`scripts/aca-common.ps1` and run `Invoke-AcaTeardown -NamePrefix <prefix>`, which checks
the ownership tag before deleting.
