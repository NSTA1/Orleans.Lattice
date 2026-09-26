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
| `infra/main.bicep` | VM + VNet + public IP + NIC (accelerated networking) + NSG + one storage account per `-WalAccountCount` + managed-identity role assignments (Table, Blob and Queue Data Contributor) + the auto-shutdown schedule. |
| `infra/cloud-init.yaml` | First-boot bootstrap (`.NET 10 SDK`, dotnet diagnostic tools, `/opt/lattice` tree). |
| `infra/bootstrap.sh` | Manual / fallback bootstrap path; idempotent. |
| `infra/lattice-silo.service` | systemd unit template for the silo (placeholders filled in by `update.ps1`). |
| `infra/lattice-producer.service` | systemd unit template for the co-located producer. |
| `scripts/parameters.ps1` | Default parameters (subscription, region, prefix, VM size). |
| `scripts/parameters.local.ps1` | **Gitignored** operator overrides; create it by copying `parameters.ps1` (no script creates it). The VM-path scripts prefer it over `parameters.ps1` when present; the Layer 3 scripts take no parameters file. |
| `scripts/deploy.ps1` | End-to-end provision: key gen, `~/.ssh/config`, Bicep deploy, cloud-init wait, bootstrap fallback, chained `update.ps1`. |
| `scripts/update.ps1` | Inner loop: `git ls-files \| tar \| ssh` -> `dotnet publish` silo+producer on the VM -> `systemctl restart`. |
| `scripts/run-cohort.ps1` | Single cohort: applies env drop-ins, restarts silo, starts producer, waits for FINAL, extracts journals, prints summary. |
| `scripts/ladder.ps1` | Thin loop over `run-cohort.ps1` for rung sweeps; writes `.ladder-results.csv`. |
| `scripts/vm.ps1` | Day-to-day helper: `start` / `stop` / `status` / `ssh` / `logs` / `refresh-ip`. |
| `scripts/_run-cohort-helpers.ps1` | Verdict-computation helpers `run-cohort.ps1` dot-sources (`run-cohort-aca.ps1` reuses its verdict-block writer). |
| `scripts/Test-CohortVerdict.ps1` | Regression tests for those helpers against literal log fixtures (pure pwsh, no Azure). |
| `scripts/deploy-aca.ps1` | Layer 3: provisions the multi-silo Azure Container Apps rig and builds its images remotely with `az acr build`. |
| `scripts/run-cohort-aca.ps1` | Layer 3: one cohort - by default empties the rig's storage first (`-ResetStorage`), pins the silos to exactly N, runs the producer job, parks the silos at zero, then harvests the producer and silo logs from Log Analytics into one cohort log. |
| `scripts/aca-common.ps1` | Shared Layer 3 helpers, also dot-sourced by `performance-report.ps1`: resource naming, the Azure CLI convention, the run-context file, silo scaling, the per-cohort storage reset, job waits and log harvest, and teardown with its ownership guard. |

## One-time setup

1. Sign in to Azure (`az login`).
2. Copy the parameters template. No script does this for you: without
   `parameters.local.ps1` the VM-path scripts read the committed `parameters.ps1`, whose
   blank `SubscriptionId` makes `deploy.ps1` stop.
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

`deploy.ps1` flags:
- `-NamePrefix <name>` -- names the resource group (`rg-<name>`), the VM resources and
  the `~/.ssh/config` host alias.
- `-VmSize <sku>` -- VM SKU override (`parameters.ps1` defaults to `Standard_D2as_v5`).
- `-ParametersFile <path>` -- explicit parameters file instead of auto-discovery.
- `-WalAccountCount <1..8>` -- WAL storage accounts to provision (default 1).
  `update.ps1` passes the extra accounts' table endpoints to the silo, which
  registers them as keyed WAL providers (`acct1`, `acct2`, ...), and
  `BENCH_WAL_ACCOUNTS` (through `-ExtraSiloEnv`) spreads
  the tree's WAL partitions across that many accounts, clamped to the number
  provisioned; the silo then logs a `[silo] wal-placement ...` line.

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
- `-NoBuild` -- just bounce the silo (no source sync, no publish).
- `-NoRestart` -- sync + publish, leave the service alone (inspect first).
- `-Clean` -- wipe `/opt/lattice/publish*` before publishing (force full rebuild).
- `-SkipUnitSync` -- skip re-rendering the systemd units when only source changed.
- `-NamePrefix <name>`, `-ParametersFile <path>` -- as for `run-cohort.ps1` below.

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
- `silo-<cohort>.log` -- silo journal (between cohort start and silo stop), with the
  runner's verdict block appended
- `producer-<cohort>.log` -- producer journal
- `sampler-<cohort>.csv` -- per-second CPU% / RSS samples from the VM

plus `counters-<cohort>.csv` when `-CaptureCounters` is set.

`ladder.ps1` flags:
- `-Rungs '<vehicles>:<tickHz>',...` -- rungs to sweep (default `1000:5`, `5000:5`,
  `10000:5`, `20000:5`, `50000:5`).
- `-DurationSec <N>` -- producer seconds per rung (default 30).
- `-CooldownSec <N>` -- pause between rungs (default 5).
- `-ResponseTimeoutSec <N>` -- `BENCH_RESPONSE_TIMEOUT_SEC` for every rung (default
  180; 30 reproduces the grain-RPC-deadline failure mode).
- `-ExtraSiloEnv @{...}` -- extra silo env merged into every cohort.
- `-DegradeThresholdPct <N>` -- stop the sweep once a rung's throughput falls below
  `(1 - N/100)` of the best rung so far (default 0: never stop early).
- `-ResultsCsv <path>` -- output CSV (default `scripts/.ladder-results.csv`).
- `-NamePrefix <name>`, `-ParametersFile <path>` -- as for `run-cohort.ps1`.

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
| `get-point` | One `GetAsync` per key, over a keyspace the silo pre-seeds at startup with one `SetManyAsync` of `BENCH_VEHICLE_COUNT` keys - on the VM (TCP ingest) path only. The silo reads the same variable as the producer, and its silo-side default of 0 skips the pre-seed: `run-cohort.ps1` sets it only for the producer, so pass it to the silo through `-ExtraSiloEnv` as well. A Layer 3 (cluster ingest) silo never pre-seeds, and neither does the Orleans-client producer, so unless `-TreeId` names a tree written earlier, a Layer 3 read cohort reads keys nothing has written. |
| `get-many` | `GetManyAsync` over the same keyspace, with the same pre-seed caveats. |

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
wal-append count indicates a real wedge-shape. Re-opening `wedge-plan.md` takes the
evidence triad its section 23.4 sets out: a `[stall-watchdog]` firing, a non-trivial
dominant `[wal-slot]` / `[wal-append]` lifecycle stage, and no `[silo] grain-rpc-deadline`
line (failures that come with that line are the harness's own RPC deadline, not a wedge).

`ladder.ps1` produces a CSV with one row per rung: vehicles, tickHz, duration,
written, failed, active seconds, steady mean, active-avg, drain-tail samples, total
elapsed, silo CPU peak and average, system CPU peak, silo RSS, verdict, and a UTC
timestamp. Default location:
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
| `BENCH_WAL_PARTITIONS` | `LatticeOptions.DefaultWalPartitions` (currently 8) | WAL grain count per tree. Pairs with `BENCH_FLUSH_CONCURRENCY`. Inherited from the shipping default so the bench tracks the library; override explicitly to A/B against a non-default fan-out. |
| `BENCH_WAL_MAX_PENDING_BATCHES` | `LatticeOptions.DefaultWalMaxPendingBatches` (currently 16) | Per-WalShardGrain pipeline depth. Inherited from the shipping default so the bench tracks the library; see [WAL Tuning](../../docs/lattice/wal-tuning.md) for the storage-account-throughput envelope above which raising this further stops helping. |
| `BENCH_SET_MANY_FANOUT_BUDGET_SEC` | `30` | Seconds `LatticeGrain.SetManyAsync` may spend awaiting its per-shard fan-out before refusing the call with `LatticeSaturatedException` (`SetManyFanOut`). **One of two knobs here that deliberately do not inherit the library default**, which is `Timeout.InfiniteTimeSpan` so that the bound is opt-in on the released line ([#3386](https://github.com/NSTA1/Orleans.Lattice/issues/3386)). An unbounded fan-out is the [#3348](https://github.com/NSTA1/Orleans.Lattice/issues/3348) collapse itself, so the rig opts in to the recommended finite budget and measures the corrected configuration. Set `0` for infinite to reproduce the pre-fix shape. |
| `BENCH_WAL_ADMISSION_CALL_BUDGET_SEC` | `15` | Seconds **one top-level call** may spend waiting at the WAL admission saturation gate, summed across every append and every retry layer (`LatticeOptions.WalAdmissionSaturationCallBudget`). The second knob that deliberately does not inherit the library default, which is `Timeout.InfiniteTimeSpan` ([#3390](https://github.com/NSTA1/Orleans.Lattice/issues/3390)). Left infinite, only the per-append `WalAdmissionSaturationWaitBudget` applies and the three nested retry layers each open a fresh one - the multiplication [#3348](https://github.com/NSTA1/Orleans.Lattice/issues/3348) names as remedy 3, recorded in its cohort logs as `10488ms of that was saturation back-off` against a 5 s per-append budget. Set `0` for infinite. |
| `BENCH_TREE_ID` | rotates per cohort | Pin to re-use an existing WAL partition; otherwise every cohort starts on an empty manifest. |

All of these can be passed via `-ExtraSiloEnv @{ BENCH_FOO = 'bar' }` to `run-cohort.ps1`,
except that the offered rate comes from `-Vehicles` / `-TickHz`: on the silo,
`BENCH_VEHICLE_COUNT` only sizes the read-mode pre-seed and `BENCH_TICK_HZ` is not read.

The silo and producer read many more `BENCH_*` variables (WAL pipeline and coalescing
toggles, saturation-sampler thresholds, the leaf-storage kind, multi-account WAL
fan-out, the Layer 3 clustering knobs). The
[`azure-throughput-rig` skill](../../.github/skills/azure-throughput-rig/SKILL.md)
catalogues them; the rig's `Silo/`, `Producer/` and `Engine/` sources are the authority.

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
  27, 30 and 34.5 (section 31 is the `Standard_D8as_v5` ceiling and section 32 a D4
  wedge observation). The saturation-knobs catalogue is in `wedge-plan.md`
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
| `-WalAppendCoalescingInFlightThreshold`, `-WalBatchedSingleEntryAppends`, `-WalSaturationRecoveryReleaseBatch`, `-WalSaturationAcuteOnly` | `-1` | A/B arms for WAL behaviours. `-1` sets nothing, so the silo keeps the value the app already carries - its own default, unless an earlier cohort on the rig pinned the variable (see below); any value from `0` up is passed to the silo as the matching `BENCH_WAL_*` variable. The silo honours only a positive coalescing threshold, so `0` for that arm falls back to the default. |
| `-ResetStorage` | `$true` | Empty the rig's storage before the cohort (above). |
| `-WalTable`, `-GrainStateTable` | `OrleansLatticeWal`, `OrleansLatticeGrainState` | Table names. Under `-ResetStorage` they are replaced by fresh `Wal<stamp>` / `Gs<stamp>` names unless passed explicitly. |
| `-TreeId`, `-CohortTag` | generated, none | Tree name (default `l3-<workload>-n<N>-<stamp>`), and a tag that keeps repeated cohorts' logs apart. |
| `-ExtraSiloEnv` | none | Extra `NAME=value` silo environment variables, applied last; they persist into later cohorts (see below). |
| `-SettleSec` | `30` | Wait for cluster membership before starting the producer. |

Silo environment variables persist across cohorts on one rig: each cohort applies its
variables with `az containerapp update --set-env-vars`, which adds or overwrites
variables but never removes one. A variable an earlier cohort pinned - an A/B arm above
or an `-ExtraSiloEnv` entry - therefore stays set for every later cohort that does not
restate it. Before comparing against a default, restate the value the arm needs (for
example `-WalSaturationAcuteOnly 1`) or provision a fresh rig: a `deploy-aca.ps1 -ReuseRg`
redeploy keeps the existing silo app and its variables.

Neither script deletes the rig. `performance-report.ps1` tears down the rigs it
provisions; for a rig deployed by hand, delete `rg-<prefix>` yourself, or dot-source
`scripts/aca-common.ps1` and run `Invoke-AcaTeardown -NamePrefix <prefix>`, which checks
the ownership tag before deleting.
