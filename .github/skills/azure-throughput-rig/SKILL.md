---
name: azure-throughput-rig
description: Operate the real-Azure azure-throughput benchmark rig in benchmark/azure-throughput/. Use when running throughput experiments against real Azure Storage - provisioning or tearing down the single-silo VM and WAL storage accounts or the multi-silo Azure Container Apps rig, running a cohort, picking a workload (set-many, atomic, cross-tree, set-point, set-point-mv, get-point, get-many), tuning any BENCH_* configuration knob, or reading the cohort result. Documents every workload option, every BENCH_* env var, and every script parameter.
---

# Operating the azure-throughput rig

A two-process Linux-VM benchmark in `benchmark/azure-throughput/` that drives a
single-silo Orleans.Lattice host against a **real Azure Storage account** (Azure Table
WAL via managed identity) and reports entries written/read per second. It is the only
benchmark in the suite backed by real Azure Storage; the local docker scenarios use
Azurite, which collapses network RTT.

## Topology

One Linux VM runs two systemd units that share a loopback TCP hop on `127.0.0.1:7000`:

- `lattice-producer.service` - emits synthetic `VehicleTelemetryEvent` JSON lines over TCP.
- `lattice-silo.service` - reads those lines and dispatches the selected `ILattice`
  operation against one lattice tree, persisting WAL traffic to Azure Tables. The silo
  authenticates with the VM's **system-assigned managed identity** (no keys/connection
  strings on the default path).

The silo prints `ops/sec` to stdout once per second; the systemd-journald log is the
canonical result surface.

## Workflow at a glance

```powershell
./scripts/deploy.ps1 -NamePrefix latperf -VmSize Standard_D8as_v5   # provision once
./scripts/vm.ps1 start                                              # ensure VM running
./scripts/update.ps1                                                # sync source, publish, restart silo
./scripts/run-cohort.ps1 -Vehicles 4000 -TickHz 5 -DurationSec 30 ` # run one measurement
  -ExtraSiloEnv @{ BENCH_WORKLOAD_MODE = 'set-point' }
./scripts/vm.ps1 logs                                               # tail the silo journal
./scripts/vm.ps1 stop                                               # deallocate compute
az group delete --name rg-latperf --yes --no-wait                   # full teardown
```

All scripts live under `benchmark/azure-throughput/scripts/`. The single-VM scripts
(`deploy`, `update`, `run-cohort`, `ladder`, `vm`) accept `-NamePrefix` to target a named
environment and `-ParametersFile` to point at an explicit parameters file; the Layer 3 ACA
scripts (`deploy-aca`, `run-cohort-aca`) take a mandatory `-NamePrefix` and read no
parameters file. The folder also holds dot-sourced helper modules (`_run-cohort-helpers.ps1`,
`_storage-metrics.ps1`, `aca-common.ps1`), `complete-cohort-aca.ps1` (the harvest-and-grade half
of a Layer 3 cohort, see below) and parameterless self-tests of the grading logic, run with
`pwsh -File` (`Test-CohortVerdict.ps1`, `Test-Layer3PreseedReport.ps1`,
`Test-Layer3UnseededRetry.ps1`, `Test-ProducerBoundReport.ps1`, `Test-PerformanceReportHarness.ps1`,
`Test-Layer3StorageMetrics.ps1`, `Test-Layer3OverlapHarvest.ps1`). The Layer 3 sweep itself is
driven by `benchmark/performance-report.ps1 -Layer 3`, which calls `run-cohort-aca.ps1` per cohort.
Between cohorts, `Set-AcaSiloCount` (in `aca-common.ps1`) does not return until every superseded
silo revision has drained (`Wait-AcaRevisionsRetired`), so an old revision's replicas cannot
overlap the next cohort's warm-up; expect roughly 30-60 s per silo-count transition (#3588).

**`performance-report.ps1` run prefix.** `-NamePrefix <prefix>` forces the prefix every resource
name derives from, except that on Layer 3 a `-ReuseAca` rig takes precedence over it; without it a
Layer 1 or Layer 2 run takes `-ReuseVm`'s prefix or mints a fresh one, and a Layer 3 run takes
`-ReuseAca`'s rig or provisions one under a fresh prefix. The report
lower-cases the prefix it is given, strips its hyphens and refuses more than nine characters, so use
three to nine lowercase letters and digits (`deploy-aca.ps1` also refuses fewer than three
alphanumerics). Common shapes:

- **Layer 3 on a rig you provisioned.** Run `./scripts/deploy-aca.ps1 -NamePrefix <prefix>`, then
  `benchmark/performance-report.ps1 -Layer 3 -ReuseAca <prefix>`. It parks the silos at zero when it
  finishes and leaves `rg-<prefix>` standing for you to delete.
- **`-DryRun`** re-renders a document from the most recently written `state.json`. Replay the
  multi-silo document with either `-Layer3 -DryRun` or `-Layer 3 -DryRun`. Replay selects the
  newest state containing the requested layer, and a Layer 1/2 replay preserves the other block.
- **Layer 2 cells are usually lower bounds.** Layer 2 rungs are chosen below saturation and never
  escalate, so a row whose usable cohorts all completed at least `-SaturationRatio` (default 0.9)
  of their offered load (rung vehicles x Hz) is published `>= X` with a note: it measured the
  offered load, not the silo's ceiling. Do not compare such a cell with an escalated Layer 3 cell.
  `get-many` at 4,000 vehicles x 5 Hz reads ~20 k keys/s on Layer 2, while Layer 3 escalates the
  same workload to ~312 k at one silo. Cells published before the steady-mean fix in #5060 can
  exceed their own offered load (up to 176%), because the old mean averaged only non-zero seconds.
- **Layer 3 on several rigs in parallel (`-Layer3Rigs K`).** The report splits the sweep
  between K rigs itself (`Split-Layer3Sweep`), deterministically, so `-Resume` finds each cell
  where it ran. With at least as many workloads as rigs it splits by workload - longest-first by
  estimated cost, never splitting a workload, so each curve and its escalation carry-over come
  from one rig. With more rigs than workloads (e.g. `-Workloads get-many -Layer3Rigs 2`) each
  workload gets one rig plus extra rigs by cost, and its silo counts are dealt into contiguous
  ascending runs, one per rig; that curve then spans rigs, so the merge records it in
  `layer3.splitCurves`, the published metadata names it (`splitCurves=get-many=a+b`) and its
  methodology says a step between rigs can be the rig difference, and each rig's escalation
  starts from the row's starting rung. No more rigs are used than there are cells. It
  provisions the rigs concurrently (prefixes
  `<NamePrefix>1..K` truncated to nine characters, or fresh random ones) or uses a
  comma-separated `-ReuseAca a,b,c` list, runs one ordinary single-rig sweep per rig as a child
  `pwsh` process with `-SkipDocUpdate` (output in `.run/performance-report/<prefix>/sweep.log`,
  provisioning in `deploy.log`), then merges the rigs' cells into
  `.run/performance-report/rigs-<prefixes>/state.json` and renders the multi-silo document once.
  The merge refuses a (workload, silo count) cell measured on two rigs and rigs that disagree on
  region or on any cell-shaping setting. Each rig has its own storage, so parallel sweeps never contend for the
  account under measurement. Rigs it provisioned are parked and deleted unless `-KeepAca`; reused
  rigs are only parked. Mind the regional vCPU quota: K rigs can hold K x max(`-SiloCounts`) x 4
  vCPU at once. Do not instead run several `-Layer 3` reports by hand against one document: each
  rewrites the whole multi-silo block with only its own cells.
- **`-Layer3OverlapHarvest`** runs a cohort's Log Analytics harvest (mostly waiting for log
  ingestion, up to ~7 minutes) in a background job while the next cohort runs, for every cohort
  whose result cannot change what the sweep does next: not a cell's first cohort (its throughput
  decides the escalation) unless `-SaturationRatio 0`, and not a read cohort (an `UNSEEDED` read
  is re-run at once). Deferred harvests are joined, and graded exactly like an inline non-first
  cohort, before the cell is checkpointed. Cohorts so harvested carry `harvestDeferred` in state.
- **`-Layer3WalAccounts N`** spreads every cohort's WAL across N storage accounts (see
  `deploy-aca.ps1 -WalAccountCount` and `run-cohort-aca.ps1 -WalAccounts`) and is recorded in
  the published metadata (`walAccounts=`); a reused rig must already have the accounts.

---

## Scripts and their parameters

### `parameters.ps1` / `parameters.local.ps1`

`parameters.ps1` holds the committed defaults; copy it to `parameters.local.ps1`
(gitignored) and edit. Every single-VM script auto-discovers `parameters.local.ps1`. Fields:

| Field | Default | Meaning |
|-------|---------|---------|
| `SubscriptionId` | `''` | Azure subscription (`az account show --query id -o tsv`). |
| `ResourceGroup` | `rg-lat` | Base resource-group name; becomes `rg-<NamePrefix>`. |
| `Location` | `westus3` | Region. **Set to the same region as the Tables account.** |
| `NamePrefix` | `lat` | Names the RG, VM, and `~/.ssh/config` host alias. |
| `VmSize` | `Standard_D2as_v5` | VM SKU (smallest with accelerated networking). |
| `AdminUsername` | `azureuser` | VM admin user. |
| `SshPublicKeyPath` | `~/.ssh/id_ed25519.pub` | SSH public key; `deploy.ps1` generates one if missing. |
| `AllowedSshSourceAddress` | `''` | NSG SSH allow-CIDR; blank auto-detects your IP. |
| `AutoShutdownTimeZone` | `UTC` | Time zone for the auto-shutdown schedule. |
| `AutoShutdownTime` | `1900` | Daily VM auto-deallocate time (HHmm). |
| `OsDiskSizeGB` | `64` | OS disk size. |

### `deploy.ps1` - provision end-to-end

Provisions VM + NIC (accelerated networking) + NSG + storage account(s) + managed-identity
role assignments + cloud-init, then chains to `update.ps1` to publish and start the silo.

| Parameter | Default | Effect |
|-----------|---------|--------|
| `-ParametersFile <path>` | auto-discover | Explicit parameters `.ps1`. |
| `-NamePrefix <name>` | from parameters | Names the RG (`rg-<name>`) and SSH host alias. |
| `-VmSize <sku>` | from parameters | VM SKU override. |
| `-WalAccountCount <1..8>` | `1` | Number of WAL storage accounts to provision up front. Values >1 create extra accounts whose table endpoints are wired into the silo as keyed WAL providers, enabling the `BENCH_WAL_ACCOUNTS` fan-out without a redeploy. |
| `-PrivateNetwork` | off | Policy-compliant topology for subscriptions that disable storage public access and block VM public IPs: no VM public IP (SSH through an Azure Bastion Standard tunnel to `127.0.0.1`), NAT-gateway egress, storage behind private endpoints with privatelink DNS. Tags the RG `latticeBenchNetwork=private`; every other VM script detects the tag and tunnels automatically. |

To run a second environment side-by-side: `./scripts/deploy.ps1 -NamePrefix lat-exp -VmSize Standard_F8as_v6`.

### `update.ps1` - inner loop (sync -> publish -> restart)

Tars the git-tracked tree, uploads it over SSH, extracts it over `/opt/lattice/src` and
deletes every file there that git no longer tracks (outside `bin/` and `obj/`, so the
publish stays incremental; extraction alone never removed a file deleted or renamed in the
repo, and the SDK kept compiling the stale copy). It then runs `dotnet publish` for the
silo and producer on the VM, re-renders the systemd units, restarts the silo, and fails
unless `lattice-silo` is `active` five seconds later (on a fresh deploy that can be the
~60 s managed-identity propagation window: wait and re-run `update.ps1 -NoBuild`).

| Parameter | Effect |
|-----------|--------|
| `-NoBuild` | Skip the source sync + publish; just bounce the silo. |
| `-NoRestart` | Sync + publish but leave the service running (inspect before restart). |
| `-Clean` | Wipe `/opt/lattice/publish*` before publishing (force a full rebuild). |
| `-SkipUnitSync` | Don't re-render the systemd units (use when only source changed). |
| `-ParametersFile <path>` | Explicit parameters file. |
| `-NamePrefix <name>` | Target a named environment. |

### `run-cohort.ps1` - run one measurement

Applies the env drop-in, restarts the silo, starts the producer with the given load,
waits for the producer to exit, lets in-flight work drain, stops the silo, and pulls the
journals back to `benchmark/.run/azure-throughput/`.

| Parameter | Default | Effect |
|-----------|---------|--------|
| `-Vehicles <N>` | `4000` | Sets `BENCH_VEHICLE_COUNT` (fleet size). |
| `-TickHz <N>` | `5` | Sets `BENCH_TICK_HZ` (samples/sec/vehicle). Offered rate = Vehicles x TickHz. |
| `-DurationSec <N>` | `45` | Sets `BENCH_DURATION_SEC` (producer run length). |
| `-ExtraSiloEnv @{...}` | `@{}` | Arbitrary silo env overrides applied via a runtime drop-in (cleared between cohorts). **This is how every workload/config arm is selected without a redeploy.** |
| `-QuiesceTimeoutSec <N>` | `60` | Max seconds to wait for the silo's in-flight gauge to drain to zero before stopping (avoids shutdown-abort artefacts on in-flight cross-tree sagas). `0` skips the wait. |
| `-CaptureCounters` | off | Attach `dotnet-counters` for the cohort; writes `counters-<cohort>.csv`. |
| `-ParametersFile <path>` | auto | Explicit parameters file. |
| `-NamePrefix <name>` | from parameters | Target a named environment. |

Artifacts per cohort under `benchmark/.run/azure-throughput/`: `silo-<cohort>.log`,
`producer-<cohort>.log`, `sampler-<cohort>.csv` (per-second CPU%/RSS), and
`counters-<cohort>.csv` when `-CaptureCounters` is set.

The human summary is `Write-Host` (console only). The script also returns one
`LatticeBench.CohortResult` object on the success stream - `CohortName`, `Written`, `Failed`,
`Unresolved`, `ActiveSec`, `SteadyMean`, `SteadyCount`, `SteadyZeroCount`, `ActiveAvg`,
`DrainTailSamples`, the CPU/RSS figures, `Verdict`, `VerdictReasons` and the artifact paths -
so `$r = ./run-cohort.ps1 ... | Select-Object -Last 1` captures the result. Setting
`BENCH_INFLIGHT_TAIL_BUDGET_SEC` above 18 in `-ExtraSiloEnv` prints a warning: the silo unit's
30 s stop window minus the engine's 10 s saturation quiesce leaves no room for more, and a
SIGKILL before `FINAL` grades the cohort `WEDGE`.

### `ladder.ps1` - rung sweep

Loops `run-cohort.ps1` over a list of rungs and appends one row per rung to a results CSV,
built from the `LatticeBench.CohortResult` object each cohort returns. A rung whose cohort
throws (an ssh/scp failure) is recorded with verdict `ERROR` and zeros, and the sweep moves
on. Invoke it in-process (`./ladder.ps1 -Rungs '1000:5','5000:5'`): under `pwsh -File` the
`-Rungs` array arrives as one comma-joined string.

| Parameter | Default | Effect |
|-----------|---------|--------|
| `-Rungs <"V:Hz"[]>` | `1000:5 ... 50000:5` | Array of `vehicles:tickHz` rungs. |
| `-DurationSec <N>` | `30` | Producer seconds per rung. |
| `-CooldownSec <N>` | `5` | Pause between rungs. |
| `-ResponseTimeoutSec <N>` | `180` | Sets `BENCH_RESPONSE_TIMEOUT_SEC` for every rung (drop to 30 to reproduce the grain-RPC-deadline failure mode). |
| `-ExtraSiloEnv @{...}` | `@{}` | Extra silo env forwarded to every cohort. |
| `-DegradeThresholdPct <N>` | `0` | Stop the sweep once a rung's steady mean drops below `(1 - N/100)` of the best observed, or once a rung yields no usable throughput (a `WEDGE` past the knee) after one that did - a "find the peak" sweep. `ERROR` rungs never stop it. |
| `-ResultsCsv <path>` | `scripts/.ladder-results.csv` | Output CSV path, rewritten from its header at the start of every sweep. |
| `-NamePrefix <name>` / `-ParametersFile <path>` | - | As above. |

### `vm.ps1 <action>` - day-to-day VM control

Mandatory positional `-Action`, plus `-NamePrefix` / `-ParametersFile`.

| Action | Effect |
|--------|--------|
| `start` | Start (allocate) the VM. |
| `stop` | Deallocate the VM (stops compute billing; storage + public IP still bill). |
| `status` | Show power state. |
| `ssh` | Open an SSH session to the VM. |
| `logs` | Tail the silo journal (`journalctl -fu lattice-silo`). |
| `refresh-ip` | Point the NSG's `AllowSshFromOperator` SSH rule at your current public IP (`/32`); run it after moving to a new network. No-op on a `-PrivateNetwork` rig. |
| `tunnel` | `-PrivateNetwork` rigs only: (re)start the Bastion SSH tunnel to the VM on its per-prefix local port. `ssh`, `logs`, `update.ps1` and `run-cohort.ps1` start it on demand. |
| `tunnel-stop` | Stop that tunnel. |

### `deploy-aca.ps1` and `run-cohort-aca.ps1` - Layer 3 (multi-silo, Azure Container Apps)

Layer 3 measures the same workloads against N silos. `deploy-aca.ps1` provisions one
resource group `rg-<prefix>` - a container registry (images are built remotely with
`az acr build`, so no local Docker is needed), one storage account for the WAL, the
Orleans clustering table and grain state, Log Analytics, a Container Apps environment,
the silo app (created resting at zero replicas; `run-cohort-aca.ps1` scales it to exactly N
per cohort), and the producer as an ACA Job in Orleans-client mode -
and builds and pushes both images.

Before provisioning it registers any of the resource providers it needs
(`Microsoft.App`, `Microsoft.ContainerRegistry`, `Microsoft.OperationalInsights`,
`Microsoft.Storage`, plus `Microsoft.Network` and `Microsoft.ManagedIdentity` with
`-PrivateNetwork`) that the subscription has not registered, and waits for them, failing
after 60 minutes. On a new subscription the first `Microsoft.App` registration can take
longer than that; registration carries on in Azure regardless, and a re-run picks up the
wait where it left off (the script creates nothing before this step).

| Parameter | Default | Effect |
|-----------|---------|--------|
| `-NamePrefix <name>` | (required) | Names and tags every resource. |
| `-SiloCount <1..30>` | `2` | Echoed in the banner and recorded in the saved rig context only: the silo app is created with `--min-replicas 0 --max-replicas 30` and rests at zero replicas until `run-cohort-aca.ps1 -SiloCount` scales it. |
| `-Location <region>` | `westus3` | Azure region. |
| `-SiloCpu <1..4>` | `4` | vCPU per silo replica, matching the Layer 2 VM's core count. |
| `-SiloMemoryGi <1..8>` | `8` | Memory per replica; ACA Consumption caps a replica at 4 vCPU / 8 GiB. |
| `-ReuseRg` | off | Provision into the resource group an earlier run created. |
| `-SkipImageBuild` | off | Reuse the images already in the registry. Only valid with `-ReuseRg`. |
| `-PrivateNetwork` | off | Internal-only workload-profiles environment on a VNet subnet with NAT-gateway egress, storage with public access and shared keys disabled behind a table private endpoint, and a user-assigned identity for storage (`BENCH_STORAGE_URI`, `BENCH_CLUSTERING_TABLE_SERVICE_URI`, `AZURE_CLIENT_ID`) and ACR pull instead of a connection-string secret and admin credentials. Tags the RG `latticeBenchNetwork=private`. |
| `-WalAccountCount <1..8>` | `1` | Storage accounts the WAL can be spread across. Values above 1 add WAL-only accounts `st<prefix>w1..` (same auth and network shape as the primary: a private endpoint and the identity's table role on `-PrivateNetwork`, a key-based connection string otherwise) and wire them into the silo app as keyed WAL providers: `BENCH_WAL_EXTRA_ACCOUNT_URIS`, plus one `BENCH_WAL_EXTRA_ACCOUNT_CONN_<i>` secret per account on the public topology. Recorded in the rig context as `walExtraAccounts`. A `-ReuseRg` run adds accounts to an existing app (then parks it) and never drops one it already has. |

`performance-report.ps1 -PrivateNetwork` passes the switch to `deploy.ps1` (Layers 1 and 2)
and `deploy-aca.ps1` (Layer 3) when it provisions; a reused rig keeps the mode it was created with.

`run-cohort-aca.ps1` runs one cohort: it scales the silos to exactly N, waits for
membership, runs the producer job, parks the silos, then hands a pending-harvest descriptor
(`<cohort log>.pending.json`: execution name, silo revision, cohort window, storage accounts) to
`complete-cohort-aca.ps1`, which harvests the producer and silo logs from Log Analytics, appends
one `[storage-metrics]` line per storage account the cohort used, grades the cohort and writes
the cohort log. With `-DeferHarvest` it returns after parking instead, leaving
`complete-cohort-aca.ps1 -PendingHarvestPath <path>` to the caller. The silos are scaled back to
zero in a `finally` either way. Its load parameters are **per silo**, so every cell offers the
same load per silo.

| Parameter | Default | Effect |
|-----------|---------|--------|
| `-NamePrefix <name>` | (required) | The rig `deploy-aca.ps1` provisioned. |
| `-SiloCount <1..30>` | (required) | Silos for this cohort - the independent variable. |
| `-WorkloadMode <mode>` | `set-many` | Sets `BENCH_WORKLOAD_MODE`. |
| `-DurationSec <N>` | `45` | Producer run length. |
| `-VehiclesPerSilo <N>` | `1200` | Per-silo fleet size; the cohort offers `VehiclesPerSilo x SiloCount` keys. |
| `-TickHz <N>` | `5` | Samples/sec/vehicle. |
| `-BatchSize <N>` / `-FlushMs <N>` | `4096` / `50` | As `BENCH_BATCH_SIZE` / `BENCH_FLUSH_MS`. |
| `-FlushConcurrencyPerSilo <N>` | `8` | Multiplied by `SiloCount` into `BENCH_FLUSH_CONCURRENCY`, and passed unmultiplied as `BENCH_POINT_FANOUT`, so the point modes hold `FlushConcurrencyPerSilo^2 x SiloCount` calls in flight (constant per-silo demand). |
| `-ShardCount <N>` | `64` | Fixed across the sweep (not per silo), so the fan-out width stays constant. |
| `-WalPartitions <N>` | `16` | Sets `BENCH_WAL_PARTITIONS`. |
| `-ClientsPerSilo <N>` | `4` | Sets `BENCH_CLIENT_COUNT` to `min(64, ClientsPerSilo x SiloCount)`. |
| `-GeneratorParallelism <0..1024>` | `0` | Sets `BENCH_GENERATOR_PARALLELISM` on the producer job; `0` lets the producer use its own `Environment.ProcessorCount` (see "Parallel Layer 3 producer" below). |
| `-ResponseTimeoutSec <N>` | `420` | Sets `BENCH_RESPONSE_TIMEOUT_SEC` on silos and producer. |
| `-InFlightTailBudgetSec <N>` | `120` | Sets `BENCH_INFLIGHT_TAIL_BUDGET_SEC`. |
| `-WalReplayQueueDepth <N>` | `64` | Sets `BENCH_WAL_REPLAY_QUEUE_DEPTH`. |
| `-WarmUpBudgetSec <N>` | `400` | Sets `BENCH_WARMUP_BUDGET_SEC`. |
| `-TreeId <id>` | `l3-<mode>-n<N>-<timestamp>` | A fresh tree per cohort; `BENCH_CLUSTER_ID` is derived from it. |
| `-CohortTag <tag>` | - | Disambiguates the log when the same cell is repeated. |
| `-WalTable <name>` / `-GrainStateTable <name>` | `OrleansLatticeWal` / `OrleansLatticeGrainState` | Table names. An explicitly supplied name is honoured unchanged. |
| `-ResetStorage <bool>` | `$true` | Start every cohort against empty storage: delete every table except the clustering table and use freshly-named WAL and grain-state tables (#3458). |
| `-SetManyFanOutBudgetSec <N>` / `-WalAdmissionCallBudgetSec <N>` | `30` / `15` | Set the two #3348 budgets explicitly; `0` = infinite (the library default). |
| `-TxRegistryShards <N>` | `8` | Saga decision registry shards per tree (`BENCH_TX_REGISTRY_SHARDS`); `1` = the unsharded library default. |
| `-WalAppendCoalescingInFlightThreshold <N>` | `-1` | `-1` sets nothing (see the note below the table); any value from `0` up sets `BENCH_WAL_APPEND_COALESCING_IN_FLIGHT_THRESHOLD` on the silos, and `0` is the #3396 control arm (coalescing off). |
| `-WalBatchedSingleEntryAppends <N>` | `-1` | `-1` sets nothing (see below); `0` / `1` pin the #3408 control / fix arms. |
| `-WalMaterialiserPinBuckets <N>` | `-1` | `-1` sets nothing (see below); any value from `1` up sets `BENCH_WAL_MATERIALISER_PIN_BUCKETS` (ACA only). |
| `-WalSaturationRecoveryReleaseBatch <N>` | `-1` | `-1` sets nothing (see below); `0` is the #3402 release-everything control arm. |
| `-WalSaturationAcuteOnly <N>` | `-1` | `-1` sets nothing (see below); `1` / `0` force it on / off. |
| `-ExtraSiloEnv <string[]>` | `@()` | Extra silo env as `NAME=value` strings, appended last so they win (a string array, not the hashtable `run-cohort.ps1` takes). Like every silo variable, they are removed from the next cohort that does not name them (see below). |
| `-SettleSec <N>` | `30` | Wait after scaling for cluster membership before the producer starts. |
| `-FreezeWedgeSec <N>` | `60` | Longest tolerated mid-run freeze (cumulative ops not advancing while work is in flight) before the cell is graded `WEDGE` rather than `HEALTHY`; `0` disables the check. |
| `-WalAccounts <1..8>` | `1` | Storage accounts the cohort's WAL partitions are spread across (`BENCH_WAL_ACCOUNTS` on silos and producer): the primary plus `WalAccounts-1` extra accounts. Fails before anything bills if the rig has fewer (`deploy-aca.ps1 -WalAccountCount`). `-ResetStorage` also empties every extra account. |
| `-DeferHarvest` | off | Return once the producer has finished and the silos are parked, with `PendingHarvestPath` on the returned object; run `complete-cohort-aca.ps1 -PendingHarvestPath <path>` later to harvest, grade and write the log. The harvest is pinned to the cohort's execution name and silo revision, so its timing changes nothing measured. |

**Silo env does not persist between cohorts.** Each cohort applies its silo env with one
`az containerapp update --set-env-vars`, which merges into the app's existing environment, so
the same update also passes `--remove-env-vars` for every variable an earlier cohort set that
this cohort does not name (#3514). Secret-backed variables and the `deploy-aca.ps1` baseline are
kept, and the removal rides the same update, so each cohort still mints exactly one revision. A
`-1`-defaulted arm therefore runs the library default. The producer job is unaffected: every
cohort sets its whole env list.

**The silo app runs in Multiple revision mode.** `deploy-aca.ps1` creates it with
`--revisions-mode multiple`, and `Set-AcaSiloCount` (via `Set-AcaSiloMultipleRevisionMode`)
switches an older rig over before every cohort update, failing closed if the mode cannot be
confirmed. In Single mode an update on a parked app briefly reactivated the last parked
revision at its old replica count; those silos restarted against the previous cohort's WAL and
grain-state tables, which `-ResetStorage` had just deleted, logged `TableBeingDeleted` and exited
139 while the new cohort warmed up. The silo app has no ingress, so Multiple mode's traffic split
is irrelevant; superseded revisions are retired explicitly.

**Invoke `run-cohort-aca.ps1` in-process, not with `pwsh -File`.** `-ExtraSiloEnv` is a string
array, and `pwsh -File` passes `@('A=1','B=2')` as one literal string, so the silos receive a
single malformed variable. From `benchmark/azure-throughput/scripts`:

```powershell
& .\run-cohort-aca.ps1 -NamePrefix <prefix> -SiloCount 1 -WorkloadMode set-point-mv -DurationSec 45 `
  -CohortTag repro -ExtraSiloEnv @('LATTICE_BENCH_AWAIT_DIAG_MS=5000','BENCH_GRAIN_TURN_DIAG_MS=5000')
```

---

## Workload options (`BENCH_WORKLOAD_MODE`)

Selects which `ILattice` operation the silo dispatches per producer batch. Case-insensitive
kebab- or concatenated form (`set` and `get` are also accepted for `set-point` and
`get-point`). Unset/unknown => `set-many`.

| Value | Operation exercised |
|-------|---------------------|
| `set-many` (default) | One `ILattice.SetManyAsync` per producer batch (`BENCH_BATCH_SIZE` entries). The batched write path. |
| `set-many-atomic` | `ILattice.SetManyAtomicAsync`, slicing each producer batch into atomic sagas of `BENCH_ATOMIC_BATCH_SIZE` keys (default 64). Single-tree all-or-nothing writes. |
| `set-many-atomic-2` | Single-tree atomic sagas pinned to **2 keys** each. Fixed-shape partner for the atomic comparison. |
| `cross-tree-atomic-2` | All-or-nothing write across **two** trees (`{treeId}` and `{treeId}-b`), 1 key per tree, via `IGrainFactory.BeginAtomicWrite(...).CommitAsync()`. |
| `cross-tree-atomic-64` | Cross-tree atomic saga of 64 keys (32 per tree). |
| `set-point` | One `ILattice.SetAsync` per key - fan-out point writes. |
| `set-point-mv` | Identical write path to `set-point`, but the silo also attaches an asynchronous materialised view (key-preserving passthrough) over the tree via `AddLatticeViews`. The A/B partner of `set-point` for measuring whether maintaining a view perturbs the source tree's point-write path. |
| `get-point` | One `ILattice.GetAsync` per key - fan-out point reads, against a keyspace pre-seeded as described below the table. |
| `get-many` | `ILattice.GetManyAsync` - batched reads, against the same pre-seeded keyspace. |

**Read-mode pre-seed.** For `get-point` and `get-many` the silo seeds the keyspace at startup,
before its TCP listener opens, with one `ILattice.SetManyAsync` of `BENCH_VEHICLE_COUNT` keys
(the producer's key derivation, 245-byte values); `ILattice.BulkLoadAsync` is not used because it
requires an empty shard and the warm-up has already materialised the root leaf. The seed runs only
when the **silo** is given `BENCH_VEHICLE_COUNT` > 0 (its default is 0, no seed), and
`run-cohort.ps1 -Vehicles` sets the same fleet size on both producer and silo, ignoring a
conflicting `BENCH_VEHICLE_COUNT` in `ExtraSiloEnv`. The silo logs
`[silo] preseed treeId=.. entries=..` when the seed ran. Layer 2 aggregation refuses read
cohorts without retained evidence of a complete seed, including legacy miss-path cohorts.
On
Layer 3 (`BENCH_INGEST_MODE=cluster`) the silo returns before its seed step, and the Orleans-client
producer seeds the same keys after warm-up instead, logging `[producer] preseed treeId=.. entries=N`;
`performance-report.ps1` marks a Layer 3 read cohort whose log lacks that line (or reports
`entries=0`) `UNSEEDED`, re-runs it at the same rung up to twice, and never publishes it, because
an unseeded cohort measures only the miss path (#3474). Layer 3 seeds in bounded slices,
allowing eight attempts per unlanded slice and 300 seconds overall (including outstanding
RPCs); progress resets attempts but never the deadline. Failure logs retain the offset,
exception and budget. Layer 2 does not retry unseeded cohorts: it excludes them from the
table. Tables state the usable throughput cohort count per row, and a failed attempted row
cannot preserve an older result under the new run's metadata.

Layer 3 harvest retains revision-scoped silo diagnostics with timestamp and replica identity
in 200-row pages under one ingestion snapshot. Non-phaseA lines carry an `[aca-silo ...]`
prefix so they cannot pollute the producer throughput series. The `set-point-mv` view name
is `bench-{treeId}` for both registration and lag reporting, isolating durable checkpoints
between cohorts that rotate their source tree but retain VM grain-state storage.

Atomic modes dispatch each saga as its own flush unit (`BenchWorkloadDispatcher.SliceIntoFlushUnits`):
one `BENCH_FLUSH_CONCURRENCY` slot, one retry ladder and one `ops`/`failed` booking per saga, so
`inFlight` counts sagas. Before #3581 a producer batch (often 1,000+ sagas) was one unit run as a
sequential chain: ops stayed at 0 until the chain's last saga returned, a saturation retry
re-committed the sagas that had landed, and one rolled-back saga booked the whole batch as failed.
An atomic cohort that reads `ops=0` with a busy cluster on an older checkout is that artefact.

> The `set-point-mv` workload and the multi-account knobs below are on `main`. Only an
> older checkout that predates the materialised-views work lacks them; there, use the
> other modes and the single-account path.

---

## Configuration (`BENCH_*` environment variables)

Pass any of these via `run-cohort.ps1 -ExtraSiloEnv @{ ... }` (silo) - the producer-side
rate vars are set for you by `run-cohort.ps1`'s `-Vehicles` / `-TickHz` / `-DurationSec`.

### Offered load (producer)

| Var | Default | Effect |
|-----|---------|--------|
| `BENCH_VEHICLE_COUNT` | producer 1000, silo 0 (`run-cohort.ps1 -Vehicles` sets both, default 4000; `run-cohort-aca.ps1` sets `VehiclesPerSilo x SiloCount` on both) | Fleet size = number of distinct keys; also the silo read-mode pre-seed size. Both cohort runners keep it equal to the offered fleet. |
| `BENCH_TICK_HZ` | 5 | Samples/sec/vehicle. **Offered rate = vehicles x tickHz.** |
| `BENCH_DURATION_SEC` | 300 (cohort sets per `-DurationSec`) | Producer run length. `0` = run forever, in both the TCP and the Orleans-client producer modes. |
| `BENCH_SILO_HOST` | `127.0.0.1` | Silo host the producer connects to. |
| `BENCH_SILO_PORT` | 7000 | Silo TCP port the producer connects to. |

### Storage and identity (silo)

| Var | Default | Effect |
|-----|---------|--------|
| `BENCH_STORAGE_URI` | - (required) | `https://{account}.table.core.windows.net` - WAL table endpoint for managed identity. |
| `BENCH_STORAGE_CONN` | - | Connection-string fallback; overrides `BENCH_STORAGE_URI` when set. |
| `BENCH_WAL_TABLE` | `OrleansLatticeWal` | WAL table name. **Use a distinct table per cohort whenever arms are to be compared** (on the VM path pass it through `-ExtraSiloEnv`; on Layer 3, `run-cohort-aca.ps1 -ResetStorage`, on by default, already does). Cohorts sharing one table accumulate each other's rows and produce bursts of 409 `EntityAlreadyExists` failures uncorrelated with anything under test; a distinct table per cohort removes them. Rotating `BENCH_TREE_ID` isolates cohorts logically but does not stop the table growing. |
| `BENCH_TREE_ID` | rotating `azure-throughput-<utc>` | Tree id. Rotates per silo restart so prior offsets don't bias the run; **pin it to re-use existing rows** (cross-run replay). `run-cohort.ps1` sets it to `cohort-<cohort-name>` unless `-ExtraSiloEnv` supplies one, so each cohort gets a fresh tree. |
| `BENCH_TCP_PORT` | 7000 | Silo TCP listen port. |

### Workload shape

| Var | Default | Effect |
|-----|---------|--------|
| `BENCH_WORKLOAD_MODE` | `set-many` | The operation under test (see workload table above). |
| `BENCH_ATOMIC_BATCH_SIZE` | 64 | Saga key-count, used only by `set-many-atomic`. |

### Batching and flush

| Var | Default | Effect |
|-----|---------|--------|
| `BENCH_BATCH_SIZE` | 4096 | Entries per `SetManyAsync`. |
| `BENCH_FLUSH_MS` | 50 | Max flush latency (ms) before a partial batch is sent. |
| `BENCH_FLUSH_CONCURRENCY` | 8 | Max in-flight `SetManyAsync` calls. Pairs with `BENCH_WAL_PARTITIONS` so parallel flushes fan out across distinct WAL grains. Drop to 1 to isolate per-leaf-turn RTT from mailbox queueing. |
| `BENCH_POINT_FANOUT` | `BENCH_FLUSH_CONCURRENCY` | Concurrent calls each flush slot fans out into for the point modes (`set-point`, `set-point-mv`, `get-point`), so their in-flight count is `BENCH_FLUSH_CONCURRENCY x BENCH_POINT_FANOUT`. Unset falls back to `BENCH_FLUSH_CONCURRENCY` (the single-VM rig behaviour). `run-cohort-aca.ps1` sets it to `-FlushConcurrencyPerSilo` so the count grows linearly with the silo count rather than quadratically (#3474). |

### WAL fan-out and pipeline

| Var | Default | Effect |
|-----|---------|--------|
| `BENCH_WAL_PARTITIONS` | `LatticeOptions.DefaultWalPartitions` (8) | WAL grains per tree - the primary write-parallelism lever. Distinct partitions => distinct Azure Tables manifest partitions. |
| `BENCH_WAL_MAX_PENDING_BATCHES` | `LatticeOptions.DefaultWalMaxPendingBatches` (16) | Per-`WalShardGrain` pipeline depth. `1` = strict single-in-flight ordering against the provider. |
| `BENCH_WAL_APPEND_COALESCING_IN_FLIGHT_THRESHOLD` | `LatticeOptions.DefaultWalAppendCoalescingInFlightThreshold` (4) | In-flight flush depth at or above which an arriving batch's final entry stops kicking its own flush, so small fanned-out slices accumulate into the next flush window instead of each paying a round trip. `0` turns coalescing off (the historical unconditional kick), which is the control arm of a sweep; a negative or unparseable value runs the default 4, and `walAppendCoalescing=` in the silo banner shows the value that ran. Positive values apply as given. The shipping default was chosen on the fan-out arithmetic (#3396), not measured, so a sweep such as `{0, 1, 2, 4, 8}` is the way to pin it. |
| `BENCH_SET_MANY_FANOUT_BUDGET_SEC` | `30` | Seconds `SetManyAsync` awaits its per-shard fan-out before refusing with `LatticeSaturatedException` (`SetManyFanOut`). Deliberately does **not** inherit the library default (`Timeout.InfiniteTimeSpan`): an unbounded fan-out is the #3348 collapse, so the rig opts in to the finite budget. `0` = infinite. |
| `BENCH_WAL_ADMISSION_CALL_BUDGET_SEC` | `15` | Bounds a call's total WAL-admission saturation back-off (`LatticeOptions.WalAdmissionSaturationCallBudget`). Deliberately does **not** inherit the library default (`Timeout.InfiniteTimeSpan`): left infinite, only the per-append wait budget applies and each nested retry layer buys a fresh one (#3348), so the rig opts in to the recommended 3x-per-append value. `0` = infinite. |
| `BENCH_TX_REGISTRY_SHARDS` | `1` (silo), `8` (cohort script) | Saga decision registry shards per tree (`LatticeOptions.TxRegistryShardCount`, #3501). The library default `1` is unsharded; the cohort script opts in to `8` so atomic cohorts are not capped at ~100 sagas/s by one registry row. Clamped to `1..256`. |
| `BENCH_WAL_REPLAY_QUEUE_DEPTH` | `LatticeOptions.DefaultWalReplayPermitQueueDepthPerPermit` (4) | Replay-admission queue depth per permit. The gate admits depth x permits concurrently replaying activations and the permit count follows the silo's CPU grant, so on a 4-vCPU silo the default refuses a cold warm-up that needs every shard root replaying at once. The bench has no foreground reader for the gate to protect; only Layer 3 raises it (`run-cohort-aca.ps1 -WalReplayQueueDepth`, default 64). |
| `BENCH_WAL_BATCHED_SINGLE_ENTRY_APPENDS` | `LatticeOptions.DefaultWalBatchedSingleEntryAppends` (on) | Routes a bulk WAL append carrying exactly **one** entry through the interleaving `AppendBatchAsync` rather than the exclusive-turn `AppendAsync` overload. Under a wide fan-out the per-leaf slice is one entry, so the exclusive turn holds the partition for a whole provider round trip, pinning `wal.append.batch_entries` at 1 and `wal.append.in_flight` at 0 - which also makes `BENCH_WAL_APPEND_COALESCING_IN_FLIGHT_THRESHOLD` unreachable, since reaching it needs the concurrency the exclusive turn removed. `0` is the control arm of the #3408 A/B (historical behaviour), `1` or unset the fix arm. Echoed in the silo banner, so an arm is self-proving against a stale image. |
| `BENCH_WAL_MATERIALISER_PIN_BUCKETS` | `LatticeOptions.DefaultWalMaterialiserPinBuckets` (1) | Floor on the durable WAL materialiser pin buckets per pin shard (`WalMaterialiserPinBuckets`). The pin store splits itself above the floor by powers of two once a slot outgrows its 64 KiB byte budget (#3576), so the default no longer overflows the Azure Table 983,040-byte entity limit on a large preseed (before the fix, `get-many` at 128k keys per silo reached 6.15 MB per shard and wedged N=6). Leave unset for published numbers so they reflect product defaults; raise it only to start from a wider layout for an A/B. Echoed in the silo banner. |
| `BENCH_WAL_ACCOUNTS` | 1 | How many provisioned storage accounts the tree's WAL partitions are spread across (index 0 = `BENCH_STORAGE_URI`, 1..N-1 = the extra accounts): partition `p` moves to account `p mod N` on the warmed, empty tree. Clamped to the number actually provisioned (`deploy.ps1` / `deploy-aca.ps1 -WalAccountCount`). The single-VM silo spreads before opening its listener; on Layer 3 (cluster ingest mode, where the silo skips that step) the Orleans-client producer spreads after its warm-up (`run-cohort-aca.ps1 -WalAccounts`). |
| `BENCH_WAL_EXTRA_ACCOUNT_URIS` | - (set by `update.ps1` / `deploy-aca.ps1`) | `;`-delimited list of extra account table endpoints, wired as keyed WAL providers `acct1, acct2, ...`. Normally you don't set this by hand - `deploy.ps1 -WalAccountCount` + `update.ps1`, or `deploy-aca.ps1 -WalAccountCount`, populate it; `run-cohort-aca.ps1` also hands it to the producer so its clamp sees the provisioned count. |
| `BENCH_WAL_EXTRA_ACCOUNT_CONN_<i>` | - (set by `deploy-aca.ps1`) | Connection string for extra account `i` (provider `acct<i>`), used instead of managed identity when set. One variable per account because a connection string contains `;`. Layer 3's public topology supplies them as secrets; the VM and `-PrivateNetwork` rigs leave them unset. |
| `BENCH_PIPELINE_PHASE2` | on | Overlap phase 2 of batch N with phases 0+1 of batch N+1 on the same shard. `0` disables. |
| `BENCH_WAL_PHASE2_COALESCING_WINDOW_MS` | 5 | How long the per-shard PhaseTwoWorker waits after the first arrival so additional commits coalesce into one Azure Tables transaction. `0` = drain on first signal. |
| `BENCH_WAL_PHASE2_COMMIT_TIMEOUT_SEC` | library default (12) | Per-commit deadline for the PhaseTwoWorker's manifest commit. `0` = unbounded; `>0` = finite deadline (a hung commit becomes a bounded timeout the resync path recovers). |
| `BENCH_DIGEST_COALESCING_WINDOW_MS` | 5 | Coalescing window (ms) for digest writes. |
| `BENCH_WAL_ELIMINATE_CANDIDATE_ROW` | library default | Toggle the hot-path candidate-row elimination optimisation. |
| `BENCH_SHARD_COUNT` | 0 (library default, 64) | Override the tree's physical shard count via `ILattice.ReshardAsync` at startup (a smaller target on a populated tree starts an online shrink, which the silo waits out before listening; any target works against an empty tree). |

### WAL transport hygiene

| Var | Default | Effect |
|-----|---------|--------|
| `BENCH_WAL_CONNECTION_REUSE` | false | Use a pooled-connection reuse transport with a finite pooled-connection lifetime + idle timeout (cloud-NAT socket hygiene). |
| `BENCH_WAL_CONN_LIFETIME_SEC` | 90 | Pooled-connection lifetime (s) when reuse is on. |
| `BENCH_WAL_NETWORK_TIMEOUT_SEC` | 0 (SDK default ~100 s) | Per-attempt network timeout for the WAL Tables client; a finite value bounds each HTTP attempt so a hung request releases its pending-batch slot. |

### Leaf / grain storage

| Var | Default | Effect |
|-----|---------|--------|
| `BENCH_LEAF_STORAGE_KIND` | `azure` | Grain-state store for leaf/internal/atomic checkpoints: `azure` (production-shape Azure Table storage), `memory` (Orleans memory storage - diagnostic), `null` (no-op writes - removes persistence to expose the WAL ceiling; not production-shape). |
| `BENCH_LEAF_STORAGE_TABLE` | `OrleansLatticeGrainState` | Table name when kind = `azure`. |
| `BENCH_LEAF_STORAGE_NUM_GRAINS` | 0 (library default, 10) | `NumStorageGrains` when kind = `memory`. |

### Saturation signal (back-pressure)

| Var | Default | Effect |
|-----|---------|--------|
| `BENCH_SATURATION_SAMPLE_MS` | `LatticeOptions.DefaultWalSaturationSampleInterval` (200) | WAL saturation sampler tick (ms). `0` disables the sampler (signal pins to Healthy; TCP-read gating becomes a no-op; the silo maps `0` to `Timeout.InfiniteTimeSpan`, the library's "disabled" value). On ACA pass it with `run-cohort-aca.ps1 -ExtraSiloEnv 'BENCH_SATURATION_SAMPLE_MS=0'`. The silo applies the saturation-signal rows as **global** `LatticeOptions`, because the library's WAL saturation sampler reads only the unnamed options; applied per tree, as the rig did before #3348, they were silently ignored. |
| `BENCH_SATURATION_THROTTLED_RATIO` | `LatticeOptions.DefaultWalSaturationThrottledRatio` (0.75) | Admission-depth ratio at/above which the tree raises Throttled. Range [0.0, 1.0]; lower = earlier throttle. |
| `BENCH_SATURATION_DISPATCH_TIMEOUT_THRESHOLD` | `LatticeOptions.DefaultWalSaturationDispatchTimeoutThreshold` (1) | Min dispatch-timeout trips per window that raise Saturated regardless of depth. |
| `BENCH_WAL_SATURATION_ACUTE_ONLY` | `1` (matches the library default `LatticeOptions.DefaultWalSaturationAcuteOnly`) | Sets `WalSaturationAcuteOnly` (#3348): an admission semaphore at its cap classifies Throttled instead of Saturated, and a gate-parked append resumes once its partition leaves Saturated. `0` measures the historical classification. `run-cohort-aca.ps1 -WalSaturationAcuteOnly 0|1`. |
| `BENCH_WAL_SATURATION_RECOVERY_RELEASE_BATCH` | `LatticeOptions.DefaultWalSaturationRecoveryReleaseBatch` (16) | Parked WAL-admission waiters a recovered partition admits per sampler tick. `0` releases the whole parked herd at once (pre-#3402 behaviour) - the control arm for the #3402 comparison. |
| `BENCH_THROTTLED_LINE_DELAY_MICROS` | library default (1000 = 1 ms) | Per-line delay applied while Throttled, slowing the TCP reader so the producer's socket blocks and the admission gate drains. `0` = no delay. |

### Lifecycle, timeouts, reporting

| Var | Default | Effect |
|-----|---------|--------|
| `BENCH_RESPONSE_TIMEOUT_SEC` | 30 | Orleans Silo + Client `ResponseTimeout` (s). **Raise to 180 when saturating** so a slow worst-partition flush doesn't trip the deadline and trigger a producer reconnect/retransmit storm. `ladder.ps1` pins this to 180. |
| `BENCH_TOTAL_DURATION_SEC` | 600 | Server-side watchdog: after this many seconds the silo triggers a graceful shutdown even if the cohort runner died. `0` disables. |
| `BENCH_EXPECTED_SILOS` | 0 (off; `run-cohort-aca.ps1` sets the replica count) | Each silo holds its startup warm-up until its cluster manifest lists this many silos. Ungated, warm-up during cluster formation pinned every shard root and WAL partition onto the first silos to join (#3348). |
| `BENCH_WARMUP_GATE_TIMEOUT_SEC` | 300 | Bound on the `BENCH_EXPECTED_SILOS` wait; the silo fails loudly rather than warming a partial cluster. |
| `BENCH_WARMUP_GATE_SETTLE_SEC` | 5 | Extra delay after the gate opens, absorbing manifest skew between silos. `0` disables. |
| `BENCH_WARMUP_BUDGET_SEC` | 480 | Producer, Orleans-client mode only: wall-clock ceiling (clamped to 30-3600 s) on the warm-up retry loop, so a warm-up that keeps hitting the response timeout is abandoned rather than billing N silos. `run-cohort-aca.ps1 -WarmUpBudgetSec` sets it (default 400). |
| `BENCH_INFLIGHT_TAIL_BUDGET_SEC` | 12 | How long the ingest engine may spend draining in-flight flushes before emitting `FINAL`; work that settles inside it lands in `ops` or `failed`, work still in flight when it expires is reported as `unresolved=` (and grades the cohort `WEDGE` on both layers). The default fits the VM unit's 30 s systemd stop window, and `run-cohort.ps1` warns above 18; `run-cohort-aca.ps1 -InFlightTailBudgetSec` raises it (default 120), as ACA jobs have no stop window. |
| `BENCH_REPORT_SEC` | 1 | stdout `ops/sec` report interval (s). |
| `BENCH_PHASEA_REPORT_SEC` | 10 | Cadence (s) of the Phase A latency-attribution `[phaseA]` diagnostic lines (count, min, p50/p75/p90/p99 and max per instrument/tree/shard/phase/status window). `performance-report.ps1` publishes a cohort's per-call quantiles by merging windows weighted by call count (`Merge-PhaseAWindows`, a piecewise-linear model of each window, since raw samples are not logged): on Layer 2 the windows lying inside the steady-state window, on Layer 3 every window at t>=15s of every silo (their clocks cannot be aligned to the producer's). It used to publish only the last window, which on a 45 s get-many cohort read p50 0.64 ms against 3.70 ms merged. `0` disables. |
| `BENCH_MV_LAG_REPORT_SEC` | 5 | `set-point-mv` only: cadence (s) of the silo-side `[silo:mv-lag] t=.. view=.. applyLag=.. backlog=..` lines, plus a `final` line at shutdown. On Layer 3 the ingest engine runs in the producer process, which cannot observe the view maintainer's instruments, so `MV-FINAL lagAtStop=n/a` there is expected and these silo lines are the lag evidence. `0` disables. |
| `BENCH_GRAIN_TURN_DIAG_MS` | unset (off) | Positive threshold (ms) for a grain-turn occupancy filter on lattice grains: each incoming grain call running at least that long inside its activation prints `[turn-diag] state=done ms=.. grain=.. method=..`, and calls still running are summarised periodically as `state=pending ... count=N`. It times execution, not mailbox queueing, so a long `done` on a non-reentrant grain is the turn that blocked its callers. Diagnostic only. |
| `LATTICE_BENCH_AWAIT_DIAG_MS` | unset (off) | Library-side positive threshold (ms) for named await regions inside the lattice grains (leaf replay, split and relocate steps, leaf timer arms such as `leaf.timer.coverage-lag arm=..` and `leaf.timer.digest`, shard serial-turn and point-write waits, `tx.terminal-hlc-leaf`). Prints `[await-diag] state=done|pending|suppressed region=.. ms=.. grain=..`; output is rate-limited per region. Read once at process start; diagnostic only. |
| `BENCH_DISABLE_STORAGE_USAGE_POLLER` | empty | Set to `1` to disable the storage-usage poller for the cohort (`StorageUsagePollInterval = 0`). |

### Multi-silo cluster (Layer 3)

`deploy-aca.ps1` and `run-cohort-aca.ps1` set these; the single-VM path leaves them unset.

| Var | Default | Effect |
|-----|---------|--------|
| `BENCH_CLUSTERING` | `localhost` | Silo clustering: `localhost` (single silo) or `azuretable` (Azure Table membership). Any other value is fatal at startup. |
| `BENCH_CLUSTERING_CONNECTION_STRING` | - | Clustering-table connection string. With `azuretable`, this or `BENCH_CLUSTERING_TABLE_SERVICE_URI` is required; the Orleans-client producer reads it too. |
| `BENCH_CLUSTERING_TABLE_SERVICE_URI` | - | Clustering-table endpoint, authenticated with `DefaultAzureCredential`; the alternative to the connection string. |
| `BENCH_CLUSTERING_TABLE` | `OrleansSiloInstances` | Membership table name. |
| `BENCH_CLUSTER_ID` | `azure-throughput` | Orleans cluster id; silo and producer must match. `run-cohort-aca.ps1` rotates it per cohort (`azure-throughput-<treeId>`) so each cohort gets an empty membership partition. |
| `BENCH_SILO_CLUSTER_PORT` | 11111 | Silo-to-silo port. |
| `BENCH_GATEWAY_PORT` | 30000 | Client gateway port. |
| `BENCH_INGEST_MODE` | `tcp` | Silo: `tcp` (read the producer's lines over TCP) or `cluster` (no TCP listener; the producer drives the cluster as an Orleans client). |
| `BENCH_PRODUCER_MODE` | `tcp` | Producer: `tcp` (send lines to one silo) or `orleans-client` (drive `ILattice` directly through Orleans clients). |
| `BENCH_CLIENT_COUNT` | 1 | Producer, Orleans-client mode: clients to build, clamped to 1-64. One client pins to one gateway, so reaching N silos needs several. |

---

## Reading a cohort result

> **Only the first cohort after a deployment is a trustworthy absolute number.**
> Measured on an 8-silo ACA deployment (#3348): the first cohort sustained
> ~329 ops/s and every later cohort on that same deployment fell to 17-45,
> independent of workload, of the option under test, and of whether each cohort
> got a fresh WAL table. The cause is not understood. Two consequences: never
> compare a cohort against one taken earlier in the same deployment's life, and
> when an A/B matters, interleave the arms (ctl, fix, ctl, fix) so the decay is
> shared by both rather than attributed to one - or redeploy and take the first
> cohort of each arm. An A/B run as "all controls, then all fix arms" on one
> deployment will report the decay as an effect of the fix.
>
> That caution predates `run-cohort-aca.ps1 -ResetStorage` (on by default since #3458),
> which starts every Layer 3 cohort against empty storage: on the 91-cohort Layer 3
> sweep run that way, repeat cohorts of most cells agreed to within a few percent
> however late in the sweep they ran. It still applies to `run-cohort.ps1` and to
> `-ResetStorage:$false`.

`run-cohort.ps1` prints a `=== Cohort complete ===` summary, writes the silo journal to
`benchmark/.run/azure-throughput/silo-<cohort>.log`, and returns the same figures as a
`LatticeBench.CohortResult` object (the summary itself is `Write-Host`, so capture the
object, not the text).

Key lines in the silo log:

- `[silo] t= ..s ops= .. ops/sec= .. inFlight= ..` - one per-second sample. `run-cohort.ps1`
  averages the `ops/sec` of the samples from the first non-zero sample at `t >= 15s` to the
  last non-zero sample (trimming the warm-up ramp and the post-producer drain) into its
  `Steady mean : .. e/s` summary line, which is the **primary cohort metric** for an A/B and
  what Layer 2 of `performance-report.ps1` publishes. Zero-rate seconds *inside* that window
  are stalls and are kept (the summary counts them as `stalled`): the historical `rate>0` form
  dropped them and so overstated a cohort that stalled mid-run by exactly its stalled fraction.
  A cohort with no interior zero second reads the same under both.
- `[silo] FINAL ops=.. failed=.. discarded=.. elapsed=..s active=..s ops/sec (avg)=.. (active avg)=..`
  - the **active avg** divides `ops` by the window from the first accepted batch to the last
  settled flush (to `FINAL` itself while flushes are still unresolved), so it excludes the
  pre-connect idle window and the idle wait for the harness to stop the silo, but not a
  post-producer drain stall. Before the window ended at the last settled flush it ran to the
  silo stop, charging the harness's quiesce poll to `active` and deflating every single-VM
  `active avg`. The summary repeats it as the secondary `FINAL active` line, marked
  `(drain-inflated; ignore)` on a WEDGE verdict. `failed` / `discarded` count unsuccessful
  operations. The line ends with `unresolved=.. unresolvedFlushes=..`: entries (and flushes)
  still in flight when the `BENCH_INFLIGHT_TAIL_BUDGET_SEC` tail budget expired. They settle
  after `FINAL`, so they are in neither `ops` nor `failed`; a cohort that abandoned work used to
  report `failed=0`. Both `run-cohort.ps1` and `run-cohort-aca.ps1` grade any `unresolved > 0`
  as `WEDGE`.
- `Verdict : HEALTHY | DEGRADED | WEDGE | FAILED` and `Drain tail : N trailing rate=0 sample(s) post-producer`
  - the run-cohort classification, appended to the log by `run-cohort.ps1` after the drain
  (the silo does not emit it). A non-zero `failed` - on the `FINAL` line or on any per-second
  sample - raises the verdict to at least `FAILED`. `run-cohort-aca.ps1` appends the same block to
  its Layer 3 log but grades only `HEALTHY` or `WEDGE`: `WEDGE` when the producer printed no
  `DONE` marker, no per-second window was productive, or the cumulative `ops` count stopped
  advancing for `-FreezeWedgeSec` seconds while work was in flight, the `FINAL` line reported
  `unresolved > 0`, or a `-WalAccounts` > 1 cohort's log lacks the producer's `wal-placement`
  line. A saturated Layer 3 cohort that kept completing work stays `HEALTHY`, its failures
  carried as data rather than as `FAILED`.
- `[silo] wal-placement treeId=.. accounts=N partitions=M version=.. -> 0:default,1:acct1,...` - emitted when
  `BENCH_WAL_ACCOUNTS > 1`; confirms which account each WAL partition landed on. Its
  absence (with accounts >1) means the arm ran single-account, unless a
  `WARN wal-placement read` line says the spread landed but could not be read back. An
  `ERROR wal-placement-spread` means the spread failed and the silo stopped before opening
  its listener, so the arm did not run. On Layer 3 the same lines carry a `[producer]` prefix;
  a failed spread there ends the producer job with no `DONE`, so the cohort grades `WEDGE`.
- `[storage-metrics] account=.. role=wal0|wal<i> transactions=.. throttled=.. throttledSuccess=..
  timeouts=.. otherErrors=.. peakTps=.. serverLatencyMs=.. e2eLatencyMs=.. e2eLatencyMaxMs=.. minutes=..`
  - Layer 3 only, one per storage account the cohort used, from Azure Monitor at one-minute
  grain over the cohort window (warm-up and drain included; every cohort runs alone on its
  rig's accounts). `throttled` counts `ServerBusyError` + `ClientThrottlingError` transactions;
  the gap between `e2eLatencyMs` and `serverLatencyMs` is time outside the storage service.
  `wal0` also carries clustering and grain-state traffic. An account whose metrics never
  landed reads `unavailable=<reason>`, never zeros. `performance-report.ps1` takes per cohort
  the throttled share across accounts and the worst account's peak TPS and latencies, and per
  cell the median, rendered in the multi-silo doc's `perf-storage:layer3` table. Not part of
  the grade.

The CPU, RSS, and diagnostics lines appear only in the printed summary, not in the log:
`Silo CPU : avg ..% / peak ..%` (of one vCPU) and `Silo RSS peak` come from the per-second
sampler (`sampler-<cohort>.csv`), whose CPU columns are deltas over the last second (the silo's
from its `/proc/<pid>/stat` CPU ticks; until this was fixed it came from a single `top -b -n1`
frame, which reports no last-second figure and understated the peak), and
`Diagnostics : stall-watchdog=.. wal-slot=.. wal-append=.. exceptions=.. failed-samples=..`
counts internal back-pressure/stall signals.

`ladder.ps1` additionally writes one CSV row per rung to its `-ResultsCsv`, with the columns
`vehicles, tickHz, durationSec, written, failed, activeSec, steadyMean, activeAvg,
drainTailSamples, totalElapsedSec, siloCpuPeakPct, siloCpuAvgPct, sysCpuPeakPct,
siloRssGiB, verdict, timestampUtc`, taken from each cohort's `LatticeBench.CohortResult`.
An `ERROR` row is a rung whose cohort threw before producing a result.

---

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

`performance-report.ps1` grades a cohort producer-bound (a warning, and its cell
rendered as a `>= X` lower bound) only when the same `DONE` line reports slip above
1,000 ms AND `genBlockedFrac` below 0.2, the cluster kept pace with what was generated
(the cohort completed at least 90% of the `DONE avg` rate), AND the generator fell behind
its offer (`DONE avg` below 90% of the offered rate); a throughput conjunct whose input is
missing counts as met. High slip with high
channel-wait time is consistent with a saturated cluster and is NOT marked
producer-bound, even when achieved throughput is below offered load. Slip still
includes lateness accumulated during channel waits; the wait fraction is what
distinguishes that case from generation falling behind without back-pressure.
The rule uses paired full-run totals, never maxima from different windows.
Legacy logs without both fields, and logs without `DONE`, cannot establish a
producer bottleneck and are not flagged. Re-run known producer-limited legacy
cells with the new producer before making a cluster-ceiling claim.

`>= X` also marks an offer-bound cell. The `performance-report.ps1` Layer 3 sweep re-runs a
cell's first HEALTHY cohort at double the per-silo rung while it completes at least
`-SaturationRatio` (default 0.9) of its offered load, at most `-MaxRungEscalations` (default 3)
times, and publishes a cell whose every HEALTHY cohort still completed that fraction at the
final rung as a lower bound; `-SaturationRatio 0` turns escalation off. A producer-bound or
`UNSEEDED` cohort never escalates.

Scaling ratios are omitted when the cell or its 1-silo anchor is producer-bound,
and charts omit affected workload curves rather than plot a misleading plateau.
An omission note appears only when a curve was actually excluded. Resume and
dry-run aggregation re-read retained logs; the evidence is retained in cohort state as
`producerSlipMaxMs`, `producerGenBlockedFrac` and `producerGeneratedPerSec`, and the grade as
`producerBound`.

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

## Auto-shutdown and teardown

`deploy.ps1` installs a DevTestLab schedule `shutdown-computevm-<prefix>-vm` that
deallocates the VM at `AutoShutdownTime` (default 19:00 UTC) daily. A long sweep can run
past that - disable the schedule for the duration, then delete the whole group when done:

```powershell
# Disable auto-shutdown for a long sweep.
az resource update --resource-group rg-latperf `
  --resource-type Microsoft.DevTestLab/schedules `
  --name shutdown-computevm-latperf-vm --set properties.status=Disabled

# Full teardown (stops all compute + storage billing).
az group delete --name rg-latperf --yes --no-wait
```

`vm.ps1 stop` only deallocates compute; the storage account(s) and public IP keep billing
until the resource group is deleted.

## First-run notes

- `az login` first. Managed-identity role propagation can take up to ~60 s after
  `deploy.ps1`; if the silo's first WAL write returns 403, wait a minute and re-run
  `update.ps1` to bounce the silo. Multi-account deploys can take longer to propagate.
- Keys are not random. Each is a vehicle GUID in `ToString("N")` form built from the vehicle's
  index (the index in its first four bytes, fixed constants in the other twelve), so a run with the
  same vehicle count reuses the same keys, and every vehicle emits once per tick. They spread
  across shards because the tree places each key by an XxHash32 hash of it, not because the keys
  are random. The derivation is duplicated across producer and pre-seed code and the copies must agree:
  `Producer/Program.cs` (the TCP producer), `Producer/ChannelGenerator.cs` (the Orleans-client
  producer and `--dry-run`) and `Engine/BenchPreseed.cs` (`KeyFor`, the keys the read-mode pre-seed
  writes). To change the key distribution, change every copy together: change one alone and the
  producers drive different keyspaces, or the read modes read keys the pre-seed never wrote.
