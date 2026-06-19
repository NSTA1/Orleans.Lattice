---
name: azure-throughput-rig
description: Operate the real-Azure azure-throughput benchmark rig in benchmark/azure-throughput/. Use when running throughput experiments against real Azure Storage - provisioning or tearing down the single-silo VM and WAL storage accounts, running a cohort, picking a workload (set-many, atomic, cross-tree, set-point, set-point-mv, get-point, get-many), tuning any BENCH_* configuration knob, or reading the cohort result. Documents every workload option, every BENCH_* env var, and every script parameter.
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

All scripts live under `benchmark/azure-throughput/scripts/` and accept `-NamePrefix` to
target a named environment and `-ParametersFile` to point at an explicit parameters file.

---

## Scripts and their parameters

### `parameters.ps1` / `parameters.local.ps1`

`parameters.ps1` holds the committed defaults; copy it to `parameters.local.ps1`
(gitignored) and edit. Every script auto-discovers `parameters.local.ps1`. Fields:

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

To run a second environment side-by-side: `./scripts/deploy.ps1 -NamePrefix lat-exp -VmSize Standard_F8as_v6`.

### `update.ps1` - inner loop (sync -> publish -> restart)

Tars the git-tracked tree, ships it over SSH, runs `dotnet publish` for the silo and
producer on the VM, re-renders the systemd units, and restarts the silo.

| Parameter | Effect |
|-----------|--------|
| `-NoBuild` | Skip rsync + publish; just bounce the silo. |
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

### `ladder.ps1` - rung sweep

Loops `run-cohort.ps1` over a list of rungs and appends one row per rung to a results CSV.

| Parameter | Default | Effect |
|-----------|---------|--------|
| `-Rungs <"V:Hz"[]>` | `1000:5 ... 50000:5` | Array of `vehicles:tickHz` rungs. |
| `-DurationSec <N>` | `30` | Producer seconds per rung. |
| `-CooldownSec <N>` | `5` | Pause between rungs. |
| `-ResponseTimeoutSec <N>` | `180` | Sets `BENCH_RESPONSE_TIMEOUT_SEC` for every rung (drop to 30 to reproduce the grain-RPC-deadline failure mode). |
| `-ExtraSiloEnv @{...}` | `@{}` | Extra silo env forwarded to every cohort. |
| `-DegradeThresholdPct <N>` | `0` | If non-zero, stop the sweep once a rung's throughput drops below `(1 - N/100)` of the best observed (a "find the peak" sweep). |
| `-ResultsCsv <path>` | `scripts/.ladder-results.csv` | Output CSV path. |
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
| `refresh-ip` | Refresh the cached public IP in `~/.ssh/config`. |

---

## Workload options (`BENCH_WORKLOAD_MODE`)

Selects which `ILattice` operation the silo dispatches per producer batch. Case-insensitive
kebab- or concatenated form. Unset/unknown => `set-many`.

| Value | Operation exercised |
|-------|---------------------|
| `set-many` (default) | One `ILattice.SetManyAsync` per producer batch (`BENCH_BATCH_SIZE` entries). The batched write path. |
| `set-many-atomic` | `ILattice.SetManyAtomicAsync`, slicing each producer batch into atomic sagas of `BENCH_ATOMIC_BATCH_SIZE` keys (default 64). Single-tree all-or-nothing writes. |
| `set-many-atomic-2` | Single-tree atomic sagas pinned to **2 keys** each. Fixed-shape partner for the atomic comparison. |
| `cross-tree-atomic-2` | All-or-nothing write across **two** trees (`{treeId}` and `{treeId}-b`), 1 key per tree, via `IGrainFactory.BeginAtomicWrite(...).CommitAsync()`. |
| `cross-tree-atomic-64` | Cross-tree atomic saga of 64 keys (32 per tree). |
| `set-point` | One `ILattice.SetAsync` per key - fan-out point writes. |
| `set-point-mv` | Identical write path to `set-point`, but the silo also attaches an asynchronous materialised view (key-preserving passthrough) over the tree via `AddLatticeViews`. The A/B partner of `set-point` for measuring whether maintaining a view perturbs the source tree's point-write path. |
| `get-point` | One `ILattice.GetAsync` per key - fan-out point reads. Keyspace is pre-seeded at startup via `ILattice.BulkLoadAsync` (size = `BENCH_VEHICLE_COUNT`). |
| `get-many` | `ILattice.GetManyAsync` - batched reads. Keyspace pre-seeded as for `get-point`. |

> The `set-point-mv` workload and the multi-account knobs below only exist on a checkout
> that includes the materialised-views work. On a checkout without it, use the other eight
> modes and the single-account path.

---

## Configuration (`BENCH_*` environment variables)

Pass any of these via `run-cohort.ps1 -ExtraSiloEnv @{ ... }` (silo) - the producer-side
rate vars are set for you by `run-cohort.ps1`'s `-Vehicles` / `-TickHz` / `-DurationSec`.

### Offered load (producer)

| Var | Default | Effect |
|-----|---------|--------|
| `BENCH_VEHICLE_COUNT` | 1000 (cohort sets 4000) | Fleet size = number of distinct keys. Also the read-mode pre-seed size on the silo. |
| `BENCH_TICK_HZ` | 5 | Samples/sec/vehicle. **Offered rate = vehicles x tickHz.** |
| `BENCH_DURATION_SEC` | 300 (cohort sets per `-DurationSec`) | Producer run length; `0` = run forever. |
| `BENCH_SILO_HOST` | `127.0.0.1` | Silo host the producer connects to. |
| `BENCH_SILO_PORT` | 7000 | Silo TCP port the producer connects to. |

### Storage and identity (silo)

| Var | Default | Effect |
|-----|---------|--------|
| `BENCH_STORAGE_URI` | - (required) | `https://{account}.table.core.windows.net` - WAL table endpoint for managed identity. |
| `BENCH_STORAGE_CONN` | - | Connection-string fallback; overrides `BENCH_STORAGE_URI` when set. |
| `BENCH_WAL_TABLE` | `OrleansLatticeWal` | WAL table name. |
| `BENCH_TREE_ID` | rotating `azure-throughput-<utc>` | Tree id. Rotates per silo restart so prior offsets don't bias the run; **pin it to re-use existing rows** (cross-run replay). |
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

### WAL fan-out and pipeline

| Var | Default | Effect |
|-----|---------|--------|
| `BENCH_WAL_PARTITIONS` | `LatticeOptions.DefaultWalPartitions` (8) | WAL grains per tree - the primary write-parallelism lever. Distinct partitions => distinct Azure Tables manifest partitions. |
| `BENCH_WAL_MAX_PENDING_BATCHES` | `LatticeOptions.DefaultWalMaxPendingBatches` (16) | Per-`WalShardGrain` pipeline depth. `1` = strict single-in-flight ordering against the provider. |
| `BENCH_WAL_ACCOUNTS` | 1 | How many provisioned storage accounts the tree's WAL partitions are spread across (index 0 = `BENCH_STORAGE_URI`, 1..N-1 = the extra accounts). Clamped to the number actually provisioned (`deploy.ps1 -WalAccountCount`). |
| `BENCH_WAL_EXTRA_ACCOUNT_URIS` | - (set by `update.ps1`) | `;`-delimited list of extra account table endpoints, wired as keyed WAL providers `acct1, acct2, ...`. Normally you don't set this by hand - `deploy.ps1 -WalAccountCount` + `update.ps1` populate it. |
| `BENCH_PIPELINE_PHASE2` | on | Overlap phase 2 of batch N with phases 0+1 of batch N+1 on the same shard. `0` disables. |
| `BENCH_WAL_PHASE2_COALESCING_WINDOW_MS` | 5 | How long the per-shard PhaseTwoWorker waits after the first arrival so additional commits coalesce into one Azure Tables transaction. `0` = drain on first signal. |
| `BENCH_WAL_PHASE2_COMMIT_TIMEOUT_SEC` | library default (3) | Per-commit deadline for the PhaseTwoWorker's manifest commit. `0` = unbounded; `>0` = finite deadline (a hung commit becomes a bounded timeout the resync path recovers). |
| `BENCH_DIGEST_COALESCING_WINDOW_MS` | 5 | Coalescing window (ms) for digest writes. |
| `BENCH_WAL_ELIMINATE_CANDIDATE_ROW` | library default | Toggle the hot-path candidate-row elimination optimisation. |
| `BENCH_SHARD_COUNT` | 0 (library default, 64) | Override the tree's physical shard count via `ILattice.ReshardAsync` at startup (grow-only against a populated tree; any target works against an empty tree). |

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
| `BENCH_SATURATION_SAMPLE_MS` | `LatticeOptions.DefaultWalSaturationSampleInterval` (200) | WAL saturation sampler tick (ms). `0` disables the sampler (signal pins to Healthy; TCP-read gating becomes a no-op). |
| `BENCH_SATURATION_THROTTLED_RATIO` | `LatticeOptions.DefaultWalSaturationThrottledRatio` (0.75) | Admission-depth ratio at/above which the tree raises Throttled. Range [0.0, 1.0]; lower = earlier throttle. |
| `BENCH_SATURATION_DISPATCH_TIMEOUT_THRESHOLD` | `LatticeOptions.DefaultWalSaturationDispatchTimeoutThreshold` (1) | Min dispatch-timeout trips per window that raise Saturated regardless of depth. |
| `BENCH_THROTTLED_LINE_DELAY_MICROS` | library default (1000 = 1 ms) | Per-line delay applied while Throttled, slowing the TCP reader so the producer's socket blocks and the admission gate drains. `0` = no delay. |
| `BENCH_WAL_APPEND_DISPATCH_TIMEOUT_SEC` | `LatticeOptions.DefaultWalAppendDispatchTimeout` | WAL append dispatch timeout override. |

### Lifecycle, timeouts, reporting

| Var | Default | Effect |
|-----|---------|--------|
| `BENCH_RESPONSE_TIMEOUT_SEC` | 30 | Orleans Silo + Client `ResponseTimeout` (s). **Raise to 180 when saturating** so a slow worst-partition flush doesn't trip the deadline and trigger a producer reconnect/retransmit storm. `ladder.ps1` pins this to 180. |
| `BENCH_TOTAL_DURATION_SEC` | 600 | Server-side watchdog: after this many seconds the silo triggers a graceful shutdown even if the cohort runner died. `0` disables. |
| `BENCH_REPORT_SEC` | 1 | stdout `ops/sec` report interval (s). |
| `BENCH_PHASEA_REPORT_SEC` | 10 | Cadence (s) of the Phase A latency-attribution `[phaseA]` diagnostic lines (p50/p90/p99 per instrument/tree/shard/phase). `0` disables. |
| `BENCH_DISABLE_STORAGE_USAGE_POLLER` | empty | Set to `1` to disable the storage-usage poller for the cohort (`StorageUsagePollInterval = 0`). |

---

## Reading a cohort result

`run-cohort.ps1` prints a `=== Cohort complete ===` summary and writes the silo journal to
`benchmark/.run/azure-throughput/silo-<cohort>.log`. Parse the **log file** directly
(`run-cohort.ps1` writes its summary with `Write-Host`, so capturing the script's stdout
into a variable yields nothing).

Key lines in the silo log:

- `[silo] FINAL ops=.. failed=.. discarded=.. elapsed=..s active=..s ops/sec (avg)=.. (active avg)=..`
  - the **active avg** is the sustained-ingest rate over the active window (it excludes the
  pre-connect idle window and the post-FINAL drain). `failed` / `discarded` count
  unsuccessful operations.
- `Verdict : HEALTHY | DEGRADED | WEDGE | FAILED` - the run-cohort classification.
- `Silo CPU : avg ..% / peak ..%` (of one vCPU) - from the sampler.
- `Diagnostics : stall-watchdog=.. wal-slot=.. wal-append=..` - internal back-pressure/stall counters.
- `[silo] wal-placement accounts=N partitions=M -> 0:default,1:acct1,...` - emitted when
  `BENCH_WAL_ACCOUNTS > 1`; confirms which account each WAL partition landed on. Its
  absence (with accounts >1) or an `ERROR wal-placement-spread` means the arm ran
  single-account.

`ladder.ps1` additionally writes one CSV row per rung (`written`, `failed`, active-avg,
CPU peak, RSS, verdict, timestamp) to its `-ResultsCsv`.

---

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
- Keys are `Guid.ToString("N")`, uniform-random across shards. Edit `Producer/Program.cs`
  to change the key distribution.
