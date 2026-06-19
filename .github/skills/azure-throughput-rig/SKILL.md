---
name: azure-throughput-rig
description: Operate the real-Azure azure-throughput benchmark rig in benchmark/azure-throughput/. Use when running throughput/scaling experiments against real Azure Storage - provisioning or tearing down the single-silo VM + WAL storage accounts, running a cohort, selecting a workload or scaling arm via BENCH_* env, reading the FINAL active-throughput number, verifying a multi-account WAL spread, driving a rung sweep, or handling auto-shutdown for a long sweep. Covers the single-account and multi-account (1..8 WAL storage accounts) topologies.
---

# Operating the azure-throughput rig

The real-Azure single-silo throughput harness lives in `benchmark/azure-throughput/`.
It is the **only** benchmark in the suite backed by *real* Azure Storage; the local
docker-compose scenarios (`benchmark.ps1 <scenario>`) use Azurite, which collapses
network RTT and does not model Azure Tables partition-server behaviour or throttling.
Reach for this rig when a throughput claim needs real-Azure numbers.

Authoritative deeper docs live next to the scripts: `benchmark/azure-throughput/README.md`
(human onboarding) and `benchmark/azure-throughput/wedge-plan.md` §23.3 (the full `BENCH_*`
knob catalogue). Read them before a first run. This skill is the operational runbook.

> **Availability:** the multi-account WAL fan-out (`deploy.ps1 -WalAccountCount`,
> `BENCH_WAL_ACCOUNTS`, the `[silo] wal-placement` line) and the MV-attached
> `set-point-mv` workload only exist once the materialised-views work has merged to
> `main`. On a checkout without them, use the single-account path only.

## Topology

A single Linux VM runs two systemd units sharing loopback TCP `127.0.0.1:7000`:
`lattice-producer.service` (synthetic fleet emitter) → `lattice-silo.service`
(`ILattice` writes) → Azure Table WAL via the VM's **system-assigned managed identity**
(no keys, no connection strings). Single VM (not ACI) gives deterministic CPU,
accelerated networking, the full `dotnet-*` diagnostic surface, and journald log capture.

## One-time setup

```powershell
az login
# benchmark/azure-throughput/scripts/parameters.local.ps1 (gitignored) holds operator overrides:
#   SubscriptionId, Location (match your Tables-account region), SshPublicKeyPath (deploy.ps1 generates the key if missing),
#   NamePrefix, VmSize, AutoShutdownTime / AutoShutdownTimeZone.
```

## Provision

```powershell
# Single account (default). Provisions VM + storage + RBAC + cloud-init, publishes silo+producer, starts the silo.
./benchmark/azure-throughput/scripts/deploy.ps1 -NamePrefix latperf -VmSize Standard_D8as_v5

# Multi-account: provision N WAL storage accounts up front (1..8). No redeploy needed between account arms.
./benchmark/azure-throughput/scripts/deploy.ps1 -NamePrefix latperf -VmSize Standard_D8as_v5 -WalAccountCount 8
```

`-NamePrefix latperf` makes the resource group `rg-latperf` and the `~/.ssh/config` host
alias `latperf`, so every other script accepts `-NamePrefix latperf`. RBAC
(managed-identity role assignments) can lag 30–60 s after deploy, more across 8 accounts;
if the silo's first WAL write 403s, wait a minute and re-run `update.ps1` to bounce it.

## Inner loop

```powershell
./benchmark/azure-throughput/scripts/update.ps1               # git ls-files | tar | ssh -> dotnet publish on VM -> restart silo
#   flags: -NoBuild (just bounce) | -NoRestart | -Clean (wipe publish*) | -SkipUnitSync
./benchmark/azure-throughput/scripts/vm.ps1 logs              # journalctl -fu lattice-silo
./benchmark/azure-throughput/scripts/vm.ps1 start|stop|status|ssh|refresh-ip
```

## Run a cohort

```powershell
./benchmark/azure-throughput/scripts/run-cohort.ps1 -NamePrefix latperf `
  -Vehicles 400 -TickHz 5 -DurationSec 30 `
  -ExtraSiloEnv @{ BENCH_WORKLOAD_MODE = 'set-point'; BENCH_WAL_ACCOUNTS = '1' }
```

`-ExtraSiloEnv` threads arbitrary env into the silo's systemd drop-in — this is how every
arm is selected **without a redeploy**. `-CaptureCounters` additionally captures
`dotnet-counters`. Each cohort writes three artifacts under `benchmark/.run/azure-throughput/`:
`silo-<cohort>.log`, `producer-<cohort>.log`, `sampler-<cohort>.csv`.

## `BENCH_*` knobs that matter

| Knob | Default | Effect |
|------|---------|--------|
| `BENCH_WORKLOAD_MODE` | `set-many` | The call shape: `set-many`, `set-many-atomic`, `set-many-atomic-2`, `cross-tree-atomic-2`, `cross-tree-atomic-64`, `set-point`, `set-point-mv`, `get-point`, `get-many`. |
| `BENCH_VEHICLE_COUNT`, `BENCH_TICK_HZ` | 4000, 5 | Offered rate (the rung). |
| `BENCH_DURATION_SEC` | 45 | Producer run length. 30 s is enough for a 5 Hz set-point rung. |
| `BENCH_WAL_PARTITIONS` | `LatticeOptions.DefaultWalPartitions` (8) | **Primary write lever.** WAL grain count per tree. |
| `BENCH_WAL_ACCOUNTS` | 1 | How many provisioned accounts to spread partitions across. Needs `deploy.ps1 -WalAccountCount >= N`. |
| `BENCH_RESPONSE_TIMEOUT_SEC` | 30 | Grain-RPC deadline. **Raise to 180 when saturating** or you'll see `grain-rpc-deadline` failures that mimic wedges. `ladder.ps1` pins this to 180. |
| `BENCH_FLUSH_CONCURRENCY` | 8 | Parallel in-flight flushes; pairs with partitions. |
| `BENCH_WAL_MAX_PENDING_BATCHES` | `LatticeOptions.DefaultWalMaxPendingBatches` (16) | Per-`WalShardGrain` pipeline depth. |
| `BENCH_BATCH_SIZE` | 4096 | Entries per `SetManyAsync` (batched modes). |
| `BENCH_TREE_ID` | rotates per cohort | Pin to re-use a WAL partition; otherwise each cohort starts on an empty manifest. |

## Reading results

`run-cohort.ps1` prints a `=== Cohort complete ===` block. The fields that matter:

- `Silo FINAL` → `[silo] FINAL ops=.. failed=.. discarded=.. elapsed=..s active=..s ops/sec (avg)=.. (active avg)=..` — the **active avg** is the honest sustained-ingest number (it excludes the silo's pre-connect idle window and the post-FINAL drain).
- `FINAL active : N entries in Ts active = X/s` — the headline throughput. **If a `(failed=..)` annotation appears, the number is drain-inflated — discard it and treat the rung as wedged.**
- `Steady mean` + `inFlight med/max` — a pinned `med/max=8/8` means the fixed pipeline depth is the parallelism cap.
- `Silo CPU : avg ..% / peak ..%` (of one vCPU) — watch this approach the box core-count under load; ~linear growth with partition count signals silo-side WAL-dispatch CPU as the binding constraint.
- `Diagnostics : stall-watchdog=.. wal-slot=.. wal-append=..` — any non-zero count, especially with a non-zero `failed`, is the wedge evidence triad.
- `Verdict : HEALTHY | DEGRADED | WEDGE | FAILED` — treat anything but `HEALTHY` as above the safe ceiling for that rung.

**Parse the silo log directly** (`benchmark/.run/azure-throughput/silo-<cohort>.log`)
rather than trusting `ladder.ps1`'s CSV verdict column, which is unreliable. Note that
`run-cohort.ps1` uses `Write-Host`, so capturing its output into a variable yields empty
parse results — read the written log files instead.

For a multi-account arm, confirm the spread fired by grepping the silo log for
`[silo] wal-placement` — e.g. `accounts=8 partitions=8 -> 0:default,1:acct1,...,7:acct7`.
Absence of that line (with `BENCH_WAL_ACCOUNTS>1`) or an `ERROR wal-placement-spread`
means the arm ran single-account and must be re-run.

## Driving a sweep

`ladder.ps1` loops `run-cohort.ps1` over rungs, but its verdict parser is unreliable —
prefer a thin custom driver that loops the arms × rungs you need and greps each silo log
for the `FINAL` / `Steady` / `Verdict` / `Silo CPU` lines. Keep `-ErrorActionPreference
Continue` and null-safe parsing so one cohort's parse hiccup doesn't abort the sweep.

## Long sweeps vs auto-shutdown

The Bicep deploys a DevTestLab schedule `shutdown-computevm-<prefix>-vm` that deallocates
the VM at **19:00 UTC daily** (configurable via `AutoShutdownTime` / `AutoShutdownTimeZone`).
A multi-arm sweep can run past that. Disable it for the sweep, then **delete the whole
group at the end regardless**:

```powershell
# Disable auto-shutdown for the duration of a long sweep.
az resource update --resource-group rg-latperf `
  --resource-type Microsoft.DevTestLab/schedules `
  --name shutdown-computevm-latperf-vm --set properties.status=Disabled

# ... run the sweep, capture all logs ...

# Tear down (stops all compute + storage billing).
az group delete --name rg-latperf --yes --no-wait
```

To merely pause between sessions without losing the deployment, `vm.ps1 stop` deallocates
compute (storage + public IP still bill at a few dollars/month).

## Caveats

- The harness measures **end-to-end commit throughput** for a *single silo, single tree*. It is not the partitioned-WAL structural probe (`benchmark/host/Bench.WalAzureTable`).
- Keys are `Guid.ToString("N")` → uniform-random across shards. Edit `Producer/Program.cs` to skew distribution.
- Managed-identity role propagation can take up to ~60 s after `deploy.ps1`; a first-write 403 usually clears by re-running `update.ps1`.
- The write knee is **metastable/bistable** — healthy rungs can bracket wedged rungs. Do not conclude a throughput ceiling from a single healthy run; confirm across repeats.
