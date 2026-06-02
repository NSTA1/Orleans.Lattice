# Azure throughput benchmark (real Azure Storage)

A two-container Azure Container Instances deployment that measures **Entries written per
second** when a single-silo Orleans.Lattice host backed by a real Azure Storage account is
fed a sustained stream of synthetic vehicle telemetry. The tree is configured with
`AzureTableWalStorageProvider` so every commit produces real WAL traffic against Azure
Tables.

This is the only benchmark in the suite that runs against **real Azure Storage** rather
than Azurite or in-memory storage. The local docker-compose scenarios (`benchmark.ps1
<scenario>`) are reproducible but Azurite collapses network RTT and does not model Azure
Tables partition-server behaviour or throttling. Use this harness when a throughput claim
needs to be backed by real-Azure numbers (for example, before/after measurements for a
WAL-path optimisation).

## Topology

```
+---------------------------+   loopback TCP   +-----------------------------+
| azure-throughput-producer | ---------------> | azure-throughput-silo       |
| (synthetic fleet emitter) |   127.0.0.1:7000 |  ILattice.SetManyAsync ->   |
|                           |                  |  AzureTableWalStorage       |
+---------------------------+                  +--------------+--------------+
                                                              |
                                                              v
                                              +-----------------------------+
                                              | Azure Storage Account       |
                                              | (Tables, managed identity)  |
                                              +-----------------------------+
```

Both containers run in the same ACI container group so they share a network namespace -
the producer's TCP connection to `127.0.0.1:7000` is a kernel loopback hop, not a network
round-trip.

The silo's cluster grain state is in-memory (`UseLocalhostClustering` +
`AddMemoryGrainStorageAsDefault`); only the WAL is durable. That keeps the harness focused
on Azure Tables WAL throughput rather than on Orleans clustering overhead.

## Files

| Path | Purpose |
|------|---------|
| `Producer/Program.cs` | Generates `VehicleTelemetryEvent` records and writes JSON lines over TCP. |
| `Producer/Dockerfile` | Producer image; build context is the repo root. |
| `Silo/Program.cs` | Single-silo lattice host; TCP listener -> `ILattice.SetManyAsync`. |
| `Silo/Dockerfile` | Silo image; build context is the repo root. |
| `scripts/00-login.ps1` | `az login --use-device-code` and active-subscription printout. |
| `scripts/10-provision.ps1` | Creates RG, ACR, storage account, user-assigned identity, role assignment. |
| `scripts/20-build-and-deploy.ps1` | `az acr build` both images, then ACI deploy via YAML. |
| `scripts/30-tail-logs.ps1` | `az container logs --follow` for the silo (or producer). |
| `scripts/90-teardown.ps1` | Deletes the entire resource group. |

## Usage

```powershell
# Phase 1: log in.
./benchmark/azure-throughput/scripts/00-login.ps1

# Phase 2: stand up infra. Idempotent.
#  - If $env:BENCH_PREFIX is set, it is used.
#  - Else if benchmark/azure-throughput/scripts/.prefix exists, that value is used.
#  - Else a random 'lat{7-hex-chars}' prefix is generated and persisted to
#    .prefix (gitignored) so every subsequent script call on this machine
#    reuses the same resource names. You can override at any time by passing
#    -Prefix or setting $env:BENCH_PREFIX before invoking the script.
./benchmark/azure-throughput/scripts/10-provision.ps1

# Phase 3: build images, deploy, run for 2 minutes, capture logs locally.
$env:BENCH_VEHICLE_COUNT = '2000'   # optional, default 1000
./benchmark/azure-throughput/scripts/20-build-and-deploy.ps1
# -> waits up to BENCH_TOTAL_DURATION_SEC (default 120s) for the run to complete,
#    force-stops the container group, then writes silo+producer logs to
#    benchmark/azure-throughput/.run/silo-{utc}.log (and prints the [silo] FINAL line).

# Phase 4 (optional): tail the live logs from another shell while phase 3 is waiting.
./benchmark/azure-throughput/scripts/30-tail-logs.ps1

# Phase 5: cleanup when done.
./benchmark/azure-throughput/scripts/90-teardown.ps1
```

By default `20-build-and-deploy.ps1` blocks until either both containers report
`Terminated` or the wall-clock deadline elapses (`BENCH_TOTAL_DURATION_SEC`, default
`120` seconds). When the deadline is reached the script issues `az container stop` so the
container group is not left running. Pass `-NoWait` to opt out of the bounded-wait
behaviour (the legacy fire-and-forget shape; the caller is then responsible for stopping
the group).

## Consuming the results

The canonical result artefact is the **silo container log**. After every bounded run,
`20-build-and-deploy.ps1` writes it to:

- `benchmark/azure-throughput/.run/silo-{utc}.log` - the full silo stdout (one
  `[silo] t= ... Entries written per second=...` line per second, plus a single
  `[silo] FINAL written=... elapsed=...s Entries written per second (avg)=...` line
  emitted on graceful shutdown);
- `benchmark/azure-throughput/.run/producer-{utc}.log` - the producer's own per-second
  rate, so a wedged producer can be distinguished from a wedged silo.

The `[silo] FINAL` line is the headline scalar. The script prints it to stdout at the end
of the run; an automated runner can grep the saved log file for the same line:

```powershell
$rate = (Get-Content .\benchmark\azure-throughput\.run\silo-20260524-101530Z.log |
         Select-String '^\[silo\] FINAL' | Select-Object -Last 1).Line
# -> '[silo] FINAL written=12,360,000 failed=0 elapsed=120.0s Entries written per second (avg)=103,000'
```

The per-second rate samples (`Entries written per second=...`) are also in the same file,
so steady-state min/avg/max can be computed by an agent without going back to Azure. The
`scripts/40-ladder.ps1` sweep parses those same lines and writes a per-rung CSV.

## What to read out of the logs

The silo emits one line per second on stdout. The relevant column is the trailing rate:

```
[silo] t=   12.0s written=     483,200 Entries written per second=    41,000
[silo] t=   13.0s written=     524,400 Entries written per second=    41,200
...
[silo] FINAL written= 12,360,000 elapsed=300.0s Entries written per second (avg)= 41,200
```

The `FINAL` line emitted on graceful shutdown is the headline result.

The producer container emits its own rate so a wedged producer can be distinguished from a
wedged silo:

```
[producer] t=   12.0s sent=     500,000 rate=    41,667 msg/s
```

When `producer rate` >> `silo Entries written per second`, the silo is the bottleneck
(expected); when they match, the producer is saturated.

## Configuration knobs

| Env var | Container | Default | Purpose |
|---------|-----------|---------|---------|
| `BENCH_PREFIX` | scripts | auto-generated | 3-10 char lowercase prefix for resource names. If unset, `10-provision.ps1` generates `lat{7-hex}` and persists it to `benchmark/azure-throughput/scripts/.prefix` (gitignored) so every subsequent script invocation on this machine reuses the same names. |
| `BENCH_LOCATION` | scripts | `westeurope` | Azure region. |
| `BENCH_VEHICLE_COUNT` | producer | `1000` | Synthetic fleet size. |
| `BENCH_TICK_HZ` | producer | `5` | Per-vehicle samples per second. |
| `BENCH_DURATION_SEC` | producer | `120` | Run duration; producer then closes the socket. |
| `BENCH_TOTAL_DURATION_SEC` | scripts | `120` | Hard wall-clock ceiling for `20-build-and-deploy.ps1`. Container group is `az container stop`'d at this deadline regardless of producer/silo state. |
| `BENCH_BATCH_SIZE` | silo | `4096` | `SetManyAsync` batch size. |
| `BENCH_FLUSH_MS` | silo | `50` | Max flush latency. |
| `BENCH_FLUSH_CONCURRENCY` | silo | `8` | Max in-flight `SetManyAsync` calls. |
| `BENCH_WAL_PARTITIONS` | silo + scripts | `8` | WAL partitions per tree. Honoured by both `20-build-and-deploy.ps1` (passed through to the ACI YAML) and `Silo/Program.cs` (read at startup). Set to `1` for a single-partition arm of an A/B run. |
| `BENCH_WAL_MAX_PENDING_BATCHES` | silo + scripts | `8` | Per-WalShardGrain pipeline depth. Honoured by both the deploy script and the silo. |
| `BENCH_PIPELINE_PHASE2` | silo | library default | Overlap phase-2 commit with the next batch (`AzureTableWalStorageOptions.PipelinePhaseTwoCommits`). Unset inherits `AzureTableWalStorageOptions.DefaultPipelinePhaseTwoCommits` (on). |
| `BENCH_WAL_ELIMINATE_CANDIDATE_ROW` | silo + scripts | library default | Elide the phase-0 candidate-row write (`AzureTableWalStorageOptions.EliminateCandidateRowOnHotPath`). Unset inherits `AzureTableWalStorageOptions.DefaultEliminateCandidateRowOnHotPath` (on). Set to `false` for the legacy inline-C-row arm of an A/B run. |
| `BENCH_TREE_ID` | silo | `azure-throughput-{utc}` | Pin to re-use an existing WAL partition; default rotates per run. |
| `BENCH_SHARD_COUNT` | silo | `0` | Override the tree's shard count via `ILattice.ReshardAsync` at startup (`0` = library default). |
| `BENCH_REPORT_SEC` | silo | `1` | Stdout report cadence. |

## A/B-ing a WAL optimisation

```powershell
# Legacy arm (inline C-row).
$env:BENCH_TREE_ID = 'azure-throughput-baseline'
$env:BENCH_WAL_ELIMINATE_CANDIDATE_ROW = 'false'
./benchmark/azure-throughput/scripts/20-build-and-deploy.ps1
./benchmark/azure-throughput/scripts/30-tail-logs.ps1  # capture FINAL line

# Default arm (C-row elided).
$env:BENCH_TREE_ID = 'azure-throughput-optimised'
$env:BENCH_WAL_ELIMINATE_CANDIDATE_ROW = 'true'
./benchmark/azure-throughput/scripts/20-build-and-deploy.ps1
./benchmark/azure-throughput/scripts/30-tail-logs.ps1  # capture FINAL line
```

Keep `BENCH_VEHICLE_COUNT`, `BENCH_TICK_HZ`, and `BENCH_DURATION_SEC` identical between
arms so the only changed variable is the option under test. `BENCH_TREE_ID` is rotated
automatically by `20-build-and-deploy.ps1` (default `azure-throughput-{utc}` per run) so
every run starts against an empty manifest partition and the first ~10s of throughput
samples are not biased by replay of a previous run's WAL. Setting `BENCH_TREE_ID`
explicitly per arm (as above) is still useful for tagging - the value appears in the
silo's startup log and in the WAL partition key - but is not required for cohort
correctness. The only reason to pin a stable id across runs is to **deliberately** measure
recovery cost against a populated WAL.

## Caveats

- The harness measures **end-to-end commit throughput** with a single silo and a single
  lattice tree. It is not a proxy for the partitioned-WAL benchmark
  (`benchmark/host/Bench.WalAzureTable`), which is a structural correctness probe.
- ACR `Basic` SKU and a single ACI container group are sized for a single-shot run.
  Re-running 20-build-and-deploy is idempotent (the container group is recreated).
- Managed identity propagation can take ~30s after `10-provision.ps1` finishes. If the
  silo's first WAL write fails with a 403, wait and re-run 20-build-and-deploy.
- The tree's keys are `Guid.ToString("N")` so the workload is uniform-random across
  shards. To skew distribution, edit `Producer/Program.cs`.
