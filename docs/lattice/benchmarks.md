# Lattice Benchmarks

The Orleans.Lattice benchmark suite is the project's regression alarm. Every
scenario is a fixed, reproducible workload that produces a small set of
summary scalars (commit p99, commits/s, ship/apply latency, cache hit ratio,
GC pressure, microbench timings) which are pushed into a long-lived history
stack and rendered as run-over-run trend lines in Grafana. A regression
caused by a refactor, a dependency bump, or a tuning change shows up as a
visible step in the trend line and the offending commit (`git_sha` is part
of every sample's labels) is one click away.

This document is the user-facing entry point. The authoritative deep-detail
reference - topology diagrams, every scenario knob, history-stack data model,
auto-discovery internals - lives in [`benchmark/README.md`](../../benchmark/README.md).

## Load generation: the vehicle fleet simulator

All docker-compose scenarios drive load through the
[Vehicle Fleet Simulator](../../samples/VehicleFleetSimulator/) - a
standalone Orleans application that simulates a population of vehicles
streaming structured telemetry events (position, speed, fuel level, route
progress) as each vehicle advances along a city graph. The benchmark stack
imports the simulator's HTTP API verbatim and points its telemetry sink at
the lattice under test, so the lattice receives per-vehicle writes at the
rate the simulator emits ticks.

A **fleet** is that simulated population. **Fleet size**
(`BENCH_FLEET_SIZE`) is the number of vehicles seeded; each vehicle becomes
one independent source of per-tick writes, so doubling the fleet size
roughly doubles the offered write load. The `fleetStats` block in every
`results.json` (see [Interpreting results](#interpreting-results) below)
reports the fleet's final state - `total`, `driving`, `idle`, and any
errored vehicles.

The `microbench` scenario does **not** drive a fleet. It bypasses the
simulator and the Orleans cluster entirely and exercises the `ILattice`
vertical in-process; the fleet-size calibration step below is consequently
skipped for it.

## Prerequisites

| Tier | Requires |
|---|---|
| `microbench` (in-process BDN, no cluster) | .NET 10 SDK, PowerShell 7+ |
| Every other scenario (docker-compose) | Docker Desktop (or any Compose v2-compatible daemon) + a successful run of `./initialise.ps1` to calibrate fleet size for this host |

`./initialise.ps1` measures the fleet size that stresses this host without
saturating it and writes the result to the gitignored
`benchmark/.fleet-size.config`. The microbench scenario does not drive a
fleet, so the calibration check is skipped for it; every docker-compose
scenario exits early with a friendly pointer at `./initialise.ps1` if the
config is missing. See the
[Calibrating fleet size for this host](../../benchmark/README.md#calibrating-fleet-size-for-this-host)
section of the benchmark stack README for the full calibration procedure.

## Running a benchmark

Every scenario is driven by the same entry point:

```powershell
# From the benchmark/ directory.
./benchmark.ps1 <scenario>
```

The script reads `scenarios/<slug>.env`, picks the right docker-compose
overlay (single-cluster or replication), brings the stack up, seeds the
fleet, runs the warmup + measurement window, captures an auto-discovered
panel of summary scalars into `.run/<scenario>/<run_id>/results.json`, and
opportunistically pushes the same scalars to the long-lived history stack
on `:8428` if it is reachable.

A few commonly-needed variants:

```powershell
# Keep the docker stack up after the run so Grafana stays accessible.
./benchmark.ps1 -Scenario current-state-single-peer -KeepRunning

# One-off run at a non-calibrated fleet size (skip the config-existence check).
./benchmark.ps1 -Scenario current-state-no-replication -FleetSizeOverride 4000 -SkipFleetSizeCheck

# Bring up the long-lived history stack (one-shot; stays up across many runs).
./benchmark.ps1 -OpenHistory

# Backfill prior local runs into the history stack.
./benchmark.ps1 -ImportHistory
```

## Scenarios

The suite ships **eighteen** scenarios spanning seven lattice-usage profiles
plus a micro-benchmark control. A condensed table is reproduced below; the
authoritative list with per-scenario knobs is in
[`benchmark/benchmark-scenarios.md`](../../benchmark/benchmark-scenarios.md).

| Profile             | Example scenario id                          | What it stresses                                          |
|---------------------|----------------------------------------------|-----------------------------------------------------------|
| Micro               | `microbench`                                 | `ILattice` algorithm cost, no Orleans dispatch            |
| Write-heavy random  | `current-state-no-replication`               | Steady-state per-vehicle current-state overwrites         |
| Write-heavy random  | `skewed-key-shard-splits`                    | Adaptive shard splitting under skewed keys                |
| Write-heavy ordered | `event-log-with-ttl`                         | Append-only event-log keyspace + TTL eviction             |
| Read-heavy          | `read-heavy-random`                          | 95:5 read:write, random key distribution                  |
| Read-heavy          | `read-heavy-ordered`                         | 95:5 read:write, sequential `ScanKeysAsync` walk          |
| Read-write mix      | `read-write-mix-random`                      | 50:50 mix, random keys (YCSB-A shape)                     |
| Read-write mix      | `read-write-mix-ordered`                     | 50:50 mix, sequential `ScanKeysAsync` walk                |
| Durable WAL         | `current-state-no-replication-azuretable`    | Same write topology with Azure Table WAL durable storage  |
| Atomic writes       | `atomic-write`                               | Sustained `SetManyAtomicAsync` saga throughput            |
| Atomic writes       | `atomic-write-replication`                   | Two-cluster bidirectional atomic-saga visibility          |
| Replication         | `current-state-single-peer`                  | Current-state tree, single-peer replication               |
| Replication         | `bidirectional-replication`                  | Two-cluster bidirectional replication                     |
| Replication         | `bidirectional-replication-azuretable`       | Two-cluster bidirectional replication with Azure Table WAL |
| Replication chaos   | `replication-backpressure`                   | Backpressure / catch-up under sender pause                |
| Replication chaos   | `receiver-crash`                             | Receiver crash mid-stream, recovery cost                  |
| Replication control | `observer-no-peer`                           | Observer-off control paired with `current-state-single-peer` |
| Replication control | `replication-key-filter`                     | Per-key replication filter cost vs no-filter baseline      |

## Interpreting results

Every run writes a `results.json` like:

```json
{
  "scenario": "current-state-no-replication",
  "run_id":   "2026-04-30T14-08-41Z",
  "git_sha":  "abc1234",
  "started":  "2026-04-30T14:03:11Z",
  "ended":    "2026-04-30T14:08:41Z",
  "duration_s": 330,
  "config":  { "BENCH_TELEMETRY_SINK": "lattice", "BENCH_FLEET_SIZE": "2000", "...": "..." },
  "metrics": {
    "lattice_commit_p99_ms":              12.3,
    "lattice_commits_per_second":         19847,
    "sink_published_per_second":          2034,
    "sink_dropped_combined_increase":     0,
    "lattice_cache_hit_ratio":            0.94,
    "replication_ship_p95_ms":            4.7,
    "replication_apply_lag_p95_ms":       6.1,
    "...": "~52 auto-discovered keys + curated extras"
  },
  "fleetStats": { "total": 2000, "driving": 2000, "...": "..." }
}
```

The metrics panel is derived from Prometheus's `/api/v1/metadata` endpoint at
capture time, not hard-coded. Adding a new instrument to the lattice source
automatically flows into the next benchmark run; the synthesised key shape
per instrument type is documented in the
[Auto-discovery section](../../benchmark/README.md#auto-discovery-of-metrics)
of the benchmark stack README.

### Cross-run comparison

```powershell
# Latest run per scenario, side-by-side, with delta vs. a reference scenario.
./benchmark.ps1 -Compare -CompareAgainst current-state-no-replication

# Without the delta column.
./benchmark.ps1 -Compare
```

Outputs land in `.run/comparison.md` (markdown, ready to paste into a PR) and
`.run/comparison.csv` (flat for spreadsheet use).

### Trend dashboard

For run-over-run trend visualisation across commits, bring up the history
stack:

```powershell
./benchmark.ps1 -OpenHistory
# ... run scenarios as normal, they auto-push when this is up ...
./benchmark.ps1 -CloseHistory
```

Then visit <http://localhost:3001>. The history Grafana hosts an Overview
dashboard plus seven persona dashboards (one per lattice-usage profile) so
each dashboard answers a single regression question without templating-var
juggling. See the
[Trend dashboard section](../../benchmark/README.md#trend-dashboard-history-stack)
of the benchmark stack README for the full dashboard catalogue.

## The `microbench` scenario

`microbench` is the in-process tier - no Docker, no Orleans cluster boot. It
hand-instantiates the `LatticeGrain` -> `ShardRootGrain` -> `BPlusLeafGrain`
vertical and routes `IGrainFactory` calls through NSubstitute mocks, then
exercises a fixed set of `[Benchmark]` methods (point reads/writes, bulk
loads, mixed workloads, atomic-write sagas) via
[BenchmarkDotNet](https://benchmarkdotnet.org/) with the `InProcessEmitToolchain`.

```powershell
./benchmark.ps1 microbench
```

Two CLI knobs scope each run:

| Knob | Values | Effect |
|---|---|---|
| `-Workloads` | Comma-separated BDN `--filter` globs, empty = full suite | E.g. `'*.PointWrite,*.Mixed_70R_30W'` |
| `-Fidelity` | `dry` (default for optimisation work) \| `quick` \| `full` | Iteration count + toolchain (in-process for `dry`/`quick`, forking for `full`) |

Both knobs also accept env-var equivalents (`BENCH_MICROBENCH_WORKLOADS`,
`BENCH_MICROBENCH_FIDELITY`); the CLI flag wins when both are set. The
committed defaults are `BENCH_MICROBENCH_FIDELITY=quick` and an empty
workload filter (full suite), in `benchmark/scenarios/microbench.env`.

The available workload method names are listed by running
`./benchmark.ps1 microbench -Workloads '*'` and reading the BDN summary
table. The suite currently ships 24 `[Benchmark]` methods covering point
reads / writes (`PointRead`, `PointWrite`, `PointReadWithVersion`,
`PointExists`), multi-key reads (`PointGetMany`, `PointGetMany_BatchSize`
parameterised over batch sizes 1-64), bulk and multi-key writes
(`BulkLoad`, `SetMany_4Shards`, `Mixed_70R_30W`), key/range scans
(`KeyScan_PageOver4Shards`), deep- and deeper-tree variants of the point
and bulk paths, atomic-write sagas (`SetManyAtomic`, `SetManyAtomic_4Shards`,
`SetManyAtomic_Concurrent` parameterised over concurrency 1-64),
atomic-tree reads (`PointRead_AtomicTreeIdle`,
`PointRead_AtomicTreeWithActiveSaga`), WAL-encoder microbenchmarks
(`WalEncodeBatch_AzureTable`), and the replication ship-envelope
microbenchmarks (`ShipTypedEnvelope`, `ShipFramingOnly`).

### Per-method allocation and CPU profiling

When BDN's `MemoryDiagnoser` says a workload allocates 312 B/op but cannot
say *which call sites contributed those bytes*, the microbench harness can
attach an EventPipe-driven per-method profiler. It dumps managed-allocation
and CPU-sample events for the duration of every `[Benchmark]` method into a
`profile.json` sidecar alongside the run's `results.json`, attributing each
event to the deepest named managed stack frame.

Activate via the `-Profile` parameter on `benchmark.ps1`:

```powershell
# Per-method allocation attribution for the Mixed_70R_30W workload:
./benchmark.ps1 microbench -Workloads '*.Mixed_70R_30W' -Fidelity dry -Profile alloc

# CPU samples:
./benchmark.ps1 microbench -Workloads '*.Mixed_70R_30W' -Fidelity dry -Profile cpu

# Both:
./benchmark.ps1 microbench -Workloads '*.Mixed_70R_30W' -Fidelity dry -Profile both
```

Or set `BENCH_MICROBENCH_PROFILE` directly (see
[`benchmark/scenarios/microbench.env`](../../benchmark/scenarios/microbench.env)).

#### Output shape

`profile.json` example (truncated):

```json
{
  "run_id": "2026-05-12T13-46-22Z",
  "git_sha": "e04b8cf",
  "captured_at": "2026-05-12T13:46:52.4321Z",
  "mode": "alloc",
  "duration_ms": 28412,
  "total_allocations_b": 12345678,
  "total_cpu_samples": 0,
  "top_allocators": [
    {
      "method": "Orleans.Lattice.BPlusTree.Grains.LatticeGrain.SetAsync",
      "module": "Orleans.Lattice",
      "alloc_b": 4194304,
      "alloc_pct": 34.0,
      "samples": 0,
      "samples_pct": 0.0
    }
  ],
  "top_cpu": []
}
```

`top_allocators` is sorted descending by `alloc_b`. `top_cpu` is sorted
descending by `samples`. Both lists are bounded by
`BENCH_MICROBENCH_PROFILE_TOPN` (default 50). Unused lists for the requested
mode (`top_cpu` under `-Profile alloc`, `top_allocators` under `-Profile cpu`)
are emitted as empty arrays so consumers can rely on a stable schema.

#### Caveats

- **Profile runs perturb measurements.** The EventPipe session adds per-event
  stack-walking inside the runtime. A profile-enabled run's `results.json`
  is NOT a valid cohort baseline; only the `profile.json` attribution table is.
  The optimisation workflow treats profile-enabled runs as a one-shot
  diagnostic, not as cohort samples - see the optimisation agent's per-method
  profiling section for the recommended flow.
- **`-Fidelity full` is incompatible** with profiling. That fidelity uses BDN's
  forking toolchain, which spawns one child process per `[Benchmark]`. The
  parent's EventPipe session does not see the child's workload activity, so
  the harness refuses to start the profiler in that mode and writes a warning
  to stderr. Use `-Fidelity dry` or `-Fidelity quick` (both use the in-process
  toolchain).
- **Attribution is to the deepest named managed frame**, not the leaf
  allocator. This surfaces lattice-level callsites instead of generic
  `System.Buffers.ArrayPool` / `System.Threading.Tasks` framework frames.
- **Pre-seed allocations are excluded** by design. The profiler starts at the
  end of `[GlobalSetup]`, after the multi-thousand pre-seed writes complete,
  so the top-N table reflects in-loop benchmark allocations only.

#### Optional raw .nettrace sidecar

Set `BENCH_MICROBENCH_PROFILE_NETTRACE_PATH` to a filesystem path to also emit
the raw `.nettrace` blob alongside the aggregated `profile.json`. Useful for
post-mortem inspection in [PerfView](https://github.com/microsoft/perfview) or
[dotnet-trace](https://learn.microsoft.com/dotnet/core/diagnostics/dotnet-trace).
When the variable is unset, the raw blob is written to a temp file and deleted
on session stop.

## The `azure-throughput` harness (real Azure Tables)

`azure-throughput` is the out-of-band tier for measuring sustained
write-throughput against a **real Azure Storage account** rather than
Azurite or the in-memory WAL. The local docker-compose scenarios are
reproducible and cheap, but Azurite collapses network RTT and does not
model Azure Tables partition-server behaviour or throttling - so any
throughput claim that needs to back a public number, or any WAL hot-
path optimisation that needs realistic Azure-side latency, runs here.

The harness deploys a single Linux VM (Standard_D4as_v5 by default,
with accelerated networking) into Azure. The producer and silo run
as co-located systemd units; the silo authenticates to a real Azure
Tables WAL via the VM's system-assigned managed identity. A cohort
runner script applies env-var drop-ins, restarts the silo, runs the
producer for the configured duration, then collects the silo and
producer journals plus a per-second VM-level CPU/RSS sampler CSV
under `benchmark/.run/azure-throughput/`.

Entry points:

```powershell
# One-time provision (Bicep + cloud-init + first publish).
./benchmark/azure-throughput/scripts/deploy.ps1

# Inner-loop sync + publish + silo restart on the existing VM.
./benchmark/azure-throughput/scripts/update.ps1

# Single cohort at the default 4,000 vehicles / 5 Hz / 45 s rung.
./benchmark/azure-throughput/scripts/run-cohort.ps1

# Rung sweep across multiple offered-load points.
./benchmark/azure-throughput/scripts/ladder.ps1 -Rungs '4000:5','6000:5','8000:5'

# Deallocate the VM when finished (no compute charges; storage + PIP idle).
./benchmark/azure-throughput/scripts/vm.ps1 stop
```

The harness is **not** driven through `./benchmark.ps1` and does not
push to the local history VictoriaMetrics stack - the result is the
`[silo] FINAL written=... failed=... elapsed=...` line in the silo
journal plus the headline summary block `run-cohort.ps1` prints to
stdout. Cohort sampling methodology, the full `BENCH_*` saturation-
knobs catalogue, the A/B procedure for WAL optimisations, the VM-SKU
sizing rule, and the cost / auto-shutdown story all live in
[`benchmark/azure-throughput/README.md`](../../benchmark/azure-throughput/README.md)
so they are not duplicated here. The empirical WAL-side findings that
inform the current shipping defaults (and the storage-account-
throughput envelope above which raising `WalMaxPendingBatches`
stops helping) live in
[WAL Tuning](wal-tuning.md).

## Where to go next

- [`benchmark/README.md`](../../benchmark/README.md) - topology, calibration,
  every scenario knob, auto-discovery internals, history-stack data model.
- [`benchmark/benchmark-scenarios.md`](../../benchmark/benchmark-scenarios.md) -
  authoritative scenario plan with every knob enumerated.
- [`benchmark/history/README.md`](../../benchmark/history/README.md) - long-lived
  trend-dashboard stack, label schema, ad-hoc PromQL query path.
- [`benchmark/azure-throughput/README.md`](../../benchmark/azure-throughput/README.md) -
  real-Azure-Tables single-VM harness: topology, knobs, A/B procedure,
  auto-shutdown safety net.
- [`docs/lattice/wal-tuning.md`](wal-tuning.md) - how `WalMaxPendingBatches`
  and `WalPartitions` interact with a durable backend's throughput envelope.
- [`docs/lattice/metrics.md`](metrics.md) - what every `orleans.lattice.*`
  meter measures and what regression each tile catches.