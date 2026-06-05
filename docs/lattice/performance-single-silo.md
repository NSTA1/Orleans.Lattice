# Performance: single-silo guide

This document is an **approximate guide** to the performance you can expect
from Orleans.Lattice on a **single silo** under steady-state load. The
numbers come from two complementary benchmark surfaces - the algorithmic
ceiling of each `ILattice` method in isolation (Layer 1) and the sustained
throughput a real silo delivers against real Azure Tables under a realistic
offered load (Layer 2) - and both are regenerated together against a
freshly-provisioned Azure VM by `benchmark/performance-report.ps1`.

The figures are **steady-state averages** taken from the productive window
of each run, with drain tails excluded. Cold starts, JIT warm-up, grain
activation storms, and bursty offered load can all introduce variance,
sometimes dramatically: a 10x latency spike on the first call to a freshly
activated grain is normal, as is a multi-second pause during a hot-shard
split or a tree-registry leader handover. The headline cells should be read
as "what the silo settles into once the cluster is warm", not as "what
every individual call will look like".

The horizontal-scaling story (multi-silo deployments, where work fans out
across activations on multiple hosts) is **out of scope for this document**
and will be covered separately once the multi-silo benchmark axis is built
out. Today's numbers are the single-silo ceiling; multi-silo throughput
will be a multiple of these figures, bounded primarily by the storage
provider's per-account budget.

You should **measure your own workload**. The shapes here cover point reads,
point writes, batched multi-key reads and writes, and atomic multi-key
sagas against a single Azure Tables Standard account; the per-cell
provenance (host SKU, region, .NET version, WAL options, BDN fidelity,
rung, cohort N, measurement date) is recorded in the meta-header of each
table's marker block and is mechanically refreshed on every regeneration.
If your keys are larger, your fan-out is different, your hot-key
distribution is skewed, or your durability requirements differ, your
numbers will differ too. The benchmark harness ships with the repository
and is easy to re-run against your own subscription - see
[Benchmarks](benchmarks.md) for the runbook and the per-layer
"How it was run" sections below for the methodology.

## Layer 1 - In-process microbench (algorithmic ceiling)

**How it was run.** Layer 1 measures the cost of one call to each
`ILattice` method when scheduling and durable storage are out of the
picture: a `BenchmarkDotNet` harness instantiates the grain layer in-process
against an in-memory storage provider, runs each operation under BDN's
in-process toolchain, and reports per-call p50, allocations, and a derived
single-thread ceiling (= `1 / p50`). There is no Orleans RPC, no network,
no Azure I/O on this path. These numbers are the upper bound on what the
implementation could theoretically sustain on a single thread if every
other layer were perfect.

The cohort is driven by `benchmark/performance-report.ps1 -Layer1` on the
same Azure VM that hosts the Layer 2 silo, so both layers share an identical
host and single-core performance gaps cannot be confounded with workload
differences. Each operation is run `N` times (default `N=3` cohorts) and
the published cell is the **median across the N cohorts** of the BDN-reported
p50. The marker block immediately below records the host SKU, .NET version,
BDN fidelity, cohort-N, and measurement date; subsequent refreshes are
mechanical and the prose around the marker is hand-editable.

<!-- perf-table:layer1:start
  schema=v1
  bdnFidelity=quick
  bdnToolchain=InProcessEmitToolchain
  cohortN=3
  dotnet=10.0.108
  host=Standard_D4as_v5
  rowsMeasured=2026-06-05
  methodology=Per-call p50 and allocations reported directly by BenchmarkDotNet. Single-thread ceiling = round(1 / p50). Cells are the median of N cohorts.
  DO-NOT-HAND-EDIT-BETWEEN-MARKERS
-->

| Operation                                | Per-call p50 | Allocations | Single-thread ceiling |
|------------------------------------------|-------------:|------------:|----------------------:|
| `GetAsync` (point read) | **1.32 us** | 456 B | **~760.5 k op/s** |
| `SetAsync` (point write) | **9.43 us** | 840 B | **~106.1 k op/s** |
| `GetManyAsync` (16 keys/call) | **8.8 us** | 7 KB | **~113.7 k calls/s** |
| `SetManyAsync` (1,000 entries/call) | **1.05 ms** | 350 KB | **956 calls/s** |
| `SetManyAtomicAsync` (16 keys/saga) | **270.42 us** | 67 KB | **~3.7 k sagas/s** |

<!-- perf-table:layer1:end -->

**Reading the numbers.** The single-thread ceiling is the derived
`1 / p50`. It represents the algorithmic cost of the operation on one
thread running it back-to-back with no other work; it is **not** a
multi-thread scaling claim. A real silo runs many of these calls in
parallel and each call pays a different mix of scheduling, RPC, and
storage cost - so Layer 2 below is the right column to consult for "what
the system delivers in production".

## Layer 2 - Sustained throughput on Azure (single silo, real storage)

**How it was run.** Layer 2 measures the end-to-end sustained throughput of
a single silo against a real Azure Tables Standard storage account. A
standalone producer streams vehicle telemetry events over TCP at a
configured rate (`Vehicles * TickHz` keys/sec); the silo ingests, batches,
and dispatches them through `ILattice` with phase-2 commit pipelining and
the shipping defaults for `WalPartitions` and `WalMaxPendingBatches`. The
workload mode driven on each `ILattice` method (point read, point write,
batched read, batched write, atomic saga) is selected per cell via
`BENCH_WORKLOAD_MODE`; the producer and silo run as co-located `systemd`
units on the same VM that hosts Layer 1.

The cohort is driven by `benchmark/performance-report.ps1 -Layer2`, which
provisions a fresh VM, publishes the silo + producer, runs `N` cohorts
(default `N=3`) per workload mode, computes the published throughput cell
as the **median across N cohorts** of the steady-state mean (per-second
silo rate samples filtered to the productive window; see
`benchmark/azure-throughput/throughput.md` section 27.1 for the exact
formula), pulls the per-call p50/p99 from the matching duration histogram's
last full reporter window, and tears the VM down. The full provenance
(host SKU, region, .NET version, WAL options, rung, response-timeout,
cohort-N, methodology, measurement date) is recorded in the marker block's
meta-header below; future refreshes are mechanical and the prose around
the marker is hand-editable.

These are the numbers to quote as **"what one Orleans.Lattice silo does
in production today"**. They reflect a fully durable write path
(WAL-before-Apply, real Azure round-trips, per-shard fan-out) and the
realistic latency the storage provider contributes.

<!-- perf-table:layer2:start
  schema=v1
  host=Standard_D4as_v5
  region=westus3
  dotnet=10.0.8
  walPartitions=8
  walMaxPendingBatches=16
  rung=4000vehicles/5Hz/45s
  responseTimeoutSec=180
  cohortN=3
  rowsMeasured=2026-06-05
  methodology=Throughput cell = median across N cohorts of the steady-state mean (silo per-second rate samples, t>=15s, rate>0; see benchmark/azure-throughput/throughput.md section 27.1). Per-call p50/p99 cells = median across N cohorts of the matching duration histogram's p50/p99 from the last full [phaseA] reporter window. Initial cells are a mix of cycle-30 (SetManyAsync, refreshed) and the pre-VM ACI campaign (other rows); subsequent regenerations via benchmark/performance-report.ps1 will write VM-grounded values for every row.
  provenanceNote=SetManyAsync row refreshed via cycle 30 (PR #594); other rows pending re-measurement on VM via benchmark/performance-report.ps1
  DO-NOT-HAND-EDIT-BETWEEN-MARKERS
-->

| Operation              | Sustained throughput            | Per-call p50 | Per-call p99 |
|------------------------|--------------------------------:|-------------:|-------------:|
| `GetAsync` (point read)              | **45,750 keys/s**           | ~0.11 ms     | ~0.18 ms     |
| `SetAsync` (point write)             | **202 keys/s sustained**, **16,381 keys/s burst max** | ~58 ms     | ~300 ms     |
| `GetManyAsync` (4,096 keys/call)     | **178,927 keys/s** (~44 calls/s) | 14.1 ms    | 68.6 ms     |
| `SetManyAsync` (4,096 entries/call)  | **21,275 entries/s** (~5.2 calls/s) (*) | not recaptured (*) | **~1.4 s** (*) |
| `SetManyAtomicAsync` (64 keys/saga)  | **465 keys/s** (~7.3 sagas/s), **1,793 keys/s burst max** | ~800 ms     | ~1,030 ms   |

<!-- perf-table:layer2:end -->

**Reading the numbers.** The biggest practical lever is **call shape**.
Batched APIs amortise grain-RPC, WAL, and Azure round-trip cost across
many entries per call, which is why `SetManyAsync` delivers orders of
magnitude higher sustained throughput than per-key `SetAsync` at the same
offered load. If your workload can naturally batch writes (telemetry tick
frames, event sourcing batches, periodic flush windows), use
`SetManyAsync`. If it cannot, your write ceiling is the `SetAsync` row.

The `SetManyAtomicAsync` row reflects the cost of all-or-nothing semantics
across multiple keys via the atomic-write saga: one saga durably commits
the configured key batch with cross-shard isolation. Atomic writes trade
throughput for transactional guarantees; reach for them when you need
them and use `SetManyAsync` when you don't.

Read paths are uniformly fast: `GetManyAsync` is at or near the Azure
Tables read-side ceiling for a single storage account on this
provisioning tier, and the read path benefits from per-silo cache layers
that the write path cannot use.

## What this guide does not promise

- **Cold starts.** The first few calls after a fresh silo activation
  pay grain-activation cost, JIT cost, and a small flurry of Azure
  storage handshakes. Plan for warm-up time before quoting steady-state
  numbers.
- **Load spikes.** A burst of writes that exceeds the silo's sustained
  ceiling will queue at the dispatcher and grow the per-call latency
  tail. The Layer 2 cells above are sustained throughput; transient
  bursts above them are possible for short windows but cannot be held.
- **Workload skew.** A hot key that concentrates writes on one shard or
  one leaf produces a different latency shape from the evenly-distributed
  workload measured here. Adaptive shard splitting will eventually
  rebalance a persistently hot shard, but the rebalance itself is a
  brief throughput dip.
- **Multi-silo.** A second silo with shard fan-out is the next campaign
  axis and is not yet measured. Numbers for a 2-, 4-, or N-silo cluster
  will appear in a follow-up document once that work lands.
- **WAL shipping defaults.** Layer 2 cells are measured against the
  library's shipping `WalPartitions` and `WalMaxPendingBatches` defaults
  recorded in the marker block's meta-header above. Both the foreground
  commit-log writer and the activation-time WAL replay loop on
  `BPlusLeafGrain` fan across every configured partition (two-pass replay
  with a post-pass reconciliation that advances every partition's
  checkpoint to the highest applied offset once deferred terminal
  mutations are drained), so a cold leaf reactivation under
  `WalPartitions > 1` rebuilds correctly. Reducing `WalPartitions` to
  `1` will deliver materially lower sustained write throughput because
  every commit serialises through one WAL partition's per-Azure-Tables-
  partition flush envelope. Reducing `WalMaxPendingBatches` to `1`
  restores the historical single-in-flight-per-partition shape (strict
  ordering against the provider; no pipeline depth); raising the cap
  above the shipping default in combination with a matching producer-
  side dispatch knob can saturate a single Azure Tables Standard storage
  account - see [WAL Tuning](wal-tuning.md) for the envelope.
- **Your specific workload.** Key size, value size, fan-out shape,
  read/write mix, durability requirements, and storage-provider tier
  all matter. **Run the benchmark harness against your own workload
  before committing to a capacity plan.** See [Benchmarks](benchmarks.md)
  for the runbook.
