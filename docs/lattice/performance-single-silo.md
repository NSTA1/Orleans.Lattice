---
agent_spec: "docs/agents/deployment.yaml"
---

# Performance: single silo

This page is an approximate guide to what one Orleans.Lattice silo costs per
call and what it sustains. Two measurements are regenerated together by
`benchmark/performance-report.ps1`:

- **Layer 1, cost per call in-process:** each `ILattice` method on one thread,
  with no network and no durable storage. See
  [how Layer 1 is measured](performance-methodology-layer1.md).
- **Layer 2, sustained throughput on Azure:** one silo writing through its
  durable write-ahead log (WAL) to a real Azure Tables account. See
  [how Layer 2 is measured](performance-methodology-layer2.md).

Each figure is the median of up to three runs, called cohorts, on a freshly
provisioned Azure VM; the line under each table says how many were used.

What adding silos buys is on a separate page:
[Performance: multi-silo scaling](performance-multi-silo.md).

The figures are steady-state averages from a warm silo. Cold starts, the first
call to a newly activated grain, shard splits and bursts can all be much
slower. Key and value sizes, fan-out, key skew, the read/write mix, durability
needs and the storage tier all move the numbers, so measure your own workload
before planning capacity: the harness is in the repository (see
[Benchmarks](benchmarks.md)).

## Layer 1: cost per call, in-process

How it is measured: [Layer 1 methodology](performance-methodology-layer1.md).

<!-- perf-table:layer1:start
  schema=v1
  bdnFidelity=quick
  bdnToolchain=InProcessEmitToolchain
  cohortN=3
  dotnet=10.0.113
  gitSha=376a3eae7
  host=Standard_D4as_v5
  rowsMeasured=2026-10-26
  methodology=Per-call p50/p75/p90/p99 and allocations reported directly by BenchmarkDotNet (linear-interpolation quantiles over the workload sample). Per-thread call rate = round(1 / p50) * batchSize, reported in keys/s so batched calls (GetMany, SetMany, SetManyAtomic) are directly comparable to single-key calls (Get, Set). Cells are the median across N cohorts of each per-cohort BDN quantile.
  DO-NOT-HAND-EDIT-BETWEEN-MARKERS
-->

| Operation                                | Per-call p50 | Per-call p75 | Per-call p90 | Per-call p99 | Allocations | Per-thread call rate (1 / p50) |
|------------------------------------------|-------------:|-------------:|-------------:|-------------:|------------:|-------------------------------:|
| `GetAsync` (point read) | **1.66 us** | 2.17 us | 3.29 us | 50.77 us | 536 B | **~604.1 k keys/s** |
| `SetAsync` (point write) | **4.58 us** | 5.12 us | 6.15 us | 29.26 us | 1 KB | **~218.4 k keys/s** |
| `GetManyAsync` (4 keys/call) | **10.27 us** | 21.87 us | 30.66 us | 209.99 us | 3 KB | **~389.6 k keys/s** |
| `SetManyAsync` (1,000 keys/call) | **1.01 ms** | 1.07 ms | 1.52 ms | 2.56 ms | 71 KB | **~990.8 k keys/s** |
| `SetManyAtomicAsync` (16 keys/saga) | **271.3 us** | 345.38 us | 398.36 us | 1.02 ms | 38 KB | **~59 k keys/s** |
| `SetManyAtomicAsync` (2 keys/saga, single-tree) | **90.03 us** | 154.84 us | 165.81 us | 183.75 us | 26 KB | **~22.2 k keys/s** |
| `SetManyAtomicAsync` (64 keys/saga, single-tree) | **211.17 us** | 234.29 us | 241.52 us | 249.74 us | 85 KB | **~303.1 k keys/s** |
| `BeginAtomicWrite` cross-tree (2 keys/saga, 2 trees) | **193.25 us** | 214.57 us | 244.83 us | 307.41 us | 62 KB | **~10.3 k keys/s** |
| `BeginAtomicWrite` cross-tree (64 keys/saga, 2 trees) | **323.6 us** | 352.17 us | 358.31 us | 363.4 us | 142 KB | **~197.8 k keys/s** |

<!-- perf-table:layer1:end -->

> Measured 2026-10-26 on Standard_D4as_v5 (.NET 10.0.113) at git sha 376a3eae7, n=3 cohorts (BDN quick).

**Reading it.**

- **Per thread, not per silo.** The call rate is what one thread achieves
  running the call back to back with nothing else to do. A silo also pays for
  grain messaging, the WAL and storage, so what it sustains is the much lower
  Layer 2 figure.
- **Batching amortises the fixed cost.** `SetManyAsync` writes 1,000 keys in
  about 1 ms, roughly 4.5 times the per-key rate of `SetAsync`.
- **Atomicity costs more per call.** A 2-key `SetManyAtomicAsync` takes about
  90 us against about 4.6 us for `SetAsync`, and spanning two trees roughly
  doubles a 2-key saga's cost (about 193 us).
- **The `GetManyAsync` row overstates its cost.** The same 4-key batch
  measures about 3.2 us, with identical allocations, when it runs in the
  harness's batch-size sweep, so this row's 10 us is an artefact of where it
  runs in the suite rather than the call's cost
  ([#5078](https://github.com/NSTA1/Orleans.Lattice/pull/5078)).

## Layer 2: sustained throughput, one silo on Azure Tables

How it is measured: [Layer 2 methodology](performance-methodology-layer2.md).

<!-- perf-table:layer2:start
  schema=v1
  batchSize=4096
  cohortN=2/3
  dotnet=10.0.113
  gitSha=376a3eae7
  host=Standard_D4as_v5
  region=westus2
  responseTimeoutSec=180
  rowsMeasured=2026-10-26
  rung=4000 vehicles / 5 Hz / 45s
  walMaxPendingBatches=16
  walPartitions=8
  methodology=Throughput cell = median across N HEALTHY cohorts of the steady-state mean (silo per-second rate samples from the first non-zero sample at t>=15s to the last non-zero sample, stalled zero-rate seconds inside that window included; see benchmark/azure-throughput/throughput.md section 27.1). Per-call p50/p75/p90/p99 cells = median across N HEALTHY cohorts of the per-mode preferred [phaseA] duration instrument (set.duration for set-point and set-point-mv, set_many.duration for set-many, saga.broadcast.duration for set-many-atomic, get.duration for get-point, get_many.duration for get-many). Only cohorts the harness graded HEALTHY are aggregated (DEGRADED, FAILED and WEDGE are excluded) so a non-representative overload tail cannot poison a cell. The rung shown above is the read-workload offered load; every write workload is driven at its own lower, fixed offered load (annotated in its operation label), chosen so each cohort reports a sustained, reproducible key-write rate rather than an overload tail. Each per-cohort quantile merges the silo's 10-second reporter windows that lie inside the cohort's steady-state window, weighted by call count (each window's quantiles come from a 4096-sample reservoir); the cell is the median of those per-cohort quantiles. Every workload mode reports the matching caller-visible duration histogram directly; no per-batch-size divisor is applied.
  DO-NOT-HAND-EDIT-BETWEEN-MARKERS
-->

| Operation                                | Sustained throughput | Per-call p50  | Per-call p75  | Per-call p90  | Per-call p99  |
|------------------------------------------|---------------------:|--------------:|--------------:|--------------:|--------------:|
| `GetAsync` (point read) | **~18.1 k keys/s** | ~20 us | ~20 us | ~30 us | ~1.02 ms |
| `SetAsync` (point write, 200 veh/5 Hz) | **723 keys/s** | ~21.99 ms | ~33.77 ms | ~49.14 ms | ~98.42 ms |
| `SetAsync` (point write + async materialised view, 200 veh/5 Hz) | **700 keys/s** | ~23.63 ms | ~34.7 ms | ~47.06 ms | ~122.38 ms |
| `GetManyAsync` (4,096 keys/call) | **~17.7 k keys/s** | ~2.83 ms | ~5.88 ms | ~10.17 ms | ~51.91 ms |
| `SetManyAsync` (4,096 keys/call, 1200 veh/5 Hz) | **>= ~6.1 k keys/s** | ~208.64 ms | ~344.46 ms | ~473.38 ms | ~762.98 ms |
| `SetManyAtomicAsync` (64 keys/saga, 100 veh/5 Hz) | **>= 469 keys/s** | ~55.61 ms | ~77.15 ms | ~106.1 ms | ~316.45 ms |
| `SetManyAtomicAsync` (2 keys/saga, single-tree, 20 veh/5 Hz) | **82 keys/s** | ~18.95 ms | ~33.94 ms | ~44.98 ms | ~100.5 ms |
| `BeginAtomicWrite` cross-tree (2 keys/saga, 2 trees, 8 veh/5 Hz) | **>= 39 keys/s** | ~9.14 ms | ~12.48 ms | ~23.19 ms | ~86.56 ms |
| `BeginAtomicWrite` cross-tree (64 keys/saga, 2 trees, 150 veh/5 Hz) | **568 keys/s** | ~55.53 ms | ~86.79 ms | ~120.88 ms | ~318.69 ms |


Cells marked >= completed (nearly) all of the load they were offered, so they measure that offered load, not the silo's ceiling: each is a lower bound on what one silo sustains.

<!-- perf-table:layer2:end -->

> Measured 2026-10-26 on Standard_D4as_v5 in westus2 (.NET 10.0.113) at git sha 376a3eae7, n=2/3 cohorts. Read workloads were driven at 4000 vehicles / 5 Hz / 45s; each write workload was driven at its own lower, fixed offered load (annotated in its operation label).

**Reading it.**

- **Write rows are not ceilings.** Each write workload is offered its own
  fixed load, lower than the reads' and shown in its label, so a write row is
  a sustained rate at that load. Rows offered different loads should be
  compared by latency, not throughput. A row marked
  `>=` kept up with all of its load.
- **Call shape is the biggest lever.** `SetManyAsync` sustains about 6.1 k
  keys/s at its load against 723 keys/s for `SetAsync`, because one call
  carries 4,096 keys through a single round of WAL and Azure work. The price
  is per-call latency: a median of about 209 ms and a p99 of about 763 ms. If
  your writes can be batched, batch them.
- **An asynchronous materialised view costs little on the write path.** With a
  view attached, the same point writes at the same load sustain 700 keys/s
  against 723 without one. The median write is slightly slower (about 23.6 ms
  against 22.0 ms) and the tail heavier (p99 about 122 ms against 98 ms),
  because the view's background updates share the silo and the storage
  account. Budget for the view's own writes on the account.
- **Atomic sagas pay for all-or-nothing.** Each saga makes a chain of durable
  writes, so its sustainable rate is a small fraction of a plain batch's: 82
  keys/s for 2-key sagas and at least 469 for 64-key sagas, against about
  6.1 k for `SetManyAsync`. Use them when you need atomicity across keys.
- **The read rows measure a hot working set.** They read the same 4,000
  pre-written keys over and over, so every read is answered from memory:
  `GetAsync` in about 20 us and a 4,096-key `GetManyAsync` in about 2.8 ms,
  well below one Azure Tables round trip. Both rows ran at a fixed 20,000
  keys/s offered load and completed about 18.1 k and 17.7 k keys/s. They are
  not read ceilings: one silo in the
  [multi-silo sweep](performance-multi-silo.md) read about 56 k keys/s with
  `GetAsync` and 316 k with `GetManyAsync`. A workload whose keys are not in
  memory reads more slowly; see
  [how Layer 2 is measured](performance-methodology-layer2.md#the-read-keyspace).

## What these numbers do not tell you

- **Cold starts.** The first calls after a silo starts pay for grain
  activation, JIT compilation and storage connection set-up.
- **Bursts.** A burst above the sustained rate queues and lengthens the latency
  tail; it can be absorbed briefly but not held.
- **Key skew.** A hot key concentrates load on one shard or leaf. Adaptive
  shard splitting rebalances a persistently hot shard, with a brief dip while
  it does.
- **More than one silo.** See
  [Performance: multi-silo scaling](performance-multi-silo.md).
- **Other WAL settings.** These figures use the shipping
  [`WalPartitions`](configuration.md#walpartitions) and
  `WalMaxPendingBatches` defaults. Fewer partitions spread the writes over
  fewer WAL streams, and more pending batches put more transactions in flight
  against the account; see [WAL Tuning](wal-tuning.md).
- **Your workload.** Run the harness against it before committing to a
  capacity plan.