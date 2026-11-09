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
  dotnet=10.0.114
  gitSha=35174338d
  host=Standard_D4as_v5
  rowsMeasured=2026-11-08
  methodology=Per-call p50/p75/p90/p99 and allocations reported directly by BenchmarkDotNet (linear-interpolation quantiles over the workload sample). Per-thread call rate = round(1 / p50) * batchSize, reported in keys/s so batched calls (GetMany, SetMany, SetManyAtomic) are directly comparable to single-key calls (Get, Set). Cells are the median across N cohorts of each per-cohort BDN quantile.
  DO-NOT-HAND-EDIT-BETWEEN-MARKERS
-->

| Operation                                | Per-call p50 | Per-call p75 | Per-call p90 | Per-call p99 | Allocations | Per-thread call rate (1 / p50) |
|------------------------------------------|-------------:|-------------:|-------------:|-------------:|------------:|-------------------------------:|
| `GetAsync` (point read) | **1.8 us** | 2.02 us | 2.64 us | 47.56 us | 536 B | **~555.9 k keys/s** |
| `SetAsync` (point write) | **5.33 us** | 7.05 us | 8.23 us | 26.44 us | 1 KB | **~187.5 k keys/s** |
| `GetManyAsync` (4 keys/call) | **10.08 us** | 17.89 us | 23.58 us | 263.51 us | 3 KB | **~396.9 k keys/s** |
| `SetManyAsync` (1,000 keys/call) | **948.17 us** | 997.86 us | 1.3 ms | 2.45 ms | 71 KB | **~1.05 M keys/s** |
| `SetManyAtomicAsync` (16 keys/saga) | **256.17 us** | 269.62 us | 304.92 us | 991.06 us | 38 KB | **~62.5 k keys/s** |
| `SetManyAtomicAsync` (2 keys/saga, single-tree) | **86.08 us** | 151.95 us | 161.43 us | 178.94 us | 26 KB | **~23.2 k keys/s** |
| `SetManyAtomicAsync` (64 keys/saga, single-tree) | **208.44 us** | 234.6 us | 244.24 us | 245.06 us | 85 KB | **~307 k keys/s** |
| `BeginAtomicWrite` cross-tree (2 keys/saga, 2 trees) | **178.55 us** | 215.77 us | 237.2 us | 283.26 us | 62 KB | **~11.2 k keys/s** |
| `BeginAtomicWrite` cross-tree (64 keys/saga, 2 trees) | **310.87 us** | 341.39 us | 348.42 us | 351.87 us | 142 KB | **~205.9 k keys/s** |

<!-- perf-table:layer1:end -->

> Measured 2026-11-08 on Standard_D4as_v5 (.NET 10.0.114) at git sha 35174338d, n=3 cohorts (BDN quick).

**Reading it.**

- **Per thread, not per silo.** The call rate is what one thread achieves
  running the call back to back with nothing else to do. A silo also pays for
  grain messaging, the WAL and storage, so what it sustains is the much lower
  Layer 2 figure.
- **Batching amortises the fixed cost.** `SetManyAsync` writes 1,000 keys in
  about 0.95 ms, roughly 5.6 times the per-key rate of `SetAsync`.
- **Atomicity costs more per call.** A 2-key `SetManyAtomicAsync` takes about
  86 us against about 5.3 us for `SetAsync`, and spanning two trees roughly
  doubles a 2-key saga's cost (about 179 us).
- **Read the `GetManyAsync` row with care.** The 4-key batch reads about 10 us per call here, against about 1.8 us for a single `GetAsync`.

## Layer 2: sustained throughput, one silo on Azure Tables

How it is measured: [Layer 2 methodology](performance-methodology-layer2.md).

<!-- perf-table:layer2:start
  schema=v1
  batchSize=4096
  cohortN=2/3
  dotnet=10.0.114
  gitSha=35174338d
  host=Standard_D4as_v5
  region=westus2
  responseTimeoutSec=180
  rowsMeasured=2026-11-08
  rung=4000 vehicles / 5 Hz / 45s
  walMaxPendingBatches=16
  walPartitions=8
  methodology=Throughput cell = median across N HEALTHY cohorts of the steady-state mean (silo per-second rate samples from the first non-zero sample at t>=15s to the last non-zero sample, stalled zero-rate seconds inside that window included; see benchmark/azure-throughput/throughput.md section 27.1). Per-call p50/p75/p90/p99 cells = median across N HEALTHY cohorts of the per-mode preferred [phaseA] duration instrument (set.duration for set-point and set-point-mv, set_many.duration for set-many, saga.broadcast.duration for set-many-atomic, get.duration for get-point, get_many.duration for get-many). Only cohorts the harness graded HEALTHY are aggregated (DEGRADED, FAILED and WEDGE are excluded) so a non-representative overload tail cannot poison a cell. The rung shown above is the read-workload offered load; every write workload is driven at its own lower, fixed offered load (annotated in its operation label), chosen so each cohort reports a sustained, reproducible key-write rate rather than an overload tail. Each per-cohort quantile merges the silo's 10-second reporter windows that lie inside the cohort's steady-state window, weighted by call count (each window's quantiles come from a 4096-sample reservoir); the cell is the median of those per-cohort quantiles. Every workload mode reports the matching caller-visible duration histogram directly; no per-batch-size divisor is applied.
  DO-NOT-HAND-EDIT-BETWEEN-MARKERS
-->

| Operation                                | Sustained throughput | Per-call p50  | Per-call p75  | Per-call p90  | Per-call p99  |
|------------------------------------------|---------------------:|--------------:|--------------:|--------------:|--------------:|
| `GetAsync` (point read) | **>= ~19.9 k keys/s** | ~20 us | ~30 us | ~30 us | ~340 us |
| `SetAsync` (point write, 200 veh/5 Hz) | **736 keys/s** | ~16.58 ms | ~31.68 ms | ~39.18 ms | ~100.29 ms |
| `SetAsync` (point write + async materialised view, 200 veh/5 Hz) | **777 keys/s** | ~13.78 ms | ~22.63 ms | ~35.26 ms | ~77.89 ms |
| `GetManyAsync` (4,096 keys/call) | **>= ~19.6 k keys/s** | ~5.23 ms | ~6.49 ms | ~10.13 ms | ~23.44 ms |
| `SetManyAsync` (4,096 keys/call, 1200 veh/5 Hz) | **>= ~6.1 k keys/s** | ~171.75 ms | ~329.83 ms | ~722.41 ms | ~1302.26 ms |
| `SetManyAtomicAsync` (64 keys/saga, 100 veh/5 Hz) | **>= 485 keys/s** | ~41.86 ms | ~61.42 ms | ~81.23 ms | ~139.69 ms |
| `SetManyAtomicAsync` (2 keys/saga, single-tree, 20 veh/5 Hz) | **>= 92 keys/s** | ~12.39 ms | ~21.05 ms | ~26.13 ms | ~67.62 ms |
| `BeginAtomicWrite` cross-tree (2 keys/saga, 2 trees, 8 veh/5 Hz) | **>= 40 keys/s** | ~9.78 ms | ~13.84 ms | ~22.17 ms | ~52.86 ms |
| `BeginAtomicWrite` cross-tree (64 keys/saga, 2 trees, 150 veh/5 Hz) | **697 keys/s** | ~73.74 ms | ~100.97 ms | ~129.37 ms | ~228.39 ms |


Cells marked >= completed (nearly) all of the load they were offered, so they measure that offered load, not the silo's ceiling: each is a lower bound on what one silo sustains.

<!-- perf-table:layer2:end -->

> Measured 2026-11-08 on Standard_D4as_v5 in westus2 (.NET 10.0.114) at git sha 35174338d, n=2/3 cohorts. Read workloads were driven at 4000 vehicles / 5 Hz / 45s; each write workload was driven at its own lower, fixed offered load (annotated in its operation label).

**Reading it.**

- **Write rows are not ceilings.** Each write workload is offered its own
  fixed load, lower than the reads' and shown in its label, so a write row is
  a sustained rate at that load. Rows offered different loads should be
  compared by latency, not throughput. A row marked
  `>=` kept up with all of its load.
- **Call shape is the biggest lever.** `SetManyAsync` sustains at least 6.1 k
  keys/s at its load against 736 keys/s for `SetAsync`, because one call
  carries 4,096 keys through a single round of WAL and Azure work. The price
  is per-call latency: a median of about 172 ms and a p99 of about 1,302 ms. If
  your writes can be batched, batch them.
- **An asynchronous materialised view showed no write-path cost.** With a
  view attached, the same point writes at the same load sustain 777 keys/s
  against 736 without one, with a lower median (about 13.8 ms against 16.6 ms)
  and p99 (about 78 ms against 100 ms). This run shows no cost from the view;
  the view's own writes still land on the account, so budget for them.
- **Atomic sagas pay for all-or-nothing.** Each saga makes a chain of durable
  writes, so its sustainable rate is a small fraction of a plain batch's: at
  least 92 keys/s for 2-key sagas and at least 485 for 64-key sagas, against at least
  6.1 k for `SetManyAsync`. Use them when you need atomicity across keys.
- **The read rows measure a hot working set.** They read the same 4,000
  pre-written keys over and over, so every read is answered from memory:
  `GetAsync` in about 20 us and a 4,096-key `GetManyAsync` in about 5.2 ms,
  well below one Azure Tables round trip. Both rows ran at a fixed 20,000
  keys/s offered load and completed at least 19.9 k and 19.6 k keys/s. They are
  not read ceilings: one silo in the
  [multi-silo sweep](performance-multi-silo.md) read about 51 k keys/s with
  `GetAsync` and 270 k with `GetManyAsync`. A workload whose keys are not in
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