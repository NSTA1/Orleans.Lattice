---
agent_spec: "docs/agents/deployment.yaml"
---

# Performance: multi-silo scaling

> [!IMPORTANT]
> **Scope: one tree, one storage account.** Every figure here comes from a cluster serving a **single tree** (two for the cross-tree rows), with its write-ahead log (WAL) and grain state on **one Azure Storage account**.
>
> - **Trees.** Some of a tree's resources have a fixed count that does not grow with the cluster: its WAL partitions ([`WalPartitions`](configuration.md#walpartitions), default 8; 16 in this sweep), its shards (64 here), and its transaction registry ([`TxRegistryShardCount`](configuration.md#txregistryshardcount), default 1; 8 here). An estate serving many trees spreads more of them across its silos, so its aggregate write and saga throughput can scale further than one tree's. Multi-tree scaling has not been measured.
> - **Storage accounts.** One account is a test constraint, not a library limit. Lattice can spread a tree's WAL partitions across several accounts; see [Multi-account fan-out](wal-storage-providers.md#multi-account-fan-out-named-providers-and-pinned-placement). [What the storage account reported](#what-the-storage-account-reported) shows what the one account did in each cell.

This page shows what adding silos buys: the same workloads as the
[single-silo](performance-single-silo.md) Layer 2 table, run on clusters of 1,
2, 4, 6 and 8 silos on Azure Container Apps. Each cell is offered more load
than the cluster can complete, so every figure is the cluster's limit at that
size. Each cell is the median of up to three 45-second runs, called cohorts;
[how Layer 3 is measured](performance-methodology-layer3.md) explains the rig,
the method and what bounds the results.

Read the shape of each curve rather than its absolute level. The rig holds
everything but the silo count constant (one storage account, a fixed shard
count, a client with bounded concurrency), which also caps the absolute
figures; a deployment tuned for throughput can beat them.

## The short version

| Workload | Shape from 1 to 8 silos | What the evidence says bounds it |
|----------|-------------------------|----------------------------------|
| `GetAsync` (point read) | ~55.8 k keys/s at one silo, ~57 k at two, then rising to ~132.5 k at eight (2.37x) | From two silos on, time spent outside the grain call: the silo-side call time holds at ~4-5 ms while the client's calls take longer |
| `GetManyAsync` (batched read) | ~316.5 k keys/s at one silo, ~193 k at two and ~216 k at four; no six- or eight-silo cell | A per-call time that grows almost as fast as the calls in flight, over a keyspace that doubles with each step; the larger keyspaces could not be seeded |
| `SetAsync`, with or without a view | ~3.3-3.6 k keys/s at one silo, ~8.5-11.2 k at eight (2.6-3.1x) | Not isolated; the storage account shows no throttling and flat server latency |
| `SetManyAsync` (batched write) | ~10.5 k keys/s at one silo, ~13-17 k from two to eight, no sustained gain after four | Not isolated: the account's transaction rate stays flat and it never throttled; up to ~20 timed-out storage transactions per run cost more keys as silos are added |
| 64-key atomic sagas (single-tree and cross-tree) | ~34-47 sagas/s at one silo, ~115-119 at eight (2.5-3.4x) | Not isolated; each saga takes longer as silos are added, under the client's bounded concurrency |
| 2-key atomic sagas (single-tree and cross-tree) | ~253 and ~139 sagas/s at one silo, ~781 and ~263 at eight (3.1x and 1.9x) | Single-tree: the saga registry's admission budget, reached at eight silos. Cross-tree: not isolated; the account's server latency rises fourfold ([#3591](https://github.com/NSTA1/Orleans.Lattice/issues/3591)) |

No workload scales linearly, and only `GetManyAsync` falls below its
one-silo figure.

## The scaling curves

<!-- perf-chart:layer3:start
  schema=v1
  DO-NOT-HAND-EDIT-BETWEEN-MARKERS
-->

![Speedup over one silo for each workload, against ideal linear scaling.](images/performance-layer3-speedup.svg)

Not drawn, because it was not measured at every silo count above (its cells are in the table): `GetManyAsync` (4,096 keys/call).

![Reads: sustained throughput vs silo count.](images/performance-layer3-reads.svg)

Not drawn, because it was not measured at every silo count above (its cells are in the table): `GetManyAsync` (4,096 keys/call).

![Point and batched writes: sustained throughput vs silo count.](images/performance-layer3-writes.svg)

![Atomic and cross-tree sagas: sustained throughput vs silo count.](images/performance-layer3-atomic.svg)

<!-- perf-chart:layer3:end -->

## The grid

How it is measured: [Layer 3 methodology](performance-methodology-layer3.md).

**Offered** is the load the cell was driven at, deliberately above what it
completed. **Speedup** is throughput over the same workload's one-silo
throughput, and **per-silo efficiency** is speedup divided by the silo count;
perfect scaling is a speedup equal to the silo count at 100%. Latency is the
time a call spends inside the silos, excluding the client's network hop; for
the atomic rows it is the saga's broadcast phase, not its whole commit. Every
cohort that completed work is kept, failures included: the last column gives
the failed keys of each cohort the cell used. Apart from one saga cohort
refused at the admission budget, every cohort that lost keys also recorded
timed-out storage transactions ([What the storage account reported](#what-the-storage-account-reported)).

<!-- perf-table:layer3:start
  schema=v1
  batchSize=4096
  cohortN=2/3
  dotnet=10.0.x
  gitSha=376a3eae7
  host=Azure Container Apps (Consumption)
  maxRungEscalations=3
  region=westus3
  responseTimeoutSec=420
  rigs=fl31,fl32,fl33,fl34,fl35
  rowsMeasured=2026-10-27
  rungPerSilo=get-point=8000 veh/silo @ 5 Hz / 45s, flushConcurrency 16/silo; get-many=16000 veh/silo @ 5 Hz / 45s, flushConcurrency 8/silo; set-point=1200 veh/silo @ 5 Hz / 45s, flushConcurrency 64/silo; set-point-mv=1200 veh/silo @ 5 Hz / 45s, flushConcurrency 64/silo; set-many=2400 veh/silo @ 5 Hz / 45s, flushConcurrency 8/silo; set-many-atomic=800 veh/silo @ 5 Hz / 45s, flushConcurrency 64/silo; set-many-atomic-2=200 veh/silo @ 5 Hz / 45s, flushConcurrency 64/silo; cross-tree-atomic-2=100 veh/silo @ 5 Hz / 45s, flushConcurrency 64/silo; cross-tree-atomic-64=800 veh/silo @ 5 Hz / 45s, flushConcurrency 64/silo
  saturationRatio=0.9
  shardCount=64
  siloCounts=1,2,4,6,8
  siloSize=4 vCPU / 8 GiB
  walAccounts=1
  walMaxPendingBatches=16
  walPartitions=16
  methodology=Each cell is the median across N HEALTHY cohorts of completed-work throughput: total successfully-completed keys at FINAL divided by the engine's active elapsed time. Layer 3 deliberately does NOT use a per-second steady-state mean. On this path the client submits 4096-key batches, so a whole batch retires inside one per-second sample and the samples between retirements are exactly zero; a mean that filters the zeros away (the historical rate>0 form) averages only the spikes and reports more throughput than was offered (measured: 9,637 keys/s reported against 5,935 keys/s actually offered). The overstatement also varies with burstiness, which varies with silo count, so it would bend the scaling curve itself. Completed-ops / active-elapsed counts only work that succeeded over the wall-clock it took, so it cannot exceed the offered load and carries no windowing bias. Per-call p50/p99 merge the [phaseA] duration windows of every silo at t>=15s, weighted by call count; the silos' clocks cannot be aligned to the producer's steady-state window, so unlike Layer 2 no window is excluded as outside it. Every cell is a true-throughput measurement, not an offered-load one: offered load is scaled with the silo count (a per-silo rung driven at rung x silo count, so per-silo demand never falls as the cluster grows), and a cell whose first cohort completes at least saturationRatio of what was offered is re-run at double the rung, up to maxRungEscalations times, so the published figure is where the cluster stopped keeping up rather than what the client asked for. A cell still offer-bound after the last escalation is published as a lower bound (>=). Speedup and per-silo efficiency are derived against the measured 1-silo cell. Every cohort starts on empty storage: with the silos parked, the harness deletes every table in the storage account except the clustering table and points the silos at a freshly named WAL table and grain-state table, so no cohort inherits trees, registry rows, or WAL backlog from an earlier one. All silo counts share ONE Azure Storage account for the WAL; the storage-side table reports what that account's own Azure Monitor metrics showed for each cell.
  DO-NOT-HAND-EDIT-BETWEEN-MARKERS
-->

| Operation | Silos | Offered | Sustained throughput | Speedup vs 1 silo | Per-silo efficiency | Per-call p50 | Per-call p99 | Failed keys per cohort |
|-----------|------:|--------:|---------------------:|------------------:|--------------------:|-------------:|-------------:|-----------------------:|
| `GetAsync` (point read) | 1 | ~80 k keys/s | **~55.8 k keys/s** | 1x | 100% | ~30 us | ~3.26 ms | 0 / 0 / 0 |
| `GetAsync` (point read) | 2 | ~160 k keys/s | **~57 k keys/s** | 1.02x | 51% | ~4.25 ms | ~19.81 ms | 0 / 0 / 0 |
| `GetAsync` (point read) | 4 | ~320 k keys/s | **~94.5 k keys/s** | 1.69x | 42% | ~4.17 ms | ~18.7 ms | 0 / 0 / 0 |
| `GetAsync` (point read) | 6 | ~480 k keys/s | **~115.8 k keys/s** | 2.07x | 35% | ~4.89 ms | ~24.27 ms | 0 / 0 / 0 |
| `GetAsync` (point read) | 8 | ~640 k keys/s | **~132.5 k keys/s** | 2.37x | 30% | ~4.82 ms | ~23.77 ms | 0 / 0 / 0 |
| `GetManyAsync` (4,096 keys/call) | 1 | ~640 k keys/s | **~316.5 k keys/s** | 1x | 100% | ~52.1 ms | ~190.76 ms | 0 / 0 / 0 |
| `GetManyAsync` (4,096 keys/call) | 2 | ~1.28 M keys/s | **~193 k keys/s** | 0.61x | 30% | ~304.24 ms | ~607.98 ms | 0 / 0 / 0 |
| `GetManyAsync` (4,096 keys/call) | 4 | ~2.56 M keys/s | **~215.6 k keys/s** | 0.68x | 17% | ~544.08 ms | ~1130.6 ms | 0 / 0 / 0 |
| `SetAsync` (point write) | 1 | ~6 k keys/s | **~3.6 k keys/s** | 1x | 100% | ~59.58 ms | ~208.47 ms | 0 / 0 / 0 |
| `SetAsync` (point write) | 2 | ~12 k keys/s | **~3.6 k keys/s** | 1.01x | 51% | ~82.25 ms | ~450.67 ms | 0 / 0 / 114 |
| `SetAsync` (point write) | 4 | ~24 k keys/s | **~6 k keys/s** | 1.67x | 42% | ~75.38 ms | ~551.04 ms | 0 / 0 / 0 |
| `SetAsync` (point write) | 6 | ~36 k keys/s | **~9.1 k keys/s** | 2.54x | 42% | ~82.16 ms | ~669.22 ms | 0 / 167 / 0 |
| `SetAsync` (point write) | 8 | ~48 k keys/s | **~11.2 k keys/s** | 3.13x | 39% | ~89.98 ms | ~1344.91 ms | 0 / 0 / 0 |
| `SetAsync` (point write + async materialised view) | 1 | ~6 k keys/s | **~3.3 k keys/s** | 1x | 100% | ~67.01 ms | ~208.68 ms | 0 / 0 / 0 |
| `SetAsync` (point write + async materialised view) | 2 | ~12 k keys/s | **~4 k keys/s** | 1.21x | 60% | ~87.41 ms | ~396.8 ms | 75 / 0 / 0 |
| `SetAsync` (point write + async materialised view) | 4 | ~24 k keys/s | **~7.4 k keys/s** | 2.24x | 56% | ~85.47 ms | ~679.89 ms | 0 / 167 / 93 |
| `SetAsync` (point write + async materialised view) | 6 | ~36 k keys/s | **~7.7 k keys/s** | 2.35x | 39% | ~87.25 ms | ~1885.39 ms | 0 / 1,023 / 773 |
| `SetAsync` (point write + async materialised view) | 8 | ~48 k keys/s | **~8.5 k keys/s** | 2.59x | 32% | ~83.31 ms | ~1684.13 ms | 139 / 54 / 819 |
| `SetManyAsync` (4,096 keys/call) | 1 | ~12 k keys/s | **~10.5 k keys/s** | 1x | 100% | ~1785.27 ms | ~7912.26 ms | 25,696 / 7,200 / 0 |
| `SetManyAsync` (4,096 keys/call) | 2 | ~24 k keys/s | **~13.1 k keys/s** | 1.25x | 63% | ~4120.23 ms | ~13086.44 ms | 12,288 / 20,480 / 0 |
| `SetManyAsync` (4,096 keys/call) | 4 | ~48 k keys/s | **~16.3 k keys/s** | 1.55x | 39% | ~7415.27 ms | ~10610.35 ms | 0 / 0 / 152,960 |
| `SetManyAsync` (4,096 keys/call) | 6 | ~72 k keys/s | **~13.9 k keys/s** | 1.33x | 22% | ~10670.81 ms | ~27760.06 ms | 176,128 / 190,656 |
| `SetManyAsync` (4,096 keys/call) | 8 | ~96 k keys/s | **~16.8 k keys/s** | 1.6x | 20% | ~11879.08 ms | ~34041.1 ms | 167,936 / 0 |
| `SetManyAtomicAsync` (64 keys/saga) | 1 | ~4 k keys/s | **~3 k keys/s** | 1x | 100% | ~511.42 ms | ~1126.08 ms | 0 / 2,624 / 0 |
| `SetManyAtomicAsync` (64 keys/saga) | 2 | ~8 k keys/s | **~3.2 k keys/s** | 1.07x | 54% | ~915.69 ms | ~1602.5 ms | 0 / 0 / 0 |
| `SetManyAtomicAsync` (64 keys/saga) | 4 | ~16 k keys/s | **~5.6 k keys/s** | 1.87x | 47% | ~1231.15 ms | ~2003.95 ms | 0 / 0 / 0 |
| `SetManyAtomicAsync` (64 keys/saga) | 6 | ~24 k keys/s | **~6.6 k keys/s** | 2.21x | 37% | ~1318.82 ms | ~2289.47 ms | 13,376 / 0 / 0 |
| `SetManyAtomicAsync` (64 keys/saga) | 8 | ~32 k keys/s | **~7.6 k keys/s** | 2.54x | 32% | ~1647.95 ms | ~2840.51 ms | 0 / 0 / 0 |
| `SetManyAtomicAsync` (2 keys/saga, single-tree) | 1 | ~1 k keys/s | **506 keys/s** | 1x | 100% | ~45.74 ms | ~149.41 ms | 0 / 0 / 0 |
| `SetManyAtomicAsync` (2 keys/saga, single-tree) | 2 | ~2 k keys/s | **618 keys/s** | 1.22x | 61% | ~72.66 ms | ~202.36 ms | 0 / 0 / 0 |
| `SetManyAtomicAsync` (2 keys/saga, single-tree) | 4 | ~4 k keys/s | **~1.1 k keys/s** | 2.27x | 57% | ~74.99 ms | ~206.42 ms | 0 / 0 / 0 |
| `SetManyAtomicAsync` (2 keys/saga, single-tree) | 6 | ~6 k keys/s | **~1.4 k keys/s** | 2.76x | 46% | ~87.06 ms | ~462.39 ms | 0 / 0 / 0 |
| `SetManyAtomicAsync` (2 keys/saga, single-tree) | 8 | ~8 k keys/s | **~1.6 k keys/s** | 3.08x | 39% | ~84.27 ms | ~319.78 ms | 0 / 250 / 0 |
| `BeginAtomicWrite` cross-tree (2 keys/saga, 2 trees) | 1 | 500 keys/s | **278 keys/s** | 1x | 100% | ~29.51 ms | ~243.22 ms | 0 / 0 / 0 |
| `BeginAtomicWrite` cross-tree (2 keys/saga, 2 trees) | 2 | ~1 k keys/s | **323 keys/s** | 1.16x | 58% | ~46.97 ms | ~209.62 ms | 0 / 0 / 0 |
| `BeginAtomicWrite` cross-tree (2 keys/saga, 2 trees) | 4 | ~2 k keys/s | **425 keys/s** | 1.53x | 38% | ~42.7 ms | ~824.16 ms | 0 / 0 / 0 |
| `BeginAtomicWrite` cross-tree (2 keys/saga, 2 trees) | 6 | ~3 k keys/s | **510 keys/s** | 1.83x | 31% | ~45.06 ms | ~783.45 ms | 0 / 0 / 0 |
| `BeginAtomicWrite` cross-tree (2 keys/saga, 2 trees) | 8 | ~4 k keys/s | **526 keys/s** | 1.89x | 24% | ~52.78 ms | ~1968.29 ms | 0 / 0 / 0 |
| `BeginAtomicWrite` cross-tree (64 keys/saga, 2 trees) | 1 | ~4 k keys/s | **~2.2 k keys/s** | 1x | 100% | ~423.26 ms | ~1546.46 ms | 704 / 0 / 0 |
| `BeginAtomicWrite` cross-tree (64 keys/saga, 2 trees) | 2 | ~8 k keys/s | **~3.1 k keys/s** | 1.42x | 71% | ~179.34 ms | ~2037.01 ms | 0 / 0 / 0 |
| `BeginAtomicWrite` cross-tree (64 keys/saga, 2 trees) | 4 | ~16 k keys/s | **~5.2 k keys/s** | 2.36x | 59% | ~235.93 ms | ~2910.06 ms | 0 / 0 / 0 |
| `BeginAtomicWrite` cross-tree (64 keys/saga, 2 trees) | 6 | ~24 k keys/s | **~4.9 k keys/s** | 2.23x | 37% | ~1742.24 ms | ~5341.72 ms | 16,912 / 0 |
| `BeginAtomicWrite` cross-tree (64 keys/saga, 2 trees) | 8 | ~32 k keys/s | **~7.4 k keys/s** | 3.37x | 42% | ~282.44 ms | ~5220.34 ms | 19,200 / 0 / 0 |

<!-- perf-table:layer3:end -->

> Measured 2026-10-27 on Azure Container Apps (Consumption) in westus3 (.NET 10.0.x) at git sha 376a3eae7, n=2/3 cohorts per cell, silo counts 1,2,4,6,8. Offered load scales with the silo count and is raised until each cell plateaus below it, so every figure is true throughput rather than offered load; every cohort starts on freshly emptied storage. All silo counts share one Azure Storage account for the WAL - see the storage-side table for what its metrics showed.

## What the storage account reported

How these columns are defined: [Layer 3 methodology](performance-methodology-layer3.md#how-to-read-the-grid).

A curve that stops climbing can be Lattice running out of headroom or the
storage account running out. Each run records what its account reported to
Azure Monitor: the transactions that **timed out** (`ServerTimeoutError` or
`ClientTimeoutError`) and their share of all transactions, the **storage
transactions** per second in the busiest minute (all accounts together), and
the **server** and **end-to-end** latency, whose gap is time spent on the
network and in client-side queueing. Whether any transaction was throttled is stated under
the table. The account also holds the cluster's grain state and membership
table, so its transactions are not only WAL writes.

<!-- perf-storage:layer3:start
  schema=v1
  DO-NOT-HAND-EDIT-BETWEEN-MARKERS
-->

| Operation | Silos | WAL accounts | Timed-out transactions | Storage transactions | Server latency | End-to-end latency |
|-----------|------:|-------------:|-----------------------:|---------------------:|---------------:|-------------------:|
| `GetAsync` (point read) | 1 | 1 | 0 | 197 tx/s | 6.0 ms | 8.2 ms |
| `GetAsync` (point read) | 2 | 1 | 0 | 398 tx/s | 6.5 ms | 7.8 ms |
| `GetAsync` (point read) | 4 | 1 | 0 | 1,017 tx/s | 5.6 ms | 6.7 ms |
| `GetAsync` (point read) | 6 | 1 | 0 | 1,793 tx/s | 6.7 ms | 7.7 ms |
| `GetAsync` (point read) | 8 | 1 | 0 | 3,146 tx/s | 6.0 ms | 6.8 ms |
| `GetManyAsync` (4,096 keys/call) | 1 | 1 | 0 | 2,588 tx/s | 5.2 ms | 6.3 ms |
| `GetManyAsync` (4,096 keys/call) | 2 | 1 | 0 | 3,811 tx/s | 5.4 ms | 6.2 ms |
| `GetManyAsync` (4,096 keys/call) | 4 | 1 | 2 (<0.01%) | 6,715 tx/s | 4.9 ms | 5.3 ms |
| `SetAsync` (point write) | 1 | 1 | 0 | 2,581 tx/s | 6.2 ms | 7.8 ms |
| `SetAsync` (point write) | 2 | 1 | 0 | 3,030 tx/s | 6.7 ms | 8.4 ms |
| `SetAsync` (point write) | 4 | 1 | 0 | 3,172 tx/s | 5.9 ms | 8.3 ms |
| `SetAsync` (point write) | 6 | 1 | 0 | 3,567 tx/s | 6.9 ms | 9.6 ms |
| `SetAsync` (point write) | 8 | 1 | 0 | 8,529 tx/s | 7.0 ms | 8.2 ms |
| `SetAsync` (point write + async materialised view) | 1 | 1 | 0 | 2,804 tx/s | 7.9 ms | 9.8 ms |
| `SetAsync` (point write + async materialised view) | 2 | 1 | 0 | 2,883 tx/s | 6.9 ms | 8.7 ms |
| `SetAsync` (point write + async materialised view) | 4 | 1 | 5 (<0.01%) | 3,192 tx/s | 8.2 ms | 10.1 ms |
| `SetAsync` (point write + async materialised view) | 6 | 1 | 9 (<0.01%) | 3,458 tx/s | 8.6 ms | 11.0 ms |
| `SetAsync` (point write + async materialised view) | 8 | 1 | 16 (<0.01%) | 7,764 tx/s | 7.7 ms | 8.7 ms |
| `SetManyAsync` (4,096 keys/call) | 1 | 1 | 3 (<0.01%) | 1,654 tx/s | 8.6 ms | 9.7 ms |
| `SetManyAsync` (4,096 keys/call) | 2 | 1 | 12 (<0.01%) | 1,681 tx/s | 8.8 ms | 10.6 ms |
| `SetManyAsync` (4,096 keys/call) | 4 | 1 | 0 | 1,470 tx/s | 8.9 ms | 10.1 ms |
| `SetManyAsync` (4,096 keys/call) | 6 | 1 | 21 (0.01%) | 1,722 tx/s | 10.0 ms | 11.2 ms |
| `SetManyAsync` (4,096 keys/call) | 8 | 1 | 9 (<0.01%) | 2,305 tx/s | 12.9 ms | 14.5 ms |
| `SetManyAtomicAsync` (64 keys/saga) | 1 | 1 | 0 | 1,802 tx/s | 6.8 ms | 9.4 ms |
| `SetManyAtomicAsync` (64 keys/saga) | 2 | 1 | 0 | 1,893 tx/s | 6.6 ms | 8.0 ms |
| `SetManyAtomicAsync` (64 keys/saga) | 4 | 1 | 0 | 2,879 tx/s | 6.3 ms | 7.9 ms |
| `SetManyAtomicAsync` (64 keys/saga) | 6 | 1 | 0 | 2,985 tx/s | 6.1 ms | 8.2 ms |
| `SetManyAtomicAsync` (64 keys/saga) | 8 | 1 | 0 | 2,859 tx/s | 6.1 ms | 7.3 ms |
| `SetManyAtomicAsync` (2 keys/saga, single-tree) | 1 | 1 | 0 | 1,887 tx/s | 4.2 ms | 4.8 ms |
| `SetManyAtomicAsync` (2 keys/saga, single-tree) | 2 | 1 | 0 | 2,453 tx/s | 5.0 ms | 5.4 ms |
| `SetManyAtomicAsync` (2 keys/saga, single-tree) | 4 | 1 | 0 | 4,433 tx/s | 5.3 ms | 5.9 ms |
| `SetManyAtomicAsync` (2 keys/saga, single-tree) | 6 | 1 | 0 | 3,961 tx/s | 5.8 ms | 6.4 ms |
| `SetManyAtomicAsync` (2 keys/saga, single-tree) | 8 | 1 | 0 | 4,144 tx/s | 6.2 ms | 6.7 ms |
| `BeginAtomicWrite` cross-tree (2 keys/saga, 2 trees) | 1 | 1 | 0 | 3,192 tx/s | 6.7 ms | 6.8 ms |
| `BeginAtomicWrite` cross-tree (2 keys/saga, 2 trees) | 2 | 1 | 0 | 3,341 tx/s | 11.0 ms | 11.3 ms |
| `BeginAtomicWrite` cross-tree (2 keys/saga, 2 trees) | 4 | 1 | 0 | 3,779 tx/s | 16.9 ms | 17.1 ms |
| `BeginAtomicWrite` cross-tree (2 keys/saga, 2 trees) | 6 | 1 | 0 | 6,015 tx/s | 23.1 ms | 23.5 ms |
| `BeginAtomicWrite` cross-tree (2 keys/saga, 2 trees) | 8 | 1 | 0 | 5,507 tx/s | 28.4 ms | 28.9 ms |
| `BeginAtomicWrite` cross-tree (64 keys/saga, 2 trees) | 1 | 1 | 0 | 2,108 tx/s | 8.0 ms | 9.7 ms |
| `BeginAtomicWrite` cross-tree (64 keys/saga, 2 trees) | 2 | 1 | 0 | 3,712 tx/s | 6.8 ms | 7.9 ms |
| `BeginAtomicWrite` cross-tree (64 keys/saga, 2 trees) | 4 | 1 | 0 | 3,655 tx/s | 8.1 ms | 9.5 ms |
| `BeginAtomicWrite` cross-tree (64 keys/saga, 2 trees) | 6 | 1 | 10 (<0.01%) | 4,254 tx/s | 9.9 ms | 11.8 ms |
| `BeginAtomicWrite` cross-tree (64 keys/saga, 2 trees) | 8 | 1 | 0 | 3,711 tx/s | 11.0 ms | 12.5 ms |

Azure Monitor recorded no throttled transactions (`ServerBusy` or `ClientThrottling`) in any cohort of any cell.

<!-- perf-storage:layer3:end -->

Timed-out transactions are rare (at most 0.01% of a cell's transactions), but
they account for nearly every failed key in the grid. Of the 128 cohorts,
every one that lost keys recorded timed-out transactions, except one 2-key
saga cohort refused at the saga admission budget (see
[the saga section](#atomic-and-cross-tree-sagas-per-saga-latency)). The
reverse does not hold: eight cohorts, mostly reads and sagas, recorded
timeouts (1 to 56) and lost nothing. One timed-out transaction can cost many keys,
because the grid counts every key of a call that failed: a failed
`SetManyAsync` call counts all 4,096 of its keys.

## Workload by workload

### `GetAsync`: scales from two silos, on client round-trip time

Point reads deliver ~55.8 k keys/s at one silo, ~57 k at two, and then
~94.5 k, ~115.8 k and ~132.5 k at four, six and eight: 2.37x at eight
silos, or 2.33x for the fourfold step from two to eight. Every cohort
reports zero failures.

The client is closed-loop at 256 calls in flight per silo. Dividing the
calls in flight by the throughput gives the mean time a read takes as the
client sees it: about 4.6 ms at one silo, 9 ms at two and 15.5 ms at eight.
The silo-side time is far smaller: a p50 of ~30 us at one silo, where the
whole read is in-process, and ~4.2-4.9 ms from two to eight, where the
shard root usually lives on another host. So from two silos on, the growth
is not inside the grain call; it is in the client-to-gateway hop, the
messaging layer and the queueing in front of them. Shard-root point reads
are interleavable ([#3474](https://github.com/NSTA1/Orleans.Lattice/issues/3474)),
so the fixed pool of 64 shard roots is not a serialisation point for them.

### `GetManyAsync`: below its one-silo figure

Batched reads deliver ~316.5 k keys/s at one silo, ~193 k at two and
~216 k at four, with zero failures. Each call reads a 4,096-key batch whose
keys land on shards on every host, and the client holds eight calls in
flight per silo. The silo-side median call time is ~52 ms at one silo,
~304 ms at two and ~544 ms at four: from two silos to four it grows almost
as fast as the calls in flight, so throughput barely moves. A batch
completes only when its slowest shard answers, and each added host puts
more of its shards across the network; the cause is not isolated further.

**The read keyspace grows with the cell.** The harness seeds as many keys
as the producer has vehicles, and the vehicle count also sets the offered
load. The one-silo cell settled at 128,000 vehicles per silo and that load
per silo carries to the larger cells, so the keyspace doubles with every
step: 128 k keys at one silo, 256 k at two and 512 k at four. This curve
therefore varies the silo count and the keyspace size together.

**There are no six- or eight-silo cells.** At this load they need 768 k and
1.02 M keys seeded through the rig's one storage account before measuring.
The six-silo seed reached 536 k keys before its batch writes were refused as
saturated (`LatticeSaturatedException`: the per-shard fan-out did not settle
within its budget) until its retries ran out, so the harness refused those
runs as unseeded rather than measure an empty keyspace. A lower load or
a multi-account WAL (`-Layer3WalAccounts`) would let them seed.

### `SetAsync` and `SetAsync` with a materialised view

Point writes deliver ~3.6 k keys/s at one and two silos, then ~6 k,
~9.1 k and ~11.2 k at four, six and eight: 3.13x at eight silos. The median
time a write spends inside the silo rises from ~60 ms at one silo to
75-90 ms from two to eight, so the gain comes from more writes in flight
across more hosts rather than from faster writes. Failed keys stay small (at
most 167 in a cohort; see the grid's last column), and only cohorts whose
account recorded timed-out transactions lost any.

The storage account shows no sign of being the limit: its busiest minute climbs from
~2.6 k to ~8.5 k transactions/s at eight silos with no throttling and
server latency flat at 6-7 ms. The pace-setter is not isolated further.
Three cohorts at four and eight silos ended with up to ~420 k offered writes
still queued, which is the offered load running far ahead of the cluster;
they count only the writes they completed.

The materialised-view variant tracks plain `SetAsync` within about a
quarter: below it at one silo (~3.3 k), above it at two and four (~4 k and
~7.4 k), and 15-24% below it at six and eight (~7.7 k and ~8.5 k, 2.59x),
where it also loses more keys to timeouts (up to 1,023 in a cohort).
Maintaining the view costs write throughput only at the largest cluster
sizes measured here.

### `SetManyAsync`: no sustained gain after four silos

Batched writes deliver ~10.5 k keys/s at one silo, ~13.1 k at two and
~16.3 k at four, then ~13.9 k and ~16.8 k at six and eight. The median call
takes ~1.8 s at one silo and 7-12 s from four silos on, so extra silos add
calls in flight without completing more of them.

What limits it is not isolated. The storage account's busiest minute stays
between ~1.5 k and ~2.3 k transactions/s at every silo count while the offered
load grows eightfold, and its server latency rises from ~9 ms to ~13 ms. The
account recorded 0 to 22 timed-out transactions in each run the grid kept, at
every silo count, but the keys they cost grow with the cluster: up to ~26 k in a
one-silo cohort and ~191 k in a six-silo one (see the grid's last column).
Those losses are storage timeouts, not Lattice back-pressure refusals. The
account never throttled, so the data does not show it to be the limit. The six- and eight-silo cells
each rest on two cohorts: one six-silo cohort's log is missing its final
report, and one eight-silo cohort stalled for 208 s and was excluded. The next
experiment is the multi-account WAL fan-out (`-Layer3WalAccounts`), which this
sweep held out so that the silo count stayed the only variable.

### Atomic and cross-tree sagas: per-saga latency

The four atomic workloads read most clearly in sagas per second:

| Workload | Keys per saga | Sagas/s at 1 / 2 / 4 / 6 / 8 silos |
|----------|--------------:|:-----------------------------------|
| `SetManyAtomicAsync` | 64 | 47 / 50 / 87 / 103 / 119 |
| `SetManyAtomicAsync` | 2 | 253 / 309 / 575 / 698 / 781 |
| `BeginAtomicWrite` cross-tree | 64 | 34 / 49 / 81 / 76 / 115 |
| `BeginAtomicWrite` cross-tree | 2 | 139 / 162 / 213 / 255 / 263 |

All four rise with the silo count: at eight silos the single-tree sagas
reach 2.5x (64 keys) and 3.1x (2 keys), the 64-key cross-tree saga 3.4x and
the 2-key cross-tree saga 1.9x. The cost is mostly per saga rather than per
key: a 2-key saga runs five to seven times as many sagas per second as a
64-key one, not thirty-two times. Spanning two trees costs a 64-key saga
about a quarter of its rate at one silo and almost nothing at eight, and a
2-key saga 45% at one silo and two thirds at eight.

**The saga registry's admission budget is reached at eight silos.** Each
completed saga leaves a tombstone in its tree's transaction registry for
`TxDecisionRetention`, and `LatticeOptions.TxRegistryAdmissionBudgetBytes`
refuses new sagas with `LatticeSaturatedException` (source
`TxRegistryCapacity`) once a registry shard's row reaches the budget. The
sweep runs eight registry shards (`LatticeOptions.TxRegistryShardCount`,
default 1), which the option documents as about 800 sagas/s per tree. The
2-key single-tree saga reached that at eight silos (~781 sagas/s): two of its
three eight-silo cohorts were refused admission repeatedly (`satRetries`
1,787 and 2,514), and while one recovered every saga on retry, the other gave
up on 250 after their retries ran out. No other atomic cohort was refused. A
tree that needs more sagas per second can raise the shard count; refusals
show up in a run's final report as `satRetries`, and those that ran out of
retries as `satExhausted`.

Below that budget, what sets the rate is how long each saga takes: the
client keeps 64 sagas in flight per silo and no cell was producer-bound, so
each saga's end-to-end time grows with the silo count. For the 2-key
cross-tree saga the storage account's server latency rises from ~7 ms at one
silo to ~28 ms at eight, against 4-6 ms for the 2-key single-tree saga. That
fits each of the saga's serial durable writes slowing as silos are added
([#3591](https://github.com/NSTA1/Orleans.Lattice/issues/3591)), but the cause
is not isolated further. The 64-key
sagas carry a 64-key WAL write on top of that chain; their failed keys (up
to ~19 k in one cross-tree cohort at eight silos) came with timed-out
storage transactions in the same cohorts.