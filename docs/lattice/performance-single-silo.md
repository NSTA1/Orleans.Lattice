---
agent_spec: "docs/agents/deployment.yaml"
---

# Performance: single-silo guide

This document is an **approximate guide** to the performance you can expect
from Orleans.Lattice on a **single silo** under steady-state load. The
numbers come from two complementary benchmark surfaces - the algorithmic
ceiling of each `ILattice` method in isolation (Layer 1) and the
end-to-end behaviour a real silo delivers against a real Azure Tables
account under a realistic offered load (Layer 2) - and both are
regenerated together against a freshly-provisioned Azure VM by
`benchmark/performance-report.ps1`. Layer 2 write cells reflect a fully
durable WAL-before-Apply path with real Azure round-trips; Layer 2 read
cells are the caller-visible envelope that includes the per-silo
read-through leaf cache, and they were measured against keys the benchmark
never wrote, so they time lookups that miss rather than reads that return
stored data (see the note under the Layer 2 table and the read-side caching
note below).

The figures are **steady-state averages** taken from the productive window
of each run, with drain tails excluded. Cold starts, JIT warm-up, grain
activation storms, and bursty offered load can all introduce variance,
sometimes dramatically: a 10x latency spike on the first call to a freshly
activated grain is normal, as is a multi-second pause during a hot-shard
split or while the tree registry re-activates on another silo. The
headline cells should be read
as "what the silo settles into once the cluster is warm", not as "what
every individual call will look like".

The horizontal-scaling story (multi-silo deployments, where work fans out
across activations on multiple hosts) has its own document:
[Performance: multi-silo scaling guide](performance-multi-silo.md). Today's
numbers are the single-silo ceiling; that document measures what adding
hosts actually buys, and where the curve flattens. Note that its throughput
figures are computed on a deliberately more conservative basis than Layer 2
here, for reasons it explains, so the two are not directly interchangeable.

You should **measure your own workload**. The shapes here cover point reads,
point writes, batched multi-key reads and writes, and atomic multi-key
sagas - both single-tree (`SetManyAtomicAsync`) and cross-tree
(`BeginAtomicWrite(...).CommitAsync()`, an all-or-nothing batch spanning two
trees) at matched batch sizes so the multi-tree coordination overhead is
directly readable - against a single Azure Tables Standard account; the
per-cell provenance (host SKU, region, .NET version, WAL options, BDN
fidelity, rung, cohort N, measurement date) is recorded in the meta-header of
each table's marker block and is mechanically refreshed on every regeneration.
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
in-process toolchain on a single thread, and reports per-call p50,
allocations, and a derived per-thread call rate (= `1 / p50 * batchSize`,
in keys/s). There is no Orleans RPC, no network, no Azure I/O on this
path. These numbers are the algorithmic upper bound for one thread; a
real silo runs many threads concurrently and pays additional costs (see
Layer 2 below and the "Reading the numbers" paragraph after the table).

The cohort is driven by `benchmark/performance-report.ps1 -Layer1` on an
Azure VM of the same SKU and region as the Layer 2 silo's host, so
single-core performance gaps between hosts cannot be confounded with
workload differences. Each operation is run `N` times (default `N=3` cohorts) and
the published cell is the **median across the N cohorts** of the BDN-reported
p50. The marker block immediately below records the host SKU, .NET version,
BDN fidelity, cohort-N, and measurement date; subsequent refreshes are
mechanical and the prose around the marker is hand-editable.

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

**Reading the numbers.** The per-thread call rate is the derived
`1 / p50` scaled by the per-call batch size (1 for `GetAsync` / `SetAsync`,
the `(N keys/call)` value otherwise), reported in keys/s so batched and
per-key calls are directly comparable. It represents the algorithmic cost
of the operation on **one thread** running it back-to-back with no other
work; on a multi-core silo the aggregate rate scales with active cores
minus scheduling, RPC, and contention overhead. It is **not** a
multi-thread scaling claim, and the headline silo throughput in Layer 2
is materially lower than `cores * per-thread-rate` because production
paths pay grain-RPC, WAL, and storage costs that the in-process bench
bypasses - so Layer 2 below is the right column to consult for "what
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
formula), merges the matching duration histogram's 10-second reporter
windows that fall inside that steady-state window, weighted by call count,
into the per-call p50/p75/p90/p99, and tears the VM down. Every rung is chosen
to stay below saturation and is never raised, so a row whose cohorts
completed at least 90% of their offered load (`-SaturationRatio`) measured
that offered load rather than the silo's ceiling; it is published as a
lower bound, marked `>=`. The full provenance
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
  methodology=Throughput cell = median across N HEALTHY cohorts of the steady-state mean (silo per-second rate samples from the first non-zero sample at t>=15s to the last non-zero sample, stalled zero-rate seconds inside that window included; see benchmark/azure-throughput/throughput.md section 27.1). Per-call p50/p75/p90/p99 cells = median across N HEALTHY cohorts of the per-mode preferred [phaseA] duration instrument (set.duration for set-point and set-point-mv, set_many.duration for set-many, saga.broadcast.duration for set-many-atomic, get.duration for get-point, get_many.duration for get-many). Cohorts the harness graded WEDGE/FAILED are excluded from aggregation so a non-representative overload tail cannot poison a cell. The rung shown above is the read-workload offered load; every write workload is driven at a reduced per-row offered load (annotated in its operation label) chosen to keep the single Azure Tables account below saturation, so each cohort reports a sustained, reproducible key-write rate rather than an overload tail. Each per-cohort quantile merges the silo's 10-second reporter windows that lie inside the cohort's steady-state window, weighted by call count (each window's quantiles come from a 4096-sample reservoir); the cell is the median of those per-cohort quantiles. Every workload mode reports the matching caller-visible duration histogram directly; no per-batch-size divisor is applied.
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


Usable throughput cohorts: `GetAsync` (point read): n=3; `SetAsync` (point write, 200 veh/5 Hz): n=3; `SetAsync` (point write + async materialised view, 200 veh/5 Hz): n=3; `GetManyAsync` (4,096 keys/call): n=3; `SetManyAsync` (4,096 keys/call, 1200 veh/5 Hz): n=3; `SetManyAtomicAsync` (64 keys/saga, 100 veh/5 Hz): n=2; `SetManyAtomicAsync` (2 keys/saga, single-tree, 20 veh/5 Hz): n=3; `BeginAtomicWrite` cross-tree (2 keys/saga, 2 trees, 8 veh/5 Hz): n=3; `BeginAtomicWrite` cross-tree (64 keys/saga, 2 trees, 150 veh/5 Hz): n=3.

Cells marked >= completed (nearly) all of the load they were offered, so they measure that offered load, not the silo's ceiling: each is a lower bound on what one silo sustains.

<!-- perf-table:layer2:end -->

> Measured 2026-10-26 on Standard_D4as_v5 in westus2 (.NET 10.0.113) at git sha 376a3eae7, n=2/3 cohorts. Read workloads were driven at 4000 vehicles / 5 Hz / 45s; each write workload was driven at a reduced per-row offered load (annotated in its operation label) to hold the single Azure Tables account below saturation.

**The two read rows timed lookups of keys that were never written.**
`performance-report.ps1 -Layer2` does not pass `BENCH_VEHICLE_COUNT` to
the silo - `run-cohort.ps1` sets it only for the producer - so the silo
takes its default of 0 and skips the read-mode pre-seed that would have
written the keys the producer then asks for. Each cohort also runs against
a fresh, timestamp-named tree, and the read modes issue no writes, so every
`GetAsync`, and every key of every `GetManyAsync` call, looked up a key that
did not exist. Those two rows therefore describe the miss path on an empty
tree, not a read that returns a stored value: do not quote them as read
latency or read throughput for a populated tree. Their throughput cells are
also bounded by the offered load - 4000 vehicles at 5 Hz offers 20,000
keys/s - so they show the silo keeping pace with that load, not a read
ceiling. The Layer 1 read rows are unaffected, because the in-process
harness writes its keys before it measures.

**Reading the numbers.** The biggest practical lever is **call shape**.
Batched APIs amortise grain-RPC, WAL, and Azure round-trip cost across
many entries per call, which is why `SetManyAsync` sustains a far higher
key-write rate per unit of offered load than per-key `SetAsync` - the
per-key win that does not surface in the throughput column is absorbed
by `SetManyAsync`'s long per-call latency tail (each call submits its
keys as a sequence of 100-entity Azure-Tables transactions against one
batch partition). If your workload can naturally batch writes (telemetry
tick frames, event sourcing batches, periodic flush windows), use
`SetManyAsync`. If it cannot, your write ceiling is the `SetAsync` row.

**An asynchronous materialised view is nearly free on the write path.**
The `SetAsync (point write + async materialised view)` row drives the
*identical* point-write path as the plain `SetAsync` row, at the same
offered load, with a key-preserving materialised view attached over the
tree. Its sustained source-write throughput is nearly unchanged
(700 keys/s, versus the plain row's 723 keys/s)
because the view is maintained **asynchronously, off the caller's
critical path**: the foreground `SetAsync` returns as soon as the source
tree's write is durable, and the view maintainer applies its coalesced
background batches afterwards (in the benchmark the view kept up with
zero apply lag). The measurable cost is in caller-visible latency: a
slightly higher median (p50 ~23.63 ms vs ~21.99 ms) and a heavier tail
(p99 ~122.38 ms vs ~98.42 ms), because the background view applies contend
with the foreground writes for the same silo CPU, WAL-flush slots, and
single Azure Tables account budget. The takeaway for capacity planning:
maintaining a view costs at most a small caller-visible latency overhead
on the source writes, not a throughput penalty - budget for the view's
own write load on the shared account rather than for a slowdown of the
tree it projects from.

**The write rows are not maximum-throughput numbers.** Each write
workload is driven at its own deliberately *sub-saturation* offered load
(the per-row vehicle / Hz annotation in the operation label), because the
single Azure Tables account that backs the WAL saturates at a very
different offered rate for each call shape. Non-atomic batched
`SetManyAsync` tolerates a high per-row rung because each flush carries
many keys; each per-key `SetAsync` call waits for a durable WAL
round-trip and each atomic saga for several serial ones, so at comparable
concurrency they complete far fewer keys per second than the batched path
(single-entry appends do interleave on the batched WAL path,
[`WalBatchedSingleEntryAppends`](configuration.md#walbatchedsingleentryappends),
on by default, rather than taking an exclusive per-partition append turn).
Driven at or near saturation those shapes
are not reproducible - a single Azure tail-latency burst times out an
in-flight flush and fails the cohort - so each is offered a load below
its own saturation point. Every write-row throughput cell is therefore a
*sustained, reproducible* key-write rate at the stated offered load,
**not** that API's ceiling, and the write rows must not be read against
one another as if they shared an offered load - even the single-tree and
cross-tree 64-key atomic rows are driven at different rungs, because
their sustainable key-write rates differ by call shape; the per-call
latency columns are the meaningful cross-shape comparison. A single Azure
Tables Standard account has a finite per-account ceiling - the binder is
per-account concurrent in-flight transactions, and bursts of
`TableTransactionFailedException` (409 Conflict + SDK timeout) are the
saturation signal. A workload that combines several of these write
shapes on one account adds their offered loads and can climb into that
saturation regime - see [WAL Tuning](wal-tuning.md) for the back-pressure
manifestations and the partition-the-storage recovery path. The binding
constraint inside the silo (independent of the account ceiling) is
per-partition WAL-flush concurrency, and for `SetManyAsync` specifically,
per-call transaction submission against a single Azure batch partition.

**A note on entities vs transactions vs keys.** Three different
"per-second" numbers appear in the perf discussion and are easy to
conflate:

- **Keys/s** - what every cell in the throughput column reports. One
  key = one `(string, byte[])` entry from the caller's point of view,
  regardless of how it gets persisted.
- **Entities/s** - the same concept on the Azure side. One Azure Tables
  entity = one row. The library generally produces one entity per key-
  write, so for the workloads above keys/s == entities/s.
- **Transactions/s** - the unit Azure Tables Standard's per-account
  ceiling is denominated in. One `EntityGroupTransaction` carries up to
  100 entities against one partition key, so a single `SetMany(4096)`
  call decomposes into ~41 transactions. Azure's documented per-account
  aspirational target is 20,000 transactions/sec; the empirical
  per-account ceiling we measure on Standard tier is roughly an order
  of magnitude lower (~2,500 transactions/sec; throughput.md section 31
  / wal-tuning.md), because the binder is per-account concurrent
  in-flight transactions, not the aspirational TPS budget.

The quick mental conversion: `keys/s ~ transactions/s x 100`
for `SetManyAsync`-shaped traffic, but for `SetAsync`-shaped traffic
(one key per transaction) the two are equal.

The `SetManyAtomicAsync` row reflects the cost of all-or-nothing
semantics across multiple keys via the atomic-write saga: one saga
durably commits the configured key batch with cross-shard isolation.
Each saga pays a chain of serial durable writes per commit (a per-leaf
prepare and terminal WAL write, the decision record, and the saga's own
checkpoints), so its sustainable key-write rate
is a small fraction of the non-atomic batched path and it is offered a
correspondingly low load (which is why its throughput cell can land near
the per-key `SetAsync` row even though the two are driven independently).
Use it when you need cross-key atomicity and fall back to `SetManyAsync`
when you don't.

Read paths are uniformly fast, but they are **fast because the
production read path is served from memory**, not because they exhaust
Azure Tables' read budget. A read resolves through the per-silo
read-through leaf cache, and whatever the cache cannot answer is read
from the leaf grain's resident state, so neither path issues a storage
request. (The optimistic shard-root point read,
[`OptimisticShardRootPointReads`](configuration.md#optimisticshardrootpointreads),
on by default, serves a validated `GetAsync` from the primary leaf's
resident state without consulting the cache; it issues no storage request
either.) That is why the `GetAsync` per-call latency in the Layer 2 table
above is roughly two orders of magnitude below a single Azure Tables
round-trip, and why a `GetManyAsync` call over 4,096 keys completes in a
few milliseconds; both rows measured lookups of keys that were never
written (see the note under the table). The
single-account read budget itself (the same ~2,500 transactions/sec
empirical ceiling that gates the write path - the Azure-published
per-account TPS target is higher but the binder for both shapes is
per-account concurrent in-flight transactions; see
`benchmark/azure-throughput/throughput.md` section 31) is spent when a
leaf has to activate and load its state. On a workload whose working
set does not stay activated - a large keyspace read at random, say -
the read envelope grows toward the round-trip cost and the read budget
starts to matter.

**A note on read-side caching.** The `GetAsync` / `GetManyAsync` per-call cells above are the caller-visible envelope, which
includes whatever the per-silo read-through leaf cache served. They do
not show the cache serving stored values: the benchmark's read cohorts
never wrote the keys they read (see the note under the Layer 2 table), so
the producer's stream - the same vehicle keys tick after tick - re-read
keys that did not exist. In the steady state of a workload that re-reads
recently-written keys, the local-silo cache absorbs most of the cost: a
same-silo revision-cookie short-circuit skips the cross-grain delta fetch
entirely and the read collapses to an in-memory dictionary `TryGetValue`.
When the primary leaf has changed since the cache last
refreshed, or is activated on another silo, the read first pulls a
delta from it - an extra grain call, and a cross-silo hop when the leaf
is remote (a remote pull can be rate-limited with
`LatticeOptions.CacheTtl`, zero by default) - and a key whose cached
payload was evicted, or that an in-flight saga has pending, is read
from the leaf directly. Either path adds a grain round-trip rather than
a storage read, typically pushing the p99 noticeably above the p50.

Including the cache is not a flaw in the method: the cache is part of the
production read path, so the envelope is what an `ILattice` consumer's
`await` actually waits for. But two practical consequences follow:

1. **Your read-side numbers will differ** if your workload has a low
   cache hit ratio (e.g. read-once-write-once analytics, large keyspace
   with random access). The histogram cannot distinguish hits from
   misses on a per-call basis; pair `get.duration` with the
   `cache.hits` / `cache.misses` counters on a dashboard to estimate
   the regime your workload sits in. A point read the optimistic path
   validates never reaches the cache, so read those counters beside
   `shard_root.optimistic_read.outcomes`, whose `validated` arm counts
   the point reads they do not see.
2. **`GetWithVersionAsync` bypasses the cache** because the returned
   `HybridLogicalClock` must reflect the primary leaf's authoritative
   ordering, which the value-only cache cannot guarantee. So
   `get_with_version.duration` will systematically run higher than
   `get.duration` for the same key distribution - if your dashboards
   ever show the inverse, that is a regression signal.

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
- **Multi-silo.** Every cell here is one silo. What a 2- to 8-silo
  cluster delivers is measured separately, on its own tier and on a more
  conservative throughput basis - see
  [Performance: multi-silo scaling guide](performance-multi-silo.md).
- **WAL shipping defaults.** Layer 2 cells are measured against the
  library's shipping `WalPartitions` and `WalMaxPendingBatches` defaults
  recorded in the marker block's meta-header above. Both the foreground
  commit-log writer and the activation-time WAL replay loop on the
  leaf grain fan across every configured partition (two-pass replay
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
