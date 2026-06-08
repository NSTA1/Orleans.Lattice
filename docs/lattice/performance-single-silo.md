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
`LeafCacheGrain` read-through cache (see the read-side caching note
below).

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
in-process toolchain on a single thread, and reports per-call p50,
allocations, and a derived per-thread call rate (= `1 / p50 * batchSize`,
in keys/s). There is no Orleans RPC, no network, no Azure I/O on this
path. These numbers are the algorithmic upper bound for one thread; a
real silo runs many threads concurrently and pays additional costs (see
Layer 2 below and the "Reading the numbers" paragraph after the table).

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
  gitSha=dc8ef37
  host=Standard_D4as_v5
  rowsMeasured=2026-06-07
  methodology=Per-call p50/p75/p90/p99 and allocations reported directly by BenchmarkDotNet (linear-interpolation quantiles over the workload sample). Per-thread call rate = round(1 / p50) * batchSize, reported in keys/s so batched calls (GetMany, SetMany, SetManyAtomic) are directly comparable to single-key calls (Get, Set). Cells are the median across N cohorts of each per-cohort BDN quantile.
  DO-NOT-HAND-EDIT-BETWEEN-MARKERS
-->

| Operation                                | Per-call p50 | Per-call p75 | Per-call p90 | Per-call p99 | Allocations | Per-thread call rate (1 / p50) |
|------------------------------------------|-------------:|-------------:|-------------:|-------------:|------------:|-------------------------------:|
| `GetAsync` (point read) | **1.34 us** | 4.18 us | 8.53 us | 133.53 us | 216 B | **~745.2 k keys/s** |
| `SetAsync` (point write) | **9.77 us** | 18.28 us | 21.23 us | 42.57 us | 784 B | **~102.4 k keys/s** |
| `GetManyAsync` (16 keys/call) | **10.45 us** | 13.41 us | 15.26 us | 74.61 us | 6 KB | **~1.53 M keys/s** |
| `SetManyAsync` (1,000 keys/call) | **971.54 us** | 980.35 us | 1.51 ms | 2.97 ms | 217 KB | **~1.03 M keys/s** |
| `SetManyAtomicAsync` (16 keys/saga) | **275.92 us** | 338.03 us | 492.59 us | 1.58 ms | 67 KB | **~58 k keys/s** |

<!-- perf-table:layer1:end -->

> Measured 2026-06-07 on Standard_D4as_v5 (.NET 10.0.108) at git sha 916ea62, n=3 cohorts (BDN quick).

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
  batchSize=4096
  cohortN=3
  dotnet=10.0.108
  gitSha=75bd693
  host=Standard_D4as_v5
  region=westus3
  responseTimeoutSec=180
  rowsMeasured=2026-06-08
  rung=4000 vehicles / 5 Hz / 45s
  walMaxPendingBatches=16
  walPartitions=8
  methodology=Throughput cell = median across N cohorts of the steady-state mean (silo per-second rate samples, t>=15s, rate>0; see benchmark/azure-throughput/throughput.md section 27.1). Per-call p50/p75/p90/p99 cells = median across N cohorts of the per-mode preferred [phaseA] duration instrument (set.duration for set-point, set_many.duration for set-many, saga.broadcast.duration for set-many-atomic, get.duration for get-point, get_many.duration for get-many). Each per-cohort quantile is computed inside the silo's 10-second reporter window from a 4096-sample reservoir; the cell is the median of those per-cohort quantiles. All five workload modes report the matching caller-visible duration histogram directly; no per-batch-size divisor is applied.
  DO-NOT-HAND-EDIT-BETWEEN-MARKERS
-->

| Operation                                | Sustained throughput | Per-call p50  | Per-call p75  | Per-call p90  | Per-call p99  |
|------------------------------------------|---------------------:|--------------:|--------------:|--------------:|--------------:|
| `GetAsync` (point read) | **~19.9 k keys/s** | ~60 us | ~80 us | ~130 us | ~200 us |
| `SetAsync` (point write) | **~4.3 k keys/s** | ~23.06 ms | ~37.28 ms | ~53.8 ms | ~96.36 ms |
| `GetManyAsync` (4,096 keys/call) | **~19.7 k keys/s** | ~3.16 ms | ~3.5 ms | ~3.7 ms | ~5.32 ms |
| `SetManyAsync` (4,096 keys/call) | **~11.4 k keys/s** | ~447.19 ms | ~556.09 ms | ~647.66 ms | ~702.8 ms |
| `SetManyAtomicAsync` (64 keys/saga) | **~3.9 k keys/s** | ~32.7 ms | ~58.76 ms | ~66.48 ms | ~72.3 ms |

<!-- perf-table:layer2:end -->

> Measured 2026-06-08 on Standard_D4as_v5 in westus3 (.NET 10.0.108) at git sha 75bd693, n=3 cohorts at 4000 vehicles / 5 Hz / 45s.

**Reading the numbers.** The biggest practical lever is **call shape**.
Batched APIs amortise grain-RPC, WAL, and Azure round-trip cost across
many entries per call, which is why `SetManyAsync` delivers materially
higher sustained key-write throughput than per-key `SetAsync` at the
same offered load (the published cells show roughly a 4x gap, with the
rest of the per-key win absorbed by `SetManyAsync`'s long per-call
latency tail - each call submits its keys as a sequence of 100-entity
Azure-Tables transactions against one batch partition). If your workload
can naturally batch writes (telemetry tick frames, event sourcing
batches, periodic flush windows), use `SetManyAsync`. If it cannot, your
write ceiling is the `SetAsync` row. **The aggregate write rows on this
VM SKU sit within striking distance of the measured single-account
ceiling for Azure Tables Standard, not Azure's documented per-account
transaction target** (`benchmark/azure-throughput/throughput.md` section
31 pinned the empirical ceiling at **~22-24 ke/s aggregate key-write
throughput** against one storage account, with `TableTransactionFailedException`
(409 Conflict + SDK timeout) bursts as the saturation signal). The
fastest write cell here (`SetManyAsync` at ~15.6 ke/s) is under that
wall, but a workload that combines `SetManyAsync` with other set-side
traffic on the same account can climb into the saturation regime - see
[WAL Tuning](wal-tuning.md) for the back-pressure manifestations and
the partition-the-storage recovery path. The binding constraint inside
the silo (independent of the account ceiling) is per-shard WAL-flush
concurrency, and for `SetManyAsync` specifically, per-call transaction
submission against a single Azure batch partition.

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

The quick mental conversion: `ke/s of keys ~ transactions/s x 100`
for `SetManyAsync`-shaped traffic, but for `SetAsync`-shaped traffic
(one key per transaction) the two are equal.

The `SetManyAtomicAsync` row reflects the cost of all-or-nothing
semantics across multiple keys via the atomic-write saga: one saga
durably commits the configured key batch with cross-shard isolation.
The published cell lands at roughly the same key-write rate as per-key
`SetAsync` and is the slowest of the write rows in per-saga latency,
because the saga pays multiple WAL round-trips (candidate, decision,
per-leaf apply) per commit. Use it when you need cross-key atomicity
and fall back to `SetManyAsync` when you don't.

Read paths are uniformly fast, but they are **fast because the
production read path is cache-served**, not because they exhaust Azure
Tables' read budget. The `GetAsync` p50 of ~60 us per call and the
`GetManyAsync` per-key cost of ~850 ns (3.47 ms / 4,096 keys) are each
roughly two orders of magnitude faster than a single Azure Tables
round-trip - the difference is the per-silo `LeafCacheGrain`. The
single-account read budget itself (the same ~2,500 transactions/sec
empirical ceiling that gates the write path - the Azure-published
per-account TPS target is higher but the binder for both shapes is
per-account concurrent in-flight transactions; see
`benchmark/azure-throughput/throughput.md` section 31) sits in front
of the cache, not behind it. On a workload with a low cache hit ratio
the read envelope grows toward the round-trip cost and the read budget
starts to matter.

**A note on read-side caching.** The `GetAsync` / `GetManyAsync` per-call cells above are the caller-visible envelope, which
includes whatever the `LeafCacheGrain` read-through cache served. In the
steady state of a workload that re-reads recently-written keys (the
producer-driven telemetry stream the benchmark drives is a representative
case - vehicles cycle through the same key set tick after tick), the
local-silo cache absorbs most of the cost: a same-silo revision-cookie
short-circuit skips the cross-grain delta fetch entirely, the read
collapses to a `Dictionary<string, LwwValue<byte[]>>.TryGetValue`, and
the envelope p50 settles into the tens-of-microseconds range you see
above. On a cache miss (the key has not been seen, or its TTL elapsed),
the cache fetches a delta from the primary leaf and the envelope grows
by an Azure-Tables round-trip - typically pushing the p99 noticeably
above the p50.

This is **not** a knock against the published numbers: the cache is part
of the production read path and an honest representation of what an
`ILattice` consumer's `await` actually waits for. But two practical
consequences follow:

1. **Your read-side numbers will differ** if your workload has a low
   cache hit ratio (e.g. read-once-write-once analytics, large keyspace
   with random access). The histogram cannot distinguish hits from
   misses on a per-call basis; pair `get.duration` with the
   `cache.hits` / `cache.misses` counters on a dashboard to estimate
   the regime your workload sits in.
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
