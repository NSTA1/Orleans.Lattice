# Performance: single-silo guide

This document is an **approximate guide** to the performance you can expect
from Orleans.Lattice on a **single silo** under steady-state load. The
numbers come from two complementary benchmark surfaces, both rerun against
the current branch HEAD and described below. They are intended to set
realistic expectations - the algorithmic ceiling of each operation in
isolation (Layer 1) and the sustained throughput a real silo delivers
against real Azure Tables under a realistic offered load (Layer 2).

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
provenance (offered fleet, host SKU, region, and whether the cell
predates the post-v6.2 WAL re-tune) is summarised under each layer. If
your keys are larger, your fan-out is different, your hot-key
distribution is skewed, or your durability requirements differ, your
numbers will differ too. The benchmark harness ships with the repository
and is easy to repoint at your own offered load - see
[Benchmarks](benchmarks.md) for the runbook.

## Layer 1 - In-process microbench (algorithmic ceiling)

**How it was run.** Layer 1 measures the cost of one call to each
`ILattice` method when scheduling and durable storage are out of the
picture: a `BenchmarkDotNet` harness instantiates the grain layer in-process
against an in-memory storage provider, runs each operation O(10^4) - O(10^6)
times, and reports mean/median latency, allocations, and confidence
intervals with very tight error bars. There is no Orleans RPC, no network,
no Azure I/O on this path. These numbers are the upper bound on what the
implementation could theoretically sustain on a single thread if every
other layer were perfect.

The figures below were measured on an AMD Ryzen 7 PRO 7840U laptop (16
logical / 8 physical cores) running .NET 10.0.8 with the BenchmarkDotNet
in-process toolchain.

| Operation              | Per-call p50 | Allocations | Single-thread ceiling |
|------------------------|-------------:|------------:|----------------------:|
| `GetAsync` (point read)              | **283 ns**       | 456 B       | **~3.54 M op/s** |
| `SetAsync` (point write)             | **2.19 us**       | 616 B       | **~458 k op/s**  |
| `GetManyAsync` (16 keys/call)        | **6.59 us**       | 6,144 B     | **~152 k calls/s** (~2.4 M keys/s) |
| `SetManyAsync` (1,000 entries/call)  | **557 us**     | 250 KB      | **~1.79 k calls/s** (~1.79 M entries/s) |
| `SetManyAtomicAsync` (16 keys/saga)  | **132 us**     | 64 KB       | **~7.57 k sagas/s** (~121 k keys/s) |

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
configured rate; the silo ingests, batches, and dispatches them through
`ILattice` with phase-2 commit pipelining and the shipping defaults for
`WalPartitions` and `WalMaxPendingBatches`. Producer and silo run as
co-located `systemd` units on a single Linux VM; the bench harness records
per-operation latency histograms throughout the run and reports the
sustained throughput over the productive window after warm-up.

The cells below are a mix of two cohort campaigns: the read-side cells
(`GetAsync`, `GetManyAsync`) and the atomic-saga cell come from the
original West Europe campaign on the older harness shape; the
`SetManyAsync` cell was re-measured under the post-v6.2 WAL re-tune on
Standard_D4as_v5 in westus3 with the new shipping default
`WalMaxPendingBatches = 16` (see [WAL Tuning](wal-tuning.md) and
`benchmark/azure-throughput/throughput.md` section 30 for the cohort).
The `SetAsync` per-key cell predates the WAL re-tune; it is still the
right floor to plan against when the workload cannot batch but may
under-report on the new default.

These are the numbers to quote as **"what one Orleans.Lattice silo does
in production today"**. They reflect a fully durable write path
(WAL-before-Apply, real Azure round-trips, per-shard fan-out) and the
realistic latency the storage provider contributes.

| Operation              | Sustained throughput            | Per-call p50 | Per-call p99 |
|------------------------|--------------------------------:|-------------:|-------------:|
| `GetAsync` (point read)              | **45,750 keys/s**           | ~0.11 ms     | ~0.18 ms     |
| `SetAsync` (point write)             | **202 keys/s sustained**, **16,381 keys/s burst max** | ~58 ms     | ~300 ms     |
| `GetManyAsync` (4,096 keys/call)     | **178,927 keys/s** (~44 calls/s) | 14.1 ms    | 68.6 ms     |
| `SetManyAsync` (4,096 entries/call)  | **21,275 entries/s** (~5.2 calls/s) (*) | not recaptured (*) | **~1.4 s** (*) |
| `SetManyAtomicAsync` (64 keys/saga)  | **465 keys/s** (~7.3 sagas/s), **1,793 keys/s burst max** | ~800 ms     | ~1,030 ms   |

(*) Re-measured under the post-v6.2 WAL re-tune on Standard_D4as_v5 in
westus3 with the new shipping default `WalMaxPendingBatches = 16` at the
4,000-vehicle / 5 Hz rung. The throughput cell is the mean of n=3 cohorts
(steady-state mean per cohort: 21,216 / 21,292 / 21,320 e/s; range 104
e/s; ~0.5% CoV). The per-call p99 cell is derived from
`leaf.commit.duration{step=wal}` p99 = 1,296-1,479 ms recorded in the
same cohort; the per-call p50 was not captured at the per-call instrument
level in this cohort. The pre-re-tune campaign recorded **13,574
entries/s** sustained with **2.0 s p50 / 2.85 s p99** per call, against
the previous default of `WalMaxPendingBatches = 8`. See [WAL Tuning](wal-tuning.md)
for the storage-account-throughput envelope above which raising the cap
further stops helping.

**Reading the numbers.** The biggest practical lever is **call shape**.
Batched APIs amortise grain-RPC, WAL, and Azure round-trip cost across
many entries per call, which is why `SetManyAsync` delivers **~105x**
the sustained throughput of per-key `SetAsync` at the same offered load
(21,275 / 202; the per-key floor predates the WAL re-tune and may be
loose against the current default). If your workload can naturally batch
writes (telemetry tick frames, event sourcing batches, periodic flush
windows), use `SetManyAsync`. If it cannot, your write ceiling is the
`SetAsync` row.

The `SetManyAtomicAsync` row reflects the cost of all-or-nothing semantics
across multiple keys via the atomic-write saga - one saga durably commits
a batch of 16 keys with cross-shard isolation in ~800 ms. Atomic writes
trade throughput for transactional guarantees; reach for them when you
need them and use `SetManyAsync` when you don't.

Read paths are uniformly fast: `GetManyAsync` at ~179,000 keys/s sustained
is approximately the documented Azure Tables read-side ceiling for a
single storage account on this provisioning tier, and the read path
benefits from per-silo cache layers that the write path cannot use.

## What this guide does not promise

- **Cold starts.** The first few calls after a fresh silo activation
  pay grain-activation cost, JIT cost, and a small flurry of Azure
  storage handshakes. Plan for warm-up time before quoting steady-state
  numbers.
- **Load spikes.** A burst of writes that exceeds the silo's sustained
  ceiling will queue at the dispatcher and grow the per-call latency
  tail. The `burst max` columns above are real burst capacity, but they
  cannot be sustained.
- **Workload skew.** A hot key that concentrates writes on one shard or
  one leaf produces a different latency shape from the evenly-distributed
  workload measured here. Adaptive shard splitting will eventually
  rebalance a persistently hot shard, but the rebalance itself is a
  brief throughput dip.
- **Multi-silo.** A second silo with shard fan-out is the next campaign
  axis and is not yet measured. Numbers for a 2-, 4-, or N-silo cluster
  will appear in a follow-up document once that work lands.
- **WAL shipping defaults: `WalPartitions = 8`, `WalMaxPendingBatches = 16`.**
  The post-v6.2 `SetManyAsync` Layer 2 cell was measured with both
  defaults in force. Both the foreground commit-log writer and the
  activation-time WAL replay loop on `BPlusLeafGrain` fan across every
  configured partition (two-pass replay with a post-pass reconciliation
  that advances every partition's checkpoint to the highest applied
  offset once deferred terminal mutations are drained), so a cold leaf
  reactivation under `WalPartitions > 1` rebuilds correctly. Reducing
  `WalPartitions` to `1` will deliver materially lower sustained write
  throughput because every commit serialises through one WAL partition's
  per-Azure-Tables-partition flush envelope. Reducing
  `WalMaxPendingBatches` to `1` restores the historical
  single-in-flight-per-partition shape (strict ordering against the
  provider; no pipeline depth); raising it above 16 in combination with
  a matching producer-side dispatch knob can saturate a single Azure
  Tables Standard storage account - see [WAL Tuning](wal-tuning.md) for
  the envelope. The Layer 2 cells above reflect what the
  default-configured silo delivers.
- **Your specific workload.** Key size, value size, fan-out shape,
  read/write mix, durability requirements, and storage-provider tier
  all matter. **Run the benchmark harness against your own workload
  before committing to a capacity plan.** See [Benchmarks](benchmarks.md)
  for the runbook.
