# WAL tuning for durable backends

This document explains how the per-shard WAL grain's concurrency knobs
interact with a durable backend's throughput envelope. The default
values are tuned for a single Azure Tables Standard storage account on
a 4-vCPU silo; lifting them above the documented envelope without a
matching fan-out change collapses per-flush latency under provider-side
throttling.

If you're sizing for a different backend or a different silo SKU, the
[Benchmarks](benchmarks.md) doc and the
[`benchmark/azure-throughput`](../../benchmark/azure-throughput/)
harness show the measurement protocol used to derive these numbers.

## The two knobs

The per-shard WAL grain's pipeline depth against
`IWalStorageProvider.AppendBatchAsync` is bounded by two independent
caps:

| Knob | Default | What it bounds |
|---|---|---|
| `LatticeOptions.WalMaxPendingBatches` | `16` | Maximum number of in-flight + just-started batches **per shard**. |
| `LatticeOptions.WalPartitions` | `8` | Number of per-shard WAL grains the producer fans out across. |

The combined ceiling on simultaneous provider calls from a single silo
is therefore `WalPartitions * WalMaxPendingBatches` - at the defaults,
`8 * 16 = 128` concurrent `AppendBatchAsync` calls.

## Why 16 is the default

The previous default of 8 was picked when the foreground commit path
was bounded by per-batch latency rather than throughput. Re-measuring
on Standard_D4as_v5 in the same region as the Tables account (the
canonical operator profile for the
[`benchmark/azure-throughput`](../../benchmark/azure-throughput/)
harness, June 2026) showed the cap=8 regime spent most of its wall
time waiting on the admission gate rather than on the provider RTT:

| Instrument (p99, last full reporter window) | cap=8 | cap=16 | Direction |
|---|---|---|---|
| `wal.writer.partition.pending_appends` | 7 | 15 | doubled (cap took effect) |
| `wal.writer.append.admission_wait` ms | 2,000-2,555 | 1,196-1,389 | -40 to -53% |
| `wal.shard.dispatch.duration` ms | 1,233 | 702-1,075 | -13 to -43% |
| `leaf.commit.duration` (step=wal) ms | 2,281 | 1,296-1,479 | -35 to -43% |
| `wal.append.provider.duration` ms | 65-120 | 85-96 | unchanged (Tables RTT floor) |
| `provider.commit.duration` (phase2) ms | 50-57 | 51-56 | unchanged |

The mechanical story is that doubling the cap lifted the admission
gate (admission_wait halved), the leaf observed it (leaf.commit
step=wal halved), and per-flush provider duration was unchanged - so
the saved wall time was pure throughput. The campaign recorded a +57%
increase in steady-state silo throughput at the 4k:5 rung with no
reliability regression (`failed=0` across three runs, no
`[stall-watchdog]` firings, no `[wal-admission-timeout]` lines).

CPU efficiency improved alongside throughput (the silo did ~10% more
useful work per percent of CPU under cap=16), which rules out the
"spinning more, not doing more" confound and points at the upstream
admission gate as the binding constraint rather than the provider
itself.

## When lifting the cap stops helping

The cap=16 default sits at the **inflection point** beyond which the
producer side stops benefiting and the storage account becomes the
bottleneck. Two independent ceilings appear when the cap is lifted
further in combination with the silo's other concurrency knobs:

1. **Producer-side offered rate.** Once the silo is no longer
   admission-gated at the 4k:5 rung, the runner's offered-rate ceiling
   (~20,000 messages/sec from a single co-located producer process)
   becomes the binding constraint. The silo's measured steady-state
   throughput is a **lower bound** on its true capacity, not a
   measurement of it. Pushing past the producer floor requires either
   a larger silo SKU or a partitioned producer.

2. **Storage-account throughput.** Azure Tables Standard has a
   sustained per-account throughput threshold around 2,500
   transactions/sec. At `WalPartitions = 8`, a `WalMaxPendingBatches`
   of `16` produces a steady-state pressure of `8 * 16 = 128`
   concurrent flushes - at ~50 ms/flush this lands at ~2,560 ops/sec,
   right at the threshold. Doubling **both** knobs together (so
   `WalPartitions = 8` and `WalMaxPendingBatches = 16` becomes some
   combined configuration that drives a higher fan-out, for example
   raising the silo's own dispatch concurrency to 16 alongside cap=16
   at 6,000 keys/s offered load) pushes the account above its budget
   and surfaces as:

   - `429 TooManyRequests` responses from Azure Tables with
     `Retry-After` headers,
   - per-flush provider duration spiking from ~50 ms to ~8 s as the
     SDK back-off engages,
   - exhaustion of the per-flush `WalAppendDispatchTimeout` (default 30 s),
   - `[wal-admission-timeout]` lines on the slowest partition,
   - a drain wedge whose phenotype differs cleanly from the wedges
     covered by the existing `WalFlushTimeout` and
     `WalAppendDispatchTimeout` bounds (the wedge is upstream of the
     grain's own bounds; the bounds fire, but the silo is now
     reliability-bound on the storage account rather than on the
     grain).

The recovery is **not** to raise the per-grain timeouts further - the
underlying constraint is the storage account, not the grain. The
recovery is to **partition the storage**: raise `WalPartitions` to
fan-out across multiple accounts (the per-partition storage resolver
seam in `LatticeOptions.WalStorageProvider` is purpose-built for
exactly this), or to a Premium account with a higher per-account
throughput target.

## Sizing rules of thumb

For a single Azure Tables Standard storage account:

| Silo SKU | Recommended `WalMaxPendingBatches` | Notes |
|---|---|---|
| 2 vCPU (Standard_D2as_v5 and smaller) | `8` | The silo is CPU-bound at the 4k:5 rung; the admission gate is not the binding constraint. Default 16 wastes admission depth on a CPU that cannot pull faster. |
| 4 vCPU (Standard_D4as_v5) | `16` (default) | The sweet spot the default is tuned for. Silo CPU sits 55-75% of box at peak; admission depth is the binding constraint and 16 unblocks it without saturating the storage account. |
| 8+ vCPU (Standard_D8as_v5 and larger) | `16` to `32` | Headroom to lift the cap further if the storage account can sustain the load. Verify by measuring `wal.append.provider.duration` p99: if it stays at the ~50-100 ms Tables RTT floor under sustained load, the cap can lift; if it climbs into the seconds, the storage account is throttling and the recovery is `WalPartitions` fan-out across accounts, not a higher cap. |

For a Premium Azure Tables account, or a fan-out across multiple
Standard accounts via `LatticeOptions.WalStorageProvider`, the
per-account throughput ceiling is higher in proportion and the cap can
be lifted accordingly. The mechanical rule does not change: keep the
combined `WalPartitions * WalMaxPendingBatches * average flush rate`
below the aggregate storage budget.

## What to measure

Three instruments tell you which regime you are in:

- **`wal.writer.append.admission_wait`** - time spent waiting at the
  per-shard admission gate. If p99 is on the order of seconds and
  `wal.append.provider.duration` is on the order of tens of
  milliseconds, you are admission-bound and lifting the cap helps.
  If admission_wait is sub-millisecond and provider duration is
  seconds, you are storage-bound and lifting the cap will not help.

- **`wal.append.provider.duration`** - per-flush wall time against
  `IWalStorageProvider.AppendBatchAsync`. If this climbs from the
  ~50-100 ms Tables RTT floor into the seconds, the storage account
  is throttling. Lifting `WalMaxPendingBatches` will not help; the
  recovery is `WalPartitions` fan-out across accounts.

- **`wal.writer.partition.pending_appends`** - the live in-flight
  count, capped at `WalMaxPendingBatches` by construction. If p99 is
  consistently pinned at the cap, the cap is the binding constraint
  and there may be headroom to lift it (subject to the storage
  envelope). If p99 sits well below the cap, the cap is not binding
  and lifting it is a no-op.

See [Metrics](metrics.md) for the full set of WAL-side instruments
and their tags.

## See also

- [WAL](wal.md) - the foreground commit pipeline and how the
  per-shard grain enforces the bounds.
- [WAL Storage Providers](wal-storage-providers.md) - the
  `IWalStorageProvider` seam and the Azure Tables provider's
  two-phase batch protocol.
- [Configuration](configuration.md) - the full options reference,
  including the validator rules that reject non-positive values.
- [Benchmarks](benchmarks.md) - the measurement harness and how to
  reproduce the numbers above on your own SKU.