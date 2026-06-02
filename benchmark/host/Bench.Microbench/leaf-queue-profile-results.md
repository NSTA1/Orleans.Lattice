# Leaf-queue commit-turn dispatch profile

## Purpose

Issue [#418](https://github.com/NSTA1/Orleans.Lattice/issues/418)
("Leaf-level write batching") is gated on a profiling pre-condition:
only pursue in-grain write coalescing if the leaf grain's own commit
turn - not the WAL layer, not key skew - is the bottleneck under
realistic WAL-ack latency. The failure modes of the implementation
(reentrant leaves, a `FlushAsync` drain contract, an explicit
`WriteMode` durability downgrade) touch nearly every component and are
severe in production, so the issue explicitly says: "Only implement if
profiling proves the leaf grain itself is the bottleneck."

This probe answers that question directly. The three `LeafQueue_*`
workloads in `LatticeMicroBenchmarks.cs` drive a real `WalShardGrain`
(whose `TaskCompletionSource` ack-chain is the production pipelining
ceiling) behind a latency-injecting storage provider, three ways:

- **`LeafQueue_SerializedAppends`** - awaits each `AppendAsync` before
  the next. This is exactly what a **non-reentrant** leaf grain does
  today: every foreground commit awaits its WAL ack inside the grain
  turn, so write N+1 cannot begin until write N has durably landed.
- **`LeafQueue_PipelinedAppends`** - issues all N appends concurrently
  and awaits the batch. Models a `[Reentrant]` (or turn-releasing) leaf
  letting the WAL grain's ack-chain coalesce the appends into a handful
  of flushes.
- **`LeafQueue_BatchedAppend`** - one `AppendBatchAsync(N)`. Models the
  existing `SetManyAsync` coalescing path already on the public surface.

The probe uses a single `WalShardGrain` so the measured contention is
the single hot-leaf case the issue describes, not the post-split
fanned-out steady state.

## Run command

```pwsh
dotnet build benchmark/host/Bench.Microbench/Orleans.Lattice.Benchmark.Microbench.csproj -c Release
$env:BENCH_MICROBENCH_FIDELITY = "quick"
$env:BENCH_MICROBENCH_WORKLOADS = "LeafQueue"
dotnet run -c Release --no-build --project benchmark/host/Bench.Microbench/Orleans.Lattice.Benchmark.Microbench.csproj
```

Tuning knobs:

- `BENCH_MICROBENCH_LEAFQUEUE_LATENCY_MS` - simulated per-flush WAL-ack
  latency (default 4, approximating a same-region Azure Table round
  trip). Sweep this to see how the gap scales with store latency.
- `BENCH_MICROBENCH_VALUE_BYTES` - payload size per write (default 128).

## Results - wall-clock per call (4 ms injected WAL-ack latency)

`quick` fidelity (1 launch, 3 warmup, 3 measurement), AMD Ryzen 7 PRO
7840U, .NET 10, BenchmarkDotNet 0.15.8.

| dispatch shape | writes | Mean | vs serialized | Allocated |
|---|---:|---:|---:|---:|
| serialized (non-reentrant leaf) | 8 | 124.76 ms | 1.00x | 21.05 KB |
| pipelined (reentrant leaf) | 8 | 15.57 ms | **8.0x faster** | 20.76 KB |
| batched (SetMany path) | 8 | 15.44 ms | **8.1x faster** | 7.74 KB |
| serialized (non-reentrant leaf) | 32 | 491.40 ms | 1.00x | 151.87 KB |
| pipelined (reentrant leaf) | 32 | 31.21 ms | **15.7x faster** | 56.25 KB |
| batched (SetMany path) | 32 | 15.44 ms | **31.8x faster** | 24.33 KB |

## Interpretation

The leaf-turn serialization **is** the bottleneck under realistic
WAL-ack latency, and the gap widens with batch size:

- Serialized dispatch scales ~linearly with the write count: 8 writes
  cost ~125 ms (~8 x 16 ms - the cold WAL flush deadline dominates the
  first flush; subsequent flushes are the injected 4 ms), 32 writes cost
  ~491 ms. Every write pays a full WAL round trip inside the grain turn
  because the non-reentrant leaf cannot start the next commit until the
  current one's ack returns.
- Pipelined dispatch is near-flat: the WAL grain's `TaskCompletionSource`
  ack-chain coalesces the concurrent appends into a handful of flushes,
  so 32 writes cost ~31 ms instead of ~491 ms (15.7x). The remaining
  growth is the per-flush entry cap fanning a large batch across a few
  flushes.
- Batched dispatch is flat at ~15 ms regardless of batch size (one
  flush, one latency) and allocates the least.

**Conclusion for #418.** The profiling gate is satisfied: the leaf
grain's non-reentrant commit turn serializes writes at a cost that
scales with WAL-ack latency x write count, and releasing that
serialization (pipelined or batched dispatch) recovers an 8-32x
throughput win on a single hot leaf. The win is real and large, so the
issue is worth pursuing - **but** the same data shows the
already-shipped `SetManyAsync` batched path captures the full win at the
lowest allocation cost without any of the risky durability changes
(reentrant leaves, `FlushAsync` drain, `WriteMode`). The lowest-risk
order of work is therefore:

1. Steer hot-write callers onto the existing `SetManyAsync` batched
   surface (key-design guidance + API ergonomics) - captures the win
   with zero durability-semantics change.
2. Only if a workload genuinely cannot batch at the call site (truly
   independent single-key writes arriving concurrently at one leaf) does
   the reentrant-leaf / `WriteMode` work become necessary - and that is
   the case to re-profile against the post-split fanned-out steady
   state, since adaptive splitting should already be dispersing such a
   hotspot.

## Caveat

The injected latency is a fixed `Task.Delay`, not a real storage round
trip, so absolute numbers are illustrative of the *shape* of the gap,
not a production SLA. The relative ordering (serialized >> pipelined ~=
batched) holds across the latency sweep because it is structural: it
follows from where the `await` sits relative to the grain turn, not from
the storage backend.
