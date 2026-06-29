# MvRegister merge HashSet-elimination A/B benchmark results

## Purpose

`MvRegister.MergeFrom` (the in-place fold behind the static
`MvRegister.Merge`, the per-write `MergeDelta` path, and every
replication state merge) built two transient
`HashSet<(string ReplicaId, long Counter)>`s - one over each side's
live dots - purely to answer "is this dot still present on the other
side?" before applying the dominance rule. A register is single-valued
in the steady state and only transiently multi-valued, so both entry
lists are tiny. The candidate replaces the two sets with a linear
`ContainsDot` scan over the other side's entries, eliminating both
allocations on the replication hot path. The same-dot-on-both-sides
survivor rule and pointwise-max context fold are preserved, so the merge
stays commutative, associative, and idempotent.

The `Crdt mvregister merge` workload in `LatticeMicroBenchmarks.cs`
isolates the primitive merge of two identity-stable concurrent-replica
states (each carrying an 8-replica shared context plus one own live
entry), with the inputs never mutated so the measured per-iteration
allocation is the steady-state cost of one register merge.

## Run command

```pwsh
dotnet run -c Release --project benchmark/host/Bench.Microbench `
    -- --filter "*CrdtMvRegisterMerge*"
```

Job=ShortRun (WarmupCount=3, IterationCount=3, LaunchCount=1),
InProcessEmitToolchain, MemoryDiagnoser. AMD Ryzen 7 PRO 7840U,
.NET 10.0.9, BenchmarkDotNet 0.15.8.

## Results

| Variant   | Mean      | Gen0   | Allocated |
|-----------|----------:|-------:|----------:|
| baseline  | 370.9 ns  | 0.1392 |  1,168 B  |
| candidate | 267.2 ns  | 0.0916 |    768 B  |

Allocated/op: **1,168 B -> 768 B (-400 B, ~34%)**; mean ~28% lower.
The two eliminated `HashSet`s account for the full 400 B drop. All 696
primitives/CRDT tests pass unchanged.
