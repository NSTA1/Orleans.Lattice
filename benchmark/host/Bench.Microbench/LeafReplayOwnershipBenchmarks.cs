using BenchmarkDotNet.Attributes;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates the per-entry ownership judgement leaf WAL replay pass 1 makes for
/// every record it reads, so the cost of the issue #3601 disjoint-range check
/// is measurable in the clear, with no Orleans cluster in the loop.
/// <para>
/// Issue #3601 extended the pass-1 gate from <c>ShouldApply</c> alone to
/// <c>ShouldApply &amp;&amp; !IsDisjointRangeDelete</c>, so a range delete that cannot
/// overlap the leaf's key range is consumed instead of spending a durable
/// unresolved-work ledger slot. The <c>Baseline</c> arms run the old gate, the
/// <c>Gate</c> arms the new one, over the three mutation shapes the gate sees:
/// a key-scoped <c>Set</c> (the common case, where the new check short-circuits
/// on kind), a disjoint range delete, and an overlapping one.
/// </para>
/// <para>
/// Every arm must report <c>0 B</c> allocated: the check is a handful of ordinal
/// string comparisons over strings the mutation and the captured ownership
/// already hold, and the mutation is passed by <c>in</c> reference.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=replayownership</c> (or
/// <c>--suite replayownership</c>); see <c>Program.cs</c>.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class LeafReplayOwnershipBenchmarks
{
    private static readonly ShardMap Map = ShardMap.CreateDefault(64, 4);

    private readonly LeafReplayOwnership _ownership = LeafReplayOwnership.Capture(
        new LeafNodeState { ShardIndex = 1, LowKeyInclusive = "d", HighKeyExclusive = "g" },
        Map);

    private readonly LatticeMutation _set = new()
    {
        TreeId = "t", Kind = MutationKind.Set, Key = "e05", ShardIndex = 1,
    };

    private readonly LatticeMutation _disjointRange = new()
    {
        TreeId = "t", Kind = MutationKind.DeleteRange, Key = "m0", EndExclusiveKey = "m9", ShardIndex = 1,
    };

    private readonly LatticeMutation _overlappingRange = new()
    {
        TreeId = "t", Kind = MutationKind.DeleteRange, Key = "e0", EndExclusiveKey = "e9", ShardIndex = 1,
    };

    /// <summary>The pre-#3601 gate over a key-scoped <c>Set</c>.</summary>
    [Benchmark(Baseline = true)]
    public bool Baseline_Set() => _ownership.ShouldApply(_set);

    /// <summary>The #3601 gate over a key-scoped <c>Set</c>.</summary>
    [Benchmark]
    public bool Gate_Set() => _ownership.ShouldApply(_set) && !_ownership.IsDisjointRangeDelete(_set);

    /// <summary>The pre-#3601 gate over a disjoint range delete.</summary>
    [Benchmark]
    public bool Baseline_DisjointRange() => _ownership.ShouldApply(_disjointRange);

    /// <summary>The #3601 gate over a disjoint range delete.</summary>
    [Benchmark]
    public bool Gate_DisjointRange() =>
        _ownership.ShouldApply(_disjointRange) && !_ownership.IsDisjointRangeDelete(_disjointRange);

    /// <summary>The pre-#3601 gate over an overlapping range delete.</summary>
    [Benchmark]
    public bool Baseline_OverlappingRange() => _ownership.ShouldApply(_overlappingRange);

    /// <summary>The #3601 gate over an overlapping range delete.</summary>
    [Benchmark]
    public bool Gate_OverlappingRange() =>
        _ownership.ShouldApply(_overlappingRange) && !_ownership.IsDisjointRangeDelete(_overlappingRange);
}
