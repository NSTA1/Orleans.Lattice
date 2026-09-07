using Orleans.Lattice;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Everything the shard root needs, in one round trip, to decide whether a
/// leaf may be reclaimed from the leaf chain when its key range has shrunk
/// back to nothing.
/// <para>
/// A reclaim pass walks the whole chain, so the cost of the decision is paid
/// once per leaf on a chain that may be thousands of leaves long. Gathering
/// the live-row count, the chain linkage, the owned range and the two safety
/// interlocks as four separate accessor calls would multiply that walk by
/// four; folding them into a single probe keeps a maintenance pass over a
/// degenerate chain proportional to its length rather than a multiple of it.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LeafReclaimProbe)]
[Immutable]
internal readonly record struct LeafReclaimProbe
{
    /// <summary>Number of live (non-tombstoned) rows the leaf currently holds.</summary>
    [Id(0)] public int LiveRowCount { get; init; }

    /// <summary>The leaf's predecessor in the chain, or <see langword="null"/> when it is the head.</summary>
    [Id(1)] public GrainId? PrevSibling { get; init; }

    /// <summary>The leaf's successor in the chain, or <see langword="null"/> when it is the tail.</summary>
    [Id(2)] public GrainId? NextSibling { get; init; }

    /// <summary>Inclusive low bound of the leaf's owned key range.</summary>
    [Id(3)] public string? LowKeyInclusive { get; init; }

    /// <summary>Exclusive high bound of the leaf's owned key range.</summary>
    [Id(4)] public string? HighKeyExclusive { get; init; }

    /// <summary>
    /// <see langword="true"/> when the leaf carries state that makes reclaiming it
    /// unsafe regardless of how empty it looks: a split that has published its
    /// intent but not completed, a moved-away slot seal whose removal would
    /// resurrect orphan values, or an unresolved prepared transaction whose commit
    /// would land rows on a leaf that is no longer in the chain.
    /// </summary>
    [Id(5)] public bool HasBlockingState { get; init; }
}
