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

    /// <summary>
    /// The sibling this leaf is currently dividing INTO, or <see langword="null"/>
    /// when no division of this leaf is in flight. See issue #2160.
    /// <para>
    /// Every other field here describes the leaf the probe was taken of. This
    /// one is the exception, and it has to be: it is read off the PREDECESSOR
    /// to decide the fate of its SUCCESSOR. A freshly seeded split sibling is
    /// indistinguishable from a reclaimable empty leaf by any evidence it
    /// carries itself - it is a brand new grain with a declared range, zero
    /// rows, <c>SplitState.Unsplit</c> and no seal, so its own
    /// <see cref="HasBlockingState"/> is legitimately false. The only
    /// participant that knows it is about to receive rows is the leaf dividing
    /// into it.
    /// </para>
    /// <para>
    /// <b>Reading it off the same probe as <see cref="NextSibling"/> is what
    /// makes the check race-free, and that is not an accident of convenience.</b>
    /// <c>SplitAsync</c> persists <c>SplitInFlight</c>, <c>SplitKey</c>,
    /// <c>SplitSiblingId</c> and <c>NextSibling</c> in ONE block before a
    /// single <c>PersistAsync()</c>, and <c>CompleteSplitAsync</c> clears the
    /// marker only at its very end, after the last row has moved. A reclaim
    /// walk reaches the new sibling only by following <see cref="NextSibling"/>,
    /// so any probe that can send the walk onto it necessarily observed the
    /// marker in the same read. There is no interval in which one is visible
    /// and the other is not.
    /// </para>
    /// <para>
    /// It is <c>SplitSiblingId</c> gated on an in-flight division, never the
    /// raw field. <c>SplitSiblingId</c> is not cleared when a division
    /// completes, so publishing it ungated would make every leaf that has ever
    /// split refuse to fold its successor for the rest of its life - disabling
    /// reclaim on exactly the busy trees it exists to tidy.
    /// </para>
    /// </summary>
    [Id(6)] public GrainId? SplitTargetSiblingId { get; init; }

    /// <summary>The leaf whose committed reclaim this predecessor still owes, or <see langword="null"/>.</summary>
    [Id(7)] public GrainId? PendingReclaimSuccessorId { get; init; }

    /// <summary>The leaf following <see cref="PendingReclaimSuccessorId"/> after the committed unlink.</summary>
    [Id(8)] public GrainId? PendingReclaimNextId { get; init; }
}
