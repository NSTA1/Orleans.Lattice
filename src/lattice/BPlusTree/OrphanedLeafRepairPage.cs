namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// One work-bounded batch of a shard's orphaned-leaf repair walk (see
/// <see cref="IShardRootGrain.RepairOrphanedLeavesAsync"/>).
/// <para>
/// The walk visits a bounded number of leaves and then returns, releasing the
/// non-reentrant shard so other traffic can interleave. The caller drives
/// batches until <see cref="ResumeFromInclusive"/> is <see langword="null"/>,
/// at which point the whole chain has been examined. This is the same shape
/// <see cref="ShardProjectionRebuildPage"/> uses, and for the same reason: a
/// single-call walk of a long chain head-of-line blocks every read and write
/// on the shard behind thousands of sequential grain calls.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.OrphanedLeafRepairPage)]
[Immutable]
internal readonly record struct OrphanedLeafRepairPage
{
    /// <summary>Leaves this batch walked.</summary>
    [Id(0)] public int LeavesWalked { get; init; }

    /// <summary>Every descent-unreachable leaf this batch found, in chain order.</summary>
    [Id(1)] public IReadOnlyList<OrphanedLeafFinding> Findings { get; init; }

    /// <summary>
    /// The key to resume from, or <see langword="null"/> when the whole chain
    /// has been examined.
    /// </summary>
    [Id(2)] public string? ResumeFromInclusive { get; init; }

    /// <summary>
    /// A completed batch that walked nothing and found nothing - what a shard
    /// with no chain to walk returns. Declining is reported as completion
    /// rather than as an error because a repair drive fans out over every
    /// physical shard and most of them will have nothing wrong with them.
    /// </summary>
    internal static OrphanedLeafRepairPage Empty { get; } = new()
    {
        LeavesWalked = 0,
        Findings = [],
        ResumeFromInclusive = null,
    };
}
