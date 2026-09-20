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
    /// Every region of this shard the batch could not establish a verdict
    /// over (issue 3301). Empty is the healthy answer; non-empty means the
    /// batch's findings do not clear the shard, however empty they are.
    /// </summary>
    [Id(3)] public IReadOnlyList<OrphanedLeafAuditGap> Gaps { get; init; }

    /// <summary>
    /// The key to resume from, or <see langword="null"/> when the whole chain
    /// has been examined.
    /// </summary>
    [Id(2)] public string? ResumeFromInclusive { get; init; }

    /// <summary>
    /// A completed batch that walked nothing and found nothing - what a shard
    /// with no chain to walk returns. A shard whose root is null or is itself
    /// a leaf has no chain in which a leaf could hide, so reporting completion
    /// here asserts something true rather than merely declining to look.
    /// </summary>
    internal static OrphanedLeafRepairPage Empty { get; } = new()
    {
        LeavesWalked = 0,
        Findings = [],
        Gaps = [],
        ResumeFromInclusive = null,
    };

    /// <summary>
    /// A batch that declined to walk at all, carrying the reason as a gap
    /// (issue 3301).
    /// <para>
    /// This used to be <see cref="Empty"/>, on the reasoning that a drive fans
    /// out over every physical shard and most have nothing wrong with them.
    /// That is true of the fan-out and false of the report: a declining shard
    /// contributed a zero indistinguishable from a clean one, so on a
    /// sixty-four shard tree the drive had sixty-four chances to turn "I did
    /// not look" into "there is nothing there".
    /// </para>
    /// </summary>
    internal static OrphanedLeafRepairPage Declined(int shardIndex, OrphanedLeafAuditGapReason reason) => new()
    {
        LeavesWalked = 0,
        Findings = [],
        Gaps = [new OrphanedLeafAuditGap { ShardIndex = shardIndex, Reason = reason }],
        ResumeFromInclusive = null,
    };
}
