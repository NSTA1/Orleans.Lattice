namespace Orleans.Lattice;

/// <summary>
/// The result of one orphaned-leaf inspection or repair across every physical
/// shard of a tree. See <see cref="ILattice.InspectOrphanedLeavesAsync"/> and
/// <see cref="ILattice.RepairOrphanedLeavesAsync"/>.
/// <para>
/// A report with an empty <see cref="Findings"/> list is the healthy answer:
/// every leaf in every shard's sibling chain was reachable by descent.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.OrphanedLeafRepairReport)]
[Immutable]
public readonly record struct OrphanedLeafRepairReport
{
    /// <summary>
    /// Whether this was an inspection (no mutation of any kind) rather than a
    /// repair. An inspection reports
    /// <see cref="OrphanedLeafDisposition.Repairable"/> where a repair would
    /// report <see cref="OrphanedLeafDisposition.Repaired"/>; every other
    /// disposition means the same thing in both.
    /// </summary>
    [Id(0)] public bool DryRun { get; init; }

    /// <summary>How many leaves were walked across every shard.</summary>
    [Id(1)] public int LeavesWalked { get; init; }

    /// <summary>
    /// Every descent-unreachable leaf found, in chain order within each shard
    /// and shard order across the tree.
    /// </summary>
    [Id(2)] public IReadOnlyList<OrphanedLeafFinding> Findings { get; init; }

    /// <summary>
    /// How many orphans were unspliced and had their materialiser pins
    /// retired. Always zero when <see cref="DryRun"/> is
    /// <see langword="true"/>.
    /// </summary>
    public int RepairedCount
        => Findings?.Count(f => f.Disposition == OrphanedLeafDisposition.Repaired) ?? 0;

    /// <summary>
    /// How many orphans the pass refused to act on. A non-zero value needs an
    /// operator: the pass fails closed, so a refusal is a leaf it does not
    /// understand well enough to touch, not a transient failure to retry.
    /// </summary>
    public int RefusedCount => Findings?.Count(f => f.IsRefusal) ?? 0;
}
