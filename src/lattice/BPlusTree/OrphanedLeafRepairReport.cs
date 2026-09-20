namespace Orleans.Lattice;

/// <summary>
/// The result of one orphaned-leaf inspection or repair across every physical
/// shard of a tree. See <see cref="ILattice.InspectOrphanedLeavesAsync"/> and
/// <see cref="ILattice.RepairOrphanedLeavesAsync"/>.
/// <para>
/// A report with an empty <see cref="Findings"/> list is the healthy answer
/// <b>only when <see cref="VerdictComplete"/> is also
/// <see langword="true"/></b>: every leaf in every shard's sibling chain was
/// reached and every one of them was reachable by descent. When
/// <see cref="Gaps"/> is non-empty the pass did not examine the whole tree,
/// and an empty findings list says nothing about the part it did not reach
/// (issue 3301).
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
    /// Every region of the tree the pass could not establish a verdict over,
    /// in shard order (issue 3301).
    /// <para>
    /// A gap is not a finding and not an error - it is the pass reporting
    /// where it did not look, which is the one thing an empty findings list
    /// could not previously express. Read it before reading
    /// <see cref="Findings"/>: a shard that declined, or a sibling chain
    /// severed part-way across the keyspace, contributes zero findings by
    /// construction, and on a tree wide enough to matter that zero is
    /// indistinguishable from health unless the gap is surfaced.
    /// </para>
    /// </summary>
    [Id(3)] public IReadOnlyList<OrphanedLeafAuditGap> Gaps { get; init; }

    /// <summary>
    /// Whether the pass examined the whole tree, and so whether
    /// <see cref="Findings"/> may be read as a verdict over it.
    /// <para>
    /// <b>An operator deciding that a tree needs no attention must check this
    /// before checking <see cref="Findings"/>.</b> False means the answer is
    /// "I could not establish this", not "there is nothing here".
    /// </para>
    /// </summary>
    public bool VerdictComplete => Gaps is null or { Count: 0 };

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
