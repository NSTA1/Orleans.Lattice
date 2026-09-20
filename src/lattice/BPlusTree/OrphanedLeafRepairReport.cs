namespace Orleans.Lattice;

/// <summary>
/// The result of one <b>bounded batch</b> of an orphaned-leaf inspection or
/// repair across the physical shards of a tree. See
/// <see cref="ILattice.InspectOrphanedLeavesAsync"/> and
/// <see cref="ILattice.RepairOrphanedLeavesAsync"/>.
/// <para>
/// <b>Read <see cref="IsComplete"/> before reading <see cref="Findings"/>.</b>
/// A batch stops when its work budget is spent and names where to resume in
/// <see cref="ResumeFrom"/>, so an empty <see cref="Findings"/> means "no
/// orphan in the part of the tree this batch reached", not "no orphan in the
/// tree". Only an empty <see cref="Findings"/> on a batch whose
/// <see cref="IsComplete"/> is <see langword="true"/> - or on the last of a
/// run of batches driven to completion - is the healthy whole-tree answer.
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
    /// The opaque position the next batch resumes from, or
    /// <see langword="null"/> when every shard of the tree has been examined to
    /// the end of its chain.
    /// <para>
    /// Hand it back unaltered to
    /// <see cref="ILattice.RepairOrphanedLeavesAsync"/> (or
    /// <see cref="ILattice.InspectOrphanedLeavesAsync"/>) to continue. It is
    /// not a handle to any server-side state and it does not expire: it names a
    /// position in the keyspace, so a pass may be resumed, abandoned, or
    /// restarted from <see langword="null"/> at any time.
    /// </para>
    /// </summary>
    [Id(3)] public string? ResumeFrom { get; init; }

    /// <summary>
    /// Whether this batch reached the end of the last shard's chain, so the
    /// whole tree has now been examined.
    /// <para>
    /// <b>This is the field that makes a partial answer legible.</b> A batch
    /// that ran out of budget reports <see langword="false"/> here and its
    /// counts describe only what it reached; treating them as whole-tree
    /// figures would read a bounded walk as a clean tree.
    /// </para>
    /// </summary>
    public bool IsComplete => ResumeFrom is null;

    /// <summary>
    /// Every descent-unreachable leaf found, in chain order within each shard
    /// and shard order across the tree.
    /// </summary>
    [Id(2)] public IReadOnlyList<OrphanedLeafFinding> Findings { get; init; }

    /// <summary>
    /// How many orphans were unspliced and had their materialiser pins
    /// retired <b>by this batch</b>. Always zero when <see cref="DryRun"/> is
    /// <see langword="true"/>.
    /// <para>
    /// It counts this batch only, so driving a pass to completion means summing
    /// it across batches. Do not read a zero on its own as "nothing was wrong":
    /// on a resumed batch it means only that this batch found nothing.
    /// </para>
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
