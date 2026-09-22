namespace Orleans.Lattice;

/// <summary>
/// The result of one <b>bounded batch</b> of an orphaned-leaf inspection or
/// repair across the physical shards of a tree. See
/// <see cref="ILattice.InspectOrphanedLeavesAsync"/> and
/// <see cref="ILattice.RepairOrphanedLeavesAsync"/>.
/// <para>
/// <b>Read <see cref="IsComplete"/> and <see cref="VerdictComplete"/> before
/// reading <see cref="Findings"/>.</b> They answer two different questions and
/// an empty findings list is the healthy whole-tree answer only when both are
/// <see langword="true"/>.
/// </para>
/// <para>
/// <see cref="IsComplete"/> asks <i>how far did the pass get</i>. A batch stops
/// when its work budget is spent and names where to resume in
/// <see cref="ResumeFrom"/>, so an empty <see cref="Findings"/> means "no orphan
/// in the part of the tree this batch reached", not "no orphan in the tree".
/// </para>
/// <para>
/// <see cref="VerdictComplete"/> asks <i>could the pass judge what it reached</i>.
/// A shard that declined, or a sibling chain severed part-way across the
/// keyspace, contributes zero findings by construction, so the pass reports
/// those regions in <see cref="Gaps"/> rather than letting the resulting zero
/// read as health (issue 3301).
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
    /// Every region of the tree the pass could not establish a verdict over,
    /// in shard order (issue 3301).
    /// <para>
    /// A gap is not a finding and not an error - it is the pass reporting
    /// where it did not look, which is the one thing an empty findings list
    /// could not previously express. It is distinct from
    /// <see cref="IsComplete"/>: a batch may stop early with its budget spent
    /// and still have judged every leaf it touched, and a batch may run to the
    /// end of the tree and still have been unable to judge part of it.
    /// </para>
    /// </summary>
    [Id(4)] public IReadOnlyList<OrphanedLeafAuditGap> Gaps { get; init; }

    /// <summary>Whether this batch requested the opt-in, read-only full key census.</summary>
    [Id(5)] public bool Survey { get; init; }

    /// <summary>Number of orphaned leaves found in this batch, including refusals.</summary>
    public int OrphanedLeafCount => Findings?.Count ?? 0;

    /// <summary>Number of leaves this batch proved safe for repair, with their positions in Findings.</summary>
    public int RepairableCount
        => Findings?.Count(f => f.Disposition == OrphanedLeafDisposition.Repairable) ?? 0;

    /// <summary>
    /// Total missing keys in this batch's survey, or null when not requested or
    /// any reached region or orphan could not be fully surveyed. Zero does not
    /// rule out routing contradictions. Not a whole-tree total until all batches have
    /// been collected; not evidence of data loss. Per-leaf counts and positions
    /// remain in Findings even when this total is unknown.
    /// </summary>
    public long? SurveyMissingKeyCount
    {
        get
        {
            if (!Survey || !VerdictComplete) return null;
            long count = 0;
            if (Findings is null) return count;
            foreach (var finding in Findings)
            {
                if (finding.SurveyMissingKeyCount is not { } missing) return null;
                count += missing;
            }
            return count;
        }
    }

    /// <summary>
    /// Whether the pass could establish a verdict over everything it reached,
    /// and so whether <see cref="Findings"/> may be read as a verdict over it.
    /// <para>
    /// <b>An operator deciding that a tree needs no attention must check this,
    /// and <see cref="IsComplete"/>, before checking <see cref="Findings"/>.</b>
    /// False means the answer is "I could not establish this", not "there is
    /// nothing here".
    /// </para>
    /// </summary>
    public bool VerdictComplete => Gaps is null or { Count: 0 };

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
