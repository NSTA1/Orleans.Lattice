using System.Collections.Immutable;

namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// The verdict of an orphaned-leaf audit or repair over one tree: every leaf found
/// spliced into a shard's sibling chain but unreachable by descent from that
/// shard's root, and what was decided about each. The control-API mirror of the
/// core orphaned-leaf repair report DTO.
/// </summary>
/// <remarks>
/// <para>
/// An orphaned leaf is left behind by a split interrupted after it linked the new
/// sibling into the chain but before its parent learned about it. It is not
/// cosmetic: every leaf publishes a write-ahead-log materialiser pin and the trim
/// floor is the minimum over all of them, so a leaf nothing routes to never
/// checkpoints, its pin never advances, and the floor never rises. The WAL then
/// never trims, and because compaction runs strictly downstream of trim it never
/// compacts.
/// </para>
/// <para>
/// An empty <see cref="Findings"/> with a non-zero <see cref="LeavesWalked"/> is a
/// clean bill of health for the tree only when <see cref="VerdictComplete"/> is true, and is then as useful an answer as a finding:
/// it rules the orphaned-leaf defect out as the cause of an unbounded WAL. That
/// reading needs <b>both</b> <see cref="IsComplete"/> and
/// <see cref="VerdictComplete"/> to be <see langword="true"/>, because they answer
/// different questions. One call is one bounded batch, so on an incomplete batch an
/// empty <see cref="Findings"/> means "nothing wrong in the part of the tree this
/// batch reached". A non-empty <see cref="Gaps"/> means the pass could not judge
/// part of what it did reach, so the zero there is not evidence either (issue 3301).
/// </para>
/// </remarks>
[GenerateSerializer]
[Alias(ApiTreeAdminTypeAliases.TreeOrphanedLeafReport)]
[Immutable]
public sealed record TreeOrphanedLeafReport
{
    /// <summary>The tree this report describes, as the caller named it.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>
    /// <see langword="true"/> when the report came from the read-only audit, which
    /// changed nothing; <see langword="false"/> when it came from the repair.
    /// </summary>
    [Id(1)] public bool DryRun { get; init; }

    /// <summary>How many leaves were walked across every shard of the tree.</summary>
    [Id(2)] public int LeavesWalked { get; init; }

    /// <summary>
    /// One entry per descent-unreachable leaf found. Empty when the tree has no
    /// orphaned leaves.
    /// </summary>
    [Id(3)] public ImmutableArray<TreeOrphanedLeafFinding> Findings { get; init; } = [];

    /// <summary>
    /// The opaque position the next batch resumes from, or <see langword="null"/>
    /// when every shard of the tree has been examined to the end of its chain.
    /// Hand it back unaltered to continue. It names a position in the keyspace
    /// rather than any server-side state, so it never expires and a pass may be
    /// resumed, abandoned, or restarted at any time.
    /// </summary>
    [Id(4)] public string? ResumeFrom { get; init; }

    /// <summary>
    /// Whether this batch reached the end of the last shard's chain, so the whole
    /// tree has now been examined. <see langword="false"/> means the batch ran out
    /// of work budget and its counts describe only the part of the tree it reached.
    /// </summary>
    public bool IsComplete => ResumeFrom is null;

    /// <summary>
    /// Every region of the tree the pass could not establish a verdict over, in
    /// shard order (issue 3301). Empty is the healthy answer.
    /// <para>
    /// Read this before reading <see cref="Findings"/>. A shard that declined, or a
    /// sibling chain severed part-way across the keyspace, contributes zero
    /// findings by construction, and on a tree wide enough to matter that zero is
    /// indistinguishable from health unless the gap is surfaced.
    /// </para>
    /// </summary>
    [Id(5)] public ImmutableArray<TreeOrphanedLeafGap> Gaps { get; init; } = [];

    /// <summary>Whether this batch requested the opt-in, read-only full key census.</summary>
    [Id(6)] public bool Survey { get; init; }

    /// <summary>Number of orphaned leaves found in this batch, including refusals.</summary>
    public int OrphanedLeafCount => Findings.IsDefaultOrEmpty ? 0 : Findings.Length;

    /// <summary>Number of leaves this batch proved safe for repair, with their positions in Findings.</summary>
    public int RepairableCount
    {
        get
        {
            var count = 0;
            if (Findings.IsDefaultOrEmpty) return count;
            foreach (var finding in Findings)
                if (finding.Disposition == TreeOrphanedLeafDisposition.Repairable) count++;
            return count;
        }
    }

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
            if (Findings.IsDefaultOrEmpty) return count;
            foreach (var finding in Findings)
            {
                if (finding.SurveyMissingKeyCount is not { } missing) return null;
                count += missing;
            }
            return count;
        }
    }

    /// <summary>
    /// Whether the pass could establish a verdict over everything it reached, and
    /// so whether <see cref="Findings"/> may be read as a verdict over it.
    /// <para>
    /// <b>An operator deciding that a tree needs no attention must check this, and
    /// <see cref="IsComplete"/>, before checking <see cref="Findings"/>.</b> False
    /// means the answer is "I could not establish this", not "there is nothing
    /// here".
    /// </para>
    /// </summary>
    public bool VerdictComplete => Gaps.IsDefaultOrEmpty;

    /// <summary>
    /// How many leaves the repair unspliced. Always zero for an audit, whose
    /// candidates are reported as
    /// <see cref="TreeOrphanedLeafDisposition.Repairable"/>.
    /// </summary>
    public int RepairedCount
    {
        get
        {
            if (Findings.IsDefaultOrEmpty)
            {
                return 0;
            }

            var count = 0;
            foreach (var finding in Findings)
            {
                if (finding.Disposition == TreeOrphanedLeafDisposition.Repaired)
                {
                    count++;
                }
            }

            return count;
        }
    }

    /// <summary>
    /// How many orphaned leaves were deliberately left alone because unsplicing
    /// them could not be shown to be safe.
    /// </summary>
    public int RefusedCount
    {
        get
        {
            if (Findings.IsDefaultOrEmpty)
            {
                return 0;
            }

            var count = 0;
            foreach (var finding in Findings)
            {
                if (finding.IsRefusal)
                {
                    count++;
                }
            }

            return count;
        }
    }
}
