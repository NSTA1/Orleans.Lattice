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
/// clean bill of health for the tree, and is as useful an answer as a finding: it
/// rules the orphaned-leaf defect out as the cause of an unbounded WAL.
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
