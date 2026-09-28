using Orleans.Lattice.Api.TreeAdmin;

namespace Orleans.Lattice.Explorer.Shell.Areas.Cluster;

/// <summary>
/// One orphaned-leaf pass driven to completion across bounded batches: the
/// findings and gaps every batch reported, accumulated. An empty finding list is a
/// clean bill of health only when the pass is complete and judged every region
/// (<see cref="IsClean"/>); otherwise it says only that the part reached was clean.
/// </summary>
/// <param name="Kind">"Audit", "Survey" or "Repair".</param>
internal sealed class ClusterOrphanPass(string Kind)
{
    /// <summary>"Audit", "Survey" or "Repair".</summary>
    public string Kind { get; } = Kind;

    /// <summary>Batches run.</summary>
    public int Batches { get; private set; }

    /// <summary>Leaves walked across every batch.</summary>
    public long LeavesWalked { get; private set; }

    /// <summary>Whether the last batch reported no resume position.</summary>
    public bool Complete { get; private set; }

    /// <summary>The resume token for the next batch, while incomplete.</summary>
    public string? ResumeFrom { get; private set; }

    /// <summary>Every finding, in the order reported.</summary>
    public List<TreeOrphanedLeafFinding> Findings { get; } = [];

    /// <summary>Every region the pass could not judge.</summary>
    public List<TreeOrphanedLeafGap> Gaps { get; } = [];

    /// <summary>Findings a repair would unsplice.</summary>
    public int Repairable => Findings.Count(finding => finding.Disposition == TreeOrphanedLeafDisposition.Repairable);

    /// <summary>Whether the pass rules orphaned leaves out: complete, every region judged, nothing found.</summary>
    public bool IsClean => Complete && Gaps.Count == 0 && Findings.Count == 0;

    /// <summary>Folds one batch's report in.</summary>
    /// <param name="report">The batch's report.</param>
    public void Add(TreeOrphanedLeafReport report)
    {
        ArgumentNullException.ThrowIfNull(report);

        Batches++;
        LeavesWalked += report.LeavesWalked;
        if (!report.Findings.IsDefault)
        {
            Findings.AddRange(report.Findings);
        }

        if (!report.Gaps.IsDefault)
        {
            Gaps.AddRange(report.Gaps);
        }

        ResumeFrom = report.ResumeFrom;
        Complete = report.ResumeFrom is null;
    }
}
