using System.Globalization;
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Api.TreeAdmin;

namespace Orleans.Lattice.Explorer.UI.Areas.Cluster;

/// <summary>
/// The totals a finished orphaned-leaf audit or repair operation reports (#4124): the
/// whole-tree pass ran on the cluster, so the page has its counts rather than each
/// leaf. The per-leaf findings are read on request with the bounded, paged audit.
/// </summary>
/// <param name="Repair">Whether the pass was a repair.</param>
/// <param name="LeavesWalked">The leaves walked.</param>
/// <param name="Orphaned">The orphaned leaves found.</param>
/// <param name="Repaired">The leaves unspliced (a repair only).</param>
/// <param name="Repairable">The leaves a repair would unsplice (an audit only).</param>
/// <param name="Refused">The leaves that could not be shown safe to unsplice.</param>
/// <param name="Gaps">The regions the pass could not judge.</param>
internal sealed record ClusterOrphanOperationResult(
    bool Repair,
    long LeavesWalked,
    long Orphaned,
    long Repaired,
    long Repairable,
    long Refused,
    long Gaps)
{
    /// <summary>Whether the pass rules orphaned leaves out: none found and every region judged.</summary>
    public bool IsClean => Orphaned == 0 && Gaps == 0;

    /// <summary>Reads the totals from a succeeded audit or repair operation's result.</summary>
    /// <param name="status">The succeeded operation's status.</param>
    /// <returns>The totals.</returns>
    public static ClusterOrphanOperationResult From(LatticeOperationStatus status)
    {
        ArgumentNullException.ThrowIfNull(status);
        var result = status.Result;
        long Number(string key) =>
            result.TryGetValue(key, out var text) && long.TryParse(text, NumberStyles.None, CultureInfo.InvariantCulture, out var value) ? value : 0;
        return new ClusterOrphanOperationResult(
            string.Equals(status.Kind, TreeAdminOperationKinds.OrphanedLeavesRepair, StringComparison.Ordinal),
            Number(TreeAdminOperationResultKeys.LeavesWalked),
            Number(TreeAdminOperationResultKeys.OrphanedLeaves),
            Number(TreeAdminOperationResultKeys.Repaired),
            Number(TreeAdminOperationResultKeys.Repairable),
            Number(TreeAdminOperationResultKeys.Refused),
            Number(TreeAdminOperationResultKeys.Gaps));
    }
}
