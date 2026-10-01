namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// The keys of the string result map a succeeded tree-administration operation
/// carries in <see cref="Operations.LatticeOperationStatus.Result"/>. Numbers are
/// written in invariant culture and booleans as <c>true</c> or <c>false</c>. Names
/// echo the caller's own unqualified tree, view and index names.
/// </summary>
public static class TreeAdminOperationResultKeys
{
    /// <summary>The view name (view kinds).</summary>
    public const string ViewName = "viewName";

    /// <summary>The view's source tree (view kinds).</summary>
    public const string SourceTreeId = "sourceTreeId";

    /// <summary>Whether a view reconcile found and repaired drift.</summary>
    public const string DriftRepaired = "driftRepaired";

    /// <summary>The tag-index name (tag-index reconcile).</summary>
    public const string IndexName = "indexName";

    /// <summary>The covered trees a tag-index sweep probed.</summary>
    public const string TreesCovered = "treesCovered";

    /// <summary>The subject keys a tag-index sweep scanned while repairing.</summary>
    public const string KeysScanned = "keysScanned";

    /// <summary>The membership rows a tag-index sweep scanned while repairing.</summary>
    public const string MembershipRowsScanned = "membershipRowsScanned";

    /// <summary>The orphan membership rows a tag-index sweep removed.</summary>
    public const string OrphanRowsRemoved = "orphanRowsRemoved";

    /// <summary>The tree (WAL move and orphaned-leaf kinds).</summary>
    public const string TreeId = "treeId";

    /// <summary>The moved WAL partition.</summary>
    public const string Partition = "partition";

    /// <summary>The provider key the partition was moved from.</summary>
    public const string FromProviderKey = "fromProviderKey";

    /// <summary>The provider key the partition was moved to.</summary>
    public const string ToProviderKey = "toProviderKey";

    /// <summary>The move outcome (a <see cref="TreeWalMoveOutcome"/> name).</summary>
    public const string Outcome = "outcome";

    /// <summary>The placement version before the move.</summary>
    public const string PreviousPlacementVersion = "previousPlacementVersion";

    /// <summary>The placement version after the move.</summary>
    public const string NewPlacementVersion = "newPlacementVersion";

    /// <summary>The first WAL offset copied, or <c>-1</c> when nothing was copied.</summary>
    public const string CopiedFromOffset = "copiedFromOffset";

    /// <summary>The last WAL offset copied, or <c>-1</c> when nothing was copied.</summary>
    public const string CopiedThroughOffset = "copiedThroughOffset";

    /// <summary>The source's highest offset at the cutover.</summary>
    public const string SourceHighestOffset = "sourceHighestOffset";

    /// <summary>The target's highest offset at the cutover.</summary>
    public const string TargetHighestOffset = "targetHighestOffset";

    /// <summary>The leaves an orphaned-leaf pass walked.</summary>
    public const string LeavesWalked = "leavesWalked";

    /// <summary>The orphaned leaves an orphaned-leaf pass found.</summary>
    public const string OrphanedLeaves = "orphanedLeaves";

    /// <summary>The orphaned leaves a repair unspliced.</summary>
    public const string Repaired = "repaired";

    /// <summary>The orphaned leaves an audit found the repair would unsplice.</summary>
    public const string Repairable = "repairable";

    /// <summary>The orphaned leaves left in place because they could not be shown safe.</summary>
    public const string Refused = "refused";

    /// <summary>The regions an orphaned-leaf pass could not judge; non-zero means the verdict is incomplete.</summary>
    public const string Gaps = "gaps";
}
