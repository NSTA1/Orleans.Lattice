namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// The operation kinds the tree-administration facade runs as tracked long-running
/// operations (<see cref="ILatticeTreeAdminOperations"/>). Every kind starts with
/// <see cref="Prefix"/>, which is how the facade scopes the shared status, list and
/// cancel verbs to its own operations.
/// </summary>
public static class TreeAdminOperationKinds
{
    /// <summary>The prefix every tree-administration operation kind starts with.</summary>
    public const string Prefix = "treeadmin.";

    /// <summary>
    /// A materialised-view rebuild. Phases <see cref="TreeAdminOperationPhases.Scanning"/>,
    /// <see cref="TreeAdminOperationPhases.Projecting"/>, <see cref="TreeAdminOperationPhases.Swapping"/>
    /// (a replicated ShipView view is rewritten in place and reports no swap). Result
    /// reference: the view name.
    /// </summary>
    public const string ViewRebuild = "treeadmin.view-rebuild";

    /// <summary>
    /// A materialised-view reconcile. Phases <see cref="TreeAdminOperationPhases.Digesting"/>,
    /// <see cref="TreeAdminOperationPhases.Scanning"/>, <see cref="TreeAdminOperationPhases.Projecting"/>,
    /// <see cref="TreeAdminOperationPhases.Comparing"/>, and <see cref="TreeAdminOperationPhases.Swapping"/>
    /// only when drift was found. Result reference: the view name; see
    /// <see cref="TreeAdminOperationResultKeys.DriftRepaired"/>.
    /// </summary>
    public const string ViewReconcile = "treeadmin.view-reconcile";

    /// <summary>
    /// A tag-index reconcile sweep. Phases <see cref="TreeAdminOperationPhases.Probing"/>
    /// and, when a covered tree diverged, <see cref="TreeAdminOperationPhases.Repairing"/>.
    /// Result reference: the index name.
    /// </summary>
    public const string TagIndexReconcile = "treeadmin.tag-index-reconcile";

    /// <summary>
    /// A WAL partition move. Phases <see cref="TreeAdminOperationPhases.Copying"/> (only
    /// when the source holds live entries), <see cref="TreeAdminOperationPhases.Verifying"/>
    /// and <see cref="TreeAdminOperationPhases.Flipping"/>; a partition already at the
    /// target reports none. Result reference: the tree id.
    /// </summary>
    public const string WalMove = "treeadmin.wal-move";

    /// <summary>
    /// A whole-tree orphaned-leaf audit. Phase <see cref="TreeAdminOperationPhases.Walking"/>.
    /// Result reference: the tree id.
    /// </summary>
    public const string OrphanedLeavesAudit = "treeadmin.orphaned-leaves-audit";

    /// <summary>
    /// A whole-tree orphaned-leaf repair. Phase <see cref="TreeAdminOperationPhases.Walking"/>.
    /// Result reference: the tree id.
    /// </summary>
    public const string OrphanedLeavesRepair = "treeadmin.orphaned-leaves-repair";
}
