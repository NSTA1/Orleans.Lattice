namespace Orleans.Lattice.Auth;

/// <summary>
/// Convenience aggregates over <see cref="LatticeOperation"/> for authoring
/// authorization rules. The core <see cref="LatticeOperation"/> flags enum is
/// per-request vocabulary and deliberately carries no "all" aggregate, so this
/// helper supplies the grant-mask convenience the policy layer needs without
/// widening the core surface.
/// </summary>
public static class LatticeAuthOperations
{
    /// <summary>
    /// Every enforceable data-plane operation. A rule whose
    /// <see cref="LatticeAuthorizationRule.Operations"/> carries this mask covers
    /// the complete set of operations an access gate can authorize.
    /// </summary>
    /// <remarks>
    /// <see cref="LatticeOperation.Telemetry"/> is deliberately <b>not</b> part of
    /// this mask: it is a cluster-wide, scopeless capability rather than a
    /// tree-scoped data-plane operation, so a whole-data-plane grant never
    /// silently confers telemetry access. It must be granted explicitly.
    /// <see cref="LatticeOperation.Replication"/> and
    /// <see cref="LatticeOperation.TreeLifecycle"/> are likewise excluded: enabling
    /// replication egresses a tree's data to another cluster, and the tree
    /// lifecycle bit authorizes irreversible / structural whole-tree operations
    /// (drop, reshard, resize, WAL move), so neither is ever silently conferred by
    /// a whole-data-plane grant. Each must be granted explicitly and on its own.
    /// </remarks>
    public const LatticeOperation All =
        LatticeOperation.Read
        | LatticeOperation.Write
        | LatticeOperation.Delete
        | LatticeOperation.RangeRead
        | LatticeOperation.RangeDelete
        | LatticeOperation.CrdtApply
        | LatticeOperation.AtomicWrite
        | LatticeOperation.BulkLoad
        | LatticeOperation.Admin
        | LatticeOperation.Backup
        | LatticeOperation.Restore
        | LatticeOperation.SchemaAdmin;
}
