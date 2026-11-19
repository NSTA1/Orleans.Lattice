namespace Orleans.Lattice.Backup;

/// <summary>
/// Why a tree's restore-copy retirement retires nothing even though it may hold
/// copies no revert can reach (issue #6103). Reported on
/// <see cref="LatticeRestoreCopyAudit.Hold"/>.
/// </summary>
public enum LatticeRestoreCopyHold
{
    /// <summary>
    /// Nothing is held: every copy in <see cref="LatticeRestoreCopyAudit.RetirablePhysicalTreeIds"/>
    /// is reachable by no revert and is retired.
    /// </summary>
    None = 0,

    /// <summary>
    /// The tree resolves to a restore shadow whose restore is still revertible but
    /// was committed before Lattice recorded the copy a revert returns to, so any
    /// of the tree's other restore copies may be the one it needs. Nothing is
    /// retired until that restore is reverted, superseded by a later cutover,
    /// released with <see cref="ILatticeBackupRestoreService.ReleaseRestoreAsync"/>,
    /// or its revert window (<see cref="LatticeBackupOptions.RestoreRevertRetention"/>)
    /// elapses.
    /// </summary>
    UnknownRevertTarget = 1,

    /// <summary>
    /// The tree resolves to a physical copy a shadow-cutover restore did not build
    /// (a resize, a schema remediation or an explicit alias), whose own lifecycle
    /// may still need a restore copy, so none is retired.
    /// </summary>
    ForeignAliasTarget = 2,

    /// <summary>The tree is not registered, so there is nothing to classify.</summary>
    TreeNotRegistered = 3,
}
