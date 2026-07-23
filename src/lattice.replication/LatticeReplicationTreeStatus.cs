namespace Orleans.Lattice.Replication;

/// <summary>
/// A read-only projection of a single tree's runtime replication configuration,
/// returned by <see cref="ILatticeReplicationConfigAuthority.GetTreeStatusAsync"/>
/// and <see cref="ILatticeReplicationConfigAuthority.GetAllTreeStatusesAsync"/>.
/// It distills the tree's <see cref="LatticeReplicationConfigEntry"/> into the
/// three facts an operator surface needs: whether the tree is enabled, its
/// unambiguous declared merge mode, and whether the mode is currently ambiguous
/// (so shipping is paused fail-closed).
/// </summary>
/// <param name="TreeId">The target tree id.</param>
/// <param name="Enabled">
/// <see langword="true"/> when the tree's enablement flag is currently set
/// (at least one live enable dot and no surviving disable dot).
/// </param>
/// <param name="Mode">
/// The single unambiguous declared merge mode, or <see langword="null"/> when no
/// mode has been assigned or the mode is ambiguous. Always <see langword="null"/>
/// when <paramref name="Ambiguous"/> is <see langword="true"/>.
/// </param>
/// <param name="Ambiguous">
/// <see langword="true"/> when the tree's merge-mode register carries more than
/// one live value, i.e. concurrent clusters assigned divergent modes that have
/// not been reconciled. While this holds the resolver fails closed and pauses
/// shipping the tree until an operator disables then re-enables it.
/// </param>
public readonly record struct LatticeReplicationTreeStatus(
    string TreeId,
    bool Enabled,
    LatticeMergeMode? Mode,
    bool Ambiguous);
