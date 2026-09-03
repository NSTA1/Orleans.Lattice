namespace Orleans.Lattice.Replication;

/// <summary>
/// A read-only projection of a single tree's effective replication
/// configuration, returned by
/// <see cref="ILatticeReplicationConfigAuthority.GetTreeStatusAsync"/> and
/// <see cref="ILatticeReplicationConfigAuthority.GetAllTreeStatusesAsync"/>. It
/// reconciles the two enrollment sources a replication-enabled host actually
/// resolves against - the runtime
/// <see cref="LatticeSystemTreeNames.ReplicationConfig"/> OR-Map and the static
/// <see cref="LatticeReplicationOptions.ReplicatedTrees"/> deployment map - into
/// the facts an operator surface needs: whether the tree is enrolled, the merge
/// mode in force, whether that mode is currently ambiguous (so shipping is
/// paused fail-closed), and which source put it in force.
/// </summary>
/// <param name="TreeId">The target tree id.</param>
/// <param name="Enabled">
/// <see langword="true"/> when the tree is <b>effectively enrolled</b>, i.e. the
/// merge-mode resolver admits its mutations for shipping. That is the runtime
/// enablement flag (at least one live enable dot and no surviving disable dot)
/// when the runtime entry is in force, and always <see langword="true"/> for a
/// tree the static deployment map declares - the static map is a floor, so a
/// runtime disable does not stop a statically declared tree.
/// </param>
/// <param name="Mode">
/// The single unambiguous merge mode in force, or <see langword="null"/> when no
/// mode has been assigned or the mode is ambiguous. Always <see langword="null"/>
/// when <paramref name="Ambiguous"/> is <see langword="true"/>.
/// </param>
/// <param name="Ambiguous">
/// <see langword="true"/> when the tree's merge-mode register carries more than
/// one live value, i.e. concurrent clusters assigned divergent modes that have
/// not been reconciled. While this holds the resolver fails closed and pauses
/// shipping the tree until an operator disables then re-enables it. Ambiguity
/// wins over a static declaration, exactly as it does on the commit path.
/// </param>
public readonly record struct LatticeReplicationTreeStatus(
    string TreeId,
    bool Enabled,
    LatticeMergeMode? Mode,
    bool Ambiguous)
{
    /// <summary>
    /// Which enrollment source put this tree's reported configuration in force -
    /// the runtime config tree, the static deployment map, or both with the
    /// runtime entry winning. Defaults to
    /// <see cref="LatticeReplicationEnrollmentSource.Runtime"/> so a status
    /// projected from a runtime entry alone reads correctly without setting it.
    /// </summary>
    public LatticeReplicationEnrollmentSource Source { get; init; }
}
