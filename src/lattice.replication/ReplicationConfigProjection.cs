namespace Orleans.Lattice.Replication;

/// <summary>
/// The projected runtime replication configuration for a single target tree,
/// distilled from that tree's <see cref="LatticeReplicationConfigEntry"/> into
/// the three facts the commit hot path needs: whether the tree is enabled, its
/// unambiguous declared merge mode (when one exists), and whether the mode is
/// ambiguous (more than one live value survived a concurrent divergent
/// assignment).
/// </summary>
/// <param name="Enabled">
/// <see langword="true"/> when the tree's enablement flag is currently set
/// (at least one live enable dot and no surviving disable dot).
/// </param>
/// <param name="Mode">
/// The single unambiguous declared merge mode, or <see langword="null"/> when
/// no mode has been assigned or the mode is ambiguous. Always
/// <see langword="null"/> when <paramref name="Ambiguous"/> is
/// <see langword="true"/>.
/// </param>
/// <param name="Ambiguous">
/// <see langword="true"/> when the tree's merge-mode register carries more than
/// one live value, i.e. concurrent clusters assigned divergent modes that have
/// not been reconciled. A reader must fail closed (pause shipping the tree)
/// while this holds.
/// </param>
internal readonly record struct ReplicationConfigProjection(
    bool Enabled,
    LatticeMergeMode? Mode,
    bool Ambiguous);
