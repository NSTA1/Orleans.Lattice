namespace Orleans.Lattice.Replication;

/// <summary>
/// Narrow read-only seam the runtime replication-configuration authoring path
/// uses to decide whether a target tree already holds data when replication is
/// enabled for it. Enabling replication on a non-empty tree must compose with a
/// snapshot bootstrap because the change feed only carries <i>new</i> mutations
/// (see <see cref="ILatticeReplicationConfigAuthority.EnableReplicationAsync"/>),
/// so the authority needs the tree's current entry count.
/// <para>
/// The seam exists so the authority can be unit-tested without an Orleans
/// cluster: the default implementation resolves the target
/// <see cref="ILattice"/> grain through the grain factory, but a test can
/// substitute a deterministic stub.
/// </para>
/// </summary>
internal interface ILatticeTreeContentProbe
{
    /// <summary>
    /// Returns the current number of live entries in <paramref name="treeId"/>.
    /// </summary>
    /// <param name="treeId">The target tree id. Must be non-empty.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The tree's live entry count.</returns>
    Task<int> CountAsync(string treeId, CancellationToken cancellationToken);
}
