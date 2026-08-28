namespace Orleans.Lattice.Replication;

/// <summary>
/// Narrow read-only seam the runtime replication-configuration authoring path
/// uses to decide whether a target tree already holds data when replication is
/// enabled for it. Enabling replication on a non-empty tree must compose with a
/// snapshot bootstrap because the change feed only carries <i>new</i> mutations
/// (see <see cref="ILatticeReplicationConfigAuthority.EnableReplicationAsync"/>),
/// so the authority needs to know whether the tree holds anything at all.
/// <para>
/// The seam deliberately exposes the <em>boolean</em> the authority actually
/// needs rather than an entry count. It previously returned a count that the
/// only caller immediately reduced to <c>count &gt; 0</c>, which made the
/// cheapest possible question - "is there at least one row?" - cost a
/// strongly-consistent whole-tree fan-out that walks every leaf chain and
/// retries whenever the shard map moves under it. Asking for existence lets the
/// implementation stop at the first row it sees.
/// </para>
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
    /// Returns <see langword="true"/> when <paramref name="treeId"/> holds at
    /// least one live entry.
    /// </summary>
    /// <param name="treeId">The target tree id. Must be non-empty.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns><see langword="true"/> when the tree holds at least one live entry.</returns>
    Task<bool> HasContentAsync(string treeId, CancellationToken cancellationToken);
}
