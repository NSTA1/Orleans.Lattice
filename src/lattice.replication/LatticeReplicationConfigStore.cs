namespace Orleans.Lattice.Replication;

/// <summary>
/// The default <see cref="ILatticeReplicationConfigStore"/>. Dogfoods the
/// reserved <see cref="LatticeSystemTreeNames.ReplicationConfig"/> <c>ILattice</c>
/// tree: the whole per-tree configuration is one
/// <see cref="Orleans.Lattice.OrMap{TKey, TValue}"/> stored under
/// <see cref="LatticeSystemTreeNames.ReplicationConfigMapKey"/>, so a single read
/// of that key returns every configured tree's
/// <see cref="LatticeReplicationConfigEntry"/>.
/// </summary>
/// <remarks>
/// The store is replication <b>infrastructure</b>: it reads the config tree that
/// feeds the membership and merge-mode seams the commit path consults, so the
/// read runs under <see cref="LatticeAccessGateContext.EnterSystemOrigin"/>. This
/// mirrors how <c>LatticeAuthorizationPolicyStore</c> scans the policy tree and
/// both avoids a bootstrap paradox (the config tree must be readable before any
/// authorization rule for it could exist) and breaks the re-entrancy cycle where
/// the snapshot maintainer's own background scan of the config tree would
/// otherwise call back into a cold access gate.
/// </remarks>
internal sealed class LatticeReplicationConfigStore(IGrainFactory grainFactory)
    : ILatticeReplicationConfigStore
{
    private ILattice ConfigTree =>
        grainFactory.GetGrain<ILattice>(LatticeSystemTreeNames.ReplicationConfig);

    /// <inheritdoc />
    public async Task<IReadOnlyDictionary<string, LatticeReplicationConfigEntry>> ReadEntriesAsync(
        CancellationToken cancellationToken = default)
    {
        var accessor = ConfigTree.OrMap<string, LatticeReplicationConfigEntry>(
            LatticeSystemTreeNames.ReplicationConfigMapKey);

        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            var map = await accessor.GetAsync(cancellationToken).ConfigureAwait(false);
            var result = new Dictionary<string, LatticeReplicationConfigEntry>(StringComparer.Ordinal);
            foreach (var treeId in map.Keys())
            {
                if (map.Get(treeId) is { } entry)
                {
                    result[treeId] = entry;
                }
            }

            return result;
        }
    }
}
