namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Core default <see cref="ILatticeOriginClusterIdResolver"/>. Returns
/// <see cref="string.Empty"/> for every tree id - single-cluster hosts have
/// no cluster id, so the resulting WAL record carries an empty
/// <see cref="WalRecord.OriginClusterId"/> and downstream consumers ignore
/// it.
/// <para>
/// Hosts that register the replication package replace this default with
/// <c>ConfiguredLatticeOriginClusterIdResolver</c>, which reads the per-tree
/// <c>LatticeReplicationOptions.ClusterId</c>.
/// </para>
/// </summary>
internal sealed class DefaultLatticeOriginClusterIdResolver : ILatticeOriginClusterIdResolver
{
    /// <summary>Always returns <see cref="string.Empty"/> - single-cluster hosts have no cluster id.</summary>
    public string Resolve(string treeId) => string.Empty;
}
