namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Core default <see cref="ILatticeMergeModeResolver"/>. Returns
/// <c>null</c> for every tree id - the WAL writer treats <c>null</c> as
/// <see cref="LatticeMergeMode.LwwRegister"/> for durability purposes, and
/// the replication-package observer short-circuits cross-cluster ship-out
/// when the resolver returns <c>null</c>.
/// <para>
/// Hosts that register the replication package replace this default with
/// <c>ConfiguredLatticeMergeModeResolver</c>, which honours the per-tree
/// <c>LatticeReplicationOptions.ReplicatedTrees</c> map.
/// </para>
/// </summary>
internal sealed class DefaultLatticeMergeModeResolver : ILatticeMergeModeResolver
{
    /// <summary>Always returns <c>null</c> - single-cluster hosts do not replicate.</summary>
    public LatticeMergeMode? Resolve(string treeId) => null;
}
