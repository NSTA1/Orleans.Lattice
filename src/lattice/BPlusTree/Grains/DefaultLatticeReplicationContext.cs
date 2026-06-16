namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Core default <see cref="ILatticeReplicationContext"/> for a single-cluster
/// host: replication is not enabled, there is no local replica id, and no tree
/// has a declared merge mode. Features that consume the seam therefore use
/// their single-writer path until the replication package replaces this
/// registration with <c>ConfiguredLatticeReplicationContext</c>.
/// </summary>
internal sealed class DefaultLatticeReplicationContext : ILatticeReplicationContext
{
    /// <summary>Always <c>false</c> - a single-cluster host does not replicate.</summary>
    public bool IsReplicationEnabled => false;

    /// <summary>Always <see cref="string.Empty"/> - a single-cluster host has no replica id.</summary>
    public string LocalReplicaId => string.Empty;

    /// <summary>Always <c>null</c> - a single-cluster host replicates no trees.</summary>
    public LatticeMergeMode? ResolveMergeMode(string treeId) => null;
}
