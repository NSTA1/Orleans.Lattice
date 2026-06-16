using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Minimal <see cref="ILatticeReplicationContext"/> stub for active-active
/// integration tests. Models a replicated host whose local replica id is
/// <paramref name="localReplicaId"/> and that declares every tree under
/// <paramref name="mode"/>, mirroring what the configured seam would report for
/// the index tree the test declares in
/// <see cref="TwoSiteClusterFixture.TreeModeOverrides"/>.
/// </summary>
internal sealed class StubReplicationContext(string localReplicaId, LatticeMergeMode mode)
    : ILatticeReplicationContext
{
    public bool IsReplicationEnabled => true;

    public string LocalReplicaId => localReplicaId;

    public LatticeMergeMode? ResolveMergeMode(string treeId) => mode;
}
