using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.Fakes;

/// <summary>
/// Test double for <see cref="ILatticeReplicationContext"/>. Lets a test declare
/// the membership convergence mode and replica id a tag index should observe
/// without standing up the replication package. Pass a non-null
/// <paramref name="mode"/> to simulate a replicated index tree declared under
/// that merge mode; leave it <c>null</c> (the default) to model a tree the
/// configured host does not replicate.
/// </summary>
internal sealed class FakeLatticeReplicationContext(
    bool isReplicationEnabled,
    string localReplicaId,
    LatticeMergeMode? mode = null) : ILatticeReplicationContext
{
    /// <summary>A disabled context: the single-cluster default behaviour.</summary>
    public static FakeLatticeReplicationContext Disabled { get; } =
        new(isReplicationEnabled: false, localReplicaId: string.Empty);

    /// <summary>An enabled context that declares every tree under <paramref name="mode"/>.</summary>
    public static FakeLatticeReplicationContext Enabled(string replicaId, LatticeMergeMode? mode) =>
        new(isReplicationEnabled: true, localReplicaId: replicaId, mode: mode);

    /// <inheritdoc />
    public bool IsReplicationEnabled => isReplicationEnabled;

    /// <inheritdoc />
    public string LocalReplicaId => localReplicaId;

    /// <inheritdoc />
    public LatticeMergeMode? ResolveMergeMode(string treeId) => mode;
}
