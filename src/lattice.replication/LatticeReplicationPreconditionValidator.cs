using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Replication;

/// <summary>
/// The default <see cref="ILatticeReplicationPreconditionValidator"/>. Enforces
/// the flag-CRDT membership invariant against the live
/// <see cref="ILatticeReplicationContext"/>: a tree declared or enabled under a
/// flag merge mode (<see cref="LatticeMergeMode.OrFlag"/> or
/// <see cref="LatticeMergeMode.RwFlag"/>) needs a non-empty
/// <see cref="ILatticeReplicationContext.LocalReplicaId"/> to author its
/// enable/disable dots, so a flag-mode request on a host without a configured
/// <see cref="LatticeReplicationOptions.ClusterId"/> is rejected.
/// </summary>
internal sealed class LatticeReplicationPreconditionValidator(
    ILatticeReplicationContext replicationContext) : ILatticeReplicationPreconditionValidator
{
    /// <inheritdoc />
    public LatticeReplicationPreconditionResult Validate(string treeId, LatticeMergeMode mode)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);

        var isFlagMode = mode is LatticeMergeMode.OrFlag or LatticeMergeMode.RwFlag;
        if (isFlagMode && string.IsNullOrEmpty(replicationContext.LocalReplicaId))
        {
            return LatticeReplicationPreconditionResult.Rejected(
                $"Tree '{treeId}' would replicate under the flag merge mode '{mode}', but no local "
                + "replica id is configured. Flag-CRDT membership authors its enable/disable dots with "
                + $"the local replica id, so {nameof(LatticeReplicationOptions)}."
                + $"{nameof(LatticeReplicationOptions.ClusterId)} must be set to a non-empty, "
                + "globally-unique cluster identifier. Set it, or use a non-flag merge mode for this "
                + "tree if it does not need multi-writer convergence.");
        }

        return LatticeReplicationPreconditionResult.Satisfied;
    }
}
