namespace Orleans.Lattice.Replication;

/// <summary>
/// Validates the runtime preconditions that must hold before a tree may
/// replicate under a given <see cref="LatticeMergeMode"/>. This is the reusable
/// seam that both the boot-time
/// <see cref="LatticeReplicationMergeModeStartupValidator"/> (over statically
/// declared trees) and the later runtime enable path share, so a flag-mode tree
/// declared or enabled without the state its CRDT needs is rejected cleanly
/// rather than faulting on its first write.
/// </summary>
/// <remarks>
/// The only precondition enforced today is the flag-CRDT membership invariant:
/// a tree declared under a flag merge mode
/// (<see cref="LatticeMergeMode.OrFlag"/> or <see cref="LatticeMergeMode.RwFlag"/>)
/// authors its enable/disable dots with the local replica id, so it requires a
/// non-empty <see cref="ILatticeReplicationContext.LocalReplicaId"/>. Additional
/// preconditions can be layered here without changing callers.
/// </remarks>
public interface ILatticeReplicationPreconditionValidator
{
    /// <summary>
    /// Checks whether <paramref name="treeId"/> may replicate under
    /// <paramref name="mode"/> given the host's current replication context.
    /// </summary>
    /// <param name="treeId">The target tree id. Must be non-empty.</param>
    /// <param name="mode">The wire merge mode the tree would replicate under.</param>
    /// <returns>
    /// <see cref="LatticeReplicationPreconditionResult.Satisfied"/> when every
    /// precondition holds; otherwise a rejected result whose
    /// <see cref="LatticeReplicationPreconditionResult.FailureReason"/> explains
    /// the violation.
    /// </returns>
    /// <exception cref="ArgumentException"><paramref name="treeId"/> is <see langword="null"/> or empty.</exception>
    LatticeReplicationPreconditionResult Validate(string treeId, LatticeMergeMode mode);
}
