namespace Orleans.Lattice.Replication;

/// <summary>
/// Producer-side seam that resolves the declared <see cref="ReplicationMode"/>
/// for a tree id at commit time. Returning <c>null</c> means "this tree is
/// not replicated" and short-circuits the commit-time observer before any
/// <see cref="IReplogSink"/> call. The default implementation reads
/// <see cref="LatticeReplicationOptions.ReplicatedTrees"/>; hosts can replace
/// the registration to source the mode map from elsewhere (e.g. a control
/// plane, a feature flag system, or a permissive test stub that opts every
/// tree in to <see cref="ReplicationMode.LwwRegister"/>).
/// </summary>
public interface IReplicationModeResolver
{
    /// <summary>
    /// Returns the declared <see cref="ReplicationMode"/> for
    /// <paramref name="treeId"/>, or <c>null</c> if the tree is not
    /// replicated. Called on the commit-time hot path; implementations
    /// should be O(1) and side-effect free.
    /// </summary>
    /// <param name="treeId">The logical tree id the mutation was committed to.</param>
    ReplicationMode? Resolve(string treeId);
}
