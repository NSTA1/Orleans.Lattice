namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Producer-side seam that resolves the declared <see cref="LatticeMergeMode"/>
/// for a tree id at commit time. Returning <c>null</c> means "this tree is
/// not replicated" — the WAL writer treats <c>null</c> as
/// <see cref="LatticeMergeMode.LwwRegister"/> so durability still records the
/// mutation, but the replication-package observer short-circuits the
/// cross-cluster ship-out before any <c>IReplogSink</c> call. Hosts replace
/// this registration to source the mode map from elsewhere (e.g. a control
/// plane, a feature flag system, or the per-tree map in
/// <c>LatticeReplicationOptions.ReplicatedTrees</c> when the replication
/// package is registered).
/// </summary>
public interface ILatticeMergeModeResolver
{
    /// <summary>
    /// Returns the declared <see cref="LatticeMergeMode"/> for
    /// <paramref name="treeId"/>, or <c>null</c> if the tree is not
    /// replicated. Called on the commit-time hot path; implementations
    /// should be O(1) and side-effect free.
    /// </summary>
    /// <param name="treeId">The logical tree id the mutation was committed to.</param>
    LatticeMergeMode? Resolve(string treeId);
}
