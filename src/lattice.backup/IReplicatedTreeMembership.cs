namespace Orleans.Lattice.Backup;

/// <summary>
/// Backup-local seam that reports which trees participate in the cross-cluster
/// replication set. The backup package cannot reference the replication package
/// (that would invert the intended layering: backup depends only on core
/// lattice), so this interface lets the fail-fast sink guard learn whether a tree
/// is replicated without a direct dependency on the replication configuration.
/// <para>
/// A default no-op implementation is registered by
/// <see cref="LatticeBackupServiceCollectionExtensions.AddLatticeBackup(Orleans.Hosting.ISiloBuilder, System.Action{LatticeBackupOptions})"/>
/// that reports nothing replicated, which is correct for a single-cluster
/// deployment where the replication package is not wired. The replication package
/// (or the host) supplies the real implementation that projects the configured
/// replicated-tree set.
/// </para>
/// </summary>
public interface IReplicatedTreeMembership
{
    /// <summary>
    /// Reports whether <paramref name="treeId"/> participates in the cross-cluster
    /// replication set. A replicated tree must be backed by a shared external sink
    /// reachable by every cluster rather than the default in-cluster sink.
    /// </summary>
    /// <param name="treeId">The tree id to test. Must not be <c>null</c>.</param>
    /// <returns><c>true</c> when the tree is replicated; otherwise <c>false</c>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="treeId"/> is <c>null</c>.</exception>
    bool IsReplicated(string treeId);

    /// <summary>
    /// Enumerates the ids of every tree that participates in the cross-cluster
    /// replication set. The default no-op implementation yields nothing.
    /// </summary>
    /// <returns>The replicated tree ids, in no particular order.</returns>
    IReadOnlyCollection<string> ReplicatedTrees { get; }
}
