namespace Orleans.Lattice.Backup;

/// <summary>
/// The default <see cref="IReplicatedTreeMembership"/> registered by
/// <see cref="LatticeBackupServiceCollectionExtensions.AddLatticeBackup(Orleans.Hosting.ISiloBuilder, System.Action{LatticeBackupOptions})"/>.
/// Reports that no tree is replicated, which is the correct answer for a
/// single-cluster deployment where the replication package is not wired. In that
/// configuration the fail-fast sink guard is a no-op and the default in-cluster
/// sink is accepted. A multi-cluster host replaces this registration with an
/// implementation that projects the configured replicated-tree set.
/// </summary>
internal sealed class NoReplicatedTreeMembership : IReplicatedTreeMembership
{
    /// <inheritdoc />
    public IReadOnlyCollection<string> ReplicatedTrees => Array.Empty<string>();

    /// <inheritdoc />
    public bool IsReplicated(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        return false;
    }
}
