using Microsoft.Extensions.Hosting;

namespace Orleans.Lattice.Backup;

/// <summary>
/// Startup guard that fails fast when a replicated tree is backed by the default
/// in-cluster <see cref="InClusterLatticeBackupSink"/>. A tree that participates in
/// the cross-cluster replication set must be backed by a shared external sink
/// reachable by every cluster: the in-cluster sink dogfoods a per-cluster reserved
/// tree, so a backup written on one cluster is invisible to the others and an
/// incremental chain could not be resolved or extended across the replication set.
/// <para>
/// The guard reads the replicated-tree set through the backup-local
/// <see cref="IReplicatedTreeMembership"/> seam, so it carries no dependency on the
/// replication package. In a single-cluster deployment the default no-op seam
/// reports nothing replicated and the guard is a no-op, leaving the in-cluster sink
/// as the accepted default.
/// </para>
/// </summary>
internal sealed class LatticeBackupReplicatedSinkStartupValidator(
    ILatticeBackupSink sink,
    IReplicatedTreeMembership membership) : IHostedService
{
    /// <inheritdoc />
    public Task StartAsync(CancellationToken cancellationToken)
    {
        // Hard type check: only the default in-cluster sink is rejected. Any other
        // resolved sink is assumed to be a shared external provider reachable by
        // every cluster.
        if (sink is not InClusterLatticeBackupSink)
        {
            return Task.CompletedTask;
        }

        foreach (var treeId in membership.ReplicatedTrees)
        {
            throw new InvalidOperationException(
                $"Tree '{treeId}' participates in the cross-cluster replication set but the "
                + $"backup sink resolved to the default in-cluster {nameof(InClusterLatticeBackupSink)}. "
                + "A replicated tree must be backed by a shared external sink reachable by every "
                + "cluster so a backup captured on one cluster is resolvable and extendable from the "
                + $"others. Register a shared external {nameof(ILatticeBackupSink)} implementation "
                + "(for example a durable off-cluster provider) before AddLatticeBackup, or remove "
                + "this tree from the replicated set.");
        }

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
