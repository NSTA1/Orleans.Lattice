namespace Orleans.Lattice.Replication;

/// <summary>
/// An <see cref="IReplogSink"/> that ignores every nudge. It is not the default:
/// <see cref="LatticeReplicationServiceCollectionExtensions.AddLatticeReplication"/>
/// registers the doorbell-ringing <see cref="ShardedReplogSink"/>.
/// </summary>
internal sealed class NoOpReplogSink : IReplogSink
{
    /// <inheritdoc />
    public Task WriteAsync(string treeId, CancellationToken cancellationToken) => Task.CompletedTask;
}
