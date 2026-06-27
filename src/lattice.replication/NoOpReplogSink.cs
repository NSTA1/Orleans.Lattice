namespace Orleans.Lattice.Replication;

/// <summary>
/// Default <see cref="IReplogSink"/> registered by
/// <see cref="LatticeReplicationServiceCollectionExtensions.AddLatticeReplication"/>.
/// Ignores every nudge. Replaced by the doorbell-ringing sharded sink in
/// later phases of the replication pipeline.
/// </summary>
internal sealed class NoOpReplogSink : IReplogSink
{
    /// <inheritdoc />
    public Task WriteAsync(string treeId, CancellationToken cancellationToken) => Task.CompletedTask;
}
