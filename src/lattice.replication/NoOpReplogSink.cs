namespace Orleans.Lattice.Replication;

/// <summary>
/// Default <see cref="IReplogSink"/> registered by
/// <see cref="LatticeReplicationServiceCollectionExtensions.AddLatticeReplication"/>.
/// Discards every entry. Replaced by the durable write-ahead-log sink in
/// later phases of the replication pipeline.
/// </summary>
internal sealed class NoOpReplogSink : IReplogSink
{
    /// <inheritdoc />
    public Task WriteAsync(ReplogEntry entry, CancellationToken cancellationToken) => Task.CompletedTask;
}
