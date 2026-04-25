namespace Orleans.Lattice.Replication;

/// <summary>
/// Default <see cref="IReplicationTransport"/> registered by
/// <see cref="LatticeReplicationServiceCollectionExtensions.AddLatticeReplication"/>.
/// Discards every payload — useful for bringing up the rest of the replication
/// pipeline before a real transport is wired in.
/// </summary>
internal sealed class NoOpReplicationTransport : IReplicationTransport
{
    /// <inheritdoc />
    public Task SendAsync(string targetClusterId, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(targetClusterId);
        return Task.CompletedTask;
    }
}
