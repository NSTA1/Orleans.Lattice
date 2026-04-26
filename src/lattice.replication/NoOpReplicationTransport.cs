namespace Orleans.Lattice.Replication;

/// <summary>
/// Default <see cref="IReplicationTransport"/> registered by
/// <see cref="LatticeReplicationServiceCollectionExtensions.AddLatticeReplication"/>.
/// Discards every payload and returns an unaccepted
/// <see cref="ReplicationAck"/> so the sender's per-peer cursor stays
/// put - useful for bringing up the rest of the replication pipeline
/// before a real transport is wired in.
/// </summary>
internal sealed class NoOpReplicationTransport : IReplicationTransport
{
    private static readonly Task<ReplicationAck> UnacceptedAck = Task.FromResult(default(ReplicationAck));

    /// <inheritdoc />
    public Task<ReplicationAck> SendAsync(ReplicationBatch batch, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(batch.TargetClusterId))
        {
            throw new ArgumentException(
                "ReplicationBatch.TargetClusterId must be non-empty.",
                nameof(batch));
        }

        if (string.IsNullOrEmpty(batch.TreeName))
        {
            throw new ArgumentException(
                "ReplicationBatch.TreeName must be non-empty.",
                nameof(batch));
        }

        if (string.IsNullOrEmpty(batch.OriginClusterId))
        {
            throw new ArgumentException(
                "ReplicationBatch.OriginClusterId must be non-empty.",
                nameof(batch));
        }

        return UnacceptedAck;
    }
}
