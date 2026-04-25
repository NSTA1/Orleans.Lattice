namespace Orleans.Lattice.Replication;

/// <summary>
/// Pluggable seam for shipping replication payloads between clusters. The
/// default registration is a no-op transport so the rest of the replication
/// pipeline can be wired up in isolation; production hosts replace it via
/// <see cref="LatticeReplicationServiceCollectionExtensions.AddLatticeReplication"/>
/// (e.g. an HTTP or gRPC implementation).
/// </summary>
public interface IReplicationTransport
{
    /// <summary>
    /// Sends a replication payload to the cluster identified by
    /// <paramref name="targetClusterId"/>. Implementations are expected to be
    /// idempotent at the batch boundary — receivers deduplicate by
    /// <c>(origin, hlc)</c>.
    /// </summary>
    /// <param name="targetClusterId">Stable identifier of the receiving cluster.</param>
    /// <param name="payload">Opaque, framed replication payload.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task SendAsync(string targetClusterId, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken);
}
