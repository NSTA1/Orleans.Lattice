using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Receiver side of the forced-gap re-seed for a custom
/// <see cref="IReplicationTransport"/>. A sender that lost records to a
/// write-ahead-log trim before shipping them stamps every push with
/// <see cref="ReplicationBatch.ReseedAfterEpoch"/> and withholds saga records
/// until the receiver reports, in <see cref="ReplicationAck.BootstrapEpoch"/>,
/// a full bootstrap from it past that epoch. A custom transport carries the
/// epoch beside the batch, and its receiving end calls this seam before handing
/// the entries to <see cref="IReplicationApplier"/>. The canonical gRPC
/// transport does the same internally.
/// </summary>
/// <remarks>
/// Both methods act on behalf of <c>authenticatedOriginClusterId</c>: the
/// re-seed starts a bootstrap that replaces the tree's contents from that
/// cluster. Pass only an origin the transport has authenticated as the caller,
/// never a value the request body merely declares. Both methods fail closed:
/// a tree that is not enrolled for replication here, or that cannot be
/// classified, is answered with no epoch and starts nothing.
/// </remarks>
public interface IReplicationReseedResponder
{
    /// <summary>
    /// Answers a sender's re-seed request and, when
    /// <see cref="LatticeReplicationOptions.AutoBootstrapOnFallOffLog"/> allows
    /// it, starts the bootstrap the sender waits for.
    /// </summary>
    /// <param name="treeName">The tree the push targets.</param>
    /// <param name="authenticatedOriginClusterId">The cluster the transport authenticated as the sender.</param>
    /// <param name="reseedAfterEpoch">The export epoch the sender asked the receiver to bootstrap past.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns>
    /// The epoch to echo in <see cref="ReplicationAck.BootstrapEpoch"/>, or
    /// <see langword="null"/> when there is none to report yet, which keeps the
    /// sender withholding.
    /// </returns>
    Task<long?> RespondAsync(
        string treeName,
        string authenticatedOriginClusterId,
        long reseedAfterEpoch,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Whether the push must be refused with a not-accepted ack because it
    /// carries a saga record from a sender this receiver is still re-seeding:
    /// such a record was pushed before the sender's re-seed marker, and applying
    /// it could stage a prepare nothing settles. A failed lookup refuses.
    /// </summary>
    /// <param name="treeName">The tree the push targets.</param>
    /// <param name="authenticatedOriginClusterId">The cluster the transport authenticated as the sender.</param>
    /// <param name="entries">The pushed entries.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    /// <returns><see langword="true"/> when the push must be refused.</returns>
    Task<bool> RefusesStragglerAsync(
        string treeName,
        string authenticatedOriginClusterId,
        IReadOnlyList<WalRecord> entries,
        CancellationToken cancellationToken = default);
}
