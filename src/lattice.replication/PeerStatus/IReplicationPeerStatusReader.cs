namespace Orleans.Lattice.Replication;

/// <summary>
/// Reads the cluster-wide per-peer replication telemetry the peer-status facade
/// reports: one bounded, ordered page per call, de-duplicated across silos.
/// The seam the facade depends on, so it can be exercised without a silo.
/// </summary>
internal interface IReplicationPeerStatusReader
{
    /// <summary>
    /// Reads at most <see cref="ReplicationPeerStatusReadRequest.EffectiveLimit"/>
    /// rows ordered strictly after the request's cursor, in
    /// <see cref="ReplicationPeerStatusOrder"/>. Fewer rows than the limit means
    /// the read is exhausted.
    /// </summary>
    /// <param name="request">The read to perform.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The ordered rows.</returns>
    Task<IReadOnlyList<ReplicationPeerStatusRow>> ReadAsync(
        ReplicationPeerStatusReadRequest request,
        CancellationToken cancellationToken);
}
