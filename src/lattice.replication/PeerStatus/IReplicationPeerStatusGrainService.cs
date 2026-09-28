using Orleans.Services;

namespace Orleans.Lattice.Replication;

/// <summary>
/// The per-silo endpoint of the peer-status read path. One instance runs on
/// every silo and answers a bounded read of that silo's own
/// <see cref="ReplicationPeerStats"/>, so a caller can assemble the cluster-wide
/// view by fanning out to each active silo. It is a grain service - a per-silo
/// system target - so a read never runs on a shipper or applier activation.
/// </summary>
[Alias(ReplicationTypeAliases.IReplicationPeerStatusGrainService)]
internal interface IReplicationPeerStatusGrainService : IGrainService
{
    /// <summary>
    /// Reads one bounded, ordered page of this silo's per-peer telemetry rows.
    /// </summary>
    /// <param name="request">The read to perform.</param>
    /// <returns>At most <see cref="ReplicationPeerStatusReadRequest.EffectiveLimit"/> rows in read order.</returns>
    Task<ReplicationPeerStatusRow[]> ReadAsync(ReplicationPeerStatusReadRequest request);
}
