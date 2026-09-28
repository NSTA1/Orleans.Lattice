using Microsoft.Extensions.Logging;
using Orleans.Concurrency;
using Orleans.Runtime;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Default <see cref="IReplicationPeerStatusGrainService"/>: answers each read
/// from the silo's <see cref="ReplicationPeerStats"/> singleton through its
/// bounded, lock-light <see cref="ReplicationPeerStats.ReadStatusPage"/> read.
/// Holds no state of its own and schedules nothing.
/// </summary>
[Reentrant]
internal sealed class ReplicationPeerStatusGrainService : GrainService, IReplicationPeerStatusGrainService
{
    private readonly ReplicationPeerStats _peerStats;

    /// <summary>Initialises the grain service.</summary>
    /// <param name="grainId">The grain service identity, supplied by the runtime.</param>
    /// <param name="silo">The hosting silo, supplied by the runtime.</param>
    /// <param name="loggerFactory">The logger factory, supplied by the runtime.</param>
    /// <param name="peerStats">The silo's per-peer telemetry state. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="peerStats"/> is <see langword="null"/>.</exception>
    public ReplicationPeerStatusGrainService(
        GrainId grainId,
        Silo silo,
        ILoggerFactory loggerFactory,
        ReplicationPeerStats peerStats)
        : base(grainId, silo, loggerFactory)
    {
        ArgumentNullException.ThrowIfNull(peerStats);
        _peerStats = peerStats;
    }

    /// <inheritdoc />
    public Task<ReplicationPeerStatusRow[]> ReadAsync(ReplicationPeerStatusReadRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);
        return Task.FromResult(_peerStats.ReadStatusPage(request));
    }
}
