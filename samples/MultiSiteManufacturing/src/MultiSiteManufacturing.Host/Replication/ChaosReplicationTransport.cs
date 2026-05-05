using Microsoft.Extensions.Logging;
using MultiSiteManufacturing.Host.Federation;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;

namespace MultiSiteManufacturing.Host.Replication;

/// <summary>
/// Decorator on the package's <see cref="IReplicationTransport"/> that
/// gates outbound ship on the operator-driven
/// <see cref="IReplicationDisconnectGrain"/> chaos flag. When the flag
/// is set, every <see cref="SendAsync"/> short-circuits with
/// <see cref="ReplicationAck.Accepted"/> set to <see langword="false"/>
/// so the package's shipper does not advance its per-peer cursor; the
/// local WAL keeps growing and replication resumes from the stationary
/// cursor when the flag is cleared.
/// </summary>
/// <remarks>
/// Re-implements the host-rolled Tier 4b chaos disconnect surface at
/// the canonical transport seam after migration step 5 removed the
/// original outbound tick that consulted the flag. Tier 5
/// (<c>docker network disconnect</c>) is transport-agnostic and remains
/// untouched.
/// </remarks>
internal sealed class ChaosReplicationTransport(
    IReplicationTransport inner,
    IGrainFactory grains,
    ILogger<ChaosReplicationTransport> logger) : IReplicationTransport
{
    /// <summary>
    /// Consults the <see cref="IReplicationDisconnectGrain"/> singleton
    /// before delegating to the wrapped transport. When the flag is set
    /// returns a not-accepted <see cref="ReplicationAck"/> so the
    /// package shipper does not advance its per-peer cursor.
    /// </summary>
    public async Task<ReplicationAck> SendAsync(
        ReplicationBatch batch,
        CancellationToken cancellationToken)
    {
        var disconnected = await grains
            .GetGrain<IReplicationDisconnectGrain>(IReplicationDisconnectGrain.SingletonKey)
            .IsDisconnectedAsync()
            .ConfigureAwait(false);

        if (disconnected)
        {
            logger.LogDebug(
                "Chaos disconnect active; short-circuiting ship to {Peer} for tree {Tree}.",
                batch.TargetClusterId,
                batch.TreeName);

            return new ReplicationAck
            {
                Accepted = false,
                HighestAppliedHlc = HybridLogicalClock.Zero,
            };
        }

        return await inner.SendAsync(batch, cancellationToken).ConfigureAwait(false);
    }
}
