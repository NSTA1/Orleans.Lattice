using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication;

/// <summary>Default <see cref="ILatticeReplicationPeerDecommissioner"/>.</summary>
internal sealed class LatticeReplicationPeerDecommissioner(
    IGrainFactory grainFactory,
    IReplicationTopology topology) : ILatticeReplicationPeerDecommissioner
{
    /// <inheritdoc />
    public async Task<LatticeReplicationPeerDecommissionOutcome> DecommissionPeerAsync(string peerClusterId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(peerClusterId);
        if (topology.CurrentPeers.Contains(peerClusterId))
        {
            throw new LatticeReplicationPeerStillConfiguredException(
                $"Peer cluster '{peerClusterId}' is still present in the configured replication peer set; remove it from ReplicationPeers before decommissioning it.",
                peerClusterId);
        }

        // Enrolment outlives a tree's membership in the currently-replicated set
        // (#4684: attach adds, detach keeps, decommission removes), so every
        // registered tree must be visited here - not just the trees that are
        // replicated today. A tree that left the replicated set can still hold
        // stale enrolment for this peer and would otherwise never be reached.
        var treeIds = await grainFactory.GetLatticeRegistry().GetAllTreeIdsAsync();
        foreach (var treeId in treeIds)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var enrolment = grainFactory.GetGrain<ICrossTreePeerEnrolmentGrain>(treeId);
            var wasEnrolled = await enrolment.DecommissionAsync(peerClusterId);
            if (!wasEnrolled)
            {
                // This tree never enrolled the peer (it never replicated here),
                // so there is no shipper state to force-detach. Skipping this
                // avoids activating and persisting a detached shipper for every
                // tree x peer pair - most of which the peer never touched.
                continue;
            }

            // Force the shipper's log detach synchronously rather than relying
            // on ReplicationDriverActivationService's async, retried detach
            // driven off ReplicationPeers. That path can still be in flight
            // when this call lands (or may not have started yet), and if the
            // peer is re-added before it catches up, the driver's own
            // CurrentPeers check skips the detach - leaving _registeredReadLog
            // set and EnrolAsync never re-called, so the re-added peer would
            // not re-bootstrap. DetachFromLogAsync is idempotent (already
            // detached is a no-op) and safe to call unconditionally once the
            // tree is known to have actually enrolled this peer.
            await grainFactory.GetGrain<IReplicationShipperGrain>($"{treeId}/{peerClusterId}")
                .DetachFromLogAsync(cancellationToken);
        }

        var registry = grainFactory.GetGrain<IReplicationDecommissionedPeerRegistryGrain>(IReplicationDecommissionedPeerRegistryGrain.SingletonKey);
        var alreadyDecommissioned = await registry.IsDecommissionedAsync(peerClusterId);
        await registry.MarkDecommissionedAsync(peerClusterId, DateTimeOffset.UtcNow);

        return new LatticeReplicationPeerDecommissionOutcome(peerClusterId, treeIds.Count, alreadyDecommissioned);
    }
}
