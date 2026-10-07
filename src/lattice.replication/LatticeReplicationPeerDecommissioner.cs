using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
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

        // Marked first (#4742): every fenced import from the peer re-reads the
        // registry on each phase tick, so from here on none of them lifts its
        // fence on the strength of the barriers abandoned below.
        var registry = grainFactory.GetGrain<IReplicationDecommissionedPeerRegistryGrain>(IReplicationDecommissionedPeerRegistryGrain.SingletonKey);
        var alreadyDecommissioned = await registry.IsDecommissionedAsync(peerClusterId);
        await registry.MarkDecommissionedAsync(peerClusterId, DateTimeOffset.UtcNow);

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

        // The receiver half (#4723, #4736, #4742): this cluster as the
        // *receiver* of operations the peer originated. It runs in two phases,
        // each over every registered tree, so the outcome does not depend on
        // the order trees are walked in. Both are independent of wasEnrolled
        // above: enrolment governs this cluster shipping TO the peer, these
        // phases the peer shipping TO this cluster.
        //
        // Phase 1 abandons every undecided barrier the peer originated, whole.
        // A barrier is keyed by (origin, operation), so every tree of its wait
        // set is one of the peer's replicas here; with the peer gone no tree is
        // left to vote, and deciding on the arrivals so far would serve a
        // verdict the operation's other trees never reach. Abandoning decides
        // nothing: each sub-saga then resolves as in flight, everywhere.
        // An abandon is not a decision, so before any barrier is abandoned every
        // tree whose import from the peer still holds its read fence is latched
        // to keep it until the peer is re-added (#4742). Otherwise an import the
        // abandoned barrier was holding would lift and serve the operation split.
        foreach (var treeId in treeIds)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await grainFactory.GetGrain<ILatticeBootstrapCoordinatorGrain>(treeId).HoldFenceForDecommissionedSourceAsync(peerClusterId);
        }

        await AbandonOriginBarriersAsync(grainFactory, treeIds, peerClusterId, includeDecided: false, cancellationToken);

        // Phase 2 discards every pending bucket the peer left on each tree. A
        // receiver stages a prepare on delivery, before its terminal, so a saga
        // whose terminal never arrived - single-tree or cross-tree - would
        // otherwise hold buckets nothing ever consumes. Each saga is settled
        // with the verdict its registry resolves, delegation included: an
        // abandoned barrier's sub-sagas abort on every tree alike, a decided
        // barrier's keep its verdict on every tree, and a single-tree saga
        // settles on its local decision.
        foreach (var treeId in treeIds)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var pending = await StalePendingClearer.CapturePendingAsync(grainFactory, treeId, peerClusterId, cancellationToken);
            var verdicts = new Dictionary<Guid, bool>();
            foreach (var txid in pending)
            {
                var status = await TxRegistryRouting.GetRegistry(grainFactory, treeId, txid).GetStatusAsync(txid);
                if (status is TxStatus.Committed or TxStatus.Aborted)
                {
                    verdicts[txid] = status == TxStatus.Committed;
                }
            }

            await StalePendingClearer.ClearAsync(
                grainFactory,
                treeId,
                peerClusterId,
                carriedSagas: new HashSet<Guid>(),
                decidedSagas: verdicts,
                cancellationToken);
        }

        return new LatticeReplicationPeerDecommissionOutcome(peerClusterId, treeIds.Count, alreadyDecommissioned);
    }

    /// <summary>
    /// Abandons, through <see cref="ILatticeCrossTreeReceiverGrain.AbandonAsync"/>,
    /// every cross-tree receiver barrier originated by <paramref name="originClusterId"/>
    /// that the barrier indexes of <paramref name="treeIds"/> name: each undecided
    /// barrier, and with <paramref name="includeDecided"/> also every decided one
    /// and every tombstone. Returns how many barriers were abandoned.
    /// </summary>
    internal static async Task<int> AbandonOriginBarriersAsync(
        IGrainFactory grainFactory,
        IEnumerable<string> treeIds,
        string originClusterId,
        bool includeDecided,
        CancellationToken cancellationToken)
    {
        var keys = new HashSet<string>(StringComparer.Ordinal);
        foreach (var treeId in treeIds)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var index = grainFactory.GetGrain<ICrossTreeBarrierIndexGrain>(treeId);
            keys.UnionWith(await index.GetAsync());
            if (includeDecided)
            {
                keys.UnionWith(await index.GetTombstonesAsync());
            }
        }

        var abandoned = 0;
        foreach (var key in keys)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var receiver = grainFactory.GetGrain<ILatticeCrossTreeReceiverGrain>(key);
            var status = await receiver.GetStatusAsync();
            if (!string.Equals(status.OriginClusterId, originClusterId, StringComparison.Ordinal)
                || (status.Decided && !includeDecided))
            {
                continue;
            }

            if (await receiver.AbandonAsync())
            {
                abandoned++;
            }
        }

        return abandoned;
    }
}
