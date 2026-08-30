using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Default <see cref="IReplogSink"/> registered by
/// <see cref="LatticeReplicationServiceCollectionExtensions.AddLatticeReplication"/>.
/// Reduced to a low-latency commit-time nudge: it rings the per-peer
/// shipper doorbells for a committed tree. It does <b>not</b> append to
/// the write-ahead log and no longer maintains any producer-side vector
/// clock state.
/// <para>
/// The write-ahead log is the single per-shard <c>IWalShardGrain</c>
/// keyed <c>{treeId}/{partition}</c> that the foreground commit-log
/// writer (<see cref="Orleans.Lattice.BPlusTree.Grains.WalCommitLogWriter"/>) appends to on every commit,
/// stamping the durable origin cluster id and vector clock. The
/// <see cref="Grains.IReplicationShipperGrain"/> tails that same log in
/// the background - it is the durable, log-first replication producer
/// (one activation per <c>(tree, peer)</c>, a per-partition sequence
/// checkpoint persisted across restarts, origin/maintenance filtering,
/// and shard onboarding). A commit therefore reaches the WAL exactly
/// once via the leaf; the historical second, redundant append on this
/// commit-time path has been removed so the foreground write no longer
/// pays a synchronous cross-grain WAL round-trip.
/// </para>
/// <para>
/// The sink rings each per-<c>(tree, peer)</c>
/// shipper grain's <see cref="IReplicationShipperGrain.OnDoorbellAsync"/>
/// - gated on <see cref="LatticeReplicationOptions.ShipDoorbellEnabled"/>
/// - so the outbound ship loop short-circuits its next steady-state
/// timer wait and pumps immediately. Doorbell fan-out is best-effort:
/// any per-peer failure is logged at <c>Trace</c> and swallowed so the
/// commit path never fails on a doorbell ring failure. A missed
/// doorbell only delays the affected peer by one timer tick (~200ms).
/// </para>
/// <para>
/// <b>Writer-side coalescing.</b> A doorbell is an idempotent,
/// edge-triggered "there is work" signal, so the sink collapses a burst
/// of per-write rings for the same <c>(tree, peer)</c> into <b>at most
/// one in-flight ring plus one pending follow-up</b>. A ring requested
/// while one is already in flight sets a single pending flag instead of
/// dispatching its own <see cref="IReplicationShipperGrain.OnDoorbellAsync"/>
/// grain call, and the in-flight ring loop fires exactly one more ring
/// once it drains - so the last write in a burst still wakes the shipper.
/// This bounds the doorbell message rate the non-reentrant shipper
/// activation sees to a small constant regardless of write throughput,
/// preventing the activation's turn queue from blowing up, doorbell
/// messages from being dropped as expired, and the keepalive reminder
/// tick that drives shipping from being starved behind a backlog. The
/// coalescing ratio is observable via
/// <see cref="LatticeReplicationMetrics.DoorbellRung"/> and
/// <see cref="LatticeReplicationMetrics.DoorbellCoalesced"/>.
/// </para>
/// <para>
/// The peer list driving the doorbell fan-out is read from
/// <see cref="IReplicationTopology.CurrentPeers"/>, not from
/// <see cref="LatticeReplicationOptions.ReplicationPeers"/> directly.
/// In the default configuration the two are equivalent because the
/// registered <see cref="OptionsReplicationTopology"/> projects the
/// same <see cref="IOptionsMonitor{TOptions}"/> instance; hosts that
/// register a custom <see cref="IReplicationTopology"/> singleton
/// (e.g. service-registry-backed) have that topology drive the
/// doorbell loop without needing to mirror membership back into
/// <see cref="LatticeReplicationOptions.ReplicationPeers"/>.
/// <see cref="LatticeReplicationOptions.ShipDoorbellEnabled"/> remains
/// options-resolved because it is a behaviour knob rather than
/// membership.
/// </para>
/// </summary>
internal sealed class ShardedReplogSink(
    IGrainFactory grainFactory,
    IOptionsMonitor<LatticeReplicationOptions> options,
    IReplicationTopology topology,
    ILogger<ShardedReplogSink> logger) : IReplogSink
{
    private readonly IReplicationTopology _topology =
        topology ?? throw new ArgumentNullException(nameof(topology));

    // Per-(tree, peer) doorbell coalescers. Bounded by the number of
    // (tree, peer) pairs this silo commits for, never by write volume.
    private readonly ConcurrentDictionary<string, DoorbellCoalescer> _doorbells =
        new(StringComparer.Ordinal);

    /// <inheritdoc />
    public Task WriteAsync(string treeId, CancellationToken cancellationToken)
    {
        var resolved = options.CurrentValue;

        // Doorbell fan-out: wake every shipper for this tree so the
        // background log-tailing producer drains the newly-committed
        // leaf WAL entries to peers at sub-second latency instead of
        // waiting for its next steady-state timer tick. Peer membership
        // is sourced from IReplicationTopology, not from
        // LatticeReplicationOptions.ReplicationPeers, so a host-supplied
        // dynamic topology drives this loop without having to mirror
        // membership back into options. ShipDoorbellEnabled stays
        // options-resolved because it is a per-tree behaviour knob, not
        // membership. Best-effort and fire-and-forget - the commit-path
        // semantics never depend on a doorbell ring.
        if (resolved.ShipDoorbellEnabled
            && treeId is { Length: > 0 } doorbellTreeId)
        {
            var peers = _topology.CurrentPeers;
            if (peers.Count > 0)
            {
                foreach (var peer in peers)
                {
                    if (string.IsNullOrEmpty(peer))
                    {
                        continue;
                    }
                    RequestDoorbell(doorbellTreeId, peer);
                }
            }
        }

        return Task.CompletedTask;
    }

    /// <summary>
    /// Requests a doorbell ring for a <c>(tree, peer)</c>, coalescing against
    /// any ring already in flight. If no ring is running the caller starts the
    /// ring loop; otherwise the request collapses into the single pending
    /// follow-up and is counted as coalesced.
    /// </summary>
    private void RequestDoorbell(string treeId, string peerClusterId)
    {
        var key = $"{treeId}/{peerClusterId}";
        var coalescer = _doorbells.GetOrAdd(key, static _ => new DoorbellCoalescer());

        if (coalescer.Request())
        {
            // We transitioned idle -> in-flight, so we own the ring loop.
            _ = RingLoopAsync(treeId, peerClusterId, coalescer);
        }
        else
        {
            // A ring is already in flight; this request folded into the
            // single pending follow-up rather than enqueuing its own
            // grain call on the non-reentrant shipper activation.
            LatticeReplicationMetrics.DoorbellCoalesced.Add(
                1,
                new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, treeId),
                new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagPeer, peerClusterId),
                LatticeTenantLabel.ForTree(treeId));
        }
    }

    /// <summary>
    /// Drains the coalesced doorbell state for a <c>(tree, peer)</c>: rings the
    /// shipper, then rings exactly once more if a request arrived while the ring
    /// was in flight, and so on until no request is pending. At most one loop
    /// runs per <c>(tree, peer)</c> at a time. Every ring failure (silo loss,
    /// transient network fault, missing activation) is logged at <c>Trace</c>
    /// and swallowed so the producer-side commit path never fails on a doorbell
    /// ring failure.
    /// </summary>
    private async Task RingLoopAsync(string treeId, string peerClusterId, DoorbellCoalescer coalescer)
    {
        var shipper = grainFactory.GetGrain<IReplicationShipperGrain>($"{treeId}/{peerClusterId}");
        while (true)
        {
            try
            {
                LatticeReplicationMetrics.DoorbellRung.Add(
                    1,
                    new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, treeId),
                    new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagPeer, peerClusterId),
                    LatticeTenantLabel.ForTree(treeId));
                await shipper.OnDoorbellAsync(CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                logger.LogTrace(ex,
                    "Doorbell ring failed for shipper ({Tree}, {Peer})",
                    treeId, peerClusterId);
            }

            if (!coalescer.CompleteAndCheckPending())
            {
                // No request arrived during the ring; the loop is idle.
                return;
            }

            // A request coalesced in while the ring was in flight; fire the
            // single follow-up so the last write in a burst is not lost.
        }
    }

    /// <summary>
    /// Coalesces doorbell ring requests for one <c>(tree, peer)</c> into at most
    /// one in-flight ring and one pending follow-up. Edge-triggered and
    /// idempotent: an arbitrary number of requests during an in-flight ring
    /// collapse to a single follow-up.
    /// </summary>
    private sealed class DoorbellCoalescer
    {
        private readonly object _gate = new();
        private bool _inFlight;
        private bool _pending;

        /// <summary>
        /// Records a ring request. Returns <see langword="true"/> when the
        /// caller must start the ring loop (the state was idle); returns
        /// <see langword="false"/> when a ring is already in flight and the
        /// request was folded into the single pending follow-up.
        /// </summary>
        public bool Request()
        {
            lock (_gate)
            {
                if (_inFlight)
                {
                    _pending = true;
                    return false;
                }

                _inFlight = true;
                return true;
            }
        }

        /// <summary>
        /// Called by the ring loop after a ring completes. Returns
        /// <see langword="true"/> when a request coalesced in during the ring
        /// (consume it and ring once more, staying in flight); returns
        /// <see langword="false"/> when nothing is pending, clearing the
        /// in-flight state so the next request starts a fresh loop.
        /// </summary>
        public bool CompleteAndCheckPending()
        {
            lock (_gate)
            {
                if (_pending)
                {
                    _pending = false;
                    return true;
                }

                _inFlight = false;
                return false;
            }
        }
    }
}
