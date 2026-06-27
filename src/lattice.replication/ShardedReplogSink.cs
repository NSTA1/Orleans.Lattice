using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Default <see cref="IReplogSink"/> registered by
/// <see cref="LatticeReplicationServiceCollectionExtensions.AddLatticeReplication"/>.
/// Reduced to a low-latency commit-time nudge: it advances the
/// producer-side local vector clock cache and rings the per-peer shipper
/// doorbells. It does <b>not</b> append to the write-ahead log.
/// <para>
/// The write-ahead log is the single per-shard <c>IWalShardGrain</c>
/// keyed <c>{treeId}/{partition}</c> that the foreground commit-log
/// writer (<see cref="WalCommitLogWriter"/>) appends to on every commit,
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
    LocalVectorClockCache localVectorClockCache,
    ILogger<ShardedReplogSink> logger) : IReplogSink
{
    private readonly IReplicationTopology _topology =
        topology ?? throw new ArgumentNullException(nameof(topology));

    /// <inheritdoc />
    public Task WriteAsync(WalRecord entry, CancellationToken cancellationToken)
    {
        var resolved = options.CurrentValue;

        // Advance the producer-side local vector clock cache's local
        // diagonal entry for this tree. The receiver-side HWM grain
        // never advances the local cluster's diagonal (the apply path
        // filters local-origin entries), so this is the only seam that
        // tracks "what is the highest HLC this silo has appended for
        // its own cluster id". A subsequent emit (range delete fan-out,
        // multi-leaf saga, follow-on write) reads the advanced value
        // when it stamps its VectorClock from the cache. Range deletes
        // (HLC.Zero) never advance the diagonal - pointwise-max in the
        // cache leaves it unchanged. Skipped entirely for foreign-origin
        // entries because foreign origins are advanced post-apply via
        // AdvanceForeign, not post-commit.
        if (entry.TreeId is { Length: > 0 } treeId
            && entry.OriginClusterId is { Length: > 0 } originClusterId
            && string.Equals(originClusterId, resolved.ClusterId, StringComparison.Ordinal))
        {
            localVectorClockCache.AdvanceLocal(treeId, originClusterId, entry.Timestamp);
        }

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
            && entry.TreeId is { Length: > 0 } doorbellTreeId)
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
                    _ = RingDoorbellAsync(doorbellTreeId, peer);
                }
            }
        }

        return Task.CompletedTask;
    }

    /// <summary>
    /// Best-effort per-peer doorbell ring. Any failure (silo loss,
    /// transient network fault, missing activation) is logged at
    /// <c>Trace</c> level and swallowed so the producer-side commit
    /// path never fails on a doorbell ring failure.
    /// </summary>
    private async Task RingDoorbellAsync(string treeId, string peerClusterId)
    {
        try
        {
            var shipper = grainFactory.GetGrain<IReplicationShipperGrain>($"{treeId}/{peerClusterId}");
            await shipper.OnDoorbellAsync(CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogTrace(ex,
                "Doorbell ring failed for shipper ({Tree}, {Peer})",
                treeId, peerClusterId);
        }
    }
}
