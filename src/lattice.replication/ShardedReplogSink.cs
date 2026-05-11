using Orleans.Lattice.BPlusTree.Grains;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Default <see cref="IReplogSink"/> registered by
/// <see cref="LatticeReplicationServiceCollectionExtensions.AddLatticeReplication"/>.
/// Routes each captured <see cref="WalRecord"/> to a single
/// <see cref="IWalShardGrain"/> activation keyed by
/// <c>{treeId}/{partition}</c>, where <c>partition</c> is a stable hash
/// of the entry's key modulo
/// <see cref="LatticeReplicationOptions.ReplogPartitions"/>.
/// <para>
/// The WAL append is awaited inline so a failure surfaces to the
/// originating writer rather than being silently swallowed in a
/// best-effort post-write append. Replaces the legacy
/// <c>NoOpReplogSink</c> as the default once the WAL grain is wired
/// into the pipeline.
/// </para>
/// <para>
/// On a successful append, the sink rings each per-<c>(tree, peer)</c>
/// shipper grain's <see cref="IReplicationShipperGrain.OnDoorbellAsync"/>
/// — gated on <see cref="LatticeReplicationOptions.ShipDoorbellEnabled"/>
/// — so the outbound ship loop short-circuits its next steady-state
/// timer wait and pumps immediately. Doorbell fan-out is best-effort:
/// any per-peer failure is logged at <c>Trace</c> and swallowed so the
/// commit path never fails on a doorbell ring failure. A missed
/// doorbell only delays the affected peer by one timer tick (~200ms).
/// </para>
/// <para>
/// The partition count is read from the unnamed (default) options
/// instance via <see cref="IOptionsMonitor{TOptions}.CurrentValue"/>.
/// Per-tree partition-count overrides are not supported - if a future
/// phase needs them, the resolution path will be widened to include the
/// tree-id named instance.
/// </para>
/// </summary>
internal sealed class ShardedReplogSink(
    IGrainFactory grainFactory,
    IOptionsMonitor<LatticeReplicationOptions> options,
    LocalVectorClockCache localVectorClockCache,
    ILogger<ShardedReplogSink> logger) : IReplogSink
{
    /// <inheritdoc />
    public async Task WriteAsync(WalRecord entry, CancellationToken cancellationToken)
    {
        var resolved = options.CurrentValue;
        var partitions = resolved.ReplogPartitions;
        var partition = WalPartitionHash.Compute(entry.Key ?? string.Empty, partitions);
        var grain = grainFactory.GetGrain<IWalShardGrain>($"{entry.TreeId}/{partition}");
        await grain.AppendAsync(entry, cancellationToken).ConfigureAwait(false);

        // Increment after the WAL grain confirms the append so the
        // counter only reflects entries that actually committed; a
        // throwing AppendAsync surfaces the original exception to the
        // caller without contributing to the throughput metric.
        LatticeReplicationMetrics.WalEntriesAppended.Add(
            1,
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, entry.TreeId ?? string.Empty));

        // Advance the producer-side local vector clock cache's local
        // diagonal entry for this tree. The receiver-side HWM grain
        // never advances the local cluster's diagonal (the apply path
        // filters local-origin entries), so this is the only seam that
        // tracks "what is the highest HLC this silo has appended for
        // its own cluster id". A subsequent emit (range delete fan-out,
        // multi-leaf saga, follow-on write) reads the advanced value
        // when it stamps its VectorClock from the cache. Range deletes
        // (HLC.Zero) never advance the diagonal — pointwise-max in the
        // cache leaves it unchanged. Skipped entirely for foreign-origin
        // entries because foreign origins are advanced post-apply via
        // AdvanceForeign, not post-WAL-append.
        if (entry.TreeId is { Length: > 0 } treeId
            && entry.OriginClusterId is { Length: > 0 } originClusterId
            && string.Equals(originClusterId, resolved.ClusterId, StringComparison.Ordinal))
        {
            localVectorClockCache.AdvanceLocal(treeId, originClusterId, entry.Timestamp);
        }

        // Doorbell fan-out: wake every configured shipper for
        // this tree so newly-committed entries reach peers at
        // sub-second latency. Best-effort and fire-and-forget — the
        // commit-path semantics never depend on a doorbell ring.
        if (resolved.ShipDoorbellEnabled
            && resolved.ReplicationPeers is { } peers
            && peers.Count > 0
            && entry.TreeId is { Length: > 0 } doorbellTreeId)
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
