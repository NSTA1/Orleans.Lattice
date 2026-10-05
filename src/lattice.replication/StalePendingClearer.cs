using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Settles a re-seeded receiver's leftover pending buckets from one source
/// (issue #4533). While a sender waits for a re-seed it withholds every saga
/// record, so after a drain from an export that postdates its request, every
/// pending bucket the receiver holds from that source was staged before the
/// re-seed or re-staged by the export. Each is settled durably, through the
/// shard root's terminal mark, so a leaf's log replay never re-stages it:
/// <list type="bullet">
/// <item>A saga the export carries as a prepared row (in flight at the cut) or
/// as an unsettled row is left for the terminal that follows the re-seed.</item>
/// <item>A saga the export carries as a decision row, or that this receiver's
/// registry has decided, is drained by that decision: the rewind cannot re-ship
/// a terminal the source already trimmed.</item>
/// <item>Any other saga was decided and purged by the source. Its bucket is
/// discarded, recording no outcome in the receiver's registry: its committed
/// values, if it committed, arrived as committed rows, and the source never
/// ships its records again (the shipper's replay filter withholds it).</item>
/// </list>
/// </summary>
internal static class StalePendingClearer
{
    /// <summary>
    /// Settles every leftover pending saga from <paramref name="sourceClusterId"/>
    /// on <paramref name="treeName"/> and returns how many there were.
    /// </summary>
    public static async Task<int> ClearAsync(
        IGrainFactory grainFactory,
        string treeName,
        string sourceClusterId,
        IReadOnlySet<Guid> carriedSagas,
        IReadOnlyDictionary<Guid, bool> decidedSagas,
        CancellationToken cancellationToken)
    {
        var registry = grainFactory.GetLatticeRegistry();
        var physicalTreeId = await registry.ResolveAsync(treeName).ConfigureAwait(false);
        var shardMap = await registry.GetShardMapAsync(treeName).ConfigureAwait(false)
            ?? ShardMap.GetOrCreateDefaultShared(
                LatticeConstants.DefaultVirtualShardCount,
                LatticeConstants.DefaultShardCount);
        var allSlots = new int[shardMap.VirtualShardCount];
        for (var i = 0; i < allSlots.Length; i++)
        {
            allSlots[i] = i;
        }

        // Per leftover saga, the shards whose leaves hold a bucket of it.
        var leftover = new Dictionary<Guid, HashSet<int>>();
        foreach (var shardIndex in shardMap.GetPhysicalShardIndices())
        {
            cancellationToken.ThrowIfCancellationRequested();
            var shard = grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{shardIndex}");
            var leafId = await shard.GetLeftmostLeafIdAsync().ConfigureAwait(false);
            while (leafId is not null)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(leafId.Value);
                foreach (var m in await leaf.GetPendingMutationsForSlotsAsync(allSlots, shardMap.VirtualShardCount).ConfigureAwait(false))
                {
                    if (m.TransactionId != Guid.Empty
                        && string.Equals(m.OriginClusterId, sourceClusterId, StringComparison.Ordinal)
                        && !carriedSagas.Contains(m.TransactionId))
                    {
                        if (!leftover.TryGetValue(m.TransactionId, out var shards))
                        {
                            leftover[m.TransactionId] = shards = new HashSet<int>();
                        }

                        shards.Add(shardIndex);
                    }
                }

                leafId = await leaf.GetNextSiblingAsync().ConfigureAwait(false);
            }
        }

        using var origin = LatticeOriginContext.With(sourceClusterId);
        foreach (var (txid, shards) in leftover)
        {
            cancellationToken.ThrowIfCancellationRequested();
            bool committed;
            if (decidedSagas.TryGetValue(txid, out var exported))
            {
                committed = exported;
            }
            else
            {
                var local = await TxRegistryRouting.GetRegistry(grainFactory, treeName, txid)
                    .GetRecordedStatusAsync(txid).ConfigureAwait(false);
                committed = local == TxStatus.Committed;
            }

            foreach (var shardIndex in shards)
            {
                await grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{shardIndex}")
                    .AppendTxTerminalAsync(txid, committed, committedValues: null, cancellationToken)
                    .ConfigureAwait(false);
            }
        }

        return leftover.Count;
    }
}
