using Orleans.Lattice.BPlusTree.State;
using Orleans.Storage;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Retires a purged tree's per-tree fence rows (epic #5605, #5668): the range-delete
/// fence's state, each shard root's hand-off marker, and each WAL partition's
/// routing-generation entries. Run by the purge before it removes the tree's registry
/// entry, so the id cannot be registered again until every row is gone.
/// </summary>
/// <remarks>
/// <para>
/// <b>Floor before delete.</b> Every step raises the tree id's routing-generation
/// floor (<see cref="TreeRoutingFloorState"/>) durably to at least every generation it
/// is about to delete, before it deletes anything. A crash between the two leaves
/// rows that a re-drive deletes, never a generation that can recur: a WAL partition
/// refuses a generation claim at or below the floor, and a re-created incarnation
/// starts above it.
/// </para>
/// <para>
/// <b>Not best-effort.</b> Any failure propagates, so the purge keeps its registry
/// removal owed and re-drives the whole sequence; every step is idempotent.
/// </para>
/// <para>
/// <b>Mixed version.</b> A WAL partition hosted by a 10.2.0 silo answers the
/// retirement call with the definite "method unknown" failure. That silo holds no
/// fence state in memory, so its rows are retired straight from storage instead. A
/// purge that finishes on a 10.2.0 silo retires nothing: that build runs no part of
/// this sequence.
/// </para>
/// </remarks>
internal static class TreeRoutingFenceRetirement
{
    /// <summary>
    /// Every initiator whose routing-generation entries a WAL partition of a tree with
    /// <paramref name="shardCount"/> allocated shards can hold: each shard root, and
    /// the tree's range-delete fence.
    /// </summary>
    internal static WalRoutingInitiator[] Initiators(int shardCount)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(shardCount);
        var initiators = new WalRoutingInitiator[shardCount + 1];
        for (var shard = 0; shard < shardCount; shard++)
        {
            initiators[shard] = WalRoutingInitiator.ShardRoot(shard);
        }

        initiators[shardCount] = WalRoutingInitiator.TreeRangeDelete;
        return initiators;
    }

    /// <summary>
    /// Retires every per-tree fence row of <paramref name="treeId"/>: the range-delete
    /// fence first (so no close of an intent it held can claim again), then the shard
    /// markers, then each WAL partition's generation entries.
    /// </summary>
    /// <param name="grainFactory">The grain factory, for the fence grain and the WAL shards.</param>
    /// <param name="storage">The tree's grain storage provider.</param>
    /// <param name="treeId">The purged tree's id.</param>
    /// <param name="shardCount">One past the highest shard index the tree ever allocated.</param>
    /// <param name="partitions">The tree's WAL partition count.</param>
    internal static async Task RetireAsync(
        IGrainFactory grainFactory,
        IGrainStorage storage,
        string treeId,
        int shardCount,
        int partitions)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(storage);
        ArgumentException.ThrowIfNullOrEmpty(treeId);

        await grainFactory.GetGrain<ITreeRangeDeleteFenceGrain>(treeId).RetireForPurgeAsync().ConfigureAwait(true);
        await RetireMarkersAsync(storage, treeId, shardCount).ConfigureAwait(true);
        await BoundedFanOut.RunAsync(
            partitions,
            BoundedFanOut.DefaultWidth,
            partition => RetirePartitionAsync(
                () => grainFactory
                    .GetGrain<IWalShardGrain>(string.Concat(treeId, "/", partition.ToString(System.Globalization.CultureInfo.InvariantCulture)))
                    .RetireRoutingGenerationsAsync(shardCount, CancellationToken.None),
                storage,
                treeId,
                partition,
                shardCount)).ConfigureAwait(true);
    }

    /// <summary>
    /// Retires one WAL partition's generation entries through its WAL shard, which
    /// drops its loaded entries too, or straight from storage when the shard is hosted
    /// by a build that predates the call.
    /// </summary>
    internal static async Task RetirePartitionAsync(
        Func<Task<long>> retireAtShard,
        IGrainStorage storage,
        string treeId,
        int partition,
        int shardCount)
    {
        ArgumentNullException.ThrowIfNull(retireAtShard);
        try
        {
            await retireAtShard().ConfigureAwait(true);
        }
        catch (Exception ex) when (WalAppendFenceMethodProbe.IsDefiniteMethodUnknown(ex))
        {
            var initiators = Initiators(shardCount);
            var keys = new string[initiators.Length];
            for (var i = 0; i < initiators.Length; i++)
            {
                keys[i] = WalAppendFenceRows.GenerationKey(treeId, partition, initiators[i].Kind, initiators[i].Index);
            }

            await RetireRowsAsync<WalRoutingGenerationState>(
                storage,
                treeId,
                WalAppendFenceRows.GenerationGrainType,
                keys,
                static row => row.Generation,
                alsoCover: 0,
                floorRaised: null).ConfigureAwait(true);
        }
    }

    /// <summary>Retires every shard root's hand-off marker row of <paramref name="treeId"/>.</summary>
    internal static Task<long> RetireMarkersAsync(IGrainStorage storage, string treeId, int shardCount)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(shardCount);
        var keys = new string[shardCount];
        for (var shard = 0; shard < shardCount; shard++)
        {
            keys[shard] = HandOffMarkerRows.ShardKey(treeId, shard);
        }

        return RetireRowsAsync<ShardHandOffState>(
            storage,
            treeId,
            HandOffMarkerRows.ShardGrainType,
            keys,
            static row => row.RoutingGeneration,
            alsoCover: 0,
            floorRaised: null);
    }

    /// <summary>
    /// Deletes the rows of <paramref name="grainType"/> under <paramref name="keys"/>,
    /// each conditionally on the ETag read, after raising <paramref name="treeId"/>'s
    /// routing-generation floor durably to at least every generation they hold and
    /// <paramref name="alsoCover"/>. A row rewritten since the read is re-read, the floor
    /// raised over it, and deleted again.
    /// </summary>
    /// <param name="storage">The tree's grain storage provider.</param>
    /// <param name="treeId">The tree id whose floor covers the rows.</param>
    /// <param name="grainType">The rows' reserved grain type.</param>
    /// <param name="keys">The rows' keys; an absent row is skipped.</param>
    /// <param name="generationOf">The routing generation a row holds.</param>
    /// <param name="alsoCover">A generation held elsewhere (in memory) the floor must cover too.</param>
    /// <param name="floorRaised">Called with the stored floor after each raise, before any row it covers is deleted.</param>
    /// <returns>The floor as stored after the last raise.</returns>
    internal static async Task<long> RetireRowsAsync<T>(
        IGrainStorage storage,
        string treeId,
        string grainType,
        IReadOnlyList<string> keys,
        Func<T, long> generationOf,
        long alsoCover,
        Func<long, Task>? floorRaised)
        where T : new()
    {
        ArgumentNullException.ThrowIfNull(storage);
        ArgumentNullException.ThrowIfNull(keys);
        ArgumentNullException.ThrowIfNull(generationOf);

        var rows = await BoundedFanOut.RunAsync(
            keys.Count,
            BoundedFanOut.DefaultWidth,
            i => WalAppendFenceRows.ReadRowAsync<T>(storage, grainType, keys[i])).ConfigureAwait(true);

        var cover = Math.Max(alsoCover, 0);
        foreach (var row in rows)
        {
            if (row.RecordExists)
            {
                cover = Math.Max(cover, generationOf(row.State));
            }
        }

        var floor = new FloorTracker(await WalAppendFenceRows.RaiseRoutingFloorAsync(storage, treeId, cover).ConfigureAwait(true));
        if (floorRaised is not null)
        {
            await floorRaised(floor.Value).ConfigureAwait(true);
        }

        await BoundedFanOut.RunAsync(
            keys.Count,
            BoundedFanOut.DefaultWidth,
            async i =>
            {
                var row = rows[i];
                for (var attempt = 1; ; attempt++)
                {
                    if (!row.RecordExists)
                    {
                        return;
                    }

                    var generation = generationOf(row.State);
                    if (generation > floor.Value)
                    {
                        var raised = await WalAppendFenceRows.RaiseRoutingFloorAsync(storage, treeId, generation).ConfigureAwait(true);
                        floor.Raise(raised);
                        if (floorRaised is not null)
                        {
                            await floorRaised(raised).ConfigureAwait(true);
                        }
                    }

                    try
                    {
                        await WalAppendFenceRows.ClearAsync(storage, grainType, keys[i], row).ConfigureAwait(true);
                        return;
                    }
                    catch (InconsistentStateException) when (attempt < WalShardGrain.MaxFenceRowAttempts)
                    {
                        // Rewritten since the read: cover what it holds now, then delete it.
                        row = await WalAppendFenceRows.ReadRowAsync<T>(storage, grainType, keys[i]).ConfigureAwait(true);
                    }
                }
            }).ConfigureAwait(true);

        return floor.Value;
    }

    /// <summary>The highest floor a retirement has stored, shared by its concurrent row deletions.</summary>
    private sealed class FloorTracker(long initial)
    {
        private long _value = initial;

        public long Value => Interlocked.Read(ref _value);

        public void Raise(long value)
        {
            var current = Interlocked.Read(ref _value);
            while (value > current)
            {
                var seen = Interlocked.CompareExchange(ref _value, value, current);
                if (seen == current)
                {
                    return;
                }

                current = seen;
            }
        }
    }
}
