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
/// <b>Floors before deletes.</b> The retirement first reads every row, then raises
/// every initiator's routing floor (<see cref="TreeRoutingFloorState"/>) to the highest
/// generation any of them holds, in one write, before it deletes anything. A row that
/// moved since that read is covered again before its delete. A crash between the two
/// leaves rows a re-drive deletes, never a generation that can recur. A WAL partition
/// refuses a generation claim at or below its initiator's floor, and a re-created
/// incarnation starts above it.
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
/// this sequence (see <c>ShardRootGrain</c>'s re-seed catch-up, #5703 review S2).
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
    /// Retires every per-tree fence row of <paramref name="treeId"/>. First every
    /// initiator's floor is raised, in one write, to the highest generation the fence,
    /// the markers and the partitions' rows hold. Then the rows go, in this order: the
    /// range-delete fence (so no close of an intent it held can claim again), then the
    /// shard markers, then each WAL partition's generation entries.
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

        var fence = grainFactory.GetGrain<ITreeRangeDeleteFenceGrain>(treeId);
        var highest = await fence.GetRetiringGenerationAsync().ConfigureAwait(true);
        highest = Math.Max(highest, await HighestMarkerGenerationAsync(storage, treeId, shardCount).ConfigureAwait(true));
        highest = Math.Max(highest, await HighestPartitionGenerationAsync(storage, treeId, partitions).ConfigureAwait(true));
        if (highest > 0)
        {
            var floors = new Dictionary<WalRoutingInitiator, long>();
            foreach (var initiator in Initiators(shardCount))
            {
                floors[initiator] = highest;
            }

            await WalAppendFenceRows.RaiseRoutingFloorsAsync(storage, treeId, floors).ConfigureAwait(true);
        }

        await fence.RetireForPurgeAsync().ConfigureAwait(true);
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

    private static async Task<long> HighestMarkerGenerationAsync(IGrainStorage storage, string treeId, int shardCount)
    {
        var rows = await BoundedFanOut.RunAsync(
            shardCount,
            BoundedFanOut.DefaultWidth,
            shard => HandOffMarkerRows.ReadShardAsync(storage, treeId, shard)).ConfigureAwait(true);
        return rows.Where(r => r.RecordExists).Select(r => r.State.RoutingGeneration).DefaultIfEmpty(0).Max();
    }

    private static async Task<long> HighestPartitionGenerationAsync(IGrainStorage storage, string treeId, int partitions)
    {
        // One row per partition, a slot per initiator (#5987).
        var rows = await BoundedFanOut.RunAsync(
            partitions,
            BoundedFanOut.DefaultWidth,
            partition => WalAppendFenceRows.ReadGenerationsAsync(storage, treeId, partition)).ConfigureAwait(true);
        return rows.Where(r => r.RecordExists).SelectMany(r => r.State.Generations.Values).DefaultIfEmpty(0).Max();
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
            // The partition's one table row, every initiator's slot in it (#5987).
            await RetireGenerationsRowAsync(storage, treeId, partition, alsoCover: null, floorsRaised: null).ConfigureAwait(true);
        }
    }

    /// <summary>
    /// Retires a WAL partition's routing-generation table (#5987): raises every slot's
    /// initiator's routing floor under <paramref name="treeId"/> durably to at least the
    /// generation it holds, and <paramref name="alsoCover"/>, in one write, then deletes
    /// the row conditionally on the ETag read. A row rewritten since the read is re-read,
    /// every slot covered again, and deleted again.
    /// </summary>
    /// <param name="storage">The tree's grain storage provider.</param>
    /// <param name="treeId">The tree id whose floors cover the row.</param>
    /// <param name="partition">The WAL partition.</param>
    /// <param name="alsoCover">Generations held elsewhere (in memory) the floors must cover too.</param>
    /// <param name="floorsRaised">
    /// Called with the stored floors and the initiators the row named, after each raise and
    /// before the row is deleted.
    /// </param>
    /// <returns>The floors as stored after the last raise.</returns>
    /// <remarks>
    /// Every floor is raised over the row's own slots before the delete, so the purge's
    /// floor covers the maximum over every slot. An absent row is covered by
    /// <paramref name="alsoCover"/> alone and has nothing to delete.
    /// </remarks>
    internal static async Task<TreeRoutingFloorState> RetireGenerationsRowAsync(
        IGrainStorage storage,
        string treeId,
        int partition,
        IReadOnlyDictionary<WalRoutingInitiator, long>? alsoCover,
        Func<TreeRoutingFloorState, IReadOnlyCollection<WalRoutingInitiator>, Task>? floorsRaised)
    {
        ArgumentNullException.ThrowIfNull(storage);
        var key = WalAppendFenceRows.GenerationsKey(treeId, partition);
        var row = await WalAppendFenceRows.ReadGenerationsAsync(storage, treeId, partition).ConfigureAwait(true);
        for (var attempt = 1; ; attempt++)
        {
            var cover = alsoCover is null
                ? new Dictionary<WalRoutingInitiator, long>()
                : new Dictionary<WalRoutingInitiator, long>(alsoCover);
            var covered = new List<WalRoutingInitiator>();
            foreach (var (tag, generation) in row.State.Generations)
            {
                if (!WalAppendFenceRows.TryParseInitiatorTag(tag, out var initiator))
                {
                    continue;
                }

                covered.Add(initiator);
                cover[initiator] = Math.Max(cover.GetValueOrDefault(initiator), generation);
            }

            var floors = await WalAppendFenceRows.RaiseRoutingFloorsAsync(storage, treeId, cover).ConfigureAwait(true);
            if (floorsRaised is not null)
            {
                await floorsRaised(floors, covered).ConfigureAwait(true);
            }

            if (!row.RecordExists)
            {
                return floors;
            }

            try
            {
                await WalAppendFenceRows.ClearAsync(storage, WalAppendFenceRows.GenerationGrainType, key, row).ConfigureAwait(true);
                return floors;
            }
            catch (InconsistentStateException) when (attempt < WalShardGrain.MaxFenceRowAttempts)
            {
                // Rewritten since the read: cover every slot it holds now, then delete it.
                row = await WalAppendFenceRows.ReadGenerationsAsync(storage, treeId, partition).ConfigureAwait(true);
            }
        }
    }

    /// <summary>Retires every shard root's hand-off marker row of <paramref name="treeId"/>.</summary>
    internal static Task<TreeRoutingFloorState> RetireMarkersAsync(IGrainStorage storage, string treeId, int shardCount)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(shardCount);
        var keys = new string[shardCount];
        var initiators = new WalRoutingInitiator[shardCount];
        for (var shard = 0; shard < shardCount; shard++)
        {
            keys[shard] = HandOffMarkerRows.ShardKey(treeId, shard);
            initiators[shard] = WalRoutingInitiator.ShardRoot(shard);
        }

        return RetireRowsAsync<ShardHandOffState>(
            storage,
            treeId,
            HandOffMarkerRows.ShardGrainType,
            keys,
            initiators,
            static row => row.RoutingGeneration,
            alsoCover: null,
            floorsRaised: null);
    }

    /// <summary>
    /// Deletes the rows of <paramref name="grainType"/> under <paramref name="keys"/>,
    /// each conditionally on the ETag read, after raising each row's initiator's routing
    /// floor under <paramref name="treeId"/> durably to at least the generation it holds
    /// and <paramref name="alsoCover"/>, in one write. A row rewritten since the read is
    /// re-read, its initiator's floor raised over it, and deleted again.
    /// </summary>
    /// <param name="storage">The tree's grain storage provider.</param>
    /// <param name="treeId">The tree id whose floors cover the rows.</param>
    /// <param name="grainType">The rows' reserved grain type.</param>
    /// <param name="keys">The rows' keys; an absent row is skipped.</param>
    /// <param name="initiators">The initiator each row in <paramref name="keys"/> belongs to.</param>
    /// <param name="generationOf">The routing generation a row holds.</param>
    /// <param name="alsoCover">Generations held elsewhere (in memory) the floors must cover too.</param>
    /// <param name="floorsRaised">Called with the stored floors after each raise, before any row they cover is deleted.</param>
    /// <returns>The floors as stored after the last raise.</returns>
    /// <remarks>
    /// A row absent at the read is skipped (#5703 review, N2): there is nothing to
    /// retire, and none can appear behind the read for the retired incarnation. Its
    /// shard roots never re-create a marker they saw retired, nor re-stamp a write at or
    /// below its floor (#5705). A WAL partition refuses a generation claim at or below
    /// the floor, and the floor is raised before this returns. No re-created incarnation
    /// can write one either, because the purge runs this before it frees the id.
    /// </remarks>
    internal static async Task<TreeRoutingFloorState> RetireRowsAsync<T>(
        IGrainStorage storage,
        string treeId,
        string grainType,
        IReadOnlyList<string> keys,
        IReadOnlyList<WalRoutingInitiator> initiators,
        Func<T, long> generationOf,
        IReadOnlyDictionary<WalRoutingInitiator, long>? alsoCover,
        Func<TreeRoutingFloorState, Task>? floorsRaised)
        where T : new()
    {
        ArgumentNullException.ThrowIfNull(storage);
        ArgumentNullException.ThrowIfNull(keys);
        ArgumentNullException.ThrowIfNull(initiators);
        ArgumentNullException.ThrowIfNull(generationOf);
        if (initiators.Count != keys.Count)
        {
            throw new ArgumentException("One initiator per key.", nameof(initiators));
        }

        var rows = await BoundedFanOut.RunAsync(
            keys.Count,
            BoundedFanOut.DefaultWidth,
            i => WalAppendFenceRows.ReadRowAsync<T>(storage, grainType, keys[i])).ConfigureAwait(true);

        var cover = new Dictionary<WalRoutingInitiator, long>();
        if (alsoCover is not null)
        {
            foreach (var (initiator, value) in alsoCover)
            {
                cover[initiator] = value;
            }
        }

        for (var i = 0; i < rows.Length; i++)
        {
            if (rows[i].RecordExists)
            {
                cover[initiators[i]] = Math.Max(cover.GetValueOrDefault(initiators[i]), generationOf(rows[i].State));
            }
        }

        var floors = new FloorTracker(await WalAppendFenceRows.RaiseRoutingFloorsAsync(storage, treeId, cover).ConfigureAwait(true));
        if (floorsRaised is not null)
        {
            await floorsRaised(floors.Snapshot).ConfigureAwait(true);
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
                    if (generation > floors.Of(initiators[i]))
                    {
                        var raised = await WalAppendFenceRows.RaiseRoutingFloorsAsync(
                            storage, treeId, new Dictionary<WalRoutingInitiator, long> { [initiators[i]] = generation }).ConfigureAwait(true);
                        floors.Replace(raised);
                        if (floorsRaised is not null)
                        {
                            await floorsRaised(raised).ConfigureAwait(true);
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

        return floors.Snapshot;
    }

    /// <summary>
    /// The floors a retirement last stored, shared by its concurrent row deletions.
    /// Floors only rise, so a later stored state covers an earlier one.
    /// </summary>
    private sealed class FloorTracker(TreeRoutingFloorState initial)
    {
        private readonly object _gate = new();
        private TreeRoutingFloorState _floors = initial;

        public TreeRoutingFloorState Snapshot
        {
            get
            {
                lock (_gate)
                {
                    return new TreeRoutingFloorState { Floors = new Dictionary<string, long>(_floors.Floors, StringComparer.Ordinal) };
                }
            }
        }

        public long Of(WalRoutingInitiator initiator)
        {
            lock (_gate)
            {
                return WalAppendFenceRows.FloorOf(_floors, initiator);
            }
        }

        public void Replace(TreeRoutingFloorState stored)
        {
            lock (_gate)
            {
                foreach (var (tag, value) in stored.Floors)
                {
                    if (!_floors.Floors.TryGetValue(tag, out var current) || current < value)
                    {
                        _floors.Floors[tag] = value;
                    }
                }
            }
        }
    }
}
