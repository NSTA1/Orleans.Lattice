using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for snapshotting a tree whose adaptive shard splits
/// allocated a physical shard above the pinned <c>ShardCount</c> (issue 3880).
/// The snapshot used to register an identity-routed destination and copy,
/// shadow-forward and lock only shards <c>0</c> to <c>ShardCount - 1</c>, so the
/// split shard's keys were dropped and the sealed copies the split left on the
/// shard that gave its slots up were copied in their place.
/// </summary>
public partial class TreeSnapshotGrainTests
{
    /// <summary>
    /// Physical index an adaptive split allocated: above the pinned
    /// <see cref="ShardCount"/> of 2, with index 2 unrouted.
    /// </summary>
    private const int SplitShardIndex = 3;

    private static readonly int[] SplitShardIndices = [0, 1, SplitShardIndex];

    /// <summary>
    /// Picks keys the default map routes to shard 0 and builds a map that moves
    /// the first one's slot to <see cref="SplitShardIndex"/>, leaving the rest
    /// on shard 0.
    /// </summary>
    private static (ShardMap Map, string MovedKey, string[] StayingKeys) SplitRouting()
    {
        const int vsc = LatticeConstants.DefaultVirtualShardCount;
        var onShard0 = Enumerable.Range(0, 200)
            .Select(i => $"key-{i:D3}")
            .Where(k => ShardMap.GetVirtualSlot(k, vsc) % ShardCount == 0)
            .ToList();
        var moved = onShard0[0];
        var movedSlot = ShardMap.GetVirtualSlot(moved, vsc);
        var staying = onShard0.Skip(1)
            .Where(k => ShardMap.GetVirtualSlot(k, vsc) != movedSlot)
            .Take(2)
            .ToArray();

        var slots = (int[])ShardMap.CreateDefault(vsc, ShardCount).Slots.Clone();
        slots[movedSlot] = SplitShardIndex;
        return (new ShardMap { Slots = slots, Version = 4 }, moved, staying);
    }

    private static void UseSplitSourceEntry(IGrainFactory grainFactory, ShardMap map)
    {
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        registry.GetEntryAsync(SourceTreeId).Returns(Task.FromResult<TreeRegistryEntry?>(new TreeRegistryEntry
        {
            MaxLeafKeys = 128,
            MaxInternalChildren = 128,
            ShardCount = ShardCount,
            ShardMap = map,
            NextShardIndex = SplitShardIndex,
        }));
    }

    [Test]
    public async Task Initiate_registers_the_destination_with_the_sources_routing_and_records_the_split_shard()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        var (map, _, _) = SplitRouting();
        UseSplitSourceEntry(grainFactory, map);

        await grain.InitiateSnapshotStateAsync(DestTreeId, SnapshotMode.Online, ShardCount,
            logicalTreeId: SourceTreeId);

        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.Received(1).RegisterAsync(DestTreeId, Arg.Is<TreeRegistryEntry>(e =>
            e!.ShardCount == ShardCount
            && e.ShardMap != null
            && !ReferenceEquals(e.ShardMap, map)
            && e.ShardMap.Slots.SequenceEqual(map.Slots)
            && e.NextShardIndex == SplitShardIndex));
        Assert.That(state.State.ShardIndices, Is.EqualTo(SplitShardIndices));
        Assert.That(state.State.SourceShardMap, Is.SameAs(map));
    }

    [Test]
    public async Task Initiate_reads_the_routing_of_the_logical_tree_it_is_given()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        var (map, _, _) = SplitRouting();
        const string logicalTreeId = "logical-tree";
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        registry.GetEntryAsync(logicalTreeId).Returns(Task.FromResult<TreeRegistryEntry?>(new TreeRegistryEntry
        {
            ShardCount = ShardCount,
            ShardMap = map,
        }));

        await grain.InitiateSnapshotStateAsync(DestTreeId, SnapshotMode.Online, ShardCount,
            operationId: "op-logical", logicalTreeId: logicalTreeId);

        // A split writes the logical tree's entry, never the aliased physical
        // copy's, so the source's own entry (unsplit here) is not the routing.
        Assert.That(state.State.ShardIndices, Is.EqualTo(SplitShardIndices));
        Assert.That(state.State.SourceShardMap, Is.SameAs(map));
    }

    [Test]
    public async Task Initiate_records_the_pinned_range_for_an_unsplit_source()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();

        await grain.InitiateSnapshotStateAsync(DestTreeId, SnapshotMode.Online, ShardCount,
            logicalTreeId: SourceTreeId);

        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.Received(1).RegisterAsync(DestTreeId, Arg.Is<TreeRegistryEntry>(e =>
            e!.ShardMap == null && e.NextShardIndex == null));
        Assert.That(state.State.ShardIndices, Is.EqualTo(new[] { 0, 1 }));
        Assert.That(state.State.SourceShardMap, Is.Null);
    }

    [Test]
    public async Task BeginShadowForward_covers_a_shard_a_split_allocated_above_the_pinned_count()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        state.State.InProgress = true;
        state.State.Phase = SnapshotPhase.ShadowBegin;
        state.State.ShardCount = ShardCount;
        state.State.ShardIndices = SplitShardIndices;
        state.State.DestinationTreeId = DestTreeId;
        state.State.OperationId = "op-split";
        state.State.Mode = SnapshotMode.Online;

        await grain.BeginShadowForwardAllShardsAsync();

        foreach (var index in SplitShardIndices)
        {
            await grainFactory.GetGrain<IShardRootGrain>($"{SourceTreeId}/{index}")
                .Received(1).BeginShadowForwardAsync(DestTreeId, "op-split", SourceTreeId);
        }
        await grainFactory.GetGrain<IShardRootGrain>($"{SourceTreeId}/2")
            .DidNotReceive().BeginShadowForwardAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>());
    }

    [Test]
    public async Task LockSourceShards_locks_a_shard_a_split_allocated_above_the_pinned_count()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        state.State.InProgress = true;
        state.State.Phase = SnapshotPhase.Lock;
        state.State.ShardCount = ShardCount;
        state.State.ShardIndices = SplitShardIndices;
        state.State.Mode = SnapshotMode.Offline;

        await grain.LockSourceShardsAsync();

        foreach (var index in SplitShardIndices)
        {
            await grainFactory.GetGrain<IShardRootGrain>($"{SourceTreeId}/{index}").Received(1).MarkDeletedAsync();
        }
    }

    [Test]
    public async Task Online_drain_copies_the_split_shard_and_drops_the_sealed_copies_left_behind()
    {
        var (grain, state, reminderRegistry, grainFactory, _) = CreateGrain();
        SetupKeepalive(reminderRegistry);
        var (map, moved, staying) = SplitRouting();

        SetupShardForSnapshot(grainFactory, SourceTreeId, 0, new Dictionary<string, byte[]>
        {
            [moved] = [0], // the sealed copy the split left behind
            [staying[0]] = [1],
            [staying[1]] = [2],
        }, GrainId.Create("leaf", "split-src-0"));
        SetupShardForSnapshot(grainFactory, SourceTreeId, 1);
        SetupShardForSnapshot(grainFactory, SourceTreeId, SplitShardIndex, new Dictionary<string, byte[]>
        {
            [moved] = [9],
        }, GrainId.Create("leaf", "split-src-3"));
        var dest0 = Substitute.For<IShardRootGrain>();
        grainFactory.GetGrain<IShardRootGrain>($"{DestTreeId}/0").Returns(dest0);
        var dest3 = Substitute.For<IShardRootGrain>();
        grainFactory.GetGrain<IShardRootGrain>($"{DestTreeId}/{SplitShardIndex}").Returns(dest3);

        state.State.InProgress = true;
        state.State.Phase = SnapshotPhase.Copy;
        state.State.NextShardIndex = 0;
        state.State.ShardCount = ShardCount;
        state.State.ShardIndices = SplitShardIndices;
        state.State.SourceShardMap = map;
        state.State.DestinationTreeId = DestTreeId;
        state.State.Mode = SnapshotMode.Online;
        state.State.OperationId = "op-split-drain";

        await grain.DrainAllShardsOnlineAsync();

        await dest0.Received(1).MergeManyAsync(Arg.Is<Dictionary<string, LwwValue<byte[]>>>(d =>
            d.Count == 2 && d.ContainsKey(staying[0]) && d.ContainsKey(staying[1])));
        await dest3.Received(1).MergeManyAsync(Arg.Is<Dictionary<string, LwwValue<byte[]>>>(d =>
            d.Count == 1 && d[moved].Value!.SequenceEqual(new byte[] { 9 })));
        foreach (var index in SplitShardIndices)
        {
            await grainFactory.GetGrain<IShardRootGrain>($"{SourceTreeId}/{index}")
                .Received(1).MarkDrainedAsync("op-split-drain");
        }
        Assert.That(state.State.NextShardIndex, Is.EqualTo(SplitShardIndices.Length));
    }

    [Test]
    public async Task Offline_copy_walks_the_split_shard_by_position_and_bulk_loads_it_under_its_own_index()
    {
        var (grain, state, reminderRegistry, grainFactory, _) = CreateGrain();
        SetupKeepalive(reminderRegistry);
        var (map, moved, _) = SplitRouting();
        SetupShardForSnapshot(grainFactory, SourceTreeId, SplitShardIndex, new Dictionary<string, byte[]>
        {
            [moved] = [9],
        }, GrainId.Create("leaf", "split-src-3-offline"));
        var dest3 = Substitute.For<IShardRootGrain>();
        grainFactory.GetGrain<IShardRootGrain>($"{DestTreeId}/{SplitShardIndex}").Returns(dest3);

        state.State.InProgress = true;
        state.State.Phase = SnapshotPhase.Copy;
        state.State.NextShardIndex = 2; // position of index 3 in [0, 1, 3]
        state.State.ShardCount = ShardCount;
        state.State.ShardIndices = SplitShardIndices;
        state.State.SourceShardMap = map;
        state.State.DestinationTreeId = DestTreeId;
        state.State.Mode = SnapshotMode.Offline;
        state.State.OperationId = "op-offline";

        await grain.ProcessNextPhaseAsync();

        await dest3.Received(1).BulkLoadRawAsync($"op-offline-snapshot-{SplitShardIndex}",
            Arg.Is<List<LwwEntry>>(e => e.Count == 1 && e[0].Key == moved));
        Assert.That(state.State.Phase, Is.EqualTo(SnapshotPhase.Unmark));

        await grain.ProcessNextPhaseAsync();

        await grainFactory.GetGrain<IShardRootGrain>($"{SourceTreeId}/{SplitShardIndex}")
            .Received(1).UnmarkDeletedAsync();
        Assert.That(state.State.NextShardIndex, Is.EqualTo(SplitShardIndices.Length));
    }

    [Test]
    public async Task Completion_clears_the_captured_routing()
    {
        var (grain, state, _, _, _) = CreateGrain();
        var (map, _, _) = SplitRouting();
        state.State.InProgress = true;
        state.State.Phase = SnapshotPhase.Copy;
        state.State.ShardCount = ShardCount;
        state.State.ShardIndices = SplitShardIndices;
        state.State.SourceShardMap = map;
        state.State.NextShardIndex = SplitShardIndices.Length;
        state.State.DestinationTreeId = DestTreeId;
        state.State.Mode = SnapshotMode.Offline;
        state.State.OperationId = "op-complete";

        await grain.CompleteSnapshotAsync();

        Assert.That(state.State.Complete, Is.True);
        Assert.That(state.State.ShardIndices, Is.Null);
        Assert.That(state.State.SourceShardMap, Is.Null);
    }
}
