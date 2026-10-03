using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for the registry entry the alias swap writes for the
/// logical tree. It used to be built from scratch with only the new sizing, so
/// every configuration override set against the logical id - PublishEvents,
/// projection digest maintenance and its latch, history retention, and the cache
/// and WAL retention ceilings - was silently reset by every resize.
/// </summary>
public partial class TreeResizeGrainTests
{
    private static void PrepareSwap(FakePersistentState<TreeResizeState> state, string snapshotTreeId)
    {
        state.State.InProgress = true;
        state.State.Phase = ResizePhase.Swap;
        state.State.NewMaxLeafKeys = 256;
        state.State.NewMaxInternalChildren = 64;
        state.State.ShardCount = ShardCount;
        state.State.SnapshotTreeId = snapshotTreeId;
    }

    [Test]
    public async Task SwapAlias_preserves_the_logical_trees_configuration_overrides()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        var snapshotTreeId = $"{TreeId}/resized/op1";
        PrepareSwap(state, snapshotTreeId);
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        registry.GetEntryAsync(TreeId).Returns(Task.FromResult<TreeRegistryEntry?>(new TreeRegistryEntry
        {
            MaxLeafKeys = 128,
            MaxInternalChildren = 128,
            ShardCount = ShardCount,
            PublishEvents = false,
            MaintainProjectionDigest = false,
            ProjectionDigestPermanentlyDisabled = true,
            HistoryRetentionMode = HistoryRetentionMode.FullValue,
            HistoryRetentionWindowTicks = TimeSpan.FromHours(6).Ticks,
            MaxCacheValueBytes = 4096,
            WalMaxRetainedBytes = 1 << 20,
        }));

        await grain.SwapAliasAsync();

        await registry.Received(1).UpdateAsync(TreeId, Arg.Is<TreeRegistryEntry>(e =>
            e.MaxLeafKeys == 256
            && e.MaxInternalChildren == 64
            && e.ShardCount == ShardCount
            && e.PublishEvents == false
            && e.MaintainProjectionDigest == false
            && e.ProjectionDigestPermanentlyDisabled == true
            && e.HistoryRetentionMode == HistoryRetentionMode.FullValue
            && e.HistoryRetentionWindowTicks == TimeSpan.FromHours(6).Ticks
            && e.MaxCacheValueBytes == 4096
            && e.WalMaxRetainedBytes == 1 << 20));
        await registry.Received(1).SwapAliasAsync(TreeId, snapshotTreeId, Arg.Any<ShardMap>(), Arg.Any<int?>(), TreeId);
    }

    [Test]
    public async Task SwapAlias_drops_the_retired_physical_trees_wal_layout_and_routes_by_the_resized_copy()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        PrepareSwap(state, $"{TreeId}/resized/op1");
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        registry.GetEntryAsync(TreeId).Returns(Task.FromResult<TreeRegistryEntry?>(new TreeRegistryEntry
        {
            ShardCount = ShardCount,
            ShardMap = ShardMap.CreateDefault(LatticeConstants.DefaultVirtualShardCount, ShardCount),
            NextShardIndex = 3,
            WalPartitions = 8,
        }));

        await grain.SwapAliasAsync();

        // The WAL layout describes the retired copy. The routing is the resized
        // copy's, which here was registered without a map (an unsplit source).
        await registry.Received(1).UpdateAsync(TreeId, Arg.Is<TreeRegistryEntry>(e =>
            e.WalPartitions == null
            && e.WalPlacement == null));
        var defaultSlots = ShardMap.CreateDefault(LatticeConstants.DefaultVirtualShardCount, ShardCount).Slots;
        await registry.Received(1).SwapAliasAsync(
            TreeId,
            $"{TreeId}/resized/op1",
            Arg.Is<ShardMap>(m => m.Slots.SequenceEqual(defaultSlots)),
            null,
            TreeId);
    }

    [Test]
    public async Task SwapAlias_carries_the_resized_copys_split_routing_onto_the_logical_tree()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        var snapshotTreeId = $"{TreeId}/resized/op1";
        PrepareSwap(state, snapshotTreeId);
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var slots = (int[])ShardMap.CreateDefault(LatticeConstants.DefaultVirtualShardCount, ShardCount).Slots.Clone();
        slots[0] = 3;
        registry.GetEntryAsync(TreeId).Returns(Task.FromResult<TreeRegistryEntry?>(new TreeRegistryEntry
        {
            ShardCount = ShardCount,
            ShardMap = new ShardMap { Slots = slots, Version = 7 },
            NextShardIndex = 3,
        }));
        registry.GetEntryAsync(snapshotTreeId).Returns(Task.FromResult<TreeRegistryEntry?>(new TreeRegistryEntry
        {
            ShardCount = ShardCount,
            ShardMap = new ShardMap { Slots = (int[])slots.Clone(), Version = 7 },
            NextShardIndex = 3,
        }));

        await grain.SwapAliasAsync();

        // Dropping the map would route the split slot back to shard 0 of the
        // resized copy, which the copy never populated for it (issue 3880).
        await registry.Received(1).SwapAliasAsync(
            TreeId,
            snapshotTreeId,
            Arg.Is<ShardMap>(m => m.Slots[0] == 3 && m.Slots.SequenceEqual(slots)),
            3,
            TreeId);
    }

    [Test]
    public async Task SwapAlias_keeps_the_current_alias_until_the_flip()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        var snapshotTreeId = $"{TreeId}/resized/op2";
        PrepareSwap(state, snapshotTreeId);
        var previousCopy = $"{TreeId}/resized/op1";
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        registry.GetEntryAsync(TreeId).Returns(Task.FromResult<TreeRegistryEntry?>(new TreeRegistryEntry
        {
            ShardCount = ShardCount,
            PhysicalTreeId = previousCopy,
        }));

        await grain.SwapAliasAsync();

        // Clearing the alias ahead of the flip would route the logical tree
        // back to its first physical copy - long since retired - in between.
        await registry.Received(1).UpdateAsync(TreeId, Arg.Is<TreeRegistryEntry>(e =>
            e.PhysicalTreeId == previousCopy));
        await registry.Received(1).SwapAliasAsync(TreeId, snapshotTreeId, Arg.Any<ShardMap>(), Arg.Any<int?>(), previousCopy);
    }

    [Test]
    public async Task SwapAlias_moves_the_alias_and_the_map_in_one_registry_write()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        var snapshotTreeId = $"{TreeId}/resized/op1";
        PrepareSwap(state, snapshotTreeId);
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        registry.GetEntryAsync(TreeId).Returns(Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry { ShardCount = ShardCount }));

        await grain.SwapAliasAsync();

        // Written as a separate map carry and alias flip, a reader resolving
        // between them paired the old tree with the resized copy's map (#4336).
        // The sizing write leaves routing alone; the swap moves both at once.
        await registry.Received(1).UpdateAsync(TreeId, Arg.Is<TreeRegistryEntry>(e =>
            e.ShardMap == null && e.AliasCutoverTarget == null && e.PhysicalTreeId == null));
        await registry.Received(1).SwapAliasAsync(TreeId, snapshotTreeId, Arg.Any<ShardMap>(), Arg.Any<int?>(), TreeId);
        await registry.DidNotReceive().SetAliasAsync(Arg.Any<string>(), Arg.Any<string>());
    }

    [Test]
    public async Task SwapAlias_resumed_after_the_swap_does_not_swap_again()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        var snapshotTreeId = $"{TreeId}/resized/op1";
        PrepareSwap(state, snapshotTreeId);
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        registry.GetEntryAsync(TreeId).Returns(Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry { ShardCount = ShardCount, PhysicalTreeId = snapshotTreeId }));

        await grain.SwapAliasAsync();

        // A second swap would overwrite the logical map, and any split the
        // resized copy committed onto it since, with the copy's original map.
        await registry.DidNotReceive().SwapAliasAsync(
            Arg.Any<string>(), Arg.Any<string>(), Arg.Any<ShardMap>(), Arg.Any<int?>(), Arg.Any<string?>());
    }

    [Test]
    public async Task SwapAlias_refuses_a_logical_tree_with_no_registry_row()
    {
        // Issue #4270: the swap used to fall back to the captured entry, or an
        // empty one, and upsert it - recreating a purged tree's row.
        var (grain, state, _, grainFactory, _) = CreateGrain();
        var snapshotTreeId = $"{TreeId}/resized/op1";
        PrepareSwap(state, snapshotTreeId);
        state.State.OldRegistryEntry = new TreeRegistryEntry { ShardCount = ShardCount, PublishEvents = true };
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        registry.GetEntryAsync(TreeId).Returns(Task.FromResult<TreeRegistryEntry?>(null));

        var ex = Assert.ThrowsAsync<LatticeTreeNotRegisteredException>(() => grain.SwapAliasAsync());

        Assert.That(ex!.TreeId, Is.EqualTo(TreeId));
        await registry.DidNotReceive().UpdateAsync(Arg.Any<string>(), Arg.Any<TreeRegistryEntry>());
        await registry.DidNotReceive().SwapAliasAsync(
            Arg.Any<string>(), Arg.Any<string>(), Arg.Any<ShardMap>(), Arg.Any<int?>(), Arg.Any<string?>());
    }
}
