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
        await registry.Received(1).SetAliasAsync(TreeId, snapshotTreeId);
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
            e.ShardMap == null
            && e.NextShardIndex == null
            && e.WalPartitions == null
            && e.WalPlacement == null));
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
        await registry.Received(1).UpdateAsync(TreeId, Arg.Is<TreeRegistryEntry>(e =>
            e.ShardMap != null
            && e.ShardMap.Slots[0] == 3
            && e.ShardMap.Slots.SequenceEqual(slots)
            && e.ShardMap.Version == 8
            && e.NextShardIndex == 3));
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
        await registry.Received(1).SetAliasAsync(TreeId, snapshotTreeId);
    }

    [Test]
    public async Task SwapAlias_falls_back_to_the_captured_entry_when_the_registry_has_none()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        PrepareSwap(state, $"{TreeId}/resized/op1");
        state.State.OldRegistryEntry = new TreeRegistryEntry { ShardCount = ShardCount, PublishEvents = true };
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        registry.GetEntryAsync(TreeId).Returns(Task.FromResult<TreeRegistryEntry?>(null));

        await grain.SwapAliasAsync();

        await registry.Received(1).UpdateAsync(TreeId, Arg.Is<TreeRegistryEntry>(e =>
            e.PublishEvents == true && e.MaxLeafKeys == 256 && e.ShardCount == ShardCount));
    }
}
