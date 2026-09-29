using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for resizing a tree whose adaptive shard splits
/// allocated a physical shard above the pinned <c>ShardCount</c> (issue 3880).
/// The reject and undo fan-outs used to walk <c>0</c> to <c>ShardCount - 1</c>,
/// so a split-added shard was never rejected after the alias swap nor released
/// by an undo.
/// </summary>
public partial class TreeResizeGrainTests
{
    /// <summary>
    /// Physical index an adaptive split allocated: above the pinned
    /// <see cref="ShardCount"/> of 2, with index 2 unrouted.
    /// </summary>
    private const int SplitShardIndex = 3;

    private static readonly int[] SplitShardIndices = [0, 1, SplitShardIndex];

    private static ShardMap SplitMap(long version = 2)
    {
        var slots = (int[])ShardMap.CreateDefault(LatticeConstants.DefaultVirtualShardCount, ShardCount).Slots.Clone();
        slots[0] = SplitShardIndex;
        return new ShardMap { Slots = slots, Version = version };
    }

    [Test]
    public async Task InitiateResize_records_every_shard_the_logical_routing_map_names()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        registry.GetEntryAsync(TreeId).Returns(Task.FromResult<TreeRegistryEntry?>(new TreeRegistryEntry
        {
            MaxLeafKeys = 128,
            MaxInternalChildren = 128,
            ShardCount = ShardCount,
            ShardMap = SplitMap(),
            NextShardIndex = SplitShardIndex,
        }));

        await grain.InitiateResizeStateAsync(256, 64);

        Assert.That(state.State.ShardIndices, Is.EqualTo(SplitShardIndices));
        Assert.That(state.State.ShardCount, Is.EqualTo(ShardCount));
    }

    [Test]
    public async Task InitiateResize_records_the_pinned_range_for_an_unsplit_tree()
    {
        var (grain, state, _, _, _) = CreateGrain();

        await grain.InitiateResizeStateAsync(256, 64);

        Assert.That(state.State.ShardIndices, Is.EqualTo(new[] { 0, 1 }));
    }

    [Test]
    public async Task RejectOldShards_rejects_a_shard_a_split_allocated_above_the_pinned_count()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        state.State.InProgress = true;
        state.State.Phase = ResizePhase.Reject;
        state.State.OperationId = "reject-split";
        state.State.ShardCount = ShardCount;
        state.State.ShardIndices = SplitShardIndices;
        state.State.OldPhysicalTreeId = TreeId;

        await grain.RejectOldShardsAsync();

        foreach (var index in SplitShardIndices)
        {
            await grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/{index}")
                .Received(1).EnterRejectingAsync("reject-split");
        }
        await grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/2")
            .DidNotReceive().EnterRejectingAsync(Arg.Any<string>());
        Assert.That(state.State.Phase, Is.EqualTo(ResizePhase.Cleanup));
    }

    [Test]
    public async Task UndoResize_during_drain_releases_a_split_allocated_shard()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        state.State.InProgress = true;
        state.State.Phase = ResizePhase.Snapshot;
        state.State.OperationId = "undo-drain-split";
        state.State.ShardCount = ShardCount;
        state.State.ShardIndices = SplitShardIndices;
        state.State.OldPhysicalTreeId = TreeId;
        state.State.SnapshotTreeId = $"{TreeId}/resized/undo-drain-split";

        await grain.UndoResizeAsync();

        foreach (var index in SplitShardIndices)
        {
            await grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/{index}")
                .Received(1).ClearShadowForwardAsync("undo-drain-split");
        }
    }

    [Test]
    public async Task UndoResize_after_swap_releases_a_split_allocated_shard()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        state.State.InProgress = true;
        state.State.Phase = ResizePhase.Cleanup;
        state.State.OperationId = "undo-swap-split";
        state.State.ShardCount = ShardCount;
        state.State.ShardIndices = SplitShardIndices;
        state.State.OldPhysicalTreeId = TreeId;
        state.State.SnapshotTreeId = $"{TreeId}/resized/undo-swap-split";

        await grain.UndoResizeAsync();

        foreach (var index in SplitShardIndices)
        {
            await grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/{index}")
                .Received(1).ClearShadowForwardAsync("undo-swap-split");
        }
    }

    [Test]
    public async Task Cleanup_records_the_logical_routing_on_a_derived_old_copy_before_deleting_it()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        var previousCopy = $"{TreeId}/resized/op0";
        state.State.InProgress = true;
        state.State.Phase = ResizePhase.Cleanup;
        state.State.OldPhysicalTreeId = previousCopy;
        var routing = SplitMap(version: 5);
        state.State.OldRegistryEntry = new TreeRegistryEntry
        {
            ShardCount = ShardCount,
            ShardMap = routing,
            NextShardIndex = SplitShardIndex,
        };
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        registry.GetEntryAsync(previousCopy).Returns(Task.FromResult<TreeRegistryEntry?>(new TreeRegistryEntry
        {
            ShardCount = ShardCount,
            DerivedFrom = TreeId,
        }));

        var order = new List<string>();
        registry.When(r => r.UpdateAsync(previousCopy, Arg.Any<TreeRegistryEntry>()))
            .Do(_ => order.Add("update"));
        var deletion = grainFactory.GetGrain<ITreeDeletionGrain>(previousCopy);
        deletion.When(d => d.DeleteDerivedPhysicalTreeAsync()).Do(_ => order.Add("delete"));

        await grain.CleanupOldTreeAsync();

        // A split after the copy was made wrote only the logical entry; the
        // deletion walk reads the copy's own entry, so it must learn of the
        // split shard before the copy is deleted.
        await registry.Received(1).UpdateAsync(previousCopy, Arg.Is<TreeRegistryEntry>(e =>
            ReferenceEquals(e.ShardMap, routing)
            && e.NextShardIndex == SplitShardIndex
            && e.DerivedFrom == TreeId));
        Assert.That(order, Is.EqualTo(new[] { "update", "delete" }));
    }

    [Test]
    public async Task Cleanup_leaves_a_derived_old_copys_entry_alone_when_the_tree_was_never_split()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        var previousCopy = $"{TreeId}/resized/op0";
        state.State.InProgress = true;
        state.State.Phase = ResizePhase.Cleanup;
        state.State.OldPhysicalTreeId = previousCopy;
        state.State.OldRegistryEntry = new TreeRegistryEntry { ShardCount = ShardCount };

        await grain.CleanupOldTreeAsync();

        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.DidNotReceive().UpdateAsync(previousCopy, Arg.Any<TreeRegistryEntry>());
        await grainFactory.GetGrain<ITreeDeletionGrain>(previousCopy).Received(1).DeleteDerivedPhysicalTreeAsync();
    }
}
