using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for issue #4453: the order of the steps an after-swap
/// undo takes. It used to clear the old copy's fence before moving the alias
/// back, and arm the resized copy's redirect after it, so in each gap a router
/// whose cached alias named the copy the registry did not could still read and
/// write it while fresh routers used the other copy. The intended order mirrors
/// the forward swap's fence-before-flip (#4362): arm the resized copy, swap,
/// then clear the old copy's fence (the shard-ownership spec's UndoArm,
/// UndoSwap and UndoClear).
/// </summary>
public partial class TreeResizeGrainTests
{
    [Test]
    public async Task UndoResize_after_swap_arms_the_resized_copy_before_the_swap_and_lifts_the_old_fence_after_it()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        SeedInFlightResize(state, ResizePhase.Reject);
        SetupOldTreeDeletion(grainFactory, isDeleted: false);
        var snapshotTreeId = $"{TreeId}/resized/{UndoSnapshotSuffix}";
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var resizedRow = new TreeRegistryEntry
        {
            ShardCount = ShardCount,
            PhysicalTreeId = snapshotTreeId,
            ShardMap = ShardMap.CreateDefault(LatticeConstants.DefaultVirtualShardCount, ShardCount),
        };
        var current = resizedRow;
        registry.GetEntryAsync(TreeId).Returns(_ => Task.FromResult<TreeRegistryEntry?>(current));

        var order = new List<string>();
        registry.SwapAliasAsync(TreeId, TreeId, Arg.Any<ShardMap>(), Arg.Any<int?>(), Arg.Any<string?>()).Returns(_ =>
        {
            order.Add("swap");
            var before = current;
            current = resizedRow with { PhysicalTreeId = TreeId };
            return Task.FromResult<TreeRegistryEntry?>(before);
        });
        for (var i = 0; i < ShardCount; i++)
        {
            var index = i;
            grainFactory.GetGrain<IShardRootGrain>($"{snapshotTreeId}/{i}")
                .MarkRetainedRedirectAsync(TreeId, $"{UndoSnapshotSuffix}:undo", TreeId)
                .Returns(_ =>
                {
                    order.Add($"arm-{index}");
                    return Task.CompletedTask;
                });
            grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/{i}")
                .ClearShadowForwardAsync(UndoSnapshotSuffix)
                .Returns(_ =>
                {
                    order.Add($"clear-{index}");
                    return Task.CompletedTask;
                });
        }

        await grain.UndoResizeAsync();

        var swap = order.IndexOf("swap");
        Assert.Multiple(() =>
        {
            Assert.That(swap, Is.GreaterThanOrEqualTo(0), "the alias must move back");
            for (var i = 0; i < ShardCount; i++)
            {
                var arm = order.IndexOf($"arm-{i}");
                var clear = order.IndexOf($"clear-{i}");
                Assert.That(arm, Is.InRange(0, swap - 1),
                    $"resized shard {i} must redirect before the alias moves back, or a router that cached the "
                    + "resized copy keeps writing to a copy the undo discards (order: " + string.Join(", ", order) + ")");
                Assert.That(clear, Is.GreaterThan(swap),
                    $"old shard {i} must stay fenced until the alias names it again, or a router that cached the "
                    + "old copy serves it while the registry routes to the resized copy (order: " + string.Join(", ", order) + ")");
            }
        });
        await AssertAfterSwapCompensationCompleteAsync(state, grainFactory);
    }

    [Test]
    public async Task UndoResize_releases_the_resized_copy_redirect_when_the_swap_back_fails()
    {
        // The arm now precedes the swap, so a swap the ownership guard refuses
        // would otherwise leave the alias naming a copy that refuses every routed
        // call while the old copy is still fenced: the tree would be unavailable.
        var (grain, state, _, grainFactory, _) = CreateGrain();
        SeedInFlightResize(state, ResizePhase.Reject);
        SetupOldTreeDeletion(grainFactory, isDeleted: false);
        var snapshotTreeId = $"{TreeId}/resized/{UndoSnapshotSuffix}";
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        registry.GetEntryAsync(TreeId).Returns(Task.FromResult<TreeRegistryEntry?>(new TreeRegistryEntry
        {
            ShardCount = ShardCount,
            PhysicalTreeId = snapshotTreeId,
        }));
        registry.ResolveAsync(TreeId).Returns(Task.FromResult(snapshotTreeId));
        registry.SwapAliasAsync(TreeId, TreeId, Arg.Any<ShardMap>(), Arg.Any<int?>(), Arg.Any<string?>())
            .ThrowsAsync(new InvalidOperationException("refused"));

        Assert.ThrowsAsync<InvalidOperationException>(() => grain.UndoResizeAsync());

        for (var i = 0; i < ShardCount; i++)
        {
            var resized = grainFactory.GetGrain<IShardRootGrain>($"{snapshotTreeId}/{i}");
            await resized.Received(1).MarkRetainedRedirectAsync(TreeId, $"{UndoSnapshotSuffix}:undo", TreeId);
            await resized.Received(1).ClearRetainedRedirectAsync($"{UndoSnapshotSuffix}:undo");
            await grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/{i}")
                .DidNotReceive().ClearShadowForwardAsync(Arg.Any<string>());
        }
        Assert.That(state.State.InProgress, Is.True, "a failed undo leaves the resize to be retried");
    }

    [Test]
    public async Task UndoResize_keeps_the_resized_copy_redirect_when_a_failed_swap_back_reached_the_registry()
    {
        var (grain, state, _, grainFactory, _) = CreateGrain();
        SeedInFlightResize(state, ResizePhase.Reject);
        SetupOldTreeDeletion(grainFactory, isDeleted: false);
        var snapshotTreeId = $"{TreeId}/resized/{UndoSnapshotSuffix}";
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        registry.GetEntryAsync(TreeId).Returns(Task.FromResult<TreeRegistryEntry?>(new TreeRegistryEntry
        {
            ShardCount = ShardCount,
            PhysicalTreeId = snapshotTreeId,
        }));
        registry.ResolveAsync(TreeId).Returns(Task.FromResult(TreeId));
        registry.SwapAliasAsync(TreeId, TreeId, Arg.Any<ShardMap>(), Arg.Any<int?>(), Arg.Any<string?>())
            .ThrowsAsync(new TimeoutException("lost reply"));

        Assert.ThrowsAsync<TimeoutException>(() => grain.UndoResizeAsync());

        for (var i = 0; i < ShardCount; i++)
        {
            await grainFactory.GetGrain<IShardRootGrain>($"{snapshotTreeId}/{i}")
                .DidNotReceive().ClearRetainedRedirectAsync(Arg.Any<string>());
        }
    }
}
