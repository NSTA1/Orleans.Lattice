using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression tests for the Class B "persisted / in-memory divergence on
/// write failure (idempotency-guarded)" anti-pattern on
/// <see cref="ShardRootGrain"/>'s lifecycle methods (<c>MarkDeletedAsync</c>
/// and <c>UnmarkDeletedAsync</c>). Each method has an idempotency guard
/// (<c>if (state.State.IsDeleted) return;</c> and its negation) that
/// short-circuits retries on the post-mutation in-memory value - identical
/// shape to <see cref="TreeDeletionGrain.DeleteTreeAsync"/> /
/// <see cref="TreeDeletionGrain.RecoverAsync"/>.
/// </summary>
public class ShardRootGrainLifecycleTests
{
    private const string ShardKey = "test-tree/0";

    private static (ShardRootGrain grain, FakePersistentState<ShardRootState> state) CreateGrain(
        FakePersistentState<ShardRootState>? existingState = null)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", ShardKey));

        var state = existingState ?? new FakePersistentState<ShardRootState>();

        var factory = Substitute.For<IGrainFactory>();
        var optionsResolver = TestOptionsResolver.Create(baseOptions: new LatticeOptions(), factory: factory);

        var grain = new ShardRootGrain(
            context,
            state,
            factory,
            optionsResolver,
            Microsoft.Extensions.Logging.Abstractions.NullLogger<ShardRootGrain>.Instance,
            TestMutationObservers.NoObservers());
        return (grain, state);
    }

    [Test]
    public void MarkDeleted_reverts_in_memory_state_when_WriteStateAsync_throws()
    {
        var (grain, state) = CreateGrain();
        Assume.That(state.State.IsDeleted, Is.False);

        state.ThrowOnWrite = new InvalidOperationException("simulated storage failure");

        Assert.ThrowsAsync<InvalidOperationException>(async () => await grain.MarkDeletedAsync());

        // Without the fix, IsDeleted=true persists in memory while storage
        // remains IsDeleted=false. The method's idempotency guard
        // `if (state.State.IsDeleted) return;` short-circuits every retry
        // from this activation - a permanent split-brain on a transient
        // storage failure.
        Assert.That(state.State.IsDeleted, Is.False);
    }

    [Test]
    public async Task UnmarkDeleted_reverts_in_memory_state_when_WriteStateAsync_throws()
    {
        // Arrange: get the grain into the IsDeleted=true state.
        var (grain, state) = CreateGrain();
        await grain.MarkDeletedAsync();
        Assume.That(state.State.IsDeleted, Is.True);

        state.ThrowOnWrite = new InvalidOperationException("simulated storage failure");

        Assert.ThrowsAsync<InvalidOperationException>(async () => await grain.UnmarkDeletedAsync());

        // Without the fix, IsDeleted=false persists in memory while storage
        // remains IsDeleted=true. The method's reverse idempotency guard
        // `if (!state.State.IsDeleted) return;` short-circuits every retry.
        Assert.That(state.State.IsDeleted, Is.True);
    }

    [Test]
    public async Task WarmUpAsync_materializes_root_leaf_on_empty_shard()
    {
        // Brand-new shard root with no RootNodeId. Warm-up runs the
        // same EnsureRootAsync path the first traffic write would run -
        // it creates the deterministic root leaf and pings it. This
        // moves the first-write cost to startup time without producing
        // any grain the first write would not have produced itself.
        //
        // EnsureRootAsync calls leafGrain.GetGrainId() (the IAddressable
        // extension), which only succeeds when the substitute also
        // implements IGrainBase and exposes a GrainContext with a real
        // GrainId. We follow the same dual-interface stubbing pattern
        // ShardRootGrainEnsureRootTests already uses.
        var leafGrainContext = Substitute.For<IGrainContext>();
        var leafGrainId = GrainId.Create("leaf", "warmup-empty-leaf");
        leafGrainContext.GrainId.Returns(leafGrainId);
        var rootLeaf = Substitute.For<IBPlusLeafGrain, IGrainBase>();
        ((IGrainBase)rootLeaf).GrainContext.Returns(leafGrainContext);
        rootLeaf.SetTreeIdAsync(Arg.Any<string>()).Returns(Task.CompletedTask);
        rootLeaf.SetShardIndexAsync(Arg.Any<int>()).Returns(Task.CompletedTask);
        rootLeaf.CountAsync().Returns(Task.FromResult(0));

        var registry = Substitute.For<ILatticeRegistry>();
        registry.RegisterAsync(Arg.Any<string>(), Arg.Any<TreeRegistryEntry?>())
            .Returns(Task.CompletedTask);

        var state = new FakePersistentState<ShardRootState>();
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", ShardKey));
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>()).Returns(rootLeaf);
        factory.GetGrain<IBPlusLeafGrain>(Arg.Any<Guid>()).Returns(rootLeaf);
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        var optionsResolver = TestOptionsResolver.Create(baseOptions: new LatticeOptions(), factory: factory);
        var grain = new ShardRootGrain(
            context,
            state,
            factory,
            optionsResolver,
            Microsoft.Extensions.Logging.Abstractions.NullLogger<ShardRootGrain>.Instance,
            TestMutationObservers.NoObservers());

        Assume.That(state.State.RootNodeId, Is.Null);

        await grain.WarmUpAsync();

        // EnsureRootAsync persisted the deterministic root mapping.
        Assert.That(state.State.RootNodeId, Is.Not.Null);
        Assert.That(state.State.RootIsLeaf, Is.True);
        // And the read-only ping landed exactly once on that root leaf.
        await rootLeaf.Received(1).CountAsync();
    }

    [Test]
    public async Task WarmUpAsync_pings_root_leaf_when_tree_is_flat()
    {
        // Arrange a shard whose root is a leaf - the empty-bench-tree
        // shape that the proactive warm-up is designed to cover.
        var leafGrain = Substitute.For<IBPlusLeafGrain>();
        leafGrain.CountAsync().Returns(Task.FromResult(0));

        var rootLeafId = GrainId.Create("leaf", "test-tree/0:L0");
        var state = new FakePersistentState<ShardRootState>();
        state.State.RootNodeId = rootLeafId;
        state.State.RootIsLeaf = true;

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", ShardKey));
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IBPlusLeafGrain>(rootLeafId).Returns(leafGrain);
        var optionsResolver = TestOptionsResolver.Create(baseOptions: new LatticeOptions(), factory: factory);
        var grain = new ShardRootGrain(
            context,
            state,
            factory,
            optionsResolver,
            Microsoft.Extensions.Logging.Abstractions.NullLogger<ShardRootGrain>.Instance,
            TestMutationObservers.NoObservers());

        await grain.WarmUpAsync();

        // Read-only ping on the root leaf must have fired exactly once,
        // forcing leaf placement-directory + grain-storage hydration
        // before any traffic-driven write touches the same grain.
        await leafGrain.Received(1).CountAsync();
    }

    [Test]
    public async Task WarmUpAsync_pings_root_internal_when_tree_has_height()
    {
        // Arrange a shard whose root is an internal node - the populated-
        // tree shape. Warm-up must absorb the first internal-node first-
        // touch without walking the whole subtree.
        var internalGrain = Substitute.For<IBPlusInternalGrain>();
        internalGrain.AreChildrenLeavesAsync().Returns(Task.FromResult(true));

        var rootInternalId = GrainId.Create("int", "test-tree/0:I0");
        var state = new FakePersistentState<ShardRootState>();
        state.State.RootNodeId = rootInternalId;
        state.State.RootIsLeaf = false;

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", ShardKey));
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IBPlusInternalGrain>(rootInternalId).Returns(internalGrain);
        var optionsResolver = TestOptionsResolver.Create(baseOptions: new LatticeOptions(), factory: factory);
        var grain = new ShardRootGrain(
            context,
            state,
            factory,
            optionsResolver,
            Microsoft.Extensions.Logging.Abstractions.NullLogger<ShardRootGrain>.Instance,
            TestMutationObservers.NoObservers());

        await grain.WarmUpAsync();

        await internalGrain.Received(1).AreChildrenLeavesAsync();
    }
}
