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
}
