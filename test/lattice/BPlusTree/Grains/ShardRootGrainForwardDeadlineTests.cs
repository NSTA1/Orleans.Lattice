using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for the bounded outbound shard-forward deadline on
/// <see cref="ShardRootGrain"/>. The deadline keeps a write forward that
/// parks against a sibling shard whose ownership is changing during a
/// reshard swap from pinning the foreground write turn indefinitely: it
/// abandons the parked forward and faults the turn with a
/// <see cref="TimeoutException"/> so the existing stale-routing retry loop
/// can re-run the operation against refreshed routing.
/// </summary>
public class ShardRootGrainForwardDeadlineTests
{
    private const string TreeId = "src-tree";
    private const string DestTreeId = "src-tree/resized/op-1";
    private const string OperationId = "op-1";
    private const int ShardIndex = 0;

    private sealed class GrainHarness
    {
        public required ShardRootGrain Grain { get; init; }
        public required IShardRootGrain ShadowTarget { get; init; }
        public required FakePersistentState<ShardRootState> State { get; init; }
    }

    private static GrainHarness CreateHarness(
        TimeSpan shardForwardTimeout,
        Func<Task>? shadowSetBehavior = null)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", $"{TreeId}/{ShardIndex}"));

        var state = new FakePersistentState<ShardRootState>();
        state.State.RootNodeId ??= GrainId.Create("leaf", "test-leaf");
        state.State.RootIsLeaf = true;

        var factory = Substitute.For<IGrainFactory>();
        var baseOptions = new LatticeOptions { ShardForwardTimeout = shardForwardTimeout };
        var optionsResolver = TestOptionsResolver.Create(baseOptions: baseOptions, factory: factory);

        var leaf = Substitute.For<IBPlusLeafGrain>();
        leaf.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>()).Returns(Task.FromResult<SplitResult?>(null));
        leaf.GetRawEntryAsync(Arg.Any<string>()).Returns(Task.FromResult<LwwEntry?>(null));
        factory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>()).Returns(leaf);

        var cache = Substitute.For<ILeafCacheGrain>();
        cache.GetAsync(Arg.Any<string>()).Returns(Task.FromResult<byte[]?>(null));
        factory.GetGrain<ILeafCacheGrain>(Arg.Any<string>()).Returns(cache);

        var shadowTarget = Substitute.For<IShardRootGrain>();
        shadowTarget.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>())
            .Returns(_ => shadowSetBehavior?.Invoke() ?? Task.CompletedTask);
        factory.GetGrain<IShardRootGrain>(Arg.Any<string>()).Returns(shadowTarget);

        var grain = new ShardRootGrain(
            context, state, factory, optionsResolver,
            Microsoft.Extensions.Logging.Abstractions.NullLogger<ShardRootGrain>.Instance,
            TestMutationObservers.NoObservers());

        return new GrainHarness { Grain = grain, ShadowTarget = shadowTarget, State = state };
    }

    private static void SetShadowPhase(FakePersistentState<ShardRootState> state, ShadowForwardPhase phase) =>
        state.State.ShadowForward = new ShadowForwardState
        {
            DestinationPhysicalTreeId = DestTreeId,
            Phase = phase,
            OperationId = OperationId,
        };

    [Test]
    public async Task SetAsync_completes_when_forward_returns_within_deadline()
    {
        var h = CreateHarness(TimeSpan.FromSeconds(30));
        SetShadowPhase(h.State, ShadowForwardPhase.Draining);

        await h.Grain.SetAsync("k", [1]);

        // The fast-returning forward was observed by the foreground turn.
        await h.ShadowTarget.Received().SetAsync("k", Arg.Any<byte[]>());
    }

    [Test]
    public void SetAsync_throws_TimeoutException_when_forward_parks_past_deadline()
    {
        // A forward that never completes simulates the reshard-swap park: the
        // bounded await must abandon it and surface a TimeoutException rather
        // than pinning the foreground turn forever.
        var neverCompletes = new TaskCompletionSource();
        var h = CreateHarness(
            TimeSpan.FromMilliseconds(150),
            shadowSetBehavior: () => neverCompletes.Task);
        SetShadowPhase(h.State, ShadowForwardPhase.Draining);

        Assert.That(async () => await h.Grain.SetAsync("k", [1]),
            Throws.InstanceOf<TimeoutException>()
                  .With.Message.Contains(nameof(LatticeOptions.ShardForwardTimeout)));
    }

    [Test]
    public async Task SetAsync_does_not_bound_forward_when_timeout_is_infinite()
    {
        // Infinite timeout restores the historical unbounded-await behaviour:
        // a fast forward still completes, and no deadline machinery interferes.
        var h = CreateHarness(Timeout.InfiniteTimeSpan);
        SetShadowPhase(h.State, ShadowForwardPhase.Draining);

        await h.Grain.SetAsync("k", [1]);

        await h.ShadowTarget.Received().SetAsync("k", Arg.Any<byte[]>());
    }

    [Test]
    public async Task SetAsync_does_not_apply_deadline_when_no_shadow_forward_active()
    {
        // No active shadow-forward: the forward fast-path is Task.CompletedTask,
        // so even a tiny deadline cannot fault the write.
        var h = CreateHarness(TimeSpan.FromMilliseconds(1));

        await h.Grain.SetAsync("k", [1]);

        await h.ShadowTarget.DidNotReceive().SetAsync(Arg.Any<string>(), Arg.Any<byte[]>());
    }
}
