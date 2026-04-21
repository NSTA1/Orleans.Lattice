using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression tests for shadow-forward task observation.
/// Validates that every dispatched shadow-forward is observed exactly once
/// (by the caller''s await, by the fault-logger continuation, or both) and
/// that <see cref="ShardRootGrain.SetManyAsync"/> preserves the local
/// exception as the primary when both local and forward fail.
/// </summary>
public class ShardRootGrainShadowForwardTrackingTests
{
    private const string TreeId = "src-tree";
    private const string DestTreeId = "src-tree/resized/op-1";
    private const string OperationId = "op-1";
    private const int ShardIndex = 0;

    private sealed class Harness
    {
        public required ShardRootGrain Grain { get; init; }
        public required IShardRootGrain ShadowTarget { get; init; }
        public required IBPlusLeafGrain Leaf { get; init; }
        public required ILogger<ShardRootGrain> Logger { get; init; }
        public required FakePersistentState<ShardRootState> State { get; init; }
    }

    private static Harness CreateHarness(
        ILogger<ShardRootGrain>? logger = null,
        Action<IShardRootGrain>? configureShadowTarget = null,
        Action<IBPlusLeafGrain>? configureLeaf = null)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", $"{TreeId}/{ShardIndex}"));

        var state = new FakePersistentState<ShardRootState>();
        state.State.RootNodeId = GrainId.Create("leaf", "test-leaf");
        state.State.RootIsLeaf = true;
        state.State.ShadowForward = new ShadowForwardState
        {
            DestinationPhysicalTreeId = DestTreeId,
            Phase = ShadowForwardPhase.Draining,
            OperationId = OperationId,
        };

        var factory = Substitute.For<IGrainFactory>();
        var optionsResolver = TestOptionsResolver.Create(baseOptions: new LatticeOptions(), factory: factory);

        var leaf = Substitute.For<IBPlusLeafGrain>();
        leaf.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>()).Returns(Task.FromResult<SplitResult?>(null));
        leaf.MergeManyAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>())
            .Returns(Task.FromResult<SplitResult?>(null));
        configureLeaf?.Invoke(leaf);
        factory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>()).Returns(leaf);

        var cache = Substitute.For<ILeafCacheGrain>();
        factory.GetGrain<ILeafCacheGrain>(Arg.Any<string>()).Returns(cache);

        var shadowTarget = Substitute.For<IShardRootGrain>();
        shadowTarget.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>()).Returns(Task.CompletedTask);
        shadowTarget.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>()).Returns(Task.CompletedTask);
        shadowTarget.MergeManyAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>()).Returns(Task.CompletedTask);
        configureShadowTarget?.Invoke(shadowTarget);
        factory.GetGrain<IShardRootGrain>(Arg.Any<string>()).Returns(shadowTarget);

        logger ??= NullLogger<ShardRootGrain>.Instance;
        var grain = new ShardRootGrain(context, state, factory, optionsResolver, logger);

        return new Harness
        {
            Grain = grain,
            ShadowTarget = shadowTarget,
            Leaf = leaf,
            Logger = logger,
            State = state,
        };
    }

    [Test]
    public async Task SetManyAsync_surfaces_local_exception_when_both_local_and_forward_fail()
    {
        // Leaf throws a non-transient exception on every local write.
        var localFailure = new InvalidOperationException("local-blew-up");
        var forwardFailure = new InvalidOperationException("forward-blew-up");

        var h = CreateHarness(
            configureLeaf: l => l.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>())
                .Returns<Task<SplitResult?>>(_ => throw localFailure),
            configureShadowTarget: t => t.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>())
                .Returns<Task>(_ => Task.FromException(forwardFailure)));

        var entries = new List<KeyValuePair<string, byte[]>> { new("k1", [1]) };

        var caught = Assert.ThrowsAsync<InvalidOperationException>(
            async () => await h.Grain.SetManyAsync(entries));

        // The local (primary) exception is preserved, not the forward one.
        Assert.That(caught!.Message, Is.EqualTo("local-blew-up"));
    }

    [Test]
    public async Task SetManyAsync_surfaces_forward_exception_when_only_forward_fails()
    {
        var forwardFailure = new InvalidOperationException("forward-blew-up");

        var h = CreateHarness(
            configureShadowTarget: t => t.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>())
                .Returns<Task>(_ => Task.FromException(forwardFailure)));

        var entries = new List<KeyValuePair<string, byte[]>> { new("k1", [1]) };

        var caught = Assert.ThrowsAsync<InvalidOperationException>(
            async () => await h.Grain.SetManyAsync(entries));
        Assert.That(caught!.Message, Is.EqualTo("forward-blew-up"));
    }

    [Test]
    public async Task SetAsync_happy_path_awaits_forward_before_returning()
    {
        var h = CreateHarness();
        await h.Grain.SetAsync("k1", [1]);

        await h.ShadowTarget.Received(1).SetAsync("k1", Arg.Any<byte[]>());
    }

    [Test]
    public void TrackShadowForward_logs_warning_when_abandoned_forward_faults()
    {
        // Scenario: shadow target's SetAsync faults asynchronously; caller
        // abandons it (never awaits). The fault-logger continuation must fire
        // and log exactly one warning so the fault is never silent.
        var logger = new CapturingLogger();
        var forwardTcs = new TaskCompletionSource();
        var h = CreateHarness(
            logger: logger,
            configureShadowTarget: t => t.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>())
                .Returns(forwardTcs.Task));

        var setTask = h.Grain.SetAsync("k1", [1]);
        forwardTcs.SetException(new InvalidOperationException("forward-fault"));

        // Caller awaits forwardTask in the happy path so it observes the
        // exception first; the continuation still fires and logs.
        Assert.ThrowsAsync<InvalidOperationException>(async () => await setTask);

        Assert.That(logger.WarningCount, Is.EqualTo(1));
    }

    private sealed class CapturingLogger : ILogger<ShardRootGrain>
    {
        private int _warningCount;
        public int WarningCount => System.Threading.Volatile.Read(ref _warningCount);

        IDisposable? ILogger.BeginScope<TState>(TState state) => null;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (logLevel == LogLevel.Warning) System.Threading.Interlocked.Increment(ref _warningCount);
        }
    }
}