using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Failure-arm coverage for the shard root's leaf-access tracking and
/// post-restart leaf-cache pre-warm (issue #332).
/// <para>
/// The feature's whole contract is that it can only ever make a start
/// warmer, never make anything fail: a missing or unusable model
/// pre-warms nothing, and every fault along the way - resolving the
/// settings, arming or firing the coalescing flush timer, priming an
/// individual leaf cache - is swallowed so that neither a routed read
/// nor warm-up can be broken by it. The happy paths are pinned by
/// <see cref="ShardRootGrainLeafAccessTrackingTests"/>; this fixture
/// pins the swallowing arms, which are the ones that matter when a
/// silo is already unhealthy.
/// </para>
/// </summary>
[TestFixture]
public sealed class ShardRootGrainLeafAccessResilienceTests
{
    private const string ShardKey = "prewarm-resilience-tree/0";

    /// <summary>
    /// Test double for <c>IOptionsMonitor&lt;LatticeOptions&gt;</c> whose
    /// <see cref="Get"/> can be armed to throw part-way through a test, so a
    /// configuration source that fails <em>after</em> activation - a user
    /// <c>ConfigureLattice</c> delegate that throws for one tree, say - can be
    /// reproduced at the exact call the arm targets.
    /// </summary>
    private sealed class ArmableOptionsMonitor(LatticeOptions options) : IOptionsMonitor<LatticeOptions>
    {
        public bool Fail { get; set; }

        public LatticeOptions CurrentValue => Get(Options.DefaultName);

        public LatticeOptions Get(string? name)
            => Fail
                ? throw new InvalidOperationException("configuration source failed for this tree")
                : options;

        public IDisposable? OnChange(Action<LatticeOptions, string?> listener) => null;
    }

    private sealed record Harness(
        ShardRootGrain Grain,
        FakePersistentState<ShardRootState> State,
        ILeafCacheGrain Cache,
        GrainId LeafId,
        ArmableOptionsMonitor Options,
        ITimerRegistry TimerRegistry);

    private static Harness CreateGrain(
        int preWarmCount = 4,
        int flushIntervalMs = LatticeOptions.DefaultLeafAccessModelFlushIntervalMs,
        ILeafCacheGrain? cache = null)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", ShardKey));

        // Unlike the happy-path fixture, a timer registry IS wired in here so
        // the coalescing flush timer actually arms and its callback can be
        // captured and fired deterministically.
        var timerRegistry = Substitute.For<ITimerRegistry>();
        timerRegistry.RegisterGrainTimer(
                Arg.Any<IGrainContext>(),
                Arg.Any<Func<Func<CancellationToken, Task>, CancellationToken, Task>>(),
                Arg.Any<Func<CancellationToken, Task>>(),
                Arg.Any<GrainTimerCreationOptions>())
            .Returns(Substitute.For<IGrainTimer>());
        var services = new ServiceCollection();
        services.AddSingleton(timerRegistry);
        context.ActivationServices.Returns(services.BuildServiceProvider());

        var state = new FakePersistentState<ShardRootState>();
        var leafId = GrainId.Create("leaf", "prewarm-resilience-leaf-0");
        state.State.RootNodeId = leafId;
        state.State.RootIsLeaf = true;

        var factory = Substitute.For<IGrainFactory>();
        var leaf = Substitute.For<IBPlusLeafGrain>();
        leaf.CountAsync().Returns(Task.FromResult(0));
        leaf.GetNextSiblingAsync().Returns(Task.FromResult<GrainId?>(null));
        factory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>()).Returns(leaf);

        if (cache is null)
        {
            // Only stub the defaults for a cache this helper owns: re-stubbing a
            // caller-supplied one would silently overwrite its injected fault and
            // leave the test asserting against a healthy path.
            cache = Substitute.For<ILeafCacheGrain>();
            cache.PreWarmAsync().Returns(Task.CompletedTask);
            cache.GetAsync(Arg.Any<string>()).Returns(Task.FromResult<byte[]?>(null));
            cache.ExistsAsync(Arg.Any<string>()).Returns(Task.FromResult(false));
        }
        factory.GetGrain<ILeafCacheGrain>(Arg.Any<string>()).Returns(cache);

        var options = new LatticeOptions
        {
            LeafCachePreWarmCount = preWarmCount,
            LeafAccessModelFlushIntervalMs = flushIntervalMs,
        };
        var monitor = new ArmableOptionsMonitor(options);

        var registry = Substitute.For<ILatticeRegistry>();
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        registry.GetEntryAsync(Arg.Any<string>()).Returns(_ => Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry { MaxLeafKeys = 128, MaxInternalChildren = 128, ShardCount = 1 }));

        var grain = new ShardRootGrain(
            context, state, factory, new LatticeOptionsResolver(factory, monitor),
            NullLogger<ShardRootGrain>.Instance,
            TestMutationObservers.NoObservers());

        return new Harness(grain, state, cache, leafId, monitor, timerRegistry);
    }

    /// <summary>
    /// The <c>Func&lt;CancellationToken, Task&gt;</c> the grain handed to the
    /// timer registry - here, the coalescing model-flush tick.
    /// </summary>
    private static Func<CancellationToken, Task> CapturedTimerCallback(ITimerRegistry registry)
    {
        var call = registry.ReceivedCalls()
            .Last(c => c.GetMethodInfo().Name == nameof(ITimerRegistry.RegisterGrainTimer));
        return (Func<CancellationToken, Task>)call.GetArguments()[2]!;
    }

    [Test]
    public async Task A_read_still_succeeds_when_resolving_the_tracking_settings_throws()
    {
        var harness = CreateGrain();

        // The configuration source starts failing before the first routed
        // read, so tracking initialisation - which runs exactly once, on that
        // read - is the call that observes the fault.
        harness.Options.Fail = true;

        Assert.That(async () => await harness.Grain.GetAsync("k1"), Throws.Nothing,
            "leaf-access tracking is an optimisation and must never fail a read");

        harness.Options.Fail = false;
        await ((IGrainBase)harness.Grain).OnDeactivateAsync(default, default);

        Assert.That(harness.State.State.LeafAccessModel, Is.Null,
            "a failed initialisation leaves tracking inactive for the activation");
    }

    [Test]
    public async Task A_failed_tracking_initialisation_is_not_retried_on_every_read()
    {
        // The initialised latch is set BEFORE the guarded block precisely so a
        // persistent fault costs one attempt, not one per read.
        var harness = CreateGrain();
        harness.Options.Fail = true;

        for (var i = 0; i < 10; i++) await harness.Grain.GetAsync($"k{i}");

        // Had the failed attempt re-armed, the recovered read below would still
        // find tracking uninitialised and start recording.
        harness.Options.Fail = false;
        await harness.Grain.GetAsync("k-after-recovery");
        await ((IGrainBase)harness.Grain).OnDeactivateAsync(default, default);

        Assert.That(harness.State.State.LeafAccessModel, Is.Null,
            "tracking stays inactive for the rest of the activation");
    }

    [Test]
    public async Task The_coalescing_flush_tick_swallows_a_failed_state_write()
    {
        var harness = CreateGrain();

        // One read arms the timer and leaves the model dirty.
        await harness.Grain.GetAsync("k1");
        var tick = CapturedTimerCallback(harness.TimerRegistry);

        harness.State.ThrowOnWrite = new InvalidOperationException("storage unavailable");

        Assert.That(async () => await tick(CancellationToken.None), Throws.Nothing,
            "a failed coalesced flush must not surface out of the timer tick");

        // The dirty flag survived, so the very next flush retries rather than
        // silently dropping the window's observations.
        var writesBefore = harness.State.WriteCount;
        await ((IGrainBase)harness.Grain).OnDeactivateAsync(default, default);

        Assert.Multiple(() =>
        {
            Assert.That(harness.State.WriteCount, Is.EqualTo(writesBefore + 1),
                "the unflushed model is retried on the next flush");
            Assert.That(harness.State.State.LeafAccessModel, Is.Not.Null);
        });
    }

    [Test]
    public async Task The_coalescing_flush_tick_is_a_no_op_when_nothing_is_dirty()
    {
        var harness = CreateGrain();
        await harness.Grain.GetAsync("k1");
        var tick = CapturedTimerCallback(harness.TimerRegistry);

        await tick(CancellationToken.None);
        var afterFirst = harness.State.WriteCount;

        // Second tick inside the same window: the model was persisted and
        // marked clean by the first, so there is nothing to write.
        await tick(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(afterFirst, Is.EqualTo(1));
            Assert.That(harness.State.WriteCount, Is.EqualTo(afterFirst));
        });
    }

    [Test]
    public async Task Warm_up_survives_a_failure_resolving_the_pre_warm_settings()
    {
        var harness = CreateGrain();
        harness.State.State.LeafAccessModel = new LeafAccessModelSnapshot
        {
            Leaves = [harness.LeafId.ToString()],
            Visits = [10L],
        };

        // Let the first warm-up settle the activation, then break the
        // configuration source so only the pre-warm resolve observes it.
        await harness.Grain.WarmUpAsync();
        harness.Cache.ClearReceivedCalls();
        harness.Options.Fail = true;

        Assert.That(async () => await harness.Grain.WarmUpAsync(), Throws.Nothing,
            "pre-warm is best-effort and must never fail warm-up");

        await harness.Cache.DidNotReceive().PreWarmAsync();
    }

    [Test]
    public async Task Warm_up_survives_a_leaf_cache_that_refuses_to_prime()
    {
        var cache = Substitute.For<ILeafCacheGrain>();
        cache.GetAsync(Arg.Any<string>()).Returns(Task.FromResult<byte[]?>(null));
        cache.PreWarmAsync().Returns(_ => Task.FromException(
            new InvalidOperationException("leaf merged away")));

        var harness = CreateGrain(cache: cache);
        harness.State.State.LeafAccessModel = new LeafAccessModelSnapshot
        {
            Leaves = [harness.LeafId.ToString()],
            Visits = [10L],
        };

        Assert.That(async () => await harness.Grain.WarmUpAsync(), Throws.Nothing,
            "a leaf that cannot be primed must not fail the whole warm-up");

        await cache.Received(1).PreWarmAsync();
    }

    [Test]
    public async Task Warm_up_primes_the_ranked_leaves_when_everything_is_healthy()
    {
        // Falsifies the two failure tests above: with no fault injected the
        // same harness really does issue the priming call.
        var harness = CreateGrain();
        harness.State.State.LeafAccessModel = new LeafAccessModelSnapshot
        {
            Leaves = [harness.LeafId.ToString()],
            Visits = [10L],
        };

        await harness.Grain.WarmUpAsync();

        await harness.Cache.Received(1).PreWarmAsync();
    }
}
