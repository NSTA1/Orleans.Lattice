using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for the bounded activation-readiness seed deadline on
/// <see cref="ShardRootGrain"/>. The first time a brand-new or freshly
/// reactivated shard prepares for an operation it runs a chain of
/// cross-grain awaits (tree-registry registration, deterministic root-leaf
/// init, the initial shard-state write) while holding the non-reentrant
/// activation gate. A startup reshard or membership change can leave one of
/// those RPCs parked against a not-yet-visible activation; without a ceiling
/// the parked seed pins the gate and every interleaved read/write on the
/// activation wedges. The deadline abandons the parked seed and faults the
/// preparing turn with a <see cref="TimeoutException"/> so the existing
/// transient-retry envelope can re-run the seed against refreshed routing.
/// </summary>
[TestFixture]
public class ShardRootGrainActivationReadyDeadlineTests
{
    private const string TreeId = "activation-ready-tree";
    private const string ShardKey = TreeId + "/0";

    private sealed class GrainHarness
    {
        public required ShardRootGrain Grain { get; init; }
        public required FakePersistentState<ShardRootState> State { get; init; }
        public required ILatticeRegistry Registry { get; init; }
    }

    /// <summary>
    /// Builds a fresh shard root (no root, unregistered) whose registry
    /// <c>RegisterAsync</c> runs <paramref name="registerBehavior"/>. The
    /// registry is the first cross-grain await in the seed chain, so
    /// parking it there is the cleanest stand-in for the real wedge.
    /// </summary>
    private static GrainHarness CreateHarness(
        TimeSpan activationReadyTimeout,
        Func<Task>? registerBehavior = null)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", ShardKey));

        var state = new FakePersistentState<ShardRootState>();
        Assert.That(state.State.RootNodeId, Is.Null);
        Assert.That(state.State.IsRegistered, Is.False);

        var factory = Substitute.For<IGrainFactory>();

        // The registry serves two roles here: option resolution
        // (GetEntryAsync) and the seed-chain registration (RegisterAsync).
        // Both are stubbed on the same substitute so the same factory backs
        // the resolver built via ForFactory below.
        var registry = Substitute.For<ILatticeRegistry>();
        registry.GetEntryAsync(Arg.Any<string>()).Returns(_ => Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry { MaxLeafKeys = 128, MaxInternalChildren = 128, ShardCount = 1 }));
        registry.RegisterAsync(Arg.Any<string>(), Arg.Any<TreeRegistryEntry?>())
            .Returns(_ => registerBehavior?.Invoke() ?? Task.CompletedTask);
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);

        var leafGrainContext = Substitute.For<IGrainContext>();
        leafGrainContext.GrainId.Returns(GrainId.Create("leaf", "activation-ready-test-leaf"));
        var leaf = Substitute.For<IBPlusLeafGrain, IGrainBase>();
        ((IGrainBase)leaf).GrainContext.Returns(leafGrainContext);
        leaf.SetTreeIdAsync(Arg.Any<string>()).Returns(Task.CompletedTask);
        leaf.SetShardIndexAsync(Arg.Any<int>()).Returns(Task.CompletedTask);
        leaf.MergeManyAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>())
            .Returns(Task.FromResult<SplitResult?>(null));
        factory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>()).Returns(leaf);
        factory.GetGrain<IBPlusLeafGrain>(Arg.Any<Guid>()).Returns(leaf);

        var baseOptions = new LatticeOptions { ActivationReadyTimeout = activationReadyTimeout };
        var optionsResolver = TestOptionsResolver.ForFactory(factory, baseOptions);

        var grain = new ShardRootGrain(
            context, state, factory, optionsResolver,
            NullLogger<ShardRootGrain>.Instance,
            TestMutationObservers.NoObservers());

        return new GrainHarness { Grain = grain, State = state, Registry = registry };
    }

    [Test]
    public async Task FirstTouch_completes_when_seed_returns_within_deadline()
    {
        // The registration returns promptly, so the seed completes and the
        // shard is initialised normally - the deadline machinery is inert.
        var h = CreateHarness(TimeSpan.FromSeconds(30));

        await h.Grain.MergeManyAsync(new Dictionary<string, LwwValue<byte[]>>());

        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.RootNodeId, Is.Not.Null,
                "Seed did not initialise a root within the deadline.");
            Assert.That(h.State.State.IsRegistered, Is.True);
        });
    }

    [Test]
    public void FirstTouch_throws_TimeoutException_when_seed_parks_past_deadline()
    {
        // A registration that never completes simulates the startup-reshard
        // park: the bounded await must abandon it and surface a
        // TimeoutException rather than pinning the activation gate forever.
        var neverCompletes = new TaskCompletionSource();
        var h = CreateHarness(
            TimeSpan.FromMilliseconds(150),
            registerBehavior: () => neverCompletes.Task);

        Assert.That(async () => await h.Grain.MergeManyAsync(new Dictionary<string, LwwValue<byte[]>>()),
            Throws.InstanceOf<ShardActivationTimeoutException>()
                  .With.Message.Contains(nameof(LatticeOptions.ActivationReadyTimeout)));
    }

    [Test]
    public void FirstTouch_typed_exception_carries_attribution_slots()
    {
        // The typed exception's slots are part of the public wire contract -
        // operator tooling (and the retry envelope's logging in future) reads
        // them rather than message-text-grepping. Pin them so a future change
        // that drops the slot population at the throw site is caught.
        var neverCompletes = new TaskCompletionSource();
        var h = CreateHarness(
            TimeSpan.FromMilliseconds(150),
            registerBehavior: () => neverCompletes.Task);

        var ex = Assert.ThrowsAsync<ShardActivationTimeoutException>(
            async () => await h.Grain.MergeManyAsync(new Dictionary<string, LwwValue<byte[]>>()));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.TreeId, Is.EqualTo(TreeId));
            Assert.That(ex.ShardIndex, Is.EqualTo(0));
            Assert.That(ex.TimeoutSeconds, Is.EqualTo(0.15).Within(0.001));
        });
    }

    [Test]
    public async Task FirstTouch_releases_gate_after_timeout_so_a_later_retry_can_seed()
    {
        // The headline liveness property: a parked seed that times out must
        // release the activation gate so a subsequent operation (the retry)
        // can acquire it and seed cleanly once the dependency recovers. The
        // first registration parks; after the timeout fires we complete a
        // fresh attempt by flipping the registry to return promptly.
        var gate = new TaskCompletionSource();
        var parkFirstCall = true;
        var h = CreateHarness(
            TimeSpan.FromMilliseconds(150),
            registerBehavior: () =>
            {
                if (parkFirstCall)
                {
                    parkFirstCall = false;
                    return gate.Task; // first call parks until released
                }
                return Task.CompletedTask; // retry succeeds
            });

        // First touch parks then times out.
        Assert.That(async () => await h.Grain.MergeManyAsync(new Dictionary<string, LwwValue<byte[]>>()),
            Throws.InstanceOf<ShardActivationTimeoutException>());

        // Let the abandoned first registration drain harmlessly.
        gate.SetResult();

        // Retry: the gate must be free, so this second first-touch seeds.
        await h.Grain.MergeManyAsync(new Dictionary<string, LwwValue<byte[]>>());

        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.RootNodeId, Is.Not.Null,
                "The retry could not seed - the activation gate was not "
                + "released after the first seed timed out.");
            Assert.That(h.State.State.IsRegistered, Is.True);
        });
    }

    [Test]
    public async Task FirstTouch_does_not_bound_seed_when_timeout_is_infinite()
    {
        // Infinite timeout restores the historical unbounded-await
        // behaviour: a fast seed still completes and no deadline machinery
        // interferes.
        var h = CreateHarness(Timeout.InfiniteTimeSpan);

        await h.Grain.MergeManyAsync(new Dictionary<string, LwwValue<byte[]>>());

        Assert.That(h.State.State.RootNodeId, Is.Not.Null);
    }

    [Test]
    public async Task SteadyState_skips_deadline_path_when_root_already_present()
    {
        // Once a root exists the activation-readiness fast path short-circuits
        // before acquiring the gate or resolving the timeout, so even a tiny
        // deadline cannot fault a steady-state operation.
        var h = CreateHarness(TimeSpan.FromMilliseconds(1));
        h.State.State.RootNodeId = GrainId.Create("leaf", "already-seeded");
        h.State.State.RootIsLeaf = true;
        h.State.State.IsRegistered = true;

        await h.Grain.MergeManyAsync(new Dictionary<string, LwwValue<byte[]>>());

        await h.Registry.DidNotReceive().RegisterAsync(Arg.Any<string>(), Arg.Any<TreeRegistryEntry?>());
    }
}
