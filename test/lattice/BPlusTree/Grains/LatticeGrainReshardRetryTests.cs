using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Pins the public-surface behaviour that
/// <see cref="ILattice.ReshardAsync"/> transparently absorbs a small bounded
/// number of <see cref="ShardActivationTimeoutException"/>s from the
/// underlying <see cref="ITreeReshardGrain"/>'s first turn before surfacing
/// the typed exception to the caller. The shard-root activation-readiness
/// seed abandons parked seeds with this exception by design, and
/// every cross-grain step in the seed is idempotent on retry; the public
/// surface must therefore not require operators to learn that retry
/// contract themselves.
/// </summary>
[TestFixture]
public class LatticeGrainReshardRetryTests
{
    private const string TreeId = "reshard-retry-tree";

    private static (LatticeGrain grain, ITreeReshardGrain reshard) CreateGrain(
        Func<Task> reshardBehavior)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("lattice", TreeId));

        var grainFactory = Substitute.For<IGrainFactory>();
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(new LatticeOptions());

        var registry = Substitute.For<ILatticeRegistry>();
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        registry.ResolveAsync(Arg.Any<string>()).Returns(c => Task.FromResult(c.Arg<string>()));

        var reshard = Substitute.For<ITreeReshardGrain>();
        reshard.ReshardAsync(Arg.Any<int>()).Returns(_ => reshardBehavior());
        grainFactory.GetGrain<ITreeReshardGrain>(TreeId).Returns(reshard);

        var optionsResolver = TestOptionsResolver.ForFactory(grainFactory);
        var services = Substitute.For<IServiceProvider>();
        var grain = new LatticeGrain(context, grainFactory, optionsMonitor, optionsResolver, services, NullLogger<LatticeGrain>.Instance);
        return (grain, reshard);
    }

    /// <summary>
    /// The cold-start race the fix targets: the underlying
    /// <see cref="ITreeReshardGrain"/> throws
    /// <see cref="ShardActivationTimeoutException"/> on its first turn, then
    /// succeeds on retry. The caller sees a clean return, not the typed
    /// exception.
    /// </summary>
    [Test]
    public async Task ReshardAsync_retries_through_first_activation_timeout_then_succeeds()
    {
        var calls = 0;
        var (grain, reshard) = CreateGrain(() =>
        {
            calls++;
            if (calls == 1) throw new ShardActivationTimeoutException("first-turn-parked")
            {
                TreeId = TreeId,
                ShardIndex = 0,
                TimeoutSeconds = 15,
            };
            return Task.CompletedTask;
        });

        await grain.ReshardAsync(8);

        Assert.That(calls, Is.EqualTo(2));
        await reshard.Received(2).ReshardAsync(8);
    }

    /// <summary>
    /// When every retry attempt fails with the typed seed-timeout, the
    /// caller eventually sees the typed exception itself - the envelope
    /// preserves the original shape rather than wrapping it.
    /// </summary>
    [Test]
    public void ReshardAsync_surfaces_typed_exception_after_retry_budget_exhausted()
    {
        var calls = 0;
        var (grain, _) = CreateGrain(() =>
        {
            calls++;
            throw new ShardActivationTimeoutException($"attempt-{calls}-parked")
            {
                TreeId = TreeId,
                ShardIndex = 0,
                TimeoutSeconds = 15,
            };
        });

        var ex = Assert.ThrowsAsync<ShardActivationTimeoutException>(async () => await grain.ReshardAsync(8));

        Assert.Multiple(() =>
        {
            Assert.That(calls, Is.EqualTo(ShardActivationRetry.MaxAttempts),
                "Envelope did not exhaust the full retry budget.");
            Assert.That(ex!.Message, Is.EqualTo($"attempt-{ShardActivationRetry.MaxAttempts}-parked"),
                "Envelope rethrew the wrong attempt's exception.");
        });
    }

    /// <summary>
    /// Unrelated exceptions on the reshard call must propagate immediately
    /// without retry - the envelope is scoped strictly to the seed-timeout
    /// shape.
    /// </summary>
    [Test]
    public void ReshardAsync_propagates_unrelated_exceptions_without_retry()
    {
        var calls = 0;
        var (grain, _) = CreateGrain(() =>
        {
            calls++;
            throw new ArgumentOutOfRangeException("newShardCount", "below current pinned");
        });

        Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await grain.ReshardAsync(8));
        Assert.That(calls, Is.EqualTo(1));
    }

    /// <summary>
    /// System-tree guard fires before the retry envelope - a caller that
    /// targets a reserved tree id sees <see cref="InvalidOperationException"/>
    /// regardless of any retry semantics.
    /// </summary>
    [Test]
    public void ReshardAsync_rejects_system_tree_before_retry_envelope()
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("lattice", LatticeConstants.RegistryTreeId));

        var grainFactory = Substitute.For<IGrainFactory>();
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(new LatticeOptions());
        var optionsResolver = TestOptionsResolver.ForFactory(grainFactory);
        var services = Substitute.For<IServiceProvider>();
        var grain = new LatticeGrain(context, grainFactory, optionsMonitor, optionsResolver, services, NullLogger<LatticeGrain>.Instance);

        Assert.ThrowsAsync<InvalidOperationException>(async () => await grain.ReshardAsync(8));
    }
}
