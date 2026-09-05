using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Pins the retry-policy and HLC-scope halves of the idempotency plumbing that
/// wraps every mutating <see cref="ILattice"/> entry point.
/// <para>
/// Both layers are strictly additive and only engage inside a
/// <see cref="LatticeIdempotencyContext"/> scope, so the default (no-scope,
/// no-policy) path bypasses them entirely. Three behaviours are pinned here that
/// the no-policy fixtures cannot reach:
/// </para>
/// <list type="number">
///   <item>
///     With <see cref="LatticeOptions.RetryPolicy"/> configured, the mutation is
///     executed <em>through</em> the policy rather than awaited directly - for
///     both the void and the value-returning entry points.
///   </item>
///   <item>
///     The key's <see cref="LatticeIdempotencyKey.Timestamp"/> is projected into
///     <see cref="LatticeHlcOverrideContext"/> so the leaf stamping path picks it
///     up through the standard ambient mechanism.
///   </item>
///   <item>
///     An HLC override the caller established itself is <em>not</em> overwritten:
///     the idempotency layer defers to it, so a replication apply path that has
///     already pinned a source HLC keeps it.
///   </item>
/// </list>
/// </summary>
[TestFixture]
public class LatticeGrainIdempotencyRetryPolicyTests
{
    private const string TreeId = "orders";

    [SetUp]
    public void Reset()
    {
        LatticeIdempotencyContext.Current = null;
        LatticeHlcOverrideContext.Current = null;
    }

    [TearDown]
    public void Clear()
    {
        LatticeIdempotencyContext.Current = null;
        LatticeHlcOverrideContext.Current = null;
    }

    private static LatticeGrain CreateGrain(ILatticeRetryPolicy? retryPolicy)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("lattice", TreeId));

        var grainFactory = Substitute.For<IGrainFactory>();
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(new LatticeOptions { RetryPolicy = retryPolicy });

        var registry = Substitute.For<ILatticeRegistry>();
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        registry.ResolveAsync(Arg.Any<string>()).Returns(c => Task.FromResult(c.Arg<string>()));
        registry.GetShardMapAsync(Arg.Any<string>()).Returns(Task.FromResult<ShardMap?>(null));
        registry.GetEntryAsync(Arg.Any<string>()).Returns(Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry { MaxLeafKeys = 128, MaxInternalChildren = 128, ShardCount = 4 }));

        var shardRoot = Substitute.For<IShardRootGrain>();
        grainFactory.GetGrain<IShardRootGrain>(Arg.Any<string>(), Arg.Any<string>()).Returns(shardRoot);

        var services = Substitute.For<IServiceProvider>();
        var optionsResolver = TestOptionsResolver.ForFactory(grainFactory);
        return new LatticeGrain(context, grainFactory, optionsMonitor, optionsResolver, services, NullLogger<LatticeGrain>.Instance);
    }

    /// <summary>
    /// A pass-through <see cref="ILatticeRetryPolicy"/> that records that it was
    /// invoked and captures the ambient HLC override observed at execution time.
    /// It runs the operation exactly once, so a test asserting on the policy is
    /// asserting on routing, not on retry behaviour (which
    /// <c>BoundedExponentialRetryPolicy</c>'s own fixtures cover).
    /// </summary>
    private sealed class RecordingRetryPolicy : ILatticeRetryPolicy
    {
        public int UntypedInvocations { get; private set; }

        public int TypedInvocations { get; private set; }

        public HybridLogicalClock? ObservedHlcOverride { get; private set; }

        public Task ExecuteAsync(Func<CancellationToken, Task> operation, CancellationToken cancellationToken)
        {
            UntypedInvocations++;
            ObservedHlcOverride = LatticeHlcOverrideContext.Current;
            return operation(cancellationToken);
        }

        public Task<T> ExecuteAsync<T>(Func<CancellationToken, Task<T>> operation, CancellationToken cancellationToken)
        {
            TypedInvocations++;
            ObservedHlcOverride = LatticeHlcOverrideContext.Current;
            return operation(cancellationToken);
        }
    }

    [Test]
    public async Task SetAsync_in_an_idempotency_scope_routes_the_mutation_through_the_configured_retry_policy()
    {
        var policy = new RecordingRetryPolicy();
        var grain = CreateGrain(policy);

        using var scope = LatticeIdempotencyContext.With(LatticeIdempotencyKey.Fresh());
        await grain.SetAsync("k", [1]);

        Assert.That(policy.UntypedInvocations, Is.EqualTo(1),
            "a configured retry policy must wrap the mutation rather than being bypassed");
    }

    [Test]
    public async Task SetAsync_projects_the_idempotency_key_timestamp_into_the_ambient_hlc_override()
    {
        var policy = new RecordingRetryPolicy();
        var grain = CreateGrain(policy);
        var key = LatticeIdempotencyKey.Fresh();

        using var scope = LatticeIdempotencyContext.With(key);
        await grain.SetAsync("k", [1]);

        Assert.That(policy.ObservedHlcOverride, Is.EqualTo(key.Timestamp),
            "the key's timestamp is what makes a retry collapse onto the same LwwValue timestamp");
    }

    [Test]
    public async Task SetAsync_does_not_overwrite_an_hlc_override_the_caller_already_established()
    {
        var policy = new RecordingRetryPolicy();
        var grain = CreateGrain(policy);
        var callerHlc = new HybridLogicalClock { WallClockTicks = 12345, Counter = 7 };
        var key = LatticeIdempotencyKey.Fresh();

        using var hlcScope = LatticeHlcOverrideContext.With(callerHlc);
        using var idScope = LatticeIdempotencyContext.With(key);
        await grain.SetAsync("k", [1]);

        Assert.That(policy.ObservedHlcOverride, Is.EqualTo(callerHlc),
            "an apply path that already pinned a source HLC must keep it; the idempotency layer defers rather than overwrites");
        Assert.That(policy.ObservedHlcOverride, Is.Not.EqualTo(key.Timestamp));
    }

    [Test]
    public async Task SetAsync_restores_the_prior_hlc_override_after_the_mutation()
    {
        var grain = CreateGrain(retryPolicy: null);
        var key = LatticeIdempotencyKey.Fresh();

        using (var scope = LatticeIdempotencyContext.With(key))
        {
            await grain.SetAsync("k", [1]);
        }

        Assert.That(LatticeHlcOverrideContext.Current, Is.Null,
            "the projected override is scoped to the mutation and must not leak past it");
    }

    [Test]
    public async Task GetOrSetAsync_in_an_idempotency_scope_routes_through_the_typed_retry_overload()
    {
        var policy = new RecordingRetryPolicy();
        var grain = CreateGrain(policy);

        using var scope = LatticeIdempotencyContext.With(LatticeIdempotencyKey.Fresh());
        await grain.GetOrSetAsync("k", [1]);

        Assert.That(policy.TypedInvocations, Is.EqualTo(1),
            "a value-returning entry point must use the typed policy overload, not the untyped one");
        Assert.That(policy.UntypedInvocations, Is.Zero);
    }

    [Test]
    public async Task GetOrSetAsync_with_a_caller_hlc_override_defers_to_it_on_the_typed_path()
    {
        var policy = new RecordingRetryPolicy();
        var grain = CreateGrain(policy);
        var callerHlc = new HybridLogicalClock { WallClockTicks = 999, Counter = 1 };

        using var hlcScope = LatticeHlcOverrideContext.With(callerHlc);
        using var idScope = LatticeIdempotencyContext.With(LatticeIdempotencyKey.Fresh());
        await grain.GetOrSetAsync("k", [1]);

        Assert.That(policy.ObservedHlcOverride, Is.EqualTo(callerHlc));
    }
}
