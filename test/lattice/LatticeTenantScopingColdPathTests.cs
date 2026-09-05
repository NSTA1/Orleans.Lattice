using NSubstitute;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Covers the tenant-scoping seam's <em>cold</em> paths, which the warm-path
/// fixture in <see cref="LatticeTenantExtensionsTests"/> cannot reach.
/// <para>
/// Two of them are easy to believe are already covered but are not. First, a
/// resolver that declines the synchronous
/// <see cref="ITenantContextResolver.TryResolveCurrent"/> fast path still yields a
/// <em>synchronously completed</em> <see cref="ValueTask{T}"/> when its
/// <see cref="ITenantContextResolver.ResolveCurrentAsync"/> returns an already-completed
/// task - so <c>GetLatticeAsync</c> stays on its no-await branch and the awaiting
/// continuation never runs. Reaching that continuation needs a resolver that
/// genuinely suspends, which is what <see cref="DeferredTenantContextResolver"/>
/// models. Second, <see cref="ITenantContextResolver.TryResolveCurrent"/> has a
/// default interface implementation that only runs for a resolver which does not
/// override it.
/// </para>
/// </summary>
[TestFixture]
public sealed class LatticeTenantScopingColdPathTests
{
    [SetUp]
    public void Reset() => LatticeActiveTenantContext.Current = null;

    /// <summary>
    /// A resolver whose asynchronous resolution is gated on a
    /// <see cref="TaskCompletionSource{TResult}"/>, so the returned
    /// <see cref="ValueTask{T}"/> is genuinely incomplete when the caller inspects
    /// it. The test completes the source explicitly, so nothing here sleeps or
    /// races.
    /// </summary>
    private sealed class DeferredTenantContextResolver(TenantId tenant) : ITenantContextResolver
    {
        private readonly TaskCompletionSource<TenantId> _gate =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public ValueTask<TenantId> ResolveCurrentAsync(CancellationToken cancellationToken = default)
            => new(_gate.Task);

        public bool TryResolveCurrent(out TenantId resolved)
        {
            resolved = default;
            return false;
        }

        public void Complete() => _gate.TrySetResult(tenant);
    }

    /// <summary>
    /// A resolver that implements only the required asynchronous member, so the
    /// interface's default <see cref="ITenantContextResolver.TryResolveCurrent"/>
    /// implementation is the one that runs.
    /// </summary>
    private sealed class AsyncOnlyTenantContextResolver(TenantId tenant) : ITenantContextResolver
    {
        public ValueTask<TenantId> ResolveCurrentAsync(CancellationToken cancellationToken = default)
            => new(tenant);
    }

    [Test]
    public async Task GetLatticeAsync_awaits_a_resolver_that_genuinely_suspends()
    {
        var lattice = Substitute.For<ILattice>();
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILattice>("t/contoso/orders").Returns(lattice);
        var resolver = new DeferredTenantContextResolver(TenantId.Parse("contoso"));

        var pending = factory.GetLatticeAsync(resolver, "orders");
        Assert.That(pending.IsCompleted, Is.False,
            "a resolver that has not yet produced a tenant must leave the handle unresolved rather than "
            + "addressing a grain under an unresolved tenant");

        resolver.Complete();
        var result = await pending;

        Assert.That(result, Is.SameAs(lattice));
        factory.Received(1).GetGrain<ILattice>("t/contoso/orders");
    }

    [Test]
    public void GetLatticeAsync_propagates_a_deferred_denial_as_a_fail_closed_throw()
    {
        var factory = Substitute.For<IGrainFactory>();
        var resolver = new DeferredTenantContextResolver(default);

        var pending = factory.GetLatticeAsync(resolver, "orders");
        resolver.Complete();

        Assert.That(async () => await pending, Throws.TypeOf<LatticeTenantAccessDeniedException>(),
            "deferring resolution must not turn a denial into an admit");
        factory.DidNotReceive().GetGrain<ILattice>(Arg.Any<string>());
    }

    [Test]
    public async Task Default_TryResolveCurrent_declines_so_a_resolver_may_implement_only_the_async_member()
    {
        ITenantContextResolver resolver = new AsyncOnlyTenantContextResolver(TenantId.Parse("contoso"));

        // A default interface implementation is only reachable through the
        // interface, never through the concrete type.
        var resolvedSynchronously = resolver.TryResolveCurrent(out var tenant);

        Assert.Multiple(() =>
        {
            Assert.That(resolvedSynchronously, Is.False,
                "the default implementation must decline so callers fall back to the async path");
            Assert.That(tenant, Is.EqualTo(default(TenantId)));
        });

        Assert.That(await resolver.ResolveCurrentAsync(), Is.EqualTo(TenantId.Parse("contoso")),
            "declining the fast path must not prevent the async path from resolving");
    }

    [Test]
    public async Task ResolveEffectiveTreeIdAsync_returns_the_bare_name_under_the_default_tenant()
    {
        var resolver = new AsyncOnlyTenantContextResolver(TenantId.Default);

        var effective = await resolver.ResolveEffectiveTreeIdAsync("orders");

        Assert.That(effective, Is.EqualTo("orders"),
            "a cluster with tenancy off must address exactly the tree the caller named");
    }

    [Test]
    public async Task ResolveEffectiveTreeIdAsync_composes_the_tenant_namespace_for_a_non_default_tenant()
    {
        var resolver = new AsyncOnlyTenantContextResolver(TenantId.Parse("contoso"));

        var effective = await resolver.ResolveEffectiveTreeIdAsync("orders");

        Assert.That(effective, Is.EqualTo("t/contoso/orders"),
            "an API facade must authorize and operate on the same composed id, or it checks one tree and acts on another");
    }

    [Test]
    public void ResolveEffectiveTreeIdAsync_fails_closed_when_no_valid_tenant_resolves()
    {
        var resolver = new AsyncOnlyTenantContextResolver(default);

        Assert.That(
            async () => await resolver.ResolveEffectiveTreeIdAsync("orders"),
            Throws.TypeOf<LatticeTenantAccessDeniedException>());
    }

    [Test]
    public void ResolveEffectiveTreeIdAsync_null_resolver_throws_argument_null()
    {
        ITenantContextResolver resolver = null!;

        Assert.That(
            () => resolver.ResolveEffectiveTreeIdAsync("orders"),
            Throws.ArgumentNullException);
    }

    [Test]
    public void ResolveEffectiveTreeIdAsync_empty_name_throws_argument()
    {
        var resolver = new AsyncOnlyTenantContextResolver(TenantId.Default);

        Assert.That(
            () => resolver.ResolveEffectiveTreeIdAsync(string.Empty),
            Throws.ArgumentException);
    }

    [Test]
    public void ResolveEffectiveTreeIdAsync_null_name_throws_argument_null()
    {
        var resolver = new AsyncOnlyTenantContextResolver(TenantId.Default);

        Assert.That(
            () => resolver.ResolveEffectiveTreeIdAsync(null!),
            Throws.ArgumentNullException);
    }
}
