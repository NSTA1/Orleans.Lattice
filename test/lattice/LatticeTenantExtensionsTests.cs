using NSubstitute;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeTenantExtensions"/>, the public
/// tenant-scoping entry point that resolves an <see cref="ILattice"/> handle for
/// an unqualified, tenant-local tree name. They confirm the warm path (default
/// tenant) addresses the bare tree id with no <c>await</c>, a non-default tenant
/// addresses the composed <c>t/{tenant}/{name}</c> id, a denial fails closed, and
/// the argument guards hold.
/// </summary>
[TestFixture]
public sealed class LatticeTenantExtensionsTests
{
    [SetUp]
    public void Reset()
    {
        LatticeActiveTenantContext.Current = null;
    }

    private static (IGrainFactory factory, ILattice lattice) CreateFactory(string expectedTreeId)
    {
        var lattice = Substitute.For<ILattice>();
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILattice>(expectedTreeId).Returns(lattice);
        return (factory, lattice);
    }

    [Test]
    public void GetLatticeAsync_default_tenant_addresses_the_bare_tree_synchronously()
    {
        var (factory, lattice) = CreateFactory("orders");
        var resolver = new FakeTenantContextResolver(TenantId.Default);

        var pending = factory.GetLatticeAsync(resolver, "orders");

        Assert.That(pending.IsCompletedSuccessfully, Is.True);
        Assert.That(pending.Result, Is.SameAs(lattice));
        Assert.That(resolver.AsyncResolutionCount, Is.Zero);
    }

    [Test]
    public async Task GetLatticeAsync_non_default_tenant_addresses_the_composed_tree()
    {
        var (factory, lattice) = CreateFactory("t/contoso/orders");
        var resolver = new FakeTenantContextResolver(TenantId.Parse("contoso"));

        var result = await factory.GetLatticeAsync(resolver, "orders");

        Assert.That(result, Is.SameAs(lattice));
        factory.Received(1).GetGrain<ILattice>("t/contoso/orders");
    }

    [Test]
    public async Task GetLatticeAsync_async_only_resolver_addresses_the_composed_tree()
    {
        var (factory, lattice) = CreateFactory("t/contoso/orders");
        var resolver = new FakeTenantContextResolver(TenantId.Parse("contoso"), resolvesSynchronously: false);

        var result = await factory.GetLatticeAsync(resolver, "orders");

        Assert.That(result, Is.SameAs(lattice));
        Assert.That(resolver.AsyncResolutionCount, Is.EqualTo(1));
    }

    [Test]
    public void GetLatticeAsync_denying_resolver_fails_closed()
    {
        var factory = Substitute.For<IGrainFactory>();
        var resolver = new FakeTenantContextResolver(default);

        Assert.That(
            () => factory.GetLatticeAsync(resolver, "orders"),
            Throws.TypeOf<LatticeTenantAccessDeniedException>());
    }

    [Test]
    public void GetLatticeAsync_null_grain_factory_throws_argument_null()
    {
        IGrainFactory factory = null!;
        var resolver = new FakeTenantContextResolver(TenantId.Default);

        Assert.That(
            () => factory.GetLatticeAsync(resolver, "orders"),
            Throws.ArgumentNullException);
    }

    [Test]
    public void GetLatticeAsync_null_resolver_throws_argument_null()
    {
        var factory = Substitute.For<IGrainFactory>();

        Assert.That(
            () => factory.GetLatticeAsync(null!, "orders"),
            Throws.ArgumentNullException);
    }

    [Test]
    public void GetLatticeAsync_empty_name_throws_argument()
    {
        var factory = Substitute.For<IGrainFactory>();
        var resolver = new FakeTenantContextResolver(TenantId.Default);

        Assert.That(
            () => factory.GetLatticeAsync(resolver, string.Empty),
            Throws.ArgumentException);
    }

    [Test]
    public async Task GetLatticeAsync_from_service_provider_resolves_the_seam_and_factory()
    {
        var (factory, lattice) = CreateFactory("t/contoso/orders");
        var resolver = new FakeTenantContextResolver(TenantId.Parse("contoso"));

        var services = Substitute.For<IServiceProvider>();
        services.GetService(typeof(IGrainFactory)).Returns(factory);
        services.GetService(typeof(ITenantContextResolver)).Returns(resolver);

        var result = await services.GetLatticeAsync("orders");

        Assert.That(result, Is.SameAs(lattice));
    }

    [Test]
    public void GetLatticeAsync_from_null_service_provider_throws_argument_null()
    {
        IServiceProvider services = null!;

        Assert.That(
            () => services.GetLatticeAsync("orders"),
            Throws.ArgumentNullException);
    }
}
