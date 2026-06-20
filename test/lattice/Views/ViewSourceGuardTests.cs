using NSubstitute;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// Unit tests for <see cref="ViewSourceGuard"/>. The guard unions the startup
/// registrations, the in-memory catalog, and the durable runtime registry, so a
/// source tree is reported as having dependent views no matter where the view was
/// declared.
/// </summary>
[TestFixture]
public class ViewSourceGuardTests
{
    private static ILatticeViewProjection Filter() => new PredicateLatticeViewProjection();

    private static (ViewSourceGuard Guard, ViewCatalog Catalog, IViewRegistryGrain Registry) CreateGuard(
        IReadOnlyList<StartupViewRegistration>? startup = null)
    {
        var catalog = new ViewCatalog();
        var registry = Substitute.For<IViewRegistryGrain>();
        registry.ListAsync().Returns(Task.FromResult<IReadOnlyList<RuntimeViewRegistration>>([]));
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<IViewRegistryGrain>(IViewRegistryGrain.SingletonKey).Returns(registry);
        var guard = new ViewSourceGuard(grainFactory, catalog, startup ?? []);
        return (guard, catalog, registry);
    }

    private static RuntimeViewRegistration Runtime(string viewName, string sourceTreeId) => new()
    {
        ViewName = viewName,
        SourceTreeId = sourceTreeId,
        ProjectionTypeName = "X",
        ProjectionVersion = "1",
        IsAggregation = false,
    };

    [Test]
    public async Task FindDependentViews_returns_empty_when_no_view_derives_from_the_tree()
    {
        var (guard, catalog, _) = CreateGuard();
        catalog.Register(new ViewRegistration("other", "other-src", Filter()));

        var dependents = await guard.FindDependentViewsAsync("people");

        Assert.That(dependents, Is.Empty);
    }

    [Test]
    public async Task FindDependentViews_finds_a_catalog_registered_view()
    {
        var (guard, catalog, _) = CreateGuard();
        catalog.Register(new ViewRegistration("adults", "people", Filter()));

        var dependents = await guard.FindDependentViewsAsync("people");

        Assert.That(dependents, Is.EqualTo(new[] { "adults" }));
    }

    [Test]
    public async Task FindDependentViews_finds_a_startup_declared_view()
    {
        var startup = new List<StartupViewRegistration>
        {
            new("adults", "people", _ => Filter()),
        };
        var (guard, _, _) = CreateGuard(startup);

        var dependents = await guard.FindDependentViewsAsync("people");

        Assert.That(dependents, Is.EqualTo(new[] { "adults" }));
    }

    [Test]
    public async Task FindDependentViews_finds_a_durable_runtime_view()
    {
        var (guard, _, registry) = CreateGuard();
        registry.ListAsync().Returns(Task.FromResult<IReadOnlyList<RuntimeViewRegistration>>(
            [Runtime("adults", "people")]));

        var dependents = await guard.FindDependentViewsAsync("people");

        Assert.That(dependents, Is.EqualTo(new[] { "adults" }));
    }

    [Test]
    public async Task FindDependentViews_deduplicates_and_orders_across_sources()
    {
        var startup = new List<StartupViewRegistration>
        {
            new("zeta", "people", _ => Filter()),
        };
        var (guard, catalog, registry) = CreateGuard(startup);
        catalog.Register(new ViewRegistration("zeta", "people", Filter()));
        catalog.Register(new ViewRegistration("alpha", "people", Filter()));
        registry.ListAsync().Returns(Task.FromResult<IReadOnlyList<RuntimeViewRegistration>>(
            [Runtime("alpha", "people"), Runtime("mid", "people")]));

        var dependents = await guard.FindDependentViewsAsync("people");

        Assert.That(dependents, Is.EqualTo(new[] { "alpha", "mid", "zeta" }));
    }

    [Test]
    public void FindDependentViews_null_source_throws()
    {
        var (guard, _, _) = CreateGuard();
        Assert.That(async () => await guard.FindDependentViewsAsync(null!), Throws.ArgumentNullException);
    }
}
