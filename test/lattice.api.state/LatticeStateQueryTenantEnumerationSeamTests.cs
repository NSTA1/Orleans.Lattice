using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// Pins the tenant enumeration seam on the two remaining catalog choke points:
/// <see cref="LatticeStateQuery.ListCoveredTreesAsync"/> and
/// <see cref="LatticeStateQuery.ListViewsAsync"/>. Both enumerate under a
/// system-origin scope, so without the seam they hand a tenant caller every
/// other tenant's tree ids and view names whenever read visibility is off (no
/// auth gate registered), which is a supported configuration because tenancy
/// and authorization are independent add-ons.
/// </summary>
[TestFixture]
public sealed class LatticeStateQueryTenantEnumerationSeamTests
{
    [SetUp]
    [TearDown]
    public void ClearAmbientTenant() => LatticeActiveTenantContext.Current = null;

    private static LatticeStateQuery CreateQuery(
        ITenantEnumerationFilter? filter,
        IReadOnlyList<string>? coveredTrees = null,
        IReadOnlyList<RuntimeViewRegistration>? views = null)
    {
        var grainFactory = Substitute.For<IGrainFactory>();

        var registry = Substitute.For<IViewRegistryGrain>();
        registry.ListAsync().Returns(
            Task.FromResult<IReadOnlyList<RuntimeViewRegistration>>(views ?? []));
        grainFactory.GetGrain<IViewRegistryGrain>(IViewRegistryGrain.SingletonKey).Returns(registry);

        var options = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        options.Get(Arg.Any<string>()).Returns(new LatticeOptions());

        var apiOptions = Options.Create(new LatticeApiStateOptions());

        var services = Substitute.For<IServiceProvider>();
        if (filter is not null)
        {
            services.GetService(typeof(ITenantEnumerationFilter)).Returns(filter);
        }

        if (coveredTrees is not null)
        {
            var index = Substitute.For<ILatticeMultiTreeTagIndex>();
            index.CoveredTreesAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(coveredTrees));

            var tagFactory = Substitute.For<ILatticeTagIndexFactory>();
            tagFactory.CreateMultiTree(Arg.Any<string>(), Arg.Any<IReadOnlyCollection<string>?>())
                .Returns(index);
            services.GetService(typeof(ILatticeTagIndexFactory)).Returns(tagFactory);
        }

        return new LatticeStateQuery(grainFactory, options, apiOptions, services, new NullTenantContextResolver());
    }

    private static RuntimeViewRegistration View(string viewName, string sourceTreeId) =>
        new()
        {
            ViewName = viewName,
            SourceTreeId = sourceTreeId,
            ProjectionTypeName = "Test.Projection",
            ProjectionVersion = "1",
        };

    [Test]
    public async Task ListCoveredTreesAsync_with_no_filter_registered_lists_every_covered_tree()
    {
        var query = CreateQuery(filter: null, coveredTrees: ["archive", "orders"]);

        var page = await query.ListCoveredTreesAsync(new CatalogRequest { IndexName = "by-status" });

        Assert.That(page.Entries, Is.EqualTo(new[] { "archive", "orders" }));
    }

    [Test]
    public async Task ListCoveredTreesAsync_with_active_filter_but_no_active_tenant_lists_every_covered_tree()
    {
        var filter = new RecordingTenantFilter(isActive: true, result: []);
        var query = CreateQuery(filter, coveredTrees: ["archive", "orders"]);

        var page = await query.ListCoveredTreesAsync(new CatalogRequest { IndexName = "by-status" });

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries, Is.EqualTo(new[] { "archive", "orders" }));
            Assert.That(filter.FilterCallCount, Is.Zero);
        });
    }

    [Test]
    public async Task ListCoveredTreesAsync_with_active_tenant_lists_only_the_tenant_subset()
    {
        // A tag index is addressed by a cluster-global name, so its covered set
        // spans every tenant that tagged into it. Without the seam this listed
        // t/other/secrets to a contoso caller.
        var filter = new RecordingTenantFilter(
            isActive: true,
            result: ["t/contoso/orders"]);
        var query = CreateQuery(
            filter,
            coveredTrees: ["t/contoso/orders", "t/other/secrets", "legacy"]);
        var tenant = TenantId.Parse("contoso");

        CoveredTreeCatalogPage page;
        using (LatticeActiveTenantContext.With(tenant))
        {
            page = await query.ListCoveredTreesAsync(new CatalogRequest { IndexName = "by-status" });
        }

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries, Is.EqualTo(new[] { "t/contoso/orders" }));
            Assert.That(filter.FilterCallCount, Is.EqualTo(1));
            Assert.That(filter.LastTenant, Is.EqualTo(tenant));
            Assert.That(
                filter.LastInput,
                Is.EquivalentTo(new[] { "t/contoso/orders", "t/other/secrets", "legacy" }));
        });
    }

    [Test]
    public async Task ListViewsAsync_with_no_filter_registered_lists_every_view()
    {
        var query = CreateQuery(
            filter: null,
            views: [View("orders", "orders"), View("reports", "reports")]);

        var page = await query.ListViewsAsync(new CatalogRequest());

        Assert.That(
            page.Entries.Select(entry => entry.ViewName),
            Is.EqualTo(new[] { "orders", "reports" }));
    }

    [Test]
    public async Task ListViewsAsync_with_active_filter_but_no_active_tenant_lists_every_view()
    {
        var filter = new RecordingTenantFilter(isActive: true, result: []);
        var query = CreateQuery(
            filter,
            views: [View("orders", "orders"), View("reports", "reports")]);

        var page = await query.ListViewsAsync(new CatalogRequest());

        Assert.Multiple(() =>
        {
            Assert.That(
                page.Entries.Select(entry => entry.ViewName),
                Is.EqualTo(new[] { "orders", "reports" }));
            Assert.That(filter.FilterCallCount, Is.Zero);
        });
    }

    [Test]
    public async Task ListViewsAsync_with_active_tenant_lists_only_the_tenant_views()
    {
        // A view's registry key carries its tenant segment, so the registry this
        // enumerates system-origin holds every tenant's. Without the seam the
        // whole cross-tenant view roster - and with it the tenant roster - came
        // back to a contoso caller.
        var filter = new RecordingTenantFilter(
            isActive: true,
            result: ["t/contoso/orders"]);
        var query = CreateQuery(
            filter,
            views:
            [
                View("t/contoso/orders", "t/contoso/orders-src"),
                View("t/other/secrets", "t/other/secrets-src"),
            ]);
        var tenant = TenantId.Parse("contoso");

        ViewCatalogPage page;
        using (LatticeActiveTenantContext.With(tenant))
        {
            page = await query.ListViewsAsync(new CatalogRequest());
        }

        Assert.Multiple(() =>
        {
            Assert.That(
                page.Entries.Select(entry => entry.ViewName),
                Is.EqualTo(new[] { "t/contoso/orders" }));
            Assert.That(filter.FilterCallCount, Is.EqualTo(1));
            Assert.That(filter.LastTenant, Is.EqualTo(tenant));
            Assert.That(
                filter.LastInput,
                Is.EquivalentTo(new[] { "t/contoso/orders", "t/other/secrets" }));
        });
    }

    [Test]
    public async Task ListViewsAsync_with_a_tenant_owning_no_views_is_empty()
    {
        var filter = new RecordingTenantFilter(isActive: true, result: []);
        var query = CreateQuery(
            filter,
            views: [View("t/other/secrets", "t/other/secrets-src")]);

        ViewCatalogPage page;
        using (LatticeActiveTenantContext.With(TenantId.Parse("fabrikam")))
        {
            page = await query.ListViewsAsync(new CatalogRequest());
        }

        Assert.That(page.Entries, Is.Empty);
    }

    private sealed class RecordingTenantFilter(bool isActive, IReadOnlyList<string>? result = null)
        : ITenantEnumerationFilter
    {
        public bool IsActive { get; } = isActive;

        public int FilterCallCount { get; private set; }

        public TenantId? LastTenant { get; private set; }

        public IReadOnlyList<string>? LastInput { get; private set; }

        public IReadOnlyList<string> Filter(TenantId tenant, IReadOnlyList<string> treeIds)
        {
            FilterCallCount++;
            LastTenant = tenant;
            LastInput = treeIds;
            return result ?? treeIds;
        }
    }
}
