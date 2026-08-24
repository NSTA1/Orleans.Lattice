using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// Pins the T3 tenant-filtered tree catalog behaviour of
/// <see cref="LatticeStateQuery.ListTreesAsync"/>. The State API dials the
/// registry enumeration directly, so it applies the same
/// <see cref="ITenantEnumerationFilter"/> seam as the core grain: with no
/// active filter (or no active tenant) the catalog lists every tree exactly as
/// before; with an active tenant it lists only the tenant-visible subset.
/// </summary>
[TestFixture]
public sealed class LatticeStateQueryTenantCatalogTests
{
    [SetUp]
    [TearDown]
    public void ClearAmbientTenant() => LatticeActiveTenantContext.Current = null;

    private static LatticeStateQuery CreateQuery(
        IReadOnlyList<string> allTreeIds,
        ITenantEnumerationFilter? filter)
    {
        var grainFactory = Substitute.For<IGrainFactory>();

        var registry = Substitute.For<ILatticeRegistry>();
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        registry.GetAllTreeIdsAsync().Returns(Task.FromResult(allTreeIds));
        registry.GetEntryAsync(Arg.Any<string>()).Returns(Task.FromResult<TreeRegistryEntry?>(null));

        var deletion = Substitute.For<ITreeDeletionGrain>();
        deletion.IsDeletedAsync().Returns(Task.FromResult(false));
        grainFactory.GetGrain<ITreeDeletionGrain>(Arg.Any<string>()).Returns(deletion);

        var options = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        options.Get(Arg.Any<string>()).Returns(new LatticeOptions());

        var apiOptions = Options.Create(new LatticeApiStateOptions());

        var services = Substitute.For<IServiceProvider>();
        if (filter is not null)
        {
            services.GetService(typeof(ITenantEnumerationFilter)).Returns(filter);
        }

        return new LatticeStateQuery(grainFactory, options, apiOptions, services);
    }

    [Test]
    public async Task ListTreesAsync_with_no_filter_registered_lists_all_trees()
    {
        IReadOnlyList<string> all = ["orders", "reports", "users"];
        var query = CreateQuery(all, filter: null);

        var page = await query.ListTreesAsync(new CatalogRequest());

        Assert.That(
            page.Entries.Select(e => e.TreeId),
            Is.EqualTo(new[] { "orders", "reports", "users" }));
    }

    [Test]
    public async Task ListTreesAsync_with_inactive_filter_lists_all_trees()
    {
        IReadOnlyList<string> all = ["orders", "reports", "users"];
        var filter = new RecordingEnumerationFilter(isActive: false);
        var query = CreateQuery(all, filter);

        var page = await query.ListTreesAsync(new CatalogRequest());

        Assert.Multiple(() =>
        {
            Assert.That(
                page.Entries.Select(e => e.TreeId),
                Is.EqualTo(new[] { "orders", "reports", "users" }));
            Assert.That(filter.FilterCallCount, Is.Zero);
        });
    }

    [Test]
    public async Task ListTreesAsync_with_active_filter_but_no_active_tenant_lists_all_trees()
    {
        IReadOnlyList<string> all = ["orders", "reports", "users"];
        var filter = new RecordingEnumerationFilter(isActive: true, result: ["orders"]);
        var query = CreateQuery(all, filter);

        var page = await query.ListTreesAsync(new CatalogRequest());

        Assert.Multiple(() =>
        {
            Assert.That(
                page.Entries.Select(e => e.TreeId),
                Is.EqualTo(new[] { "orders", "reports", "users" }));
            Assert.That(filter.FilterCallCount, Is.Zero);
        });
    }

    [Test]
    public async Task ListTreesAsync_with_active_tenant_lists_only_the_tenant_subset()
    {
        IReadOnlyList<string> all = ["orders", "reports", "users"];
        var filter = new RecordingEnumerationFilter(isActive: true, result: ["orders", "users"]);
        var query = CreateQuery(all, filter);
        var tenant = TenantId.Parse("contoso");

        TreeCatalogPage page;
        using (LatticeActiveTenantContext.With(tenant))
        {
            page = await query.ListTreesAsync(new CatalogRequest());
        }

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries.Select(e => e.TreeId), Is.EqualTo(new[] { "orders", "users" }));
            Assert.That(filter.FilterCallCount, Is.EqualTo(1));
            Assert.That(filter.LastTenant, Is.EqualTo(tenant));
            Assert.That(filter.LastInput, Is.SameAs(all));
        });
    }

    [Test]
    public async Task ListTreesAsync_with_active_tenant_that_owns_no_trees_is_empty()
    {
        IReadOnlyList<string> all = ["orders", "reports", "users"];
        var filter = new RecordingEnumerationFilter(isActive: true, result: []);
        var query = CreateQuery(all, filter);

        TreeCatalogPage page;
        using (LatticeActiveTenantContext.With(TenantId.Parse("fabrikam")))
        {
            page = await query.ListTreesAsync(new CatalogRequest());
        }

        Assert.That(page.Entries, Is.Empty);
    }

    [Test]
    public async Task ListTreesAsync_with_empty_registry_is_empty()
    {
        IReadOnlyList<string> all = [];
        var query = CreateQuery(all, filter: null);

        var page = await query.ListTreesAsync(new CatalogRequest());

        Assert.That(page.Entries, Is.Empty);
    }

    private sealed class RecordingEnumerationFilter(bool isActive, IReadOnlyList<string>? result = null)
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
