using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.Tests.Connection;
using Orleans.Lattice.Explorer.UI.Areas.Schema;
using Orleans.Lattice.Explorer.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Schema;

/// <summary>
/// Everything the Schema area remembers - its visibility, per-tree grants, the
/// tree list and the directory listing - is keyed on the tenant the circuit
/// asserts, so a tenant switch reads again and never lists one tenant's trees
/// under another.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class SchemaTenantCacheTests : SchemaTestContext
{
    private readonly FakeActiveTenantProvider _tenant = new("acme");

    public SchemaTenantCacheTests()
    {
        Services.AddSingleton<ILatticeActiveTenantProvider>(_tenant);

        // The cluster lists the asserted tenant's trees.
        Explorer.Connection
            .ListTreesAsync(Arg.Any<CatalogRequest>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(new TreeCatalogPage { Entries = [SchemaTestData.Entry(_tenant.AssertedTenant + "-orders")] }));
    }

    [Test]
    public async Task The_tree_list_is_read_again_after_a_tenant_switch_even_while_fresh()
    {
        var catalog = Services.GetRequiredService<SchemaTreeCatalog>();

        var acme = await catalog.GetAsync(refresh: false, CancellationToken.None);
        var cached = await catalog.GetAsync(refresh: false, CancellationToken.None);
        _tenant.Set("globex");
        var globex = await catalog.GetAsync(refresh: false, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(acme, Is.EqualTo(new[] { "acme-orders" }));
            Assert.That(cached, Is.SameAs(acme));
            Assert.That(globex, Is.EqualTo(new[] { "globex-orders" }));
        });
    }

    [Test]
    public async Task The_directory_listing_is_never_served_under_another_tenant()
    {
        var acme = await Directory.GetAsync(refresh: false, CancellationToken.None);
        _tenant.Set("globex");
        var last = Directory.Last;
        var globex = await Directory.GetAsync(refresh: false, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(acme.Rows.Select(row => row.TreeId), Is.EqualTo(new[] { "acme-orders" }));
            Assert.That(last, Is.Null, "acme's listing is not the last listing under globex");
            Assert.That(globex.Rows.Select(row => row.TreeId), Is.EqualTo(new[] { "globex-orders" }));
            Assert.That(Directory.Last, Is.SameAs(globex));
        });
    }

    [Test]
    public async Task The_area_visibility_and_tree_grants_are_asked_again_under_a_new_tenant()
    {
        Schema.DefaultCapabilities = tree => _tenant.AssertedTenant == "acme" ? FakeSchemaControl.All(tree) : FakeSchemaControl.None(tree);
        var access = Services.GetRequiredService<SchemaAccess>();

        var acmeArea = await access.GetAvailabilityAsync(CancellationToken.None);
        var acmeGrants = await access.GetGrantsAsync("orders", refresh: false, CancellationToken.None);
        _tenant.Set("globex");
        var globexArea = await access.GetAvailabilityAsync(CancellationToken.None);
        var globexGrants = await access.GetGrantsAsync("orders", refresh: false, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(acmeArea.Kind, Is.EqualTo(AreaAvailabilityKind.Visible));
            Assert.That(acmeGrants.HasAny, Is.True);
            Assert.That(globexArea.Kind, Is.Not.EqualTo(AreaAvailabilityKind.Visible));
            Assert.That(globexGrants.HasAny, Is.False);
        });
    }

    [Test]
    public void Without_tenancy_the_facades_assert_no_tenant()
    {
        using var services = new ServiceCollection().BuildServiceProvider();

        Assert.That(new SchemaFacades(services).AssertedTenant, Is.Null);
    }
}
