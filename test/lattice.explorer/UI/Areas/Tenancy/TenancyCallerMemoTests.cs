using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.UI.Areas.Tenancy;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Tenancy;

/// <summary>
/// Issue #4019 for the Tenancy area: what the catalogue proved and what the
/// accessible-tenant list offers belong to the caller who asked, so a sign-in as
/// someone else inside the circuit is never served the previous caller's standing
/// or handed the previous caller's tenant.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class TenancyCallerMemoTests : TenancyTestContext
{
    [Test]
    public async Task The_last_standing_is_not_the_next_callers()
    {
        UseTenancyAs(isOperator: true);
        var catalog = Catalog;

        var proven = await catalog.GetStandingAsync(CancellationToken.None);
        Auth.SignIn("bob@example.com");

        Assert.Multiple(() =>
        {
            Assert.That(proven.IsOperator, Is.True);
            Assert.That(catalog.LastStanding, Is.Null, "the operator's standing is not bob's");
        });
    }

    [Test]
    public async Task A_new_identity_is_not_handed_the_tenant_the_previous_one_was_established_in()
    {
        UseTenancyAs(isOperator: false);
        Cluster.WithTenant("zeta", admins: ["bob@example.com"]);
        var context = new ExplorerTenantContext { ActiveTenant = new ExplorerTenantId("acme") };
        var source = new TenancyAccessibleTenantSource(Catalog, context);

        var first = await source.GetAccessibleTenantsAsync();

        // Bob signs in inside the circuit while the context still holds acme, and his
        // read of the tenant list fails.
        Auth.SignIn("bob@example.com");
        Cluster.Fail(nameof(FakeTenancyCluster.ListAccessibleTenantsAsync), FakeTenancyCluster.Denied());
        var bob = await source.GetAccessibleTenantsAsync();

        Assert.Multiple(() =>
        {
            Assert.That(first.Select(tenant => tenant.Value), Does.Contain("acme"));
            Assert.That(bob.Select(tenant => tenant.Value), Does.Not.Contain("acme"), "the previous caller's tenant is not carried to bob");
        });
    }

    [Test]
    public async Task A_tenant_established_for_the_new_identity_leads_again()
    {
        UseTenancyAs(isOperator: false);
        var context = new ExplorerTenantContext { ActiveTenant = new ExplorerTenantId("acme") };
        var source = new TenancyAccessibleTenantSource(Catalog, context);
        await source.GetAccessibleTenantsAsync();

        Auth.SignIn("bob@example.com");
        await source.GetAccessibleTenantsAsync();
        context.ActiveTenant = new ExplorerTenantId("bobs-tenant");
        var bob = await source.GetAccessibleTenantsAsync();

        Assert.That(bob[0].Value, Is.EqualTo("bobs-tenant"), "a tenant written for bob is his established tenant, and leads");
    }
}
