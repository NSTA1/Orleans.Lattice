using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.UI.Areas.Tenancy;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Tenancy;

/// <summary>
/// Issue #3962: the top-bar tenant switcher reads the same accessible-tenant list
/// as the Tenancy directory and the address line's <c>t/</c> completions, so the
/// three can never disagree, and that list is read again for every identity.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class TenancyTenantSwitchTests : TenancyTestContext
{
    [Test]
    public async Task The_switcher_offers_an_operator_the_directorys_tenants_and_the_default_tenant()
    {
        UseTenancyAs(isOperator: true);
        Cluster.WithTenant("globex");
        var tenantSwitch = SwitchOverTheDirectory();

        var choices = await tenantSwitch.RefreshAsync();

        Assert.Multiple(() =>
        {
            Assert.That(choices.Offered, Is.True);
            Assert.That(choices.Tenants, Is.EqualTo(new[] { "acme", TenantId.DefaultId, "globex" }));
        });
    }

    [Test]
    public async Task A_tenant_admin_of_a_single_tenant_is_offered_nothing()
    {
        UseTenancyAs(isOperator: false);
        var tenantSwitch = SwitchOverTheDirectory();

        var choices = await tenantSwitch.RefreshAsync();

        Assert.That(choices.Offered, Is.False);
    }

    [Test]
    public async Task The_catalogue_reads_the_tenant_list_again_for_a_new_identity_and_not_otherwise()
    {
        UseTenancyAs(isOperator: true);

        await Catalog.GetTenantsAsync(CancellationToken.None);
        await Catalog.GetTenantsAsync(CancellationToken.None);
        var sameIdentity = Reads();

        Auth.SignIn("someone-else@example.com");
        await Catalog.GetTenantsAsync(CancellationToken.None);
        var newIdentity = Reads();

        await Auth.LogoutAsync();
        await Catalog.GetTenantsAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(sameIdentity, Is.EqualTo(1), "an unchanged identity is served from memory");
            Assert.That(newIdentity, Is.EqualTo(2), "a sign-in as someone else never reads the previous caller's list");
            Assert.That(Reads(), Is.EqualTo(3), "nor does a sign-out");
        });
    }

    private int Reads() => Cluster.Calls.Count(call => call == nameof(FakeTenancyCluster.ListAccessibleTenantsAsync));

    // The chrome's reading of tenancy, over the Tenancy area's own accessible-tenant
    // source rather than the chrome context's scripted list.
    private ExplorerTenantSwitch SwitchOverTheDirectory()
    {
        var view = Services.GetRequiredService<IExplorerTenantView>();
        var source = new TenancyAccessibleTenantSource(Catalog, new ExplorerTenantContext { ActiveTenant = new ExplorerTenantId("acme") });
        var tenancy = new ExplorerTenancy(view, Switcher, source);
        return new ExplorerTenantSwitch(
            tenancy,
            Services.GetRequiredService<ExplorerNavigator>(),
            Services.GetRequiredService<LtToastService>(),
            Auth);
    }
}
