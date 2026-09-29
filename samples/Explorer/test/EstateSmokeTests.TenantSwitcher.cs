using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.UI.Layout;

namespace Orleans.Lattice.Samples.Explorer.Tests;

/// <summary>
/// Issue #3962: the console's top-bar tenant switcher is offered to the platform
/// operator, who can switch between the default tenant, acme and globex, and is
/// absent - not disabled - for acme-admin, who administers a single tenant.
/// </summary>
public sealed partial class EstateSmokeTests
{
    [Test]
    public async Task The_operator_sees_the_tenant_switcher_listing_default_acme_and_globex()
    {
        var home = await SampleTestHost.GetHomeAsync(_sample);
        await using var circuit = await ConsoleCircuit.OpenAsync(_sample, SampleIdentities.Administrator, SampleIdentities.AcmeTenant);

        var switcher = await circuit.RenderAsync<TenantSwitcher>();
        var tenants = await circuit.ListAccessibleTenantsAsync();

        Assert.Multiple(() =>
        {
            Assert.That(home, Does.Contain("data-lt-command=\"tenant.switch\""), "the operator's console header carries the switcher");
            Assert.That(switcher, Does.Contain("data-lt-command=\"tenant.switch\"").And.Contain(SampleIdentities.AcmeTenant),
                "at /t/acme the switcher names acme as the active tenant");
            Assert.That(tenants, Is.EquivalentTo(new[] { TenantId.DefaultId, SampleIdentities.AcmeTenant, SampleIdentities.GlobexTenant }),
                "the switcher lists the default tenant, acme and globex");
        });
    }

    [Test]
    public async Task A_tenant_admin_of_a_single_tenant_does_not_see_the_tenant_switcher()
    {
        await using var circuit = await ConsoleCircuit.OpenAsync(_sample, SampleIdentities.AcmeAdmin, SampleIdentities.AcmeTenant);

        var switcher = await circuit.RenderAsync<TenantSwitcher>();
        var tenants = await circuit.ListAccessibleTenantsAsync();

        Assert.Multiple(() =>
        {
            Assert.That(tenants, Is.EqualTo(new[] { SampleIdentities.AcmeTenant }), "acme-admin reaches acme alone");
            Assert.That(switcher.Trim(), Is.Empty, "the switcher is absent, not disabled");
        });
    }
}
