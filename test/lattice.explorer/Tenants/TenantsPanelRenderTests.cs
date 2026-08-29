using Orleans.Lattice.Explorer.DesignSystem.Tokens;
using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Tenancy;
using Orleans.Lattice.Explorer.Tenants;

namespace Orleans.Lattice.Explorer.Tests.Tenants;

/// <summary>
/// What the Tenants panel actually renders: the gate states an operator sees,
/// the tenant list at each breakpoint, and - the point of the fixture - that
/// no quota figure and no grant can reach the screen without saying what it is.
/// </summary>
[TestFixture]
public sealed class TenantsPanelRenderTests
{
    [Test]
    public async Task An_operator_sees_the_tenant_list()
    {
        var html = await TenantsRenderHarness.RenderPanelAsync(TenantsRenderHarness.SeededDomain());

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain(SampleTenants.Acme));
            Assert.That(html, Does.Contain("Active"));
            Assert.That(html, Does.Contain("New tenant"));
        });
    }

    [Test]
    public async Task A_non_operator_sees_the_area_disabled_with_the_reason()
    {
        var html = await TenantsRenderHarness.RenderPanelAsync(
            TenantsRenderHarness.SeededDomain(),
            ExplorerPluginAccess.Deny("not an administrator"));

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("reserved for platform operators"));
            Assert.That(html, Does.Contain("not an administrator"));

            // Disabled, not hidden: the surface still renders so a caller can see
            // it exists and is not theirs.
            Assert.That(html, Does.Contain("lxt-panel"));
            Assert.That(html, Does.Contain("Tenants"));
        });
    }

    [Test]
    public async Task A_non_operator_is_offered_no_tenant_data_at_all()
    {
        var html = await TenantsRenderHarness.RenderPanelAsync(
            TenantsRenderHarness.SeededDomain(),
            ExplorerPluginAccess.Denied);

        Assert.That(html, Does.Not.Contain(SampleTenants.Acme));
    }

    [Test]
    public async Task An_unauthenticated_connection_is_offered_a_sign_in_rather_than_a_refusal()
    {
        var html = await TenantsRenderHarness.RenderPanelAsync(
            TenantsRenderHarness.SeededDomain(),
            ExplorerPluginAccess.AuthenticationRequired);

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("not signed in"));
            Assert.That(html, Does.Contain("sign in"));
            Assert.That(html, Does.Not.Contain("reserved for platform operators"));
        });
    }

    [Test]
    public async Task A_tenancy_absent_cluster_renders_nothing_but_the_explanation()
    {
        var html = await TenantsRenderHarness.RenderPanelAsync(
            TenantsRenderHarness.SeededDomain(),
            ExplorerPluginAccess.ReportUnavailable("no tenancy add-on here"));

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("does not serve tenant administration"));
            Assert.That(html, Does.Contain("no tenancy add-on here"));

            // Nothing else at all: no tabs, no list, no actions.
            Assert.That(html, Does.Not.Contain(SampleTenants.Acme));
            Assert.That(html, Does.Not.Contain("New tenant"));
            Assert.That(html, Does.Not.Contain("Refresh"));
            Assert.That(html, Does.Not.Contain("role=\"tablist\""));
        });
    }

    [Test]
    public async Task The_tenant_list_renders_as_a_table_from_medium_up()
    {
        var html = await TenantsRenderHarness.RenderPanelAsync(
            TenantsRenderHarness.SeededDomain(),
            breakpoint: LatticeBreakpoint.Expanded);

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("<table"));
            Assert.That(html, Does.Not.Contain("lx-cardlist"));
        });
    }

    [Test]
    public async Task The_tenant_list_reflows_to_cards_at_compact()
    {
        var html = await TenantsRenderHarness.RenderPanelAsync(
            TenantsRenderHarness.SeededDomain(),
            breakpoint: LatticeBreakpoint.Compact);

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("lx-cardlist"));
            Assert.That(html, Does.Not.Contain("<table"));

            // The row-action column opts out of the card presentation, so a card
            // stays readable rather than growing a button row.
            Assert.That(html, Does.Not.Contain("lxt-row-actions"));
        });
    }

    [Test]
    public async Task The_panel_supplies_its_own_adaptive_root_when_no_shell_cascades_one()
    {
        var html = await TenantsRenderHarness.RenderPanelAsync(
            TenantsRenderHarness.SeededDomain(),
            breakpoint: null);

        Assert.That(html, Does.Contain("lxt-root"));
    }

    [Test]
    public async Task The_panel_yields_to_a_shell_provided_root()
    {
        var html = await TenantsRenderHarness.RenderPanelAsync(TenantsRenderHarness.SeededDomain());

        Assert.That(html, Does.Not.Contain("lxt-root"), "the plugin must not own a second viewport root");
    }

    [Test]
    public async Task An_empty_cluster_says_so_rather_than_rendering_a_bare_table()
    {
        var html = await TenantsRenderHarness.RenderPanelAsync(new FakeTenancyDomain());

        Assert.That(html, Does.Contain("No tenants are visible to your account."));
    }

    [Test]
    public async Task The_reserved_default_tenant_is_marked_and_its_destructive_actions_are_disabled()
    {
        var domain = new FakeTenancyDomain();
        domain.Service.Tenants.Add(SampleTenants.Summary(SampleTenants.DefaultTenant, isDefault: true));
        domain.Service.Usage[SampleTenants.DefaultTenant] = SampleTenants.Usage(SampleTenants.DefaultTenant);

        var html = await TenantsRenderHarness.RenderPanelAsync(domain);

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("Default"));
            Assert.That(html, Does.Contain("cannot be deleted"));
            Assert.That(html, Does.Contain("cannot be suspended"));
        });
    }

    [Test]
    public async Task An_over_quota_tenant_is_flagged_in_the_list()
    {
        var html = await TenantsRenderHarness.RenderPanelAsync(TenantsRenderHarness.SeededDomain());

        // The sample reading caps resident memory at zero with 64 bytes resident.
        Assert.That(html, Does.Contain("Over quota"));
    }

    [Test]
    public async Task A_tenant_whose_usage_could_not_be_read_says_so_rather_than_showing_zero()
    {
        var domain = new FakeTenancyDomain();
        domain.Service.Tenants.Add(SampleTenants.Summary());
        domain.Service.Fail(FakeTenantAdminService.Op.Usage, TenantOperationStatus.Denied);

        var html = await TenantsRenderHarness.RenderPanelAsync(domain);

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain(TenantRow.UsageUnavailableText));
            Assert.That(html, Does.Contain(SampleTenants.Acme));
        });
    }

    [Test]
    public async Task Every_sub_surface_is_offered_in_the_strip()
    {
        var html = await TenantsRenderHarness.RenderPanelAsync(TenantsRenderHarness.SeededDomain());

        Assert.Multiple(() =>
        {
            foreach (var tab in TenantsSurfaces.Tabs)
            {
                Assert.That(html, Does.Contain(tab.Label), tab.Id);
            }
        });
    }

    [Test]
    public async Task The_strip_offers_a_refresh_action_to_an_operator()
    {
        var html = await TenantsRenderHarness.RenderPanelAsync(TenantsRenderHarness.SeededDomain());

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("lxt-refresh-action"));

            // Enabled for an operator: the action renders without the disabled
            // attribute the gate applies when the caller is refused.
            Assert.That(html, Does.Not.Contain("lxt-refresh-action\" disabled"));
        });
    }

    [Test]
    public async Task A_refused_caller_gets_a_disabled_refresh_action_rather_than_none()
    {
        var html = await TenantsRenderHarness.RenderPanelAsync(
            TenantsRenderHarness.SeededDomain(),
            ExplorerPluginAccess.Denied);

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("lxt-refresh-action"));
            Assert.That(html, Does.Contain("lxt-refresh-action\" disabled"));
        });
    }
}
