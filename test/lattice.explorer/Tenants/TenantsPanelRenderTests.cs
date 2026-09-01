using Orleans.Lattice.Explorer.Core.Vocabulary;
using Orleans.Lattice.Explorer.DesignSystem.Tokens;
using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Plugins.Tenancy;
using Orleans.Lattice.Explorer.Plugins.Tenants;

namespace Orleans.Lattice.Explorer.Tests.Tenants;

/// <summary>
/// What the tenant administration panel actually renders: the gate states an
/// operator sees, the tenant list at each breakpoint, and - the point of the
/// fixture - that no quota figure and no grant can reach the screen without
/// saying what it is.
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
            // The refusal comes from the shared copy layer, so it names the
            // surface the settled way rather than inventing a second wording.
            Assert.That(html, Does.Contain("is not available to your account"));
            Assert.That(html, Does.Contain("not an administrator"));

            // Disabled, not hidden: the surface still renders so a caller can see
            // it exists and is not theirs.
            Assert.That(html, Does.Contain("lxt-panel"));
            Assert.That(html, Does.Contain(ExplorerVocabulary.TenantAdministrationArea));
        });
    }

    [Test]
    public async Task A_denial_states_the_remedy_the_gate_declared_and_not_the_area_label()
    {
        var html = await TenantsRenderHarness.RenderPanelAsync(
            TenantsRenderHarness.SeededDomain(),
            ExplorerPluginAccess.Deny(
                "not an administrator",
                ExplorerAccessRemedy.Requiring("Admin", "an operator")));

        Assert.Multiple(() =>
        {
            // The missing permission and who issues it - the two facts a denial
            // that says only "not available for your account" leaves out.
            Assert.That(html, Does.Contain("Requires the Admin permission"));
            Assert.That(html, Does.Contain("an operator"));
            Assert.That(html, Does.Contain(ExplorerVocabulary.RemedyLabel));
        });
    }

    [Test]
    public async Task A_denial_that_declares_no_remedy_still_states_one()
    {
        var html = await TenantsRenderHarness.RenderPanelAsync(
            TenantsRenderHarness.SeededDomain(),
            ExplorerPluginAccess.Deny("not an administrator"));

        // Falls back to the copy layer's general remedy rather than to nothing:
        // a refusal with no remedy is the defect this path exists to prevent.
        Assert.That(html, Does.Contain("Ask an operator to grant your account access"));
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
            Assert.That(html, Does.Contain("Sign in to use " + ExplorerVocabulary.TenantAdministrationArea));
            Assert.That(html, Does.Contain("only to a signed-in identity"));

            // An anonymous caller is never told the grant was withheld: that is
            // the measured defect this area was one of.
            Assert.That(html, Does.Not.Contain("is not available to your account"));
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
            Assert.That(html, Does.Contain("is not enabled on this cluster"));
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

            // The lifecycle-action column opts out of the card presentation, so a
            // card stays readable rather than growing a button row.
            Assert.That(html, Does.Not.Contain("Manage"));
            Assert.That(html, Does.Not.Contain("Suspend"));
            Assert.That(html, Does.Not.Contain("lxt-danger"));

            // Scoping the Explorer to a tenant is how a caller reaches the rest
            // of the product, so unlike the lifecycle actions it survives the
            // reflow.
            Assert.That(html, Does.Contain("Set as active tenant"));
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
    public async Task An_empty_cluster_says_which_kind_of_empty_it_is()
    {
        var html = await TenantsRenderHarness.RenderPanelAsync(new FakeTenancyDomain());

        Assert.Multiple(() =>
        {
            // Genuine absence, and it says so explicitly rather than leaving the
            // reader to guess between absent, scoped out and not permitted.
            Assert.That(html, Does.Contain("No tenants yet"));
            Assert.That(html, Does.Contain("Nothing is being hidden from you"));
            Assert.That(html, Does.Not.Contain("You cannot see tenants here"));
            Assert.That(html, Does.Not.Contain("No tenants in this tenant"));
        });
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

            // The rule is reachable rather than sitting in a title attribute:
            // once as the badge's own visually-hidden expansion, and once as the
            // description the inert controls point at.
            Assert.That(html, Does.Contain("cannot be suspended, deleted"));
            Assert.That(html, Does.Contain("aria-describedby=\"lxt-default-tenant-rule\""));
            Assert.That(html, Does.Not.Contain("title=\"The reserved default tenant"));
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
