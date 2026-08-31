using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.Core.Vocabulary;
using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Plugins.Tenancy;
using Orleans.Lattice.Explorer.Plugins.Tenants;
using Orleans.Lattice.Explorer.Plugins.Tenants.Workspace;

namespace Orleans.Lattice.Explorer.Tests.Tenants;

/// <summary>
/// "Set as active tenant": the action that drives the shell's tenant picker from
/// the tenant list, and refuses honestly when the cluster will not have it.
/// </summary>
[TestFixture]
public sealed class TenantsWorkspaceScopeTests
{
    [Test]
    public async Task Setting_a_tenant_active_switches_the_scope_through_the_shared_seam()
    {
        var (workspace, domain) = SampleTenants.Seeded();
        using var guard = workspace;
        await workspace.InitializeAsync();

        await workspace.SetActiveTenantAsync(SampleTenants.Acme);

        Assert.Multiple(() =>
        {
            // The one source of truth: the domain's own switcher, which is the
            // same value the shell's picker renders from.
            Assert.That(domain.ActiveTenant?.Value, Is.EqualTo(SampleTenants.Acme));
            Assert.That(workspace.ActiveTenantId, Is.EqualTo(SampleTenants.Acme));
            Assert.That(workspace.LastStatus, Is.EqualTo(TenantOperationStatus.Succeeded));
            Assert.That(workspace.LastMessage, Is.EqualTo("Active tenant: acme"));
        });
    }

    [Test]
    public async Task A_refused_switch_leaves_the_scope_alone_and_says_so()
    {
        var (workspace, domain) = SampleTenants.Seeded();
        using var guard = workspace;
        await workspace.InitializeAsync();

        // The switch is operator-gated and fails closed; the caller is not one.
        domain.IsOperator = false;

        await workspace.SetActiveTenantAsync(SampleTenants.Acme);

        Assert.Multiple(() =>
        {
            Assert.That(domain.ActiveTenant, Is.Null);
            Assert.That(workspace.LastStatus, Is.EqualTo(TenantOperationStatus.Denied));
            Assert.That(workspace.LastMessage, Is.EqualTo(TenantsWorkspace.SwitchRefusedMessage));
        });
    }

    [Test]
    public async Task A_refused_switch_is_reported_rather_than_silently_doing_nothing()
    {
        var (workspace, domain) = SampleTenants.Seeded();
        using var guard = workspace;
        await workspace.InitializeAsync();
        domain.IsOperator = false;

        var announcements = 0;
        workspace.Changed += () => announcements++;
        await workspace.SetActiveTenantAsync(SampleTenants.Acme);

        Assert.Multiple(() =>
        {
            Assert.That(announcements, Is.GreaterThan(0));
            Assert.That(workspace.LastMessage, Is.Not.Null.And.Not.Empty);
        });
    }

    [Test]
    public async Task The_tenant_already_active_is_not_offered_the_action_again()
    {
        var (workspace, domain) = SampleTenants.Seeded();
        using var guard = workspace;
        await workspace.InitializeAsync();
        await workspace.SetActiveTenantAsync(SampleTenants.Acme);

        Assert.Multiple(() =>
        {
            Assert.That(workspace.IsActiveTenant(SampleTenants.Acme), Is.True);
            Assert.That(workspace.CanSetActiveTenant(SampleTenants.Acme), Is.False);
            Assert.That(workspace.CanSetActiveTenant(SampleTenants.Globex), Is.True);
            Assert.That(domain.ActiveTenant?.Value, Is.EqualTo(SampleTenants.Acme));
        });
    }

    [Test]
    public async Task A_refused_caller_is_offered_no_switch_at_all()
    {
        var domain = new FakeTenancyDomain();
        var store = new ExplorerPluginAccessStore();
        store.Set(TenantsPluginKeys.PluginId, ExplorerPluginAccess.Denied);
        using var workspace = new TenantsWorkspace(domain, store);
        await workspace.InitializeAsync();

        Assert.That(workspace.CanSetActiveTenant(SampleTenants.Acme), Is.False);
    }

    [Test]
    public async Task A_deployment_without_tenancy_offers_no_switch()
    {
        var (workspace, domain) = SampleTenants.Seeded();
        using var guard = workspace;
        await workspace.InitializeAsync();

        domain.IsTenancyEnabled = false;

        Assert.That(workspace.CanSetActiveTenant(SampleTenants.Acme), Is.False);
    }

    [Test]
    public async Task An_absent_tenant_id_is_neither_active_nor_switchable()
    {
        var (workspace, _) = SampleTenants.Seeded();
        using var guard = workspace;
        await workspace.InitializeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(workspace.CanSetActiveTenant(null), Is.False);
            Assert.That(workspace.CanSetActiveTenant(string.Empty), Is.False);
            Assert.That(workspace.IsActiveTenant(null), Is.False);
            Assert.That(workspace.IsActiveTenant(string.Empty), Is.False);
        });
    }

    [Test]
    public void Setting_a_null_tenant_active_throws()
    {
        var (workspace, _, _) = SampleTenants.Workspace();
        using var guard = workspace;

        Assert.That(
            async () => await workspace.SetActiveTenantAsync(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public async Task A_switch_the_workspace_itself_refuses_makes_no_call_at_all()
    {
        var (workspace, domain) = SampleTenants.Seeded();
        using var guard = workspace;
        await workspace.InitializeAsync();
        await workspace.SetActiveTenantAsync(SampleTenants.Acme);
        var before = domain.ActiveTenant;

        // Already active, so the action is not offered and must not be taken.
        await workspace.SetActiveTenantAsync(SampleTenants.Acme);

        Assert.Multiple(() =>
        {
            Assert.That(domain.ActiveTenant, Is.EqualTo(before));
            Assert.That(workspace.LastStatus, Is.Null);
        });
    }

    [Test]
    public async Task The_all_tenants_scope_is_reported_from_the_same_seam_the_picker_reads()
    {
        var (workspace, domain) = SampleTenants.Seeded();
        using var guard = workspace;
        await workspace.InitializeAsync();

        Assert.That(workspace.IsAllTenantsScope, Is.False);

        domain.RequestedVisibility = ExplorerTenantVisibility.AllTenants;

        Assert.That(workspace.IsAllTenantsScope, Is.True);
    }

    [Test]
    public async Task The_refusal_names_the_authority_rather_than_the_tenant()
    {
        var (workspace, domain) = SampleTenants.Seeded();
        using var guard = workspace;
        await workspace.InitializeAsync();
        domain.IsOperator = false;

        await workspace.SetActiveTenantAsync(SampleTenants.Acme);

        Assert.Multiple(() =>
        {
            Assert.That(workspace.LastMessage, Does.Contain("platform operators"));
            Assert.That(workspace.LastMessage, Does.Not.Contain(SampleTenants.Acme));
        });
    }
}

/// <summary>
/// The gate contract as this area renders it: the four states, and the remedy a
/// denial carries.
/// </summary>
[TestFixture]
public sealed class TenantsWorkspaceAccessCopyTests
{
    private static TenantsWorkspace Workspace(ExplorerPluginAccess access)
    {
        var store = new ExplorerPluginAccessStore();
        store.Set(TenantsPluginKeys.PluginId, access);
        return new TenantsWorkspace(new FakeTenancyDomain(), store);
    }

    [Test]
    public void An_anonymous_caller_is_told_to_sign_in_and_never_that_they_are_denied()
    {
        using var workspace = Workspace(ExplorerPluginAccess.AuthenticationRequired);

        Assert.Multiple(() =>
        {
            Assert.That(workspace.AuthenticationRequired, Is.True);
            Assert.That(workspace.Allowed, Is.False);
            Assert.That(
                workspace.AccessMessage?.Kind,
                Is.EqualTo(ExplorerStateKind.SignInRequired));
            Assert.That(
                workspace.ListMessage?.Kind,
                Is.EqualTo(ExplorerStateKind.SignInRequired));

            // The measured defect: this area told an anonymous caller the surface
            // was not available for their account.
            Assert.That(
                workspace.AccessMessage?.Kind,
                Is.Not.EqualTo(ExplorerStateKind.NotPermitted));
        });
    }

    [Test]
    public void A_denial_renders_the_remedy_the_gate_declared()
    {
        using var workspace = Workspace(ExplorerPluginAccess.Deny(
            "not an administrator",
            ExplorerAccessRemedy.Requiring("Admin", "a platform administrator")));

        Assert.Multiple(() =>
        {
            Assert.That(workspace.AccessRemedy.Permission, Is.EqualTo("Admin"));
            Assert.That(workspace.AccessRemedy.Audience, Is.EqualTo("a platform administrator"));
            Assert.That(
                workspace.AccessRemedyText,
                Is.EqualTo("Requires the Admin permission - ask a platform administrator."));
        });
    }

    [Test]
    public void A_denial_whose_gate_declared_no_remedy_still_states_one()
    {
        using var workspace = Workspace(ExplorerPluginAccess.Deny("not an administrator"));

        Assert.Multiple(() =>
        {
            Assert.That(workspace.AccessRemedy.IsSpecified, Is.False);
            Assert.That(workspace.AccessRemedyText, Is.Not.Null.And.Not.Empty);
            Assert.That(workspace.AccessRemedyText, Is.EqualTo(workspace.AccessMessage?.Remedy));
        });
    }

    [Test]
    public void An_allowed_caller_composes_no_refusal_at_all()
    {
        using var workspace = Workspace(ExplorerPluginAccess.Allowed);

        Assert.Multiple(() =>
        {
            Assert.That(workspace.AccessMessage, Is.Null);
            Assert.That(workspace.AccessRemedyText, Is.Null);
            Assert.That(workspace.AccessStatusClass, Is.EqualTo("is-denied"));
        });
    }

    [Test]
    public void An_absent_capability_is_a_statement_about_the_cluster()
    {
        using var workspace = Workspace(ExplorerPluginAccess.ReportUnavailable("no tenancy here"));

        Assert.Multiple(() =>
        {
            Assert.That(workspace.Unavailable, Is.True);
            Assert.That(workspace.AccessMessage?.Kind, Is.EqualTo(ExplorerStateKind.Unavailable));
            Assert.That(workspace.AccessStatusClass, Is.EqualTo("is-unavailable"));
            Assert.That(workspace.AccessMessage!.IsDenial, Is.False);
        });
    }

    [Test]
    public void The_refusal_names_the_area_the_settled_way()
    {
        using var workspace = Workspace(ExplorerPluginAccess.Denied);

        Assert.That(
            workspace.AccessMessage?.Headline,
            Does.Contain(ExplorerVocabulary.TenantAdministrationArea));
    }
}
