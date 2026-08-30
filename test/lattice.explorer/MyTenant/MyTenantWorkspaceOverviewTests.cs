using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.Plugins.MyTenant;
using Orleans.Lattice.Explorer.Plugins.MyTenant.Workspace;
using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Plugins.Tenancy;

namespace Orleans.Lattice.Explorer.Tests.MyTenant;

/// <summary>
/// The workspace's lifecycle and Overview surface: the gate posture it starts
/// from, the one tenant every other surface is scoped to, and the tenant switch
/// and visibility request - both of which report a refusal rather than degrading
/// in silence.
/// </summary>
[TestFixture]
public sealed class MyTenantWorkspaceOverviewTests
{
    [Test]
    public void An_unprobed_gate_fails_closed_rather_than_being_optimistic()
    {
        var domain = new FakeTenancyDomain();
        var workspace = new MyTenantWorkspace(domain, new ExplorerPluginAccessStore());

        Assert.Multiple(() =>
        {
            Assert.That(workspace.Allowed, Is.False);
            Assert.That(workspace.AuthenticationRequired, Is.False);
            Assert.That(workspace.Unavailable, Is.False);
        });
    }

    [Test]
    public async Task A_denied_gate_loads_nothing_at_all()
    {
        var harness = await MyTenantWorkspaceHarness.CreateAsync(
            access: ExplorerPluginAccess.Deny("not a tenant admin"));

        Assert.Multiple(() =>
        {
            Assert.That(harness.Workspace.Allowed, Is.False);
            Assert.That(harness.Workspace.AccessReason, Is.EqualTo("not a tenant admin"));
            Assert.That(harness.Workspace.TenantId, Is.Null);
            Assert.That(harness.Service.TenantIdsTouched, Is.Empty);
        });
    }

    [Test]
    public async Task An_unavailable_gate_is_reported_so_the_panel_renders_nothing()
    {
        var harness = await MyTenantWorkspaceHarness.CreateAsync(
            access: ExplorerPluginAccess.ReportUnavailable("no tenancy add-on"));

        Assert.Multiple(() =>
        {
            Assert.That(harness.Workspace.Unavailable, Is.True);
            Assert.That(harness.Workspace.Allowed, Is.False);
        });
    }

    [Test]
    public async Task An_unauthenticated_gate_is_distinguished_from_a_denial()
    {
        var harness = await MyTenantWorkspaceHarness.CreateAsync(
            access: ExplorerPluginAccess.RequireAuthentication("sign in"));

        Assert.Multiple(() =>
        {
            Assert.That(harness.Workspace.AuthenticationRequired, Is.True);
            Assert.That(harness.Workspace.Unavailable, Is.False);
        });
    }

    [Test]
    public async Task The_surface_scopes_itself_to_the_active_tenant()
    {
        var harness = await MyTenantWorkspaceHarness.CreateAsync();

        Assert.Multiple(() =>
        {
            Assert.That(harness.Workspace.TenantId, Is.EqualTo(MyTenantSample.TenantId));
            Assert.That(harness.Workspace.ActiveSurfaceId, Is.EqualTo(MyTenantSurfaces.Overview));
            Assert.That(harness.Workspace.Tenant, Is.Not.Null);
        });
    }

    [Test]
    public async Task An_operator_who_switched_tenant_sees_the_tenant_they_switched_to()
    {
        // The identity seam is the authority, not the caller's own credential:
        // GetCurrentTenantAsync answers for the credential and would pin the
        // surface to the operator's own tenant.
        var harness = await MyTenantWorkspaceHarness.CreateAsync(domain =>
        {
            domain.ActiveTenant = new ExplorerTenantId(MyTenantSample.OtherTenantId);
            domain.Service.CurrentTenant = TenantOperationResult<ExplorerTenantSummary>.Success(
                MyTenantSample.Summary(MyTenantSample.TenantId),
                "ok");
        });

        Assert.Multiple(() =>
        {
            Assert.That(harness.Workspace.TenantId, Is.EqualTo(MyTenantSample.OtherTenantId));
            Assert.That(
                harness.Workspace.CredentialTenant?.TenantId,
                Is.EqualTo(MyTenantSample.TenantId),
                "the credential's own tenant is still reported, so the difference is visible");
            Assert.That(
                harness.Service.TenantIdsTouched,
                Has.All.EqualTo(MyTenantSample.OtherTenantId),
                "every read is scoped to the switched-to tenant");
        });
    }

    [Test]
    public async Task The_credentials_tenant_is_the_fallback_when_no_active_tenant_is_established()
    {
        var harness = await MyTenantWorkspaceHarness.CreateAsync(domain => domain.ActiveTenant = null);

        Assert.That(harness.Workspace.TenantId, Is.EqualTo(MyTenantSample.TenantId));
    }

    [Test]
    public async Task A_refused_current_tenant_read_becomes_a_notice()
    {
        var harness = await MyTenantWorkspaceHarness.CreateAsync(domain =>
        {
            domain.ActiveTenant = null;
            domain.Service.CurrentTenant = TenantOperationResult<ExplorerTenantSummary>.Failure(
                TenantOperationStatus.Denied,
                "refused");
        });

        Assert.Multiple(() =>
        {
            Assert.That(harness.Workspace.CredentialTenant, Is.Null);
            Assert.That(harness.Workspace.TenantId, Is.Null);
            Assert.That(harness.Workspace.LastNotice?.Message, Is.EqualTo("refused"));
        });
    }

    [Test]
    public async Task A_refused_detail_read_becomes_a_notice_and_leaves_no_stale_tenant()
    {
        var harness = await MyTenantWorkspaceHarness.CreateAsync(domain =>
            domain.Service.Detail = TenantOperationResult<ExplorerTenantDetail>.Failure(
                TenantOperationStatus.NotFound,
                "no such tenant"));

        Assert.Multiple(() =>
        {
            Assert.That(harness.Workspace.Tenant, Is.Null);
            Assert.That(harness.Workspace.LastNotice?.Status, Is.EqualTo(TenantOperationStatus.NotFound));
            Assert.That(harness.Workspace.LastNotice?.Message, Is.EqualTo("no such tenant"));
        });
    }

    [Test]
    public async Task A_suspended_tenant_is_reported_as_suspended()
    {
        var harness = await MyTenantWorkspaceHarness.CreateAsync(domain =>
            domain.Service.Detail = TenantOperationResult<ExplorerTenantDetail>.Success(
                MyTenantSample.Detail(status: ExplorerTenantLifecycle.Suspended),
                "ok"));

        Assert.That(harness.Workspace.IsSuspended, Is.True);
    }

    [Test]
    public async Task The_switcher_stays_hidden_for_a_caller_who_administers_one_tenant()
    {
        var harness = await MyTenantWorkspaceHarness.CreateAsync();

        Assert.Multiple(() =>
        {
            Assert.That(harness.Workspace.AccessibleTenants, Has.Count.EqualTo(1));
            Assert.That(harness.Workspace.CanSwitchTenant, Is.False);
        });
    }

    [Test]
    public async Task The_switcher_appears_for_a_caller_who_administers_several_tenants()
    {
        var harness = await MyTenantWorkspaceHarness.CreateAsync(domain =>
            domain.Service.AccessibleTenants =
                TenantOperationResult<IReadOnlyList<ExplorerTenantSummary>>.Success(
                    [MyTenantSample.Summary(), MyTenantSample.Summary(MyTenantSample.OtherTenantId)],
                    "ok"));

        Assert.That(harness.Workspace.CanSwitchTenant, Is.True);
    }

    [Test]
    public async Task A_refused_accessible_tenant_read_leaves_an_empty_list_rather_than_throwing()
    {
        var harness = await MyTenantWorkspaceHarness.CreateAsync(domain =>
            domain.Service.AccessibleTenants =
                TenantOperationResult<IReadOnlyList<ExplorerTenantSummary>>.Failure(
                    TenantOperationStatus.Denied,
                    "no"));

        Assert.Multiple(() =>
        {
            Assert.That(harness.Workspace.AccessibleTenants, Is.Empty);
            Assert.That(harness.Workspace.CanSwitchTenant, Is.False);
        });
    }

    [Test]
    public async Task Switching_to_an_accessible_tenant_rescopes_the_whole_surface()
    {
        var harness = await MyTenantWorkspaceHarness.CreateAsync(domain =>
            domain.Service.AccessibleTenants =
                TenantOperationResult<IReadOnlyList<ExplorerTenantSummary>>.Success(
                    [MyTenantSample.Summary(), MyTenantSample.Summary(MyTenantSample.OtherTenantId)],
                    "ok"));

        var switched = await harness.Workspace.SwitchTenantAsync(MyTenantSample.OtherTenantId);

        Assert.Multiple(() =>
        {
            Assert.That(switched, Is.True);
            Assert.That(harness.Workspace.TenantId, Is.EqualTo(MyTenantSample.OtherTenantId));
            Assert.That(harness.Domain.SwitchedTo, Is.EqualTo(new[] { MyTenantSample.OtherTenantId }));
        });
    }

    [Test]
    public async Task A_refused_switch_says_so_rather_than_appearing_to_work()
    {
        var harness = await MyTenantWorkspaceHarness.CreateAsync(domain =>
        {
            domain.AllowSwitch = false;
            domain.Service.AccessibleTenants =
                TenantOperationResult<IReadOnlyList<ExplorerTenantSummary>>.Success(
                    [MyTenantSample.Summary(), MyTenantSample.Summary(MyTenantSample.OtherTenantId)],
                    "ok");
        });

        var switched = await harness.Workspace.SwitchTenantAsync(MyTenantSample.OtherTenantId);

        Assert.Multiple(() =>
        {
            Assert.That(switched, Is.False);
            Assert.That(harness.Workspace.TenantId, Is.EqualTo(MyTenantSample.TenantId));
            Assert.That(
                harness.Workspace.LastNotice?.Message,
                Is.EqualTo(MyTenantWorkspace.SwitchDeniedMessage));
        });
    }

    [Test]
    public async Task A_switch_to_a_tenant_the_caller_was_never_offered_never_reaches_the_switcher()
    {
        var harness = await MyTenantWorkspaceHarness.CreateAsync();

        var switched = await harness.Workspace.SwitchTenantAsync(MyTenantSample.ThirdTenantId);

        Assert.Multiple(() =>
        {
            Assert.That(switched, Is.False);
            Assert.That(
                harness.Domain.SwitchedTo,
                Is.Empty,
                "a switch cannot be used to probe for tenants the caller was not told about");
        });
    }

    [Test]
    public async Task A_refused_visibility_request_reports_the_fail_closed_degradation()
    {
        var harness = await MyTenantWorkspaceHarness.CreateAsync();

        var applied = await harness.Workspace.RequestVisibilityAsync(ExplorerTenantVisibility.AllTenants);

        Assert.Multiple(() =>
        {
            Assert.That(applied, Is.False);
            Assert.That(
                harness.Workspace.RequestedVisibility,
                Is.EqualTo(ExplorerTenantVisibility.ActiveTenant),
                "an unvalidated cross-tenant request degrades to the active tenant");
            Assert.That(
                harness.Workspace.LastNotice?.Message,
                Is.EqualTo(MyTenantWorkspace.VisibilityDegradedMessage));
        });
    }

    [Test]
    public async Task An_operators_visibility_request_is_applied()
    {
        var harness = await MyTenantWorkspaceHarness.CreateAsync(domain =>
        {
            domain.IsOperator = true;
            domain.AllowVisibilityChange = true;
        });

        var applied = await harness.Workspace.RequestVisibilityAsync(ExplorerTenantVisibility.AllTenants);

        Assert.Multiple(() =>
        {
            Assert.That(applied, Is.True);
            Assert.That(
                harness.Workspace.RequestedVisibility,
                Is.EqualTo(ExplorerTenantVisibility.AllTenants));
        });
    }

    [Test]
    public async Task The_operator_gate_diagnostic_is_surfaced_when_the_head_is_misordered()
    {
        var harness = await MyTenantWorkspaceHarness.CreateAsync(
            operatorGateDiagnostic: MyTenantOperatorGateDiagnostic.PlaceholderGateMessage);

        Assert.That(
            harness.Workspace.OperatorGateDiagnostic,
            Is.EqualTo(MyTenantOperatorGateDiagnostic.PlaceholderGateMessage));
    }

    [Test]
    public async Task No_diagnostic_is_surfaced_on_a_correctly_ordered_head()
    {
        var harness = await MyTenantWorkspaceHarness.CreateAsync();

        Assert.That(harness.Workspace.OperatorGateDiagnostic, Is.Null);
    }

    [Test]
    public async Task Selecting_an_unknown_surface_leaves_the_current_one_active()
    {
        var harness = await MyTenantWorkspaceHarness.CreateAsync();

        await harness.OpenAsync("retired-surface");

        Assert.That(harness.Workspace.ActiveSurfaceId, Is.EqualTo(MyTenantSurfaces.Overview));
    }

    [Test]
    public async Task Selecting_a_surface_clears_the_previous_surfaces_notice()
    {
        var harness = await MyTenantWorkspaceHarness.CreateAsync();
        await harness.Workspace.RequestVisibilityAsync(ExplorerTenantVisibility.AllTenants);

        await harness.OpenAsync(MyTenantSurfaces.Members);

        Assert.Multiple(() =>
        {
            Assert.That(harness.Workspace.ActiveSurfaceId, Is.EqualTo(MyTenantSurfaces.Members));
            Assert.That(harness.Workspace.LastNotice, Is.Null);
        });
    }

    [Test]
    public async Task The_change_event_fires_so_the_panel_re_renders()
    {
        var harness = MyTenantWorkspaceHarness.Create();
        var changes = 0;
        harness.Workspace.Changed += () => changes++;

        await harness.Workspace.InitializeAsync();

        Assert.That(changes, Is.GreaterThan(0));
    }

    [Test]
    public async Task Disposing_unsubscribes_from_the_access_store()
    {
        var harness = await MyTenantWorkspaceHarness.CreateAsync();
        var changes = 0;
        harness.Workspace.Changed += () => changes++;

        harness.Workspace.Dispose();
        harness.Store.Set(MyTenantPluginKeys.PluginId, ExplorerPluginAccess.Deny("later"));

        Assert.Multiple(() =>
        {
            Assert.That(changes, Is.Zero);
            Assert.That(harness.Workspace.Allowed, Is.True, "the disposed workspace stopped tracking");
        });
    }

    [Test]
    public void Null_constructor_arguments_are_rejected()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                () => new MyTenantWorkspace(null!, new ExplorerPluginAccessStore()),
                Throws.InstanceOf<ArgumentNullException>());
            Assert.That(
                () => new MyTenantWorkspace(new FakeTenancyDomain(), null!),
                Throws.InstanceOf<ArgumentNullException>());
        });
    }
}
