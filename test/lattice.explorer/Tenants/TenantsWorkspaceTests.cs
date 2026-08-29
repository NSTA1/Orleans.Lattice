using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Tenancy;
using Orleans.Lattice.Explorer.Tenants;
using Orleans.Lattice.Explorer.Tenants.Workspace;

namespace Orleans.Lattice.Explorer.Tests.Tenants;

/// <summary>
/// The Tenants workspace's core: the fail-closed gate reading, the tenant list,
/// its paging, and the selection that drives the tenant-scoped sub-surfaces.
/// </summary>
[TestFixture]
public sealed class TenantsWorkspaceTests
{
    [Test]
    public void Constructor_null_domain_throws()
    {
        Assert.That(
            () => new TenantsWorkspace(null!, new ExplorerPluginAccessStore()),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_null_store_throws()
    {
        Assert.That(
            () => new TenantsWorkspace(new FakeTenancyDomain(), null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void An_unprobed_gate_reads_as_denied_so_the_surface_is_never_optimistic()
    {
        var domain = new FakeTenancyDomain();
        var store = new ExplorerPluginAccessStore();

        using var workspace = new TenantsWorkspace(domain, store);

        Assert.Multiple(() =>
        {
            Assert.That(workspace.Allowed, Is.False);
            Assert.That(workspace.AuthenticationRequired, Is.False);
            Assert.That(workspace.Unavailable, Is.False);
        });
    }

    [Test]
    public void A_denied_gate_reads_its_reason()
    {
        var (workspace, _, _) = SampleTenants.Workspace(ExplorerPluginAccess.Deny("not an operator"));
        using var _guard = workspace;

        Assert.Multiple(() =>
        {
            Assert.That(workspace.Allowed, Is.False);
            Assert.That(workspace.AccessReason, Is.EqualTo("not an operator"));
        });
    }

    [Test]
    public void An_unauthenticated_gate_is_distinguished_from_a_refusal()
    {
        var (workspace, _, _) = SampleTenants.Workspace(ExplorerPluginAccess.AuthenticationRequired);
        using var _guard = workspace;

        Assert.Multiple(() =>
        {
            Assert.That(workspace.AuthenticationRequired, Is.True);
            Assert.That(workspace.Unavailable, Is.False);
        });
    }

    [Test]
    public void A_tenancy_absent_cluster_is_distinguished_from_a_refusal()
    {
        var (workspace, _, _) = SampleTenants.Workspace(ExplorerPluginAccess.Unavailable);
        using var _guard = workspace;

        Assert.Multiple(() =>
        {
            Assert.That(workspace.Unavailable, Is.True);
            Assert.That(workspace.Allowed, Is.False);
            Assert.That(workspace.AuthenticationRequired, Is.False);
        });
    }

    [Test]
    public async Task A_denied_caller_loads_nothing_at_all()
    {
        var (workspace, domain, _) = SampleTenants.Workspace(ExplorerPluginAccess.Denied);
        using var _guard = workspace;
        domain.Service.Tenants.Add(SampleTenants.Summary());

        await workspace.InitializeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(domain.Service.Calls, Is.Empty, "a denied caller must not reach the cluster");
            Assert.That(workspace.Page, Is.Empty);
            Assert.That(workspace.TenantCount, Is.Zero);
        });
    }

    [Test]
    public async Task A_tenancy_absent_caller_loads_nothing_at_all()
    {
        var (workspace, domain, _) = SampleTenants.Workspace(ExplorerPluginAccess.Unavailable);
        using var _guard = workspace;
        domain.Service.Tenants.Add(SampleTenants.Summary());

        await workspace.InitializeAsync();

        Assert.That(domain.Service.Calls, Is.Empty);
    }

    [Test]
    public async Task Initialize_lists_the_tenants_and_projects_the_first_page()
    {
        var (workspace, domain, _) = SampleTenants.Workspace();
        using var _guard = workspace;
        domain.Service.Tenants.Add(SampleTenants.Summary(SampleTenants.Acme));
        domain.Service.Tenants.Add(SampleTenants.Summary(SampleTenants.Globex));
        domain.Service.Usage[SampleTenants.Acme] = SampleTenants.Usage(SampleTenants.Acme);
        domain.Service.Usage[SampleTenants.Globex] = SampleTenants.Usage(SampleTenants.Globex);

        await workspace.InitializeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(workspace.TenantCount, Is.EqualTo(2));
            Assert.That(workspace.Page.Select(row => row.TenantId),
                Is.EqualTo(new[] { SampleTenants.Acme, SampleTenants.Globex }));
            Assert.That(workspace.Page[0].StoredText, Is.EqualTo("250 B"));
        });
    }

    [Test]
    public async Task A_refused_usage_reading_leaves_the_row_honest_rather_than_zeroed()
    {
        var (workspace, domain, _) = SampleTenants.Workspace();
        using var _guard = workspace;
        domain.Service.Tenants.Add(SampleTenants.Summary());
        domain.Service.Fail(FakeTenantAdminService.Op.Usage, TenantOperationStatus.Denied);

        await workspace.InitializeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(workspace.Page, Has.Count.EqualTo(1));
            Assert.That(workspace.Page[0].StoredText, Is.EqualTo(TenantRow.UsageUnavailableText));

            // The tenant itself is still listed: one refused reading must not
            // hide a tenant from the operator.
            Assert.That(workspace.Page[0].TenantId, Is.EqualTo(SampleTenants.Acme));
        });
    }

    [Test]
    public async Task A_refused_listing_is_reported_and_clears_the_page()
    {
        var (workspace, domain, _) = SampleTenants.Workspace();
        using var _guard = workspace;
        domain.Service.Tenants.Add(SampleTenants.Summary());
        domain.Service.Fail(FakeTenantAdminService.Op.List, TenantOperationStatus.Denied, "no");

        await workspace.InitializeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(workspace.LastStatus, Is.EqualTo(TenantOperationStatus.Denied));
            Assert.That(workspace.LastMessage, Does.Contain("refused this operation"));
            Assert.That(workspace.LastResultClass, Is.EqualTo("is-denied"));
            Assert.That(workspace.Page, Is.Empty);
        });
    }

    [Test]
    public async Task The_page_buffer_is_reused_across_reloads()
    {
        var (workspace, domain, _) = SampleTenants.Workspace();
        using var _guard = workspace;
        domain.Service.Tenants.Add(SampleTenants.Summary());
        domain.Service.Usage[SampleTenants.Acme] = SampleTenants.Usage();

        await workspace.InitializeAsync();
        var first = workspace.Page;
        await workspace.ReloadAsync();

        // The list re-renders on every gate change and every busy transition, so
        // it must not allocate a fresh collection per data change either.
        Assert.That(workspace.Page, Is.SameAs(first));
    }

    [Test]
    public async Task Paging_walks_the_list_a_page_at_a_time()
    {
        var (workspace, domain, _) = SampleTenants.Workspace();
        using var _guard = workspace;
        for (var i = 0; i < TenantsWorkspace.PageSize + 3; i++)
        {
            var id = "tenant-" + i.ToString("D3", System.Globalization.CultureInfo.InvariantCulture);
            domain.Service.Tenants.Add(SampleTenants.Summary(id));
            domain.Service.Usage[id] = SampleTenants.Usage(id);
        }

        await workspace.InitializeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(workspace.PageCount, Is.EqualTo(2));
            Assert.That(workspace.Page, Has.Count.EqualTo(TenantsWorkspace.PageSize));
            Assert.That(workspace.HasPreviousPage, Is.False);
            Assert.That(workspace.HasNextPage, Is.True);
        });

        await workspace.NextPageAsync();

        Assert.Multiple(() =>
        {
            Assert.That(workspace.PageIndex, Is.EqualTo(1));
            Assert.That(workspace.Page, Has.Count.EqualTo(3));
            Assert.That(workspace.HasNextPage, Is.False);
            Assert.That(workspace.HasPreviousPage, Is.True);
        });

        await workspace.PreviousPageAsync();

        Assert.That(workspace.PageIndex, Is.Zero);
    }

    [Test]
    public async Task Paging_reads_usage_only_for_the_page_in_view()
    {
        var (workspace, domain, _) = SampleTenants.Workspace();
        using var _guard = workspace;
        for (var i = 0; i < TenantsWorkspace.PageSize + 3; i++)
        {
            var id = "tenant-" + i.ToString("D3", System.Globalization.CultureInfo.InvariantCulture);
            domain.Service.Tenants.Add(SampleTenants.Summary(id));
            domain.Service.Usage[id] = SampleTenants.Usage(id);
        }

        await workspace.InitializeAsync();

        var readsAfterFirstPage = domain.Service.Calls
            .Count(call => call.StartsWith(FakeTenantAdminService.Op.Usage, StringComparison.Ordinal));

        Assert.That(readsAfterFirstPage, Is.EqualTo(TenantsWorkspace.PageSize));

        await workspace.NextPageAsync();

        var readsAfterSecondPage = domain.Service.Calls
            .Count(call => call.StartsWith(FakeTenantAdminService.Op.Usage, StringComparison.Ordinal));

        Assert.That(readsAfterSecondPage, Is.EqualTo(TenantsWorkspace.PageSize + 3));
    }

    [Test]
    public async Task Paging_back_over_an_already_read_page_reads_nothing_again()
    {
        var (workspace, domain, _) = SampleTenants.Workspace();
        using var _guard = workspace;
        for (var i = 0; i < TenantsWorkspace.PageSize + 3; i++)
        {
            var id = "tenant-" + i.ToString("D3", System.Globalization.CultureInfo.InvariantCulture);
            domain.Service.Tenants.Add(SampleTenants.Summary(id));
            domain.Service.Usage[id] = SampleTenants.Usage(id);
        }

        await workspace.InitializeAsync();
        await workspace.NextPageAsync();
        var before = domain.Service.Calls.Count;
        await workspace.PreviousPageAsync();

        Assert.That(domain.Service.Calls, Has.Count.EqualTo(before));
    }

    [Test]
    public async Task Paging_beyond_either_end_does_nothing()
    {
        var (workspace, domain, _) = SampleTenants.Workspace();
        using var _guard = workspace;
        domain.Service.Tenants.Add(SampleTenants.Summary());
        domain.Service.Usage[SampleTenants.Acme] = SampleTenants.Usage();

        await workspace.InitializeAsync();
        await workspace.PreviousPageAsync();
        await workspace.NextPageAsync();

        Assert.Multiple(() =>
        {
            Assert.That(workspace.PageIndex, Is.Zero);
            Assert.That(workspace.PageCount, Is.EqualTo(1), "an empty or single page still reads as one page");
        });
    }

    [Test]
    public async Task An_empty_list_still_reads_as_one_page()
    {
        var (workspace, _, _) = SampleTenants.Workspace();
        using var _guard = workspace;

        await workspace.InitializeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(workspace.TenantCount, Is.Zero);
            Assert.That(workspace.PageCount, Is.EqualTo(1));
            Assert.That(workspace.Page, Is.Empty);
        });
    }

    [Test]
    public async Task Selecting_a_tenant_reads_its_detail()
    {
        var (workspace, _) = SampleTenants.Seeded();
        using var _guard = workspace;

        await workspace.InitializeAsync();
        await workspace.SelectTenantAsync(SampleTenants.Acme);

        Assert.Multiple(() =>
        {
            Assert.That(workspace.SelectedTenantId, Is.EqualTo(SampleTenants.Acme));
            Assert.That(workspace.SelectedDetail, Is.Not.Null);
            Assert.That(workspace.SelectedDetail!.TenantId, Is.EqualTo(SampleTenants.Acme));
            Assert.That(workspace.SelectedIsDefault, Is.False);
        });
    }

    [Test]
    public async Task Selecting_a_tenant_whose_read_is_refused_reports_it_and_holds_no_detail()
    {
        var (workspace, domain) = SampleTenants.Seeded();
        using var _guard = workspace;
        await workspace.InitializeAsync();
        domain.Service.Fail(FakeTenantAdminService.Op.Get, TenantOperationStatus.NotFound);

        await workspace.SelectTenantAsync(SampleTenants.Acme);

        Assert.Multiple(() =>
        {
            Assert.That(workspace.SelectedDetail, Is.Null);
            Assert.That(workspace.LastStatus, Is.EqualTo(TenantOperationStatus.NotFound));
            Assert.That(workspace.LastMessage, Does.Contain("No such tenant"));
        });
    }

    [Test]
    public async Task Selecting_the_reserved_default_tenant_is_recognised()
    {
        var (workspace, domain, _) = SampleTenants.Workspace();
        using var _guard = workspace;
        domain.Service.Tenants.Add(SampleTenants.Summary(SampleTenants.DefaultTenant, isDefault: true));
        domain.Service.Details[SampleTenants.DefaultTenant] =
            SampleTenants.Detail(SampleTenants.DefaultTenant, isDefault: true);
        domain.Service.Usage[SampleTenants.DefaultTenant] = SampleTenants.Usage(SampleTenants.DefaultTenant);

        await workspace.InitializeAsync();
        await workspace.SelectTenantAsync(SampleTenants.DefaultTenant);

        Assert.That(workspace.SelectedIsDefault, Is.True);
    }

    [Test]
    public async Task A_denied_caller_cannot_select_a_tenant()
    {
        var (workspace, domain, store) = SampleTenants.Workspace();
        using var _guard = workspace;
        domain.Service.Details[SampleTenants.Acme] = SampleTenants.Detail();
        store.Set(TenantsPluginKeys.PluginId, ExplorerPluginAccess.Denied);

        await workspace.SelectTenantAsync(SampleTenants.Acme);

        Assert.Multiple(() =>
        {
            Assert.That(workspace.SelectedTenantId, Is.Null);
            Assert.That(domain.Service.Calls, Is.Empty);
        });
    }

    [Test]
    public async Task The_active_sub_surface_starts_on_the_tenant_list()
    {
        var (workspace, _) = SampleTenants.Seeded();
        using var _guard = workspace;

        await workspace.InitializeAsync();

        Assert.That(workspace.ActiveSurfaceId, Is.EqualTo(TenantsSurfaces.Tenants));
    }

    [Test]
    public void Selecting_a_null_surface_throws()
    {
        var (workspace, _, _) = SampleTenants.Workspace();
        using var _guard = workspace;

        Assert.That(async () => await workspace.SelectSurfaceAsync(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Selecting_a_null_tenant_throws()
    {
        var (workspace, _, _) = SampleTenants.Workspace();
        using var _guard = workspace;

        Assert.That(async () => await workspace.SelectTenantAsync(null!), Throws.ArgumentNullException);
    }

    [Test]
    public async Task Switching_sub_surface_loads_that_surface_for_the_selected_tenant()
    {
        var (workspace, domain) = SampleTenants.Seeded();
        using var _guard = workspace;
        await workspace.InitializeAsync();
        await workspace.SelectTenantAsync(SampleTenants.Acme);

        await workspace.SelectSurfaceAsync(TenantsSurfaces.Regions);

        Assert.Multiple(() =>
        {
            Assert.That(workspace.ActiveSurfaceId, Is.EqualTo(TenantsSurfaces.Regions));
            Assert.That(workspace.Regions, Has.Count.EqualTo(2));
            Assert.That(
                domain.Service.Calls,
                Has.Some.StartsWith(FakeTenantAdminService.Op.RegionStatus));
        });
    }

    [Test]
    public async Task Switching_to_the_same_sub_surface_is_a_no_op()
    {
        var (workspace, domain) = SampleTenants.Seeded();
        using var _guard = workspace;
        await workspace.InitializeAsync();
        var before = domain.Service.Calls.Count;

        await workspace.SelectSurfaceAsync(TenantsSurfaces.Tenants);

        Assert.That(domain.Service.Calls, Has.Count.EqualTo(before));
    }

    [Test]
    public async Task Selecting_a_different_tenant_drops_the_previous_tenants_scoped_state()
    {
        var (workspace, domain) = SampleTenants.Seeded();
        using var _guard = workspace;
        domain.Service.Tenants.Add(SampleTenants.Summary(SampleTenants.Globex));
        domain.Service.Details[SampleTenants.Globex] = SampleTenants.Detail(SampleTenants.Globex);
        domain.Service.Usage[SampleTenants.Globex] = SampleTenants.Usage(SampleTenants.Globex);

        await workspace.InitializeAsync();
        await workspace.SelectSurfaceAsync(TenantsSurfaces.Access);
        await workspace.SelectTenantAsync(SampleTenants.Acme);

        Assert.That(workspace.AdminSubjects, Has.Count.EqualTo(2));

        await workspace.SelectTenantAsync(SampleTenants.Globex);

        // Globex has no admin subjects seeded, so a leaked list would show
        // acme's two here.
        Assert.That(workspace.AdminSubjects, Is.Empty);
    }

    [Test]
    public async Task A_reload_that_loses_the_selected_tenant_clears_the_selection()
    {
        var (workspace, domain) = SampleTenants.Seeded();
        using var _guard = workspace;
        await workspace.InitializeAsync();
        await workspace.SelectTenantAsync(SampleTenants.Acme);

        domain.Service.Tenants.Clear();
        await workspace.ReloadAsync();

        Assert.Multiple(() =>
        {
            Assert.That(workspace.SelectedTenantId, Is.Null);
            Assert.That(workspace.SelectedDetail, Is.Null);
        });
    }

    [Test]
    public async Task A_reload_that_keeps_the_selected_tenant_re_reads_it()
    {
        var (workspace, domain) = SampleTenants.Seeded();
        using var _guard = workspace;
        await workspace.InitializeAsync();
        await workspace.SelectTenantAsync(SampleTenants.Acme);

        domain.Service.Details[SampleTenants.Acme] =
            SampleTenants.Detail(status: ExplorerTenantLifecycle.Suspended);

        await workspace.ReloadAsync();

        Assert.That(workspace.SelectedDetail!.Status, Is.EqualTo(ExplorerTenantLifecycle.Suspended));
    }

    [Test]
    public async Task A_gate_that_opens_after_mount_loads_the_list()
    {
        var (workspace, domain, store) = SampleTenants.Workspace(ExplorerPluginAccess.Denied);
        using var _guard = workspace;
        domain.Service.Tenants.Add(SampleTenants.Summary());
        domain.Service.Usage[SampleTenants.Acme] = SampleTenants.Usage();

        await workspace.InitializeAsync();
        Assert.That(workspace.Page, Is.Empty);

        // The fake completes every call synchronously, so the load the gate
        // change triggers has finished by the time Set returns. Nothing here
        // waits on a duration or on a scheduler.
        store.Set(TenantsPluginKeys.PluginId, ExplorerPluginAccess.Allowed);

        Assert.Multiple(() =>
        {
            Assert.That(workspace.Allowed, Is.True);
            Assert.That(workspace.Page, Has.Count.EqualTo(1));
            Assert.That(workspace.Busy, Is.False);
        });
    }

    [Test]
    public void A_sibling_plugins_decision_does_not_disturb_this_one()
    {
        var (workspace, _, store) = SampleTenants.Workspace();
        using var _guard = workspace;
        var notifications = 0;
        workspace.Changed += () => notifications++;

        store.Set("orleans.lattice.backups", ExplorerPluginAccess.Denied);

        Assert.Multiple(() =>
        {
            Assert.That(notifications, Is.Zero);
            Assert.That(workspace.Allowed, Is.True);
        });
    }

    [Test]
    public void Dispose_unsubscribes_from_the_access_store()
    {
        var (workspace, _, store) = SampleTenants.Workspace();
        var notifications = 0;
        workspace.Changed += () => notifications++;

        workspace.Dispose();
        store.Set(TenantsPluginKeys.PluginId, ExplorerPluginAccess.Denied);

        Assert.Multiple(() =>
        {
            Assert.That(notifications, Is.Zero);

            // The workspace's own view of the gate is frozen at disposal, so a
            // later store change cannot resurrect it.
            Assert.That(workspace.Allowed, Is.True);
        });
    }

    [Test]
    public void Dispose_is_idempotent()
    {
        var (workspace, _, _) = SampleTenants.Workspace();

        workspace.Dispose();

        Assert.That(() => workspace.Dispose(), Throws.Nothing);
    }
}
