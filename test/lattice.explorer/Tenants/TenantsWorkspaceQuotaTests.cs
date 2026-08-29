using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Tenancy;
using Orleans.Lattice.Explorer.Tenants;
using Orleans.Lattice.Explorer.Tenants.Workspace;

namespace Orleans.Lattice.Explorer.Tests.Tenants;

/// <summary>
/// The quota surface: reading usage against ceilings without flattening either
/// absence, captioning the enforcement scope, and authoring ceilings where blank
/// means unbounded.
/// </summary>
[TestFixture]
public sealed class TenantsWorkspaceQuotaTests
{
    private static async Task<(TenantsWorkspace Workspace, FakeTenancyDomain Domain)> OnQuotasAsync()
    {
        var (workspace, domain) = SampleTenants.Seeded();
        await workspace.InitializeAsync();
        await workspace.SelectTenantAsync(SampleTenants.Acme);
        await workspace.SelectSurfaceAsync(TenantsSurfaces.Quotas);
        return (workspace, domain);
    }

    [Test]
    public async Task The_surface_projects_one_row_per_dimension()
    {
        var (workspace, _) = await OnQuotasAsync();
        using var _guard = workspace;

        Assert.That(
            workspace.QuotaRows.Select(row => row.Kind),
            Is.EqualTo(ExplorerTenantQuotaUsage.Dimensions));
    }

    [Test]
    public async Task Each_dimension_keeps_its_own_reading_state()
    {
        var (workspace, _) = await OnQuotasAsync();
        using var _guard = workspace;

        Assert.Multiple(() =>
        {
            Assert.That(workspace.QuotaRows[0].State, Is.EqualTo(TenantQuotaReadingState.Bounded));
            Assert.That(workspace.QuotaRows[3].State, Is.EqualTo(TenantQuotaReadingState.Unlimited));
            Assert.That(workspace.QuotaRows[4].State, Is.EqualTo(TenantQuotaReadingState.NotMeasured));
        });
    }

    [Test]
    public async Task An_unlimited_dimension_never_draws_a_bar()
    {
        var (workspace, _) = await OnQuotasAsync();
        using var _guard = workspace;

        var unlimited = workspace.QuotaRows[3];

        Assert.Multiple(() =>
        {
            Assert.That(unlimited.LimitText, Is.EqualTo(TenantQuotaFormat.UnlimitedText));
            Assert.That(unlimited.ShowsBar, Is.False);
        });
    }

    [Test]
    public async Task An_unmeasured_dimension_never_draws_a_bar()
    {
        var (workspace, _) = await OnQuotasAsync();
        using var _guard = workspace;

        var unmeasured = workspace.QuotaRows[4];

        Assert.Multiple(() =>
        {
            Assert.That(unmeasured.UsageText, Is.EqualTo(TenantQuotaFormat.NotMeasuredText));
            Assert.That(unmeasured.ShowsBar, Is.False);
        });
    }

    [Test]
    public async Task The_row_buffer_is_reused_across_refreshes()
    {
        var (workspace, _) = await OnQuotasAsync();
        using var _guard = workspace;
        var first = workspace.QuotaRows;

        await workspace.RefreshQuotasAsync();

        Assert.That(workspace.QuotaRows, Is.SameAs(first));
    }

    [Test]
    public async Task A_global_reading_is_captioned_as_a_cross_cluster_total()
    {
        var (workspace, _) = await OnQuotasAsync();
        using var _guard = workspace;

        Assert.That(workspace.QuotaScopeCaption, Is.EqualTo(TenantQuotaFormat.GlobalScopeCaption));
    }

    [Test]
    public async Task A_per_cluster_reading_is_captioned_as_this_clusters_view_only()
    {
        var (workspace, domain) = SampleTenants.Seeded();
        using var _guard = workspace;
        domain.Service.Usage[SampleTenants.Acme] =
            SampleTenants.Usage(scope: ExplorerTenantQuotaEnforcement.PerCluster);

        await workspace.InitializeAsync();
        await workspace.SelectTenantAsync(SampleTenants.Acme);
        await workspace.SelectSurfaceAsync(TenantsSurfaces.Quotas);

        Assert.Multiple(() =>
        {
            Assert.That(workspace.QuotaScopeCaption, Is.EqualTo(TenantQuotaFormat.PerClusterScopeCaption));

            // A per-cluster figure presented as a global total is a lie an
            // operator would act on.
            Assert.That(workspace.QuotaScopeCaption, Does.Contain("this cluster's local view"));
        });
    }

    [Test]
    public async Task A_reading_with_no_usage_at_all_is_captioned_rather_than_zeroed()
    {
        var (workspace, domain) = SampleTenants.Seeded();
        using var _guard = workspace;
        domain.Service.Usage[SampleTenants.Acme] = SampleTenants.Usage(hasUsage: false);

        await workspace.InitializeAsync();
        await workspace.SelectTenantAsync(SampleTenants.Acme);
        await workspace.SelectSurfaceAsync(TenantsSurfaces.Quotas);

        Assert.Multiple(() =>
        {
            Assert.That(workspace.QuotaHasUsage, Is.False);
            Assert.That(workspace.QuotaNoUsageCaption, Is.EqualTo(TenantQuotaFormat.NoUsageCaption));
            Assert.That(workspace.QuotaNoUsageCaption, Does.Contain("absent rather than zero"));
        });
    }

    [Test]
    public async Task A_warm_reading_carries_no_no_usage_caption()
    {
        var (workspace, _) = await OnQuotasAsync();
        using var _guard = workspace;

        Assert.Multiple(() =>
        {
            Assert.That(workspace.QuotaHasUsage, Is.True);
            Assert.That(workspace.QuotaNoUsageCaption, Is.Empty);
        });
    }

    [Test]
    public async Task The_editor_loads_the_authored_ceilings_with_unbounded_blank()
    {
        var (workspace, _) = await OnQuotasAsync();
        using var _guard = workspace;

        Assert.Multiple(() =>
        {
            Assert.That(workspace.QuotaDraft.MaxBytes, Is.EqualTo("1000"));
            Assert.That(workspace.QuotaDraft.MaxMemoryBytes, Is.EqualTo("0"));
            Assert.That(workspace.QuotaDraft.MaxTreeCount, Is.Empty);
        });
    }

    [Test]
    public async Task Saving_a_blank_ceiling_authors_unbounded_rather_than_zero()
    {
        var (workspace, domain) = await OnQuotasAsync();
        using var _guard = workspace;
        workspace.QuotaDraft.MaxBytes = string.Empty;

        await workspace.SaveQuotasAsync();

        Assert.Multiple(() =>
        {
            Assert.That(domain.Service.LastQuotaLimits!.Value.MaxBytes, Is.Null);
            Assert.That(workspace.LastStatus, Is.EqualTo(TenantOperationStatus.Succeeded));
        });
    }

    [Test]
    public async Task Saving_a_zero_ceiling_authors_a_real_cap_of_nothing()
    {
        var (workspace, domain) = await OnQuotasAsync();
        using var _guard = workspace;
        workspace.QuotaDraft.MaxKeys = "0";

        await workspace.SaveQuotasAsync();

        Assert.That(domain.Service.LastQuotaLimits!.Value.MaxKeys, Is.EqualTo(0L));
    }

    [Test]
    public async Task Saving_an_all_blank_editor_reports_the_tenant_unbounded()
    {
        var (workspace, _) = await OnQuotasAsync();
        using var _guard = workspace;
        workspace.QuotaDraft.MaxBytes = string.Empty;
        workspace.QuotaDraft.MaxKeys = string.Empty;
        workspace.QuotaDraft.MaxMemoryBytes = string.Empty;
        workspace.QuotaDraft.MaxTreeCount = string.Empty;
        workspace.QuotaDraft.MaxOpsPerSecond = string.Empty;

        await workspace.SaveQuotasAsync();

        Assert.That(workspace.LastMessage, Does.Contain("unbounded on every dimension"));
    }

    [Test]
    public async Task An_invalid_ceiling_is_refused_before_the_call()
    {
        var (workspace, domain) = await OnQuotasAsync();
        using var _guard = workspace;
        workspace.QuotaDraft.MaxBytes = "-1";

        await workspace.SaveQuotasAsync();

        Assert.Multiple(() =>
        {
            Assert.That(workspace.LastStatus, Is.EqualTo(TenantOperationStatus.InvalidRequest));
            Assert.That(workspace.LastMessage, Is.EqualTo(TenantQuotaDraft.InvalidLimitMessage));
            Assert.That(domain.Service.LastQuotaLimits, Is.Null);
        });
    }

    [Test]
    public async Task Saving_with_no_tenant_selected_is_refused_before_the_call()
    {
        var (workspace, domain, _) = SampleTenants.Workspace();
        using var _guard = workspace;
        await workspace.InitializeAsync();

        await workspace.SaveQuotasAsync();

        Assert.Multiple(() =>
        {
            Assert.That(workspace.LastMessage, Is.EqualTo(TenantsWorkspace.QuotasNeedTenantMessage));
            Assert.That(domain.Service.LastQuotaLimits, Is.Null);
        });
    }

    [Test]
    public async Task A_refused_save_is_reported_with_its_own_meaning()
    {
        var (workspace, domain) = await OnQuotasAsync();
        using var _guard = workspace;
        domain.Service.Fail(FakeTenantAdminService.Op.SetQuotas, TenantOperationStatus.Denied);

        await workspace.SaveQuotasAsync();

        Assert.Multiple(() =>
        {
            Assert.That(workspace.LastStatus, Is.EqualTo(TenantOperationStatus.Denied));
            Assert.That(workspace.LastMessage, Does.Contain("refused this operation"));
        });
    }

    [Test]
    public async Task A_wire_collapsed_refusal_renders_the_servers_own_reason()
    {
        var (workspace, domain) = await OnQuotasAsync();
        using var _guard = workspace;
        domain.Service.Fail(
            FakeTenantAdminService.Op.SetQuotas,
            TenantOperationStatus.PreconditionFailed,
            "the default tenant cannot be capped");

        await workspace.SaveQuotasAsync();

        Assert.That(workspace.LastMessage, Does.Contain("the default tenant cannot be capped"));
    }

    [Test]
    public async Task A_saved_ceiling_shows_on_the_authoritative_detail_immediately()
    {
        var (workspace, _) = await OnQuotasAsync();
        using var _guard = workspace;
        workspace.QuotaDraft.MaxBytes = "4096";

        await workspace.SaveQuotasAsync();

        Assert.That(workspace.SelectedDetail!.Quotas.MaxBytes, Is.EqualTo(4096L));
    }

    [Test]
    public async Task A_reading_that_lags_a_just_saved_ceiling_is_captioned_rather_than_hidden()
    {
        var (workspace, _) = await OnQuotasAsync();
        using var _guard = workspace;
        workspace.QuotaDraft.MaxBytes = "4096";

        await workspace.SaveQuotasAsync();

        Assert.Multiple(() =>
        {
            // The reading is deliberately coherent within itself rather than
            // pairing a new ceiling with old usage, so it lags by one cycle.
            Assert.That(workspace.QuotaReadingIsBehind, Is.True);
            Assert.That(TenantsWorkspace.QuotaReadingBehindCaption, Does.Contain("one coherent snapshot"));
        });
    }

    [Test]
    public async Task A_reading_that_matches_the_authored_ceilings_is_not_captioned_as_behind()
    {
        var (workspace, _) = await OnQuotasAsync();
        using var _guard = workspace;

        Assert.That(workspace.QuotaReadingIsBehind, Is.False);
    }

    [Test]
    public async Task A_refused_reading_leaves_no_rows_and_reports_the_refusal()
    {
        var (workspace, domain) = SampleTenants.Seeded();
        using var _guard = workspace;
        await workspace.InitializeAsync();
        await workspace.SelectTenantAsync(SampleTenants.Acme);
        domain.Service.Fail(FakeTenantAdminService.Op.Usage, TenantOperationStatus.Denied);

        await workspace.SelectSurfaceAsync(TenantsSurfaces.Quotas);

        Assert.Multiple(() =>
        {
            Assert.That(workspace.QuotaUsage, Is.Null);
            Assert.That(workspace.QuotaRows, Is.Empty);
            Assert.That(workspace.LastStatus, Is.EqualTo(TenantOperationStatus.Denied));
        });
    }

    [Test]
    public async Task A_denied_caller_cannot_save_quotas()
    {
        var (workspace, domain, store) = SampleTenants.Workspace();
        using var _guard = workspace;
        var service = domain.Service;
        service.Tenants.Add(SampleTenants.Summary());
        service.Details[SampleTenants.Acme] = SampleTenants.Detail();
        service.Usage[SampleTenants.Acme] = SampleTenants.Usage();

        await workspace.InitializeAsync();
        await workspace.SelectTenantAsync(SampleTenants.Acme);
        await workspace.SelectSurfaceAsync(TenantsSurfaces.Quotas);

        store.Set(TenantsPluginKeys.PluginId, ExplorerPluginAccess.Denied);
        workspace.QuotaDraft.MaxBytes = "1";
        await workspace.SaveQuotasAsync();

        Assert.That(service.LastQuotaLimits, Is.Null);
    }

    [Test]
    public async Task Refreshing_quotas_re_reads_the_tenants_usage()
    {
        var (workspace, domain) = await OnQuotasAsync();
        using var _guard = workspace;
        var before = domain.Service.Calls
            .Count(call => call.StartsWith(FakeTenantAdminService.Op.Usage, StringComparison.Ordinal));

        await workspace.RefreshQuotasAsync();

        Assert.That(
            domain.Service.Calls.Count(call => call.StartsWith(FakeTenantAdminService.Op.Usage, StringComparison.Ordinal)),
            Is.GreaterThan(before));
    }

    [Test]
    public async Task Selecting_a_new_tenant_drops_the_previous_readings()
    {
        var (workspace, domain) = SampleTenants.Seeded();
        using var _guard = workspace;
        domain.Service.Tenants.Add(SampleTenants.Summary(SampleTenants.Globex));
        domain.Service.Details[SampleTenants.Globex] = SampleTenants.Detail(SampleTenants.Globex);

        await workspace.InitializeAsync();
        await workspace.SelectTenantAsync(SampleTenants.Acme);
        await workspace.SelectSurfaceAsync(TenantsSurfaces.Quotas);
        Assert.That(workspace.QuotaRows, Is.Not.Empty);

        // Globex has no reading seeded, so a leaked projection would still show
        // acme's rows here.
        await workspace.SelectTenantAsync(SampleTenants.Globex);

        Assert.That(workspace.QuotaRows, Is.Empty);
    }
}
