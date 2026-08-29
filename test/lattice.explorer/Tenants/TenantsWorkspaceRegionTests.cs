using Orleans.Lattice.Explorer.Tenancy;
using Orleans.Lattice.Explorer.Tenants;
using Orleans.Lattice.Explorer.Tenants.Workspace;

namespace Orleans.Lattice.Explorer.Tests.Tenants;

/// <summary>
/// The allowed-regions surface: authorizing regions, and the refusal that
/// matters most here - revoking a region the tenant is still resident in, which
/// is predicted from state already held and reported with the rule it breaks
/// rather than round-tripped for a refusal the wire has stripped of its reason.
/// </summary>
[TestFixture]
public sealed class TenantsWorkspaceRegionTests
{
    private static async Task<(TenantsWorkspace Workspace, FakeTenancyDomain Domain)> OnRegionsAsync()
    {
        var (workspace, domain) = SampleTenants.Seeded();
        await workspace.InitializeAsync();
        await workspace.SelectTenantAsync(SampleTenants.Acme);
        await workspace.SelectSurfaceAsync(TenantsSurfaces.Regions);
        return (workspace, domain);
    }

    [Test]
    public async Task The_surface_lists_every_region_that_is_allowed_or_resident()
    {
        var (workspace, _) = await OnRegionsAsync();
        using var _guard = workspace;

        Assert.That(
            workspace.Regions.Select(region => region.RegionId),
            Is.EqualTo(new[] { SampleTenants.Region, SampleTenants.OtherRegion }));
    }

    [Test]
    public async Task A_freshly_loaded_surface_has_no_pending_change()
    {
        var (workspace, _) = await OnRegionsAsync();
        using var _guard = workspace;

        Assert.Multiple(() =>
        {
            Assert.That(workspace.HasRegionChanges, Is.False);
            Assert.That(workspace.WouldStrandResidency, Is.False);
        });
    }

    [Test]
    public async Task Revoking_a_resident_region_is_refused_locally_with_the_rule_it_breaks()
    {
        var (workspace, domain) = await OnRegionsAsync();
        using var _guard = workspace;

        // westeurope is online, so the tenant is resident there.
        workspace.SetRegionAllowed(SampleTenants.Region, allow: false);

        Assert.That(workspace.WouldStrandResidency, Is.True);

        await workspace.RequestAuthorizeRegionsAsync();

        Assert.Multiple(() =>
        {
            // Reported under the same classification the facade itself would use,
            // not as a generic precondition failure.
            Assert.That(workspace.LastStatus, Is.EqualTo(TenantOperationStatus.RegionNotAllowed));
            Assert.That(workspace.LastMessage, Does.Contain(SampleTenants.Region));
            Assert.That(workspace.LastMessage, Does.Contain(TenantRefusal.ResidentRegionRule));

            // Nothing was sent, and nothing was confirmed.
            Assert.That(
                domain.Service.Calls,
                Has.None.StartsWith(FakeTenantAdminService.Op.AuthorizeRegions));
            Assert.That(workspace.IsAwaitingConfirmation, Is.False);
        });
    }

    [Test]
    public async Task Revoking_an_authorization_the_tenant_never_used_asks_for_confirmation()
    {
        var (workspace, domain) = await OnRegionsAsync();
        using var _guard = workspace;

        // eastus is allowed but carries no residency, so revoking it is legal.
        workspace.SetRegionAllowed(SampleTenants.OtherRegion, allow: false);

        await workspace.RequestAuthorizeRegionsAsync();

        Assert.Multiple(() =>
        {
            Assert.That(workspace.IsAwaitingConfirmation, Is.True);
            Assert.That(workspace.Confirmation!.Kind, Is.EqualTo(TenantConfirmationKind.RevokeRegion));
            Assert.That(workspace.Confirmation.Body, Does.Contain(SampleTenants.OtherRegion));
            Assert.That(
                domain.Service.Calls,
                Has.None.StartsWith(FakeTenantAdminService.Op.AuthorizeRegions));
        });
    }

    [Test]
    public async Task Confirming_a_revocation_sends_the_complete_desired_allowed_set()
    {
        var (workspace, domain) = await OnRegionsAsync();
        using var _guard = workspace;
        domain.Service.AuthorizedRegions.Add(SampleTenants.Region);
        workspace.SetRegionAllowed(SampleTenants.OtherRegion, allow: false);
        await workspace.RequestAuthorizeRegionsAsync();

        await workspace.ConfirmAsync();

        Assert.Multiple(() =>
        {
            Assert.That(domain.Service.LastAuthorizedRegions, Is.EqualTo(new[] { SampleTenants.Region }));
            Assert.That(workspace.LastStatus, Is.EqualTo(TenantOperationStatus.Succeeded));
            Assert.That(workspace.LastMessage, Does.Contain(SampleTenants.Region));
        });
    }

    [Test]
    public async Task A_purely_additive_change_runs_without_a_confirmation()
    {
        var (workspace, domain) = await OnRegionsAsync();
        using var _guard = workspace;
        domain.Service.AuthorizedRegions.AddRange([SampleTenants.Region, SampleTenants.OtherRegion, "apac"]);
        workspace.AddRegionId = "apac";
        workspace.AddRegion();

        await workspace.RequestAuthorizeRegionsAsync();

        Assert.Multiple(() =>
        {
            // Adding an authorization takes nothing away, so there is nothing to
            // confirm.
            Assert.That(workspace.IsAwaitingConfirmation, Is.False);
            Assert.That(
                domain.Service.LastAuthorizedRegions,
                Is.EqualTo(new[] { SampleTenants.Region, SampleTenants.OtherRegion, "apac" }));
        });
    }

    [Test]
    public async Task Adding_a_region_marks_it_as_a_pending_authorization()
    {
        var (workspace, _) = await OnRegionsAsync();
        using var _guard = workspace;
        workspace.AddRegionId = "apac";

        workspace.AddRegion();

        var added = workspace.Regions.Single(region => region.RegionId == "apac");

        Assert.Multiple(() =>
        {
            Assert.That(added.Allow, Is.True);
            Assert.That(added.IsAllowed, Is.False);
            Assert.That(added.IsChanged, Is.True);
            Assert.That(added.Status, Is.EqualTo(ExplorerTenantRegionLifecycle.None));
            Assert.That(workspace.AddRegionId, Is.Empty);
        });
    }

    [Test]
    public async Task Adding_a_region_that_is_already_listed_is_refused()
    {
        var (workspace, _) = await OnRegionsAsync();
        using var _guard = workspace;
        workspace.AddRegionId = SampleTenants.Region;

        workspace.AddRegion();

        Assert.Multiple(() =>
        {
            Assert.That(workspace.LastMessage, Is.EqualTo(TenantsWorkspace.RegionAlreadyListedMessage));
            Assert.That(workspace.Regions, Has.Count.EqualTo(2));
        });
    }

    [Test]
    public async Task Adding_a_blank_region_is_refused()
    {
        var (workspace, _) = await OnRegionsAsync();
        using var _guard = workspace;
        workspace.AddRegionId = "   ";

        workspace.AddRegion();

        Assert.That(workspace.LastMessage, Is.EqualTo(TenantsWorkspace.RegionAlreadyListedMessage));
    }

    [Test]
    public async Task Applying_with_nothing_changed_says_so_and_sends_nothing()
    {
        var (workspace, domain) = await OnRegionsAsync();
        using var _guard = workspace;

        await workspace.RequestAuthorizeRegionsAsync();

        Assert.Multiple(() =>
        {
            Assert.That(workspace.LastMessage, Is.EqualTo(TenantsWorkspace.RegionsUnchangedMessage));
            Assert.That(
                domain.Service.Calls,
                Has.None.StartsWith(FakeTenantAdminService.Op.AuthorizeRegions));
        });
    }

    [Test]
    public async Task Applying_with_no_tenant_selected_is_refused_before_the_call()
    {
        var (workspace, domain, _) = SampleTenants.Workspace();
        using var _guard = workspace;
        await workspace.InitializeAsync();

        await workspace.RequestAuthorizeRegionsAsync();

        Assert.Multiple(() =>
        {
            Assert.That(workspace.LastMessage, Is.EqualTo(TenantsWorkspace.RegionsNeedTenantMessage));
            Assert.That(
                domain.Service.Calls,
                Has.None.StartsWith(FakeTenantAdminService.Op.AuthorizeRegions));
        });
    }

    [Test]
    public async Task A_server_side_region_refusal_is_reported_with_the_resident_rule()
    {
        var (workspace, domain) = await OnRegionsAsync();
        using var _guard = workspace;
        domain.Service.Fail(FakeTenantAdminService.Op.AuthorizeRegions, TenantOperationStatus.RegionNotAllowed);
        workspace.SetRegionAllowed(SampleTenants.OtherRegion, allow: false);
        await workspace.RequestAuthorizeRegionsAsync();

        await workspace.ConfirmAsync();

        Assert.Multiple(() =>
        {
            Assert.That(workspace.LastStatus, Is.EqualTo(TenantOperationStatus.RegionNotAllowed));
            Assert.That(workspace.LastMessage, Is.EqualTo(TenantRefusal.ResidentRegionRule));
        });
    }

    [Test]
    public async Task A_wire_collapsed_region_refusal_renders_the_reason_and_the_rule()
    {
        var (workspace, domain) = await OnRegionsAsync();
        using var _guard = workspace;
        domain.Service.Fail(
            FakeTenantAdminService.Op.AuthorizeRegions,
            TenantOperationStatus.PreconditionFailed,
            "tenant acme is still resident in eastus");

        workspace.SetRegionAllowed(SampleTenants.OtherRegion, allow: false);
        await workspace.RequestAuthorizeRegionsAsync();
        await workspace.ConfirmAsync();

        Assert.Multiple(() =>
        {
            // The binding keeps the specific reason only in the message, so it
            // must be rendered verbatim or five refusals become one grey failure.
            Assert.That(workspace.LastMessage, Does.Contain("tenant acme is still resident in eastus"));
            Assert.That(workspace.LastMessage, Does.Contain(TenantRefusal.ResidentRegionRule));
        });
    }

    [Test]
    public async Task A_last_region_refusal_is_reported_with_its_own_meaning()
    {
        var (workspace, domain) = await OnRegionsAsync();
        using var _guard = workspace;
        domain.Service.Fail(FakeTenantAdminService.Op.AuthorizeRegions, TenantOperationStatus.LastRegion);
        workspace.SetRegionAllowed(SampleTenants.OtherRegion, allow: false);
        await workspace.RequestAuthorizeRegionsAsync();

        await workspace.ConfirmAsync();

        Assert.That(workspace.LastMessage, Does.Contain("resident in no region"));
    }

    [Test]
    public async Task A_refused_region_read_reports_it_and_leaves_no_rows()
    {
        var (workspace, domain) = SampleTenants.Seeded();
        using var _guard = workspace;
        await workspace.InitializeAsync();
        await workspace.SelectTenantAsync(SampleTenants.Acme);
        domain.Service.Fail(FakeTenantAdminService.Op.RegionStatus, TenantOperationStatus.Denied);

        await workspace.SelectSurfaceAsync(TenantsSurfaces.Regions);

        Assert.Multiple(() =>
        {
            Assert.That(workspace.Regions, Is.Empty);
            Assert.That(workspace.LastStatus, Is.EqualTo(TenantOperationStatus.Denied));
        });
    }

    [Test]
    public async Task An_authorization_that_ends_with_no_regions_says_so()
    {
        var (workspace, domain) = await OnRegionsAsync();
        using var _guard = workspace;
        workspace.SetRegionAllowed(SampleTenants.OtherRegion, allow: false);
        await workspace.RequestAuthorizeRegionsAsync();

        await workspace.ConfirmAsync();

        Assert.That(workspace.LastMessage, Does.Contain("authorized for no regions"));
    }

    [Test]
    public void Setting_a_null_region_throws()
    {
        var (workspace, _, _) = SampleTenants.Workspace();
        using var _guard = workspace;

        Assert.That(() => workspace.SetRegionAllowed(null!, allow: true), Throws.ArgumentNullException);
    }

    [Test]
    public async Task Setting_an_unlisted_region_changes_nothing()
    {
        var (workspace, _) = await OnRegionsAsync();
        using var _guard = workspace;

        workspace.SetRegionAllowed("nowhere", allow: false);

        Assert.That(workspace.HasRegionChanges, Is.False);
    }

    [Test]
    public async Task Refreshing_discards_pending_edits()
    {
        var (workspace, _) = await OnRegionsAsync();
        using var _guard = workspace;
        workspace.SetRegionAllowed(SampleTenants.OtherRegion, allow: false);
        Assert.That(workspace.HasRegionChanges, Is.True);

        await workspace.RefreshRegionsAsync();

        Assert.That(workspace.HasRegionChanges, Is.False);
    }
}
