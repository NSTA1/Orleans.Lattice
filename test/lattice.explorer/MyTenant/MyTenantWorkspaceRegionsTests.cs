using Orleans.Lattice.Explorer.Plugins.MyTenant;
using Orleans.Lattice.Explorer.Plugins.MyTenant.Workspace;
using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Tenancy;

namespace Orleans.Lattice.Explorer.Tests.MyTenant;

/// <summary>
/// The Regions surface: residency editing against the operator-authorized
/// allowed set, with both invariants refused client-side before the cluster is
/// asked and the cluster's own refusal rendered when it happens anyway.
/// </summary>
[TestFixture]
public sealed class MyTenantWorkspaceRegionsTests
{
    private static async Task<MyTenantWorkspaceHarness> OpenAsync(Action<FakeTenancyDomain>? configure = null)
    {
        var harness = await MyTenantWorkspaceHarness.CreateAsync(configure);
        await harness.OpenAsync(MyTenantSurfaces.Regions);
        return harness;
    }

    private static MyTenantWorkspaceHarness WithRegions(IReadOnlyList<ExplorerTenantRegion> regions)
    {
        var harness = MyTenantWorkspaceHarness.Create(domain =>
        {
            domain.Service.Detail = TenantOperationResult<ExplorerTenantDetail>.Success(
                MyTenantSample.Detail(regions: regions),
                "ok");
            domain.Service.RegionStatus =
                TenantOperationResult<IReadOnlyList<ExplorerTenantRegion>>.Success(regions, "ok");
        });

        return harness;
    }

    [Test]
    public async Task The_detail_read_seeds_the_plan_so_the_surface_opens_populated()
    {
        var harness = await MyTenantWorkspaceHarness.CreateAsync();

        Assert.Multiple(() =>
        {
            Assert.That(harness.Workspace.Regions.Rows, Is.Not.Empty);
            Assert.That(harness.Workspace.Regions.AllowedCount, Is.EqualTo(2));
            Assert.That(harness.Workspace.Regions.ResidentCount, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task A_refused_region_read_leaves_no_stale_rows()
    {
        var harness = MyTenantWorkspaceHarness.Create(domain =>
        {
            domain.Service.Detail = TenantOperationResult<ExplorerTenantDetail>.Failure(
                TenantOperationStatus.Denied,
                "no detail");
            domain.Service.RegionStatus =
                TenantOperationResult<IReadOnlyList<ExplorerTenantRegion>>.Failure(
                    TenantOperationStatus.Denied,
                    "refused");
        });

        await harness.Workspace.InitializeAsync();
        await harness.OpenAsync(MyTenantSurfaces.Regions);

        Assert.Multiple(() =>
        {
            Assert.That(harness.Workspace.Regions.Rows, Is.Empty);
            Assert.That(harness.Workspace.LastNotice?.Message, Is.EqualTo("refused"));
        });
    }

    [Test]
    public async Task Adding_an_allowed_region_marks_the_plan_pending()
    {
        var harness = await OpenAsync();

        harness.Workspace.ToggleRegion("eastus");

        Assert.Multiple(() =>
        {
            Assert.That(harness.Workspace.HasPendingResidencyChange, Is.True);
            Assert.That(harness.Workspace.Regions.PlannedResidentCount, Is.EqualTo(2));
            Assert.That(harness.Workspace.LastNotice, Is.Null, "a permitted toggle is not a refusal");
        });
    }

    [Test]
    public async Task A_region_outside_the_allowed_set_is_refused_with_the_two_set_reason()
    {
        var harness = WithRegions(MyTenantSample.Regions());
        await harness.Workspace.InitializeAsync();
        await harness.OpenAsync(MyTenantSurfaces.Regions);

        harness.Workspace.ToggleRegion("northeurope");

        Assert.Multiple(() =>
        {
            Assert.That(harness.Workspace.HasPendingResidencyChange, Is.False);
            Assert.That(
                harness.Workspace.LastNotice?.Status,
                Is.EqualTo(TenantOperationStatus.RegionNotAllowed));
            Assert.That(
                harness.Workspace.LastNotice?.Message,
                Is.EqualTo(MyTenantWorkspace.RegionNotAllowedRefusal));
            Assert.That(
                harness.Workspace.LastNotice?.Guidance,
                Is.EqualTo(MyTenantNotice.RegionNotAllowedGuidance));
        });
    }

    [Test]
    public async Task The_last_resident_region_is_refused_before_the_server_ever_sees_it()
    {
        var harness = WithRegions(MyTenantSample.SingleResidencyRegions());
        await harness.Workspace.InitializeAsync();
        await harness.OpenAsync(MyTenantSurfaces.Regions);

        harness.Workspace.ToggleRegion("westeurope");

        Assert.Multiple(() =>
        {
            Assert.That(harness.Workspace.LastNotice?.Status, Is.EqualTo(TenantOperationStatus.LastRegion));
            Assert.That(
                harness.Workspace.LastNotice?.Message,
                Is.EqualTo(MyTenantWorkspace.LastRegionRefusal));
            Assert.That(
                harness.Workspace.LastNotice?.Guidance,
                Is.EqualTo(MyTenantNotice.LastRegionGuidance));
            Assert.That(harness.Service.ResidencyCalls, Is.Empty);
        });
    }

    [Test]
    public async Task A_resident_region_that_is_no_longer_allowed_gets_its_own_reason()
    {
        var harness = WithRegions(
        [
            MyTenantSample.Region("eastus", ExplorerTenantRegionLifecycle.Online, isAllowed: true),
            MyTenantSample.Region("westeurope", ExplorerTenantRegionLifecycle.Online, isAllowed: false),
        ]);
        await harness.Workspace.InitializeAsync();
        await harness.OpenAsync(MyTenantSurfaces.Regions);

        // Leaving is permitted; re-entering is not, and says something different.
        harness.Workspace.ToggleRegion("westeurope");
        harness.Workspace.ToggleRegion("westeurope");

        Assert.That(
            harness.Workspace.LastNotice?.Message,
            Is.EqualTo(MyTenantWorkspace.RegionNoLongerAllowedRefusal));
    }

    [Test]
    public async Task Applying_sends_the_complete_planned_residency_set()
    {
        var harness = await OpenAsync();
        harness.Workspace.ToggleRegion("eastus");

        await harness.Workspace.ApplyResidencyAsync();

        Assert.Multiple(() =>
        {
            Assert.That(harness.Service.ResidencyCalls, Has.Count.EqualTo(1));
            Assert.That(harness.Service.ResidencyCalls[0].TenantId, Is.EqualTo(MyTenantSample.TenantId));
            Assert.That(
                harness.Service.ResidencyCalls[0].Regions,
                Is.EqualTo(new[] { "eastus", "westeurope" }));
        });
    }

    [Test]
    public async Task Applying_adopts_the_residency_the_cluster_committed()
    {
        var harness = await OpenAsync();
        harness.Workspace.ToggleRegion("eastus");

        await harness.Workspace.ApplyResidencyAsync();

        Assert.Multiple(() =>
        {
            Assert.That(harness.Workspace.HasPendingResidencyChange, Is.False);
            Assert.That(
                harness.Workspace.Regions.ResidentCount,
                Is.EqualTo(1),
                "the committed reading wins over the plan the caller sent");
        });
    }

    [Test]
    public async Task Applying_an_unchanged_plan_is_refused_rather_than_sent()
    {
        var harness = await OpenAsync();

        await harness.Workspace.ApplyResidencyAsync();

        Assert.Multiple(() =>
        {
            Assert.That(harness.Service.ResidencyCalls, Is.Empty);
            Assert.That(
                harness.Workspace.LastNotice?.Message,
                Is.EqualTo(MyTenantWorkspace.NoResidencyChangeMessage));
        });
    }

    [Test]
    public async Task A_servers_last_region_refusal_is_rendered_with_its_guidance()
    {
        var harness = await OpenAsync(domain =>
            domain.Service.ResidencyChange =
                TenantOperationResult<ExplorerTenantResidencyChange>.Failure(
                    TenantOperationStatus.LastRegion,
                    "would leave the tenant resident nowhere"));
        harness.Workspace.ToggleRegion("eastus");

        await harness.Workspace.ApplyResidencyAsync();

        Assert.Multiple(() =>
        {
            Assert.That(
                harness.Workspace.LastNotice?.Message,
                Is.EqualTo("would leave the tenant resident nowhere"));
            Assert.That(
                harness.Workspace.LastNotice?.Guidance,
                Is.EqualTo(MyTenantNotice.LastRegionGuidance));
        });
    }

    [Test]
    public async Task A_servers_precondition_refusal_keeps_its_message_verbatim()
    {
        const string ServerMessage = "Tenant 'acme' must remain resident in at least one region.";

        var harness = await OpenAsync(domain =>
            domain.Service.ResidencyChange =
                TenantOperationResult<ExplorerTenantResidencyChange>.Failure(
                    TenantOperationStatus.PreconditionFailed,
                    ServerMessage));
        harness.Workspace.ToggleRegion("eastus");

        await harness.Workspace.ApplyResidencyAsync();

        Assert.Multiple(() =>
        {
            Assert.That(harness.Workspace.LastNotice?.Message, Is.EqualTo(ServerMessage));
            Assert.That(harness.Workspace.LastNotice?.Severity, Is.EqualTo("is-refused"));
        });
    }

    [Test]
    public async Task Reverting_discards_the_pending_edit()
    {
        var harness = await OpenAsync();
        harness.Workspace.ToggleRegion("eastus");

        harness.Workspace.RevertResidency();

        Assert.Multiple(() =>
        {
            Assert.That(harness.Workspace.HasPendingResidencyChange, Is.False);
            Assert.That(harness.Workspace.Regions.PlannedResidentCount, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task A_denied_gate_makes_every_residency_action_a_no_op()
    {
        var harness = await MyTenantWorkspaceHarness.CreateAsync(
            access: ExplorerPluginAccess.Denied);

        harness.Workspace.ToggleRegion("eastus");
        await harness.Workspace.ApplyResidencyAsync();

        Assert.Multiple(() =>
        {
            Assert.That(harness.Service.ResidencyCalls, Is.Empty);
            Assert.That(harness.Workspace.HasPendingResidencyChange, Is.False);
        });
    }

    [Test]
    public async Task An_empty_region_id_is_ignored()
    {
        var harness = await OpenAsync();

        harness.Workspace.ToggleRegion(string.Empty);

        Assert.That(harness.Workspace.HasPendingResidencyChange, Is.False);
    }
}
