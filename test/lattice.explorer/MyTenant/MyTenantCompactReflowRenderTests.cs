using Orleans.Lattice.Explorer.DesignSystem.Tokens;
using Orleans.Lattice.Explorer.Plugins.MyTenant;
using Orleans.Lattice.Explorer.Plugins.MyTenant.Components;
using Orleans.Lattice.Explorer.Plugins.MyTenant.Workspace;
using Orleans.Lattice.Explorer.Plugins.Tenancy;
using Orleans.Lattice.Explorer.Tests.Plugins;

namespace Orleans.Lattice.Explorer.Tests.MyTenant;

/// <summary>
/// The compact reflow, asserted at render level on the three My Tenant surfaces
/// that ship a <c>LatticeAdaptiveTable</c>: the tenants the caller can reach,
/// the tenant's admin subjects, and its region residency (issue #1782).
/// </summary>
/// <remarks>
/// Every render is driven by a real <see cref="MyTenantWorkspace"/> over a
/// scripted <see cref="FakeTenancyDomain"/>, so the view under test is the real
/// one over real state and no test here depends on a clock, an ordering, or a
/// background task.
/// </remarks>
[TestFixture]
public sealed class MyTenantCompactReflowRenderTests
{
    // ---- Overview: the accessible-tenants table ---------------------------

    private const string OverviewSurface = "MyTenantOverview";

    [Test]
    public async Task The_accessible_tenants_table_renders_a_table_at_expanded()
    {
        var html = await RenderOverviewAsync(LatticeBreakpoint.Expanded);

        AdaptiveReflowAssert.RendersATable(html, OverviewSurface);
    }

    [Test]
    public async Task The_accessible_tenants_table_reflows_to_a_card_list_at_compact()
    {
        var html = await RenderOverviewAsync(LatticeBreakpoint.Compact);

        AdaptiveReflowAssert.ReflowsToCards(html, OverviewSurface);
    }

    [Test]
    public async Task The_accessible_tenants_table_keeps_every_column_across_the_reflow()
    {
        var expanded = await RenderOverviewAsync(LatticeBreakpoint.Expanded);
        var compact = await RenderOverviewAsync(LatticeBreakpoint.Compact);

        Assert.Multiple(() =>
        {
            AdaptiveReflowAssert.TableShowsColumn(expanded, "Tenant", OverviewSurface);
            AdaptiveReflowAssert.TableShowsColumn(expanded, "Status", OverviewSurface);
            AdaptiveReflowAssert.TableShowsColumn(expanded, "Default", OverviewSurface);

            AdaptiveReflowAssert.CardShowsTitle(compact, MyTenantSample.TenantId, OverviewSurface);
            AdaptiveReflowAssert.CardShowsField(compact, "Status", "Active", OverviewSurface);

            // Default opts out of the card by declaration, so the omission has
            // to stay distinguishable from a column that started vanishing.
            AdaptiveReflowAssert.CardOmitsField(compact, "Default", OverviewSurface);
        });
    }

    [Test]
    public async Task The_tenant_switch_action_survives_the_reflow()
    {
        var expanded = await RenderOverviewAsync(LatticeBreakpoint.Expanded);
        var compact = await RenderOverviewAsync(LatticeBreakpoint.Compact);

        Assert.Multiple(() =>
        {
            AdaptiveReflowAssert.ControlSurvivesTheReflow(
                expanded, compact, "lxm-switch-action", OverviewSurface);
            AdaptiveReflowAssert.ControlSurvivesTheReflow(
                expanded, compact, ">Switch to", OverviewSurface);
        });
    }

    private static async Task<string> RenderOverviewAsync(LatticeBreakpoint breakpoint)
    {
        // Two accessible tenants, so the row action is a live switch rather
        // than the disabled "Viewing" state the active tenant renders.
        var harness = await MyTenantWorkspaceHarness.CreateAsync(domain =>
            domain.Service.AccessibleTenants =
                TenantOperationResult<IReadOnlyList<ExplorerTenantSummary>>.Success(
                    [
                        MyTenantSample.Summary(),
                        MyTenantSample.Summary(MyTenantSample.OtherTenantId),
                    ],
                    "ok"));

        await harness.OpenAsync(MyTenantSurfaces.Overview);
        return await RenderSurfaceAsync<MyTenantOverview>(harness, breakpoint);
    }

    // ---- Members: the admin-subject table ---------------------------------

    private const string MembersSurface = "MyTenantMembers";

    [Test]
    public async Task The_admin_subject_table_renders_a_table_at_expanded()
    {
        var html = await RenderMembersAsync(LatticeBreakpoint.Expanded);

        AdaptiveReflowAssert.RendersATable(html, MembersSurface);
    }

    [Test]
    public async Task The_admin_subject_table_reflows_to_a_card_list_at_compact()
    {
        var html = await RenderMembersAsync(LatticeBreakpoint.Compact);

        AdaptiveReflowAssert.ReflowsToCards(html, MembersSurface);
    }

    [Test]
    public async Task The_admin_subject_table_keeps_every_column_across_the_reflow()
    {
        var expanded = await RenderMembersAsync(LatticeBreakpoint.Expanded);
        var compact = await RenderMembersAsync(LatticeBreakpoint.Compact);

        Assert.Multiple(() =>
        {
            AdaptiveReflowAssert.TableShowsColumn(expanded, "Subject", MembersSurface);
            AdaptiveReflowAssert.CardShowsTitle(compact, "user:ada", MembersSurface);
        });
    }

    [Test]
    public async Task The_admin_subject_revoke_action_survives_the_reflow()
    {
        var expanded = await RenderMembersAsync(LatticeBreakpoint.Expanded);
        var compact = await RenderMembersAsync(LatticeBreakpoint.Compact);

        Assert.Multiple(() =>
        {
            AdaptiveReflowAssert.ControlSurvivesTheReflow(
                expanded, compact, "lxm-destructive-action", MembersSurface);
            AdaptiveReflowAssert.ControlSurvivesTheReflow(
                expanded, compact, ">Remove</button>", MembersSurface);
        });
    }

    private static async Task<string> RenderMembersAsync(LatticeBreakpoint breakpoint)
    {
        var harness = await MyTenantWorkspaceHarness.CreateAsync();
        await harness.OpenAsync(MyTenantSurfaces.Members);
        return await RenderSurfaceAsync<MyTenantMembers>(harness, breakpoint);
    }

    // ---- Regions: the residency table -------------------------------------

    private const string RegionsSurface = "MyTenantRegions";

    [Test]
    public async Task The_region_residency_table_renders_a_table_at_expanded()
    {
        var html = await RenderRegionsAsync(LatticeBreakpoint.Expanded);

        AdaptiveReflowAssert.RendersATable(html, RegionsSurface);
    }

    [Test]
    public async Task The_region_residency_table_reflows_to_a_card_list_at_compact()
    {
        var html = await RenderRegionsAsync(LatticeBreakpoint.Compact);

        AdaptiveReflowAssert.ReflowsToCards(html, RegionsSurface);
    }

    [Test]
    public async Task The_region_residency_table_keeps_every_column_across_the_reflow()
    {
        var expanded = await RenderRegionsAsync(LatticeBreakpoint.Expanded);
        var compact = await RenderRegionsAsync(LatticeBreakpoint.Compact);

        Assert.Multiple(() =>
        {
            AdaptiveReflowAssert.TableShowsColumn(expanded, "Region", RegionsSurface);
            AdaptiveReflowAssert.TableShowsColumn(expanded, "Allowed", RegionsSurface);
            AdaptiveReflowAssert.TableShowsColumn(expanded, "Residency", RegionsSurface);
            AdaptiveReflowAssert.TableShowsColumn(expanded, "Lifecycle", RegionsSurface);

            AdaptiveReflowAssert.CardShowsTitle(compact, "eastus", RegionsSurface);

            // The two-set model is the whole point of this surface: a card that
            // kept residency but dropped the allowed flag would read as though
            // any region could be joined.
            AdaptiveReflowAssert.CardShowsField(compact, "Allowed", "Allowed", RegionsSurface);
            AdaptiveReflowAssert.CardShowsField(compact, "Residency", "Resident", RegionsSurface);

            // Lifecycle opts out of the card by declaration.
            AdaptiveReflowAssert.CardOmitsField(compact, "Lifecycle", RegionsSurface);
        });
    }

    [Test]
    public async Task The_region_residency_toggle_survives_the_reflow()
    {
        var expanded = await RenderRegionsAsync(LatticeBreakpoint.Expanded);
        var compact = await RenderRegionsAsync(LatticeBreakpoint.Compact);

        Assert.Multiple(() =>
        {
            AdaptiveReflowAssert.ControlSurvivesTheReflow(
                expanded, compact, ">Add</button>", RegionsSurface);
            AdaptiveReflowAssert.ControlSurvivesTheReflow(
                expanded, compact, ">Remove</button>", RegionsSurface);
        });
    }

    private static async Task<string> RenderRegionsAsync(LatticeBreakpoint breakpoint)
    {
        // Two resident regions so a removal is permitted, plus an allowed one
        // the tenant is not resident in, so both toggle wordings are on the
        // surface and neither is disabled by the last-region invariant.
        var harness = await MyTenantWorkspaceHarness.CreateAsync(domain =>
        {
            IReadOnlyList<ExplorerTenantRegion> regions =
            [
                MyTenantSample.Region("eastus", ExplorerTenantRegionLifecycle.Online),
                MyTenantSample.Region("westeurope", ExplorerTenantRegionLifecycle.Online),
                MyTenantSample.Region("northeurope", ExplorerTenantRegionLifecycle.None),
            ];

            domain.Service.Detail = TenantOperationResult<ExplorerTenantDetail>.Success(
                MyTenantSample.Detail(regions: regions),
                "ok");
            domain.Service.RegionStatus =
                TenantOperationResult<IReadOnlyList<ExplorerTenantRegion>>.Success(regions, "ok");
        });

        await harness.OpenAsync(MyTenantSurfaces.Regions);
        return await RenderSurfaceAsync<MyTenantRegions>(harness, breakpoint);
    }

    /// <summary>
    /// Renders a My Tenant surface over the harness's loaded workspace. The
    /// surface takes the workspace as its single parameter and resolves no
    /// service of its own, so the shared per-selection harness renders it as it
    /// stands.
    /// </summary>
    /// <typeparam name="TSurface">The surface to render.</typeparam>
    /// <param name="harness">The loaded workspace harness.</param>
    /// <param name="breakpoint">The breakpoint to cascade.</param>
    private static Task<string> RenderSurfaceAsync<TSurface>(
        MyTenantWorkspaceHarness harness,
        LatticeBreakpoint breakpoint)
        where TSurface : Microsoft.AspNetCore.Components.IComponent =>
        SelectionViewRenderHarness.RenderComponentAsync<TSurface>(
            new Dictionary<string, object?> { ["State"] = harness.Workspace },
            breakpoint);
}
