using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.Plugins.Tenancy;
using Orleans.Lattice.Explorer.Plugins.Tenants;
using Orleans.Lattice.Explorer.Plugins.Tenants.Views;
using Orleans.Lattice.Explorer.Plugins.Tenants.Workspace;
using Orleans.Lattice.Explorer.Tests.DesignSystem;

namespace Orleans.Lattice.Explorer.Tests.Tenants;

/// <summary>
/// What the tenant-scoped sub-surfaces render.
/// <para>
/// This is where the two headline correctness requirements are proved at the
/// pixel: a quota dimension can never reach the screen as a bar that lies about
/// an absent ceiling or an absent sample, and a cross-tenant grant can never
/// reach it without saying whether it authorizes anything.
/// </para>
/// </summary>
[TestFixture]
public sealed class TenantsViewRenderTests
{
    private static Task<string> RenderAsync<TView>(TenantsWorkspace workspace)
        where TView : IComponent =>
        DesignSystemRenderHarness.RenderAsync<TView>(new Dictionary<string, object?>
        {
            ["State"] = workspace,
        });

    private static async Task<TenantsWorkspace> OnSurfaceAsync(
        string surfaceId,
        Action<FakeTenancyDomain>? arrange = null)
    {
        var (workspace, domain) = SampleTenants.Seeded();
        arrange?.Invoke(domain);
        await workspace.InitializeAsync();
        await workspace.SelectTenantAsync(SampleTenants.Acme);
        await workspace.SelectSurfaceAsync(surfaceId);
        return workspace;
    }

    // ---- quotas -------------------------------------------------------------

    [Test]
    public async Task The_quota_surface_names_every_dimension()
    {
        using var workspace = await OnSurfaceAsync(TenantsSurfaces.Quotas);

        var html = await RenderAsync<TenantQuotaView>(workspace);

        Assert.Multiple(() =>
        {
            foreach (var kind in ExplorerTenantQuotaUsage.Dimensions)
            {
                Assert.That(html, Does.Contain(TenantQuotaFormat.Label(kind)), kind.ToString());
            }
        });
    }

    [Test]
    public async Task An_unbounded_dimension_renders_the_word_unlimited_and_no_bar()
    {
        using var workspace = await OnSurfaceAsync(TenantsSurfaces.Quotas);

        var html = await RenderAsync<TenantQuotaView>(workspace);

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain(TenantQuotaFormat.UnlimitedText));
            Assert.That(html, Does.Contain("No ceiling on this dimension"));
        });
    }

    [Test]
    public async Task An_unmeasured_dimension_renders_the_words_not_measured_and_no_bar()
    {
        using var workspace = await OnSurfaceAsync(TenantsSurfaces.Quotas);

        var html = await RenderAsync<TenantQuotaView>(workspace);

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain(TenantQuotaFormat.NotMeasuredText));
            Assert.That(html, Does.Contain("carries no consumption figure"));
        });
    }

    [Test]
    public async Task A_bar_is_drawn_only_for_the_dimensions_that_have_one_to_draw()
    {
        using var workspace = await OnSurfaceAsync(TenantsSurfaces.Quotas);

        var html = await RenderAsync<TenantQuotaView>(workspace);
        var bars = DesignSystemRenderHarness.CountOccurrences(html, "lxt-bar-fill");
        var expected = workspace.QuotaRows.Count(row => row.ShowsBar);

        Assert.Multiple(() =>
        {
            Assert.That(bars, Is.EqualTo(expected));

            // Three of the sample's five dimensions are bounded and measured; the
            // unbounded and the unmeasured ones draw nothing.
            Assert.That(expected, Is.EqualTo(3));
        });
    }

    [Test]
    public async Task An_all_unbounded_tenant_draws_no_bar_at_all()
    {
        using var workspace = await OnSurfaceAsync(
            TenantsSurfaces.Quotas,
            domain => domain.Service.Usage[SampleTenants.Acme] = new ExplorerTenantQuotaUsage
            {
                TenantId = SampleTenants.Acme,
                HasUsage = true,
                Bytes = new ExplorerTenantQuotaDimension { Usage = 10 },
                Keys = new ExplorerTenantQuotaDimension { Usage = 10 },
                MemoryBytes = new ExplorerTenantQuotaDimension { Usage = 10 },
                TreeCount = new ExplorerTenantQuotaDimension { Usage = 10 },
                OpsPerSecond = default,
                Limits = ExplorerTenantQuotaLimits.Unbounded,
            });

        var html = await RenderAsync<TenantQuotaView>(workspace);

        Assert.Multiple(() =>
        {
            // The failure this guards against renders an unlimited tenant as a
            // row of full bars.
            Assert.That(html, Does.Not.Contain("lxt-bar-fill"));
            Assert.That(
                DesignSystemRenderHarness.CountOccurrences(html, TenantQuotaFormat.UnlimitedText),
                Is.GreaterThanOrEqualTo(5));
        });
    }

    [Test]
    public async Task The_enforcement_scope_is_captioned_beside_the_figures()
    {
        using var workspace = await OnSurfaceAsync(
            TenantsSurfaces.Quotas,
            domain => domain.Service.Usage[SampleTenants.Acme] =
                SampleTenants.Usage(scope: ExplorerTenantQuotaEnforcement.PerCluster));

        var html = await RenderAsync<TenantQuotaView>(workspace);

        Assert.That(html, Does.Contain("this cluster&#x27;s local view only")
            .Or.Contain("this cluster's local view only"));
    }

    [Test]
    public async Task The_editor_offers_a_blank_field_for_an_unbounded_ceiling()
    {
        using var workspace = await OnSurfaceAsync(TenantsSurfaces.Quotas);

        var html = await RenderAsync<TenantQuotaView>(workspace);

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("Leave a field blank for an unbounded dimension"));
            Assert.That(html, Does.Contain("placeholder=\"" + TenantQuotaFormat.UnlimitedText + "\""));
        });
    }

    // ---- regions ------------------------------------------------------------

    [Test]
    public async Task The_region_surface_shows_both_the_allowed_set_and_the_residency()
    {
        using var workspace = await OnSurfaceAsync(TenantsSurfaces.Regions);

        var html = await RenderAsync<TenantRegionView>(workspace);

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain(SampleTenants.Region));
            Assert.That(html, Does.Contain(SampleTenants.OtherRegion));
            Assert.That(html, Does.Contain("Online"));
            Assert.That(html, Does.Contain("Not provisioned"));
            Assert.That(html, Does.Contain("Currently allowed"));
        });
    }

    [Test]
    public async Task Revoking_a_resident_region_is_warned_about_on_the_row()
    {
        using var workspace = await OnSurfaceAsync(TenantsSurfaces.Regions);
        workspace.SetRegionAllowed(SampleTenants.Region, allow: false);

        var html = await RenderAsync<TenantRegionView>(workspace);

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("still resident here"));
            Assert.That(html, Does.Contain("would strand the tenant"));
        });
    }

    [Test]
    public async Task An_unchanged_allowed_set_offers_no_warning()
    {
        using var workspace = await OnSurfaceAsync(TenantsSurfaces.Regions);

        var html = await RenderAsync<TenantRegionView>(workspace);

        Assert.That(html, Does.Not.Contain("still resident here"));
    }

    // ---- tenant access ------------------------------------------------------

    [Test]
    public async Task Every_grant_renders_its_state_and_what_it_authorizes()
    {
        using var workspace = await OnSurfaceAsync(TenantsSurfaces.Access);

        var html = await RenderAsync<TenantAccessView>(workspace);

        Assert.Multiple(() =>
        {
            // A pending offer and a live grant are both present, and neither can
            // be read as the other.
            Assert.That(html, Does.Contain("Pending approval"));
            Assert.That(html, Does.Contain("Authorizes nothing yet"));
            Assert.That(html, Does.Contain("Active"));
            Assert.That(html, Does.Contain("Authorizes read and write now."));
        });
    }

    [Test]
    public async Task A_grant_row_never_renders_without_its_authority_line()
    {
        using var workspace = await OnSurfaceAsync(TenantsSurfaces.Access);

        var html = await RenderAsync<TenantAccessView>(workspace);

        var rows = DesignSystemRenderHarness.CountOccurrences(html, "lxt-grant-head");
        var authorityLines = DesignSystemRenderHarness.CountOccurrences(html, "lxt-grant-authority");

        Assert.Multiple(() =>
        {
            Assert.That(rows, Is.EqualTo(2));
            Assert.That(authorityLines, Is.EqualTo(rows), "one authority statement per grant row");
        });
    }

    [Test]
    public async Task Only_a_pending_grant_offers_approve_and_reject()
    {
        using var workspace = await OnSurfaceAsync(TenantsSurfaces.Access);

        var html = await RenderAsync<TenantAccessView>(workspace);

        Assert.Multiple(() =>
        {
            Assert.That(DesignSystemRenderHarness.CountOccurrences(html, ">Approve<"), Is.EqualTo(1));
            Assert.That(DesignSystemRenderHarness.CountOccurrences(html, ">Reject<"), Is.EqualTo(1));
            Assert.That(DesignSystemRenderHarness.CountOccurrences(html, ">Withdraw<"), Is.EqualTo(1));
        });
    }

    [Test]
    public async Task A_closed_grant_offers_no_transition_and_says_so()
    {
        using var workspace = await OnSurfaceAsync(
            TenantsSurfaces.Access,
            domain => domain.Service.Grants[SampleTenants.Acme] = new ExplorerTenantGrants
            {
                TenantId = SampleTenants.Acme,
                Issued = [SampleTenants.Grant(ExplorerTenantGrantState.Revoked)],
                Received = [],
            });

        var html = await RenderAsync<TenantAccessView>(workspace);

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("Revoked"));
            Assert.That(html, Does.Contain("Closed. No transition remains."));
            Assert.That(html, Does.Not.Contain(">Approve<"));
            Assert.That(html, Does.Not.Contain(">Withdraw<"));
        });
    }

    [Test]
    public async Task The_pending_inbox_is_flagged_when_a_grant_awaits_an_answer()
    {
        using var workspace = await OnSurfaceAsync(
            TenantsSurfaces.Access,
            domain => domain.Service.Grants[SampleTenants.Acme] = new ExplorerTenantGrants
            {
                TenantId = SampleTenants.Acme,
                Issued = [],
                Received =
                [
                    SampleTenants.Grant(ExplorerTenantGrantState.Pending, grantee: SampleTenants.Acme),
                ],
            });

        var html = await RenderAsync<TenantAccessView>(workspace);

        Assert.That(html, Does.Contain("1 awaiting an answer"));
    }

    [Test]
    public async Task The_surface_explains_that_only_an_active_grant_authorizes_anything()
    {
        using var workspace = await OnSurfaceAsync(TenantsSurfaces.Access);

        var html = await RenderAsync<TenantAccessView>(workspace);

        Assert.That(html, Does.Contain("Only an active grant authorizes anything"));
    }

    [Test]
    public async Task The_last_admin_subject_cannot_be_revoked_from_the_surface()
    {
        using var workspace = await OnSurfaceAsync(
            TenantsSurfaces.Access,
            domain => domain.Service.AdminSubjects[SampleTenants.Acme] = [SampleTenants.Subject]);

        var html = await RenderAsync<TenantAccessView>(workspace);

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain(TenantRefusal.LastAdminSubjectRule));
            Assert.That(html, Does.Contain("disabled"));
        });
    }

    [Test]
    public async Task An_empty_grant_direction_says_so_rather_than_rendering_an_empty_list()
    {
        using var workspace = await OnSurfaceAsync(TenantsSurfaces.Access);

        var html = await DesignSystemRenderHarness.RenderAsync<TenantGrantList>(
            new Dictionary<string, object?>
            {
                ["State"] = workspace,
                ["Rows"] = Array.Empty<TenantGrantRow>(),
                ["EmptyText"] = "Nothing offered.",
            });

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("Nothing offered."));
            Assert.That(html, Does.Not.Contain("lxt-grant-list"));
        });
    }

    [Test]
    public async Task The_grant_list_renders_one_row_per_grant_each_with_its_state()
    {
        using var workspace = await OnSurfaceAsync(TenantsSurfaces.Access);

        var rows = new[]
        {
            TenantGrantRow.From(
                SampleTenants.Grant(ExplorerTenantGrantState.Pending),
                TenantGrantDirection.Issued),
            TenantGrantRow.From(
                SampleTenants.Grant(ExplorerTenantGrantState.Active, grantId: "grant-3"),
                TenantGrantDirection.Issued),
        };

        var html = await DesignSystemRenderHarness.RenderAsync<TenantGrantList>(
            new Dictionary<string, object?>
            {
                ["State"] = workspace,
                ["Rows"] = rows,
            });

        Assert.Multiple(() =>
        {
            Assert.That(DesignSystemRenderHarness.CountOccurrences(html, "lxt-grant-head"), Is.EqualTo(2));
            Assert.That(DesignSystemRenderHarness.CountOccurrences(html, "lxt-grant-authority"), Is.EqualTo(2));
            Assert.That(html, Does.Contain("Pending approval"));
            Assert.That(html, Does.Contain("Active"));
        });
    }

    // ---- confirmation dialog ------------------------------------------------

    [Test]
    public async Task The_delete_confirmation_renders_the_cascade_size()
    {
        using var workspace = await OnSurfaceAsync(TenantsSurfaces.Tenants);
        await workspace.RequestDeleteAsync(SampleTenants.Acme);

        var html = await RenderAsync<TenantConfirmDialog>(workspace);

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("role=\"alertdialog\""));
            Assert.That(html, Does.Contain("3 trees"));
            Assert.That(html, Does.Contain("Delete tenant and its trees"));
            Assert.That(html, Does.Contain("Cancel"));
        });
    }

    [Test]
    public async Task Nothing_renders_when_no_confirmation_is_pending()
    {
        using var workspace = await OnSurfaceAsync(TenantsSurfaces.Tenants);

        var html = await RenderAsync<TenantConfirmDialog>(workspace);

        Assert.That(html, Is.Empty);
    }

    [Test]
    public async Task A_reserved_tenant_confirmation_renders_its_caution()
    {
        var (workspace, domain) = SampleTenants.Seeded(SampleTenants.DefaultTenant);
        using var _guard = workspace;
        domain.Service.Tenants.Clear();
        domain.Service.Tenants.Add(SampleTenants.Summary(SampleTenants.DefaultTenant, isDefault: true));
        await workspace.InitializeAsync();
        await workspace.RequestDeleteAsync(SampleTenants.DefaultTenant);

        var html = await RenderAsync<TenantConfirmDialog>(workspace);

        Assert.That(html, Does.Contain("will refuse to delete it"));
    }
}
