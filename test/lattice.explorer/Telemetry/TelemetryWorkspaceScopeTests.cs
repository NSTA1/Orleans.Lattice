using Orleans.Lattice.Explorer.Plugins.Telemetry;

namespace Orleans.Lattice.Explorer.Tests.Telemetry;

/// <summary>
/// The tenant boundary of the telemetry surface: that a panel renders the scope
/// the facade pinned rather than the one it asked for, that a degrade is
/// surfaced, that the tenant-pinned mount can only ever ask for the caller's own
/// tenant, and that the tenancy-absent deployment renders the same panels.
/// </summary>
/// <remarks>
/// None of these assertions is a security control - the facade is the
/// enforcement point and is routable precisely because a desktop head cannot be
/// one. They are correctness controls: they stop a fail-closed answer being
/// presented as the question it was not answering.
/// </remarks>
[TestFixture]
public sealed class TelemetryWorkspaceScopeTests
{
    [Test]
    public async Task The_requested_visibility_is_the_shells_and_is_read_fresh_on_every_request()
    {
        using var harness = await TelemetryWorkspaceHarness.CreateAsync(
            domain => domain.RequestedVisibility = ExplorerTelemetryVisibility.AllTenants);

        Assert.That(
            harness.Domain.LastRequest!.RequestedVisibility,
            Is.EqualTo(ExplorerTelemetryVisibility.AllTenants));

        // The shell's switcher moves; the next request must carry the new
        // answer, not the one cached at mount.
        harness.Domain.RequestedVisibility = ExplorerTelemetryVisibility.ActiveTenant;
        await harness.Workspace.ReevaluateAsync();

        Assert.That(
            harness.Domain.LastRequest!.RequestedVisibility,
            Is.EqualTo(ExplorerTelemetryVisibility.ActiveTenant));
    }

    [Test]
    public async Task No_request_ever_carries_a_tenant_id_the_client_chose()
    {
        // There is no control that names a tenant, and no code path that fills
        // this field: the tenant is derived server-side from the authenticated
        // caller, and a client-supplied one would be a value nothing validated.
        using var harness = await TelemetryWorkspaceHarness.CreateAsync(
            domain => domain.RequestedVisibility = ExplorerTelemetryVisibility.AllTenants);

        await harness.Workspace.SelectRangeAsync("1h");
        await harness.Workspace.RefreshAsync();

        Assert.That(harness.Domain.Requests.Select(request => request.RequestedTenantId), Is.All.Null);
    }

    [Test]
    public async Task The_effective_scope_rendered_is_the_one_the_facade_reported()
    {
        using var harness = await TelemetryWorkspaceHarness.CreateAsync(
            domain => domain.Result = ExplorerTelemetrySample.Result(ExplorerTelemetrySample.CrossTenantScope()));

        Assert.Multiple(() =>
        {
            Assert.That(harness.Workspace.Scope.IsCrossTenant, Is.True);
            Assert.That(harness.Workspace.WasDowngraded, Is.False);
            Assert.That(harness.Workspace.Caption.Text, Does.Contain("every tenant"));
        });
    }

    [Test]
    public async Task A_refused_cross_tenant_request_is_reported_as_degraded_rather_than_charted_silently()
    {
        // The whole point of the surface. The caller asked for the cluster; the
        // facade served one tenant. A panel that rendered this as an ordinary
        // answer would present one tenant's traffic as the whole cluster's.
        using var harness = await TelemetryWorkspaceHarness.CreateAsync(domain =>
        {
            domain.RequestedVisibility = ExplorerTelemetryVisibility.AllTenants;
            domain.Result = ExplorerTelemetrySample.Result(ExplorerTelemetrySample.DowngradedScope());
        });

        Assert.Multiple(() =>
        {
            Assert.That(harness.Workspace.WasDowngraded, Is.True);
            Assert.That(harness.Workspace.Caption.IsDegraded, Is.True);
            Assert.That(harness.Workspace.Caption.Severity, Is.EqualTo(TelemetryScopeSeverity.Degraded));
            Assert.That(harness.Workspace.Scope.EffectiveVisibility, Is.EqualTo(ExplorerTelemetryVisibility.ActiveTenant));
        });
    }

    [Test]
    public async Task A_degrade_is_cleared_once_the_facade_honours_the_request()
    {
        using var harness = await TelemetryWorkspaceHarness.CreateAsync(domain =>
        {
            domain.RequestedVisibility = ExplorerTelemetryVisibility.AllTenants;
            domain.Result = ExplorerTelemetrySample.Result(ExplorerTelemetrySample.DowngradedScope());
        });

        harness.Domain.Result = ExplorerTelemetrySample.Result(ExplorerTelemetrySample.CrossTenantScope());
        await harness.Workspace.ReevaluateAsync();

        Assert.That(harness.Workspace.WasDowngraded, Is.False);
    }

    [Test]
    public async Task Before_anything_is_evaluated_the_scope_is_the_fail_closed_one()
    {
        using var harness = TelemetryWorkspaceHarness.Create();

        Assert.Multiple(() =>
        {
            Assert.That(harness.Workspace.Scope, Is.EqualTo(ExplorerTelemetryScope.None));
            Assert.That(harness.Workspace.HasEvaluated, Is.False);
            Assert.That(
                harness.Workspace.Caption.Text,
                Is.Not.Null.And.Not.Empty,
                "a view rendered before the first evaluation still needs a caption");
        });

        await Task.CompletedTask;
    }

    // ---- the tenant-pinned mount -------------------------------------------

    [Test]
    public async Task The_pinned_mount_asks_for_the_active_tenant_even_when_the_shell_asks_for_every_tenant()
    {
        // A platform operator with the shell switched to a cross-tenant view is
        // still looking at a section headed "your tenant". Inheriting that
        // intent would label the whole cluster's traffic as one tenant's own.
        using var harness = await TelemetryWorkspaceHarness.CreateAsync(
            domain => domain.RequestedVisibility = ExplorerTelemetryVisibility.AllTenants,
            pinnedToOwnTenant: true);

        Assert.Multiple(() =>
        {
            Assert.That(harness.Workspace.IsPinnedToOwnTenant, Is.True);
            Assert.That(
                harness.Domain.LastRequest!.RequestedVisibility,
                Is.EqualTo(ExplorerTelemetryVisibility.ActiveTenant));
        });
    }

    [Test]
    public async Task The_pinned_mount_never_sends_any_other_visibility_however_it_is_driven()
    {
        using var harness = await TelemetryWorkspaceHarness.CreateAsync(
            domain => domain.RequestedVisibility = ExplorerTelemetryVisibility.SingleTenant,
            pinnedToOwnTenant: true);

        await harness.Workspace.SelectRangeAsync("1h");
        await harness.Workspace.SelectStepAsync("5m");
        await harness.Workspace.SelectQueryAsync(ExplorerTelemetrySample.InstantQueryId);
        await harness.Workspace.RefreshAsync();
        await harness.Workspace.ReevaluateAsync();

        Assert.Multiple(() =>
        {
            Assert.That(harness.Domain.Requests, Is.Not.Empty);
            Assert.That(
                harness.Domain.Requests.Select(request => request.RequestedVisibility),
                Is.All.EqualTo(ExplorerTelemetryVisibility.ActiveTenant));
            Assert.That(harness.Domain.Requests.Select(request => request.RequestedTenantId), Is.All.Null);
        });
    }

    [Test]
    public async Task The_pinned_mount_still_reports_a_degrade_the_facade_declares()
    {
        // The pin is a request, not a grant. If the facade says it served
        // something narrower than asked, the section says so too.
        using var harness = await TelemetryWorkspaceHarness.CreateAsync(
            domain => domain.Result = ExplorerTelemetrySample.Result(ExplorerTelemetrySample.DowngradedScope()),
            pinnedToOwnTenant: true);

        Assert.That(harness.Workspace.Caption.IsDegraded, Is.True);
    }

    [Test]
    public async Task The_unpinned_mount_is_not_pinned()
    {
        using var harness = await TelemetryWorkspaceHarness.CreateAsync();

        Assert.That(harness.Workspace.IsPinnedToOwnTenant, Is.False);
    }

    // ---- the tenancy-absent deployment -------------------------------------

    [Test]
    public async Task With_tenancy_absent_the_same_catalogue_and_the_same_entry_are_rendered()
    {
        using var withTenancy = await TelemetryWorkspaceHarness.CreateAsync();
        using var without = await TelemetryWorkspaceHarness.CreateAsync(
            domain => domain.IsTenancyEnabled = false);

        Assert.Multiple(() =>
        {
            Assert.That(without.Workspace.IsTenancyEnabled, Is.False);
            Assert.That(
                without.Workspace.Catalog.Count,
                Is.EqualTo(withTenancy.Workspace.Catalog.Count),
                "there are deliberately no tenancy-on and tenancy-off panel variants");
            Assert.That(
                without.Workspace.Selected?.QueryId,
                Is.EqualTo(withTenancy.Workspace.Selected?.QueryId));
            Assert.That(
                without.Workspace.Chart.Plots.Count,
                Is.EqualTo(withTenancy.Workspace.Chart.Plots.Count));
        });
    }

    [Test]
    public async Task With_tenancy_absent_the_caption_stops_naming_a_tenant()
    {
        using var harness = await TelemetryWorkspaceHarness.CreateAsync(
            domain => domain.IsTenancyEnabled = false);

        Assert.Multiple(() =>
        {
            Assert.That(harness.Workspace.Caption.IsDegraded, Is.False);
            Assert.That(harness.Workspace.Caption.Text, Does.Contain("one tenant"));
        });
    }

    [Test]
    public async Task With_tenancy_absent_the_request_still_carries_the_fail_closed_visibility()
    {
        using var harness = await TelemetryWorkspaceHarness.CreateAsync(
            domain => domain.IsTenancyEnabled = false);

        Assert.That(
            harness.Domain.LastRequest!.RequestedVisibility,
            Is.EqualTo(ExplorerTelemetryVisibility.ActiveTenant));
    }

    // ---- the tree filter ---------------------------------------------------

    [Test]
    public async Task The_tree_filter_offers_only_the_trees_the_answer_actually_contained()
    {
        using var harness = await TelemetryWorkspaceHarness.CreateAsync(domain =>
            domain.Result = ExplorerTelemetrySample.Result(
                null,
                ExplorerTelemetrySample.Series("t/acme/orders", ExplorerTelemetrySample.TenantId, 1),
                ExplorerTelemetrySample.Series("t/acme/audit", ExplorerTelemetrySample.TenantId, 2)));

        Assert.That(harness.Workspace.TreeChoices, Is.EqualTo(new[] { "t/acme/audit", "t/acme/orders" }));
    }

    [Test]
    public async Task Narrowing_to_a_tree_redraws_from_the_answer_in_hand_without_a_round_trip()
    {
        using var harness = await TelemetryWorkspaceHarness.CreateAsync(domain =>
            domain.Result = ExplorerTelemetrySample.Result(
                null,
                ExplorerTelemetrySample.Series("t/acme/orders", ExplorerTelemetrySample.TenantId, 1),
                ExplorerTelemetrySample.Series("t/acme/audit", ExplorerTelemetrySample.TenantId, 2)));

        var requests = harness.Domain.Requests.Count;
        harness.Workspace.SelectTree("t/acme/audit");

        Assert.Multiple(() =>
        {
            Assert.That(harness.Workspace.TreeFilter, Is.EqualTo("t/acme/audit"));
            Assert.That(harness.Workspace.Chart.Plots, Has.Count.EqualTo(1));
            Assert.That(harness.Domain.Requests, Has.Count.EqualTo(requests));
        });
    }

    [Test]
    public async Task A_tree_the_answer_did_not_contain_cannot_be_selected()
    {
        using var harness = await TelemetryWorkspaceHarness.CreateAsync();

        harness.Workspace.SelectTree("t/someone/else");

        Assert.That(harness.Workspace.TreeFilter, Is.Null);
    }

    [Test]
    public async Task Clearing_the_tree_filter_draws_every_series_again()
    {
        using var harness = await TelemetryWorkspaceHarness.CreateAsync(domain =>
            domain.Result = ExplorerTelemetrySample.Result(
                null,
                ExplorerTelemetrySample.Series("a", null, 1),
                ExplorerTelemetrySample.Series("b", null, 2)));

        harness.Workspace.SelectTree("a");
        harness.Workspace.SelectTree(string.Empty);

        Assert.Multiple(() =>
        {
            Assert.That(harness.Workspace.TreeFilter, Is.Null);
            Assert.That(harness.Workspace.Chart.Plots, Has.Count.EqualTo(2));
        });
    }

    [Test]
    public async Task A_filter_the_next_answer_no_longer_contains_widens_rather_than_charting_nothing()
    {
        // Keeping it would draw an empty chart and imply the tree had gone
        // quiet, when in fact the answer simply no longer mentions it.
        using var harness = await TelemetryWorkspaceHarness.CreateAsync(domain =>
            domain.Result = ExplorerTelemetrySample.Result(
                null,
                ExplorerTelemetrySample.Series("a", null, 1),
                ExplorerTelemetrySample.Series("b", null, 2)));

        harness.Workspace.SelectTree("b");

        harness.Domain.Result = ExplorerTelemetrySample.Result(
            null,
            ExplorerTelemetrySample.Series("a", null, 1));
        await harness.Workspace.ReevaluateAsync();

        Assert.Multiple(() =>
        {
            Assert.That(harness.Workspace.TreeFilter, Is.Null);
            Assert.That(harness.Workspace.Chart.Plots, Has.Count.EqualTo(1));
        });
    }

    [Test]
    public async Task The_tree_filter_is_never_put_on_a_request()
    {
        using var harness = await TelemetryWorkspaceHarness.CreateAsync(domain =>
            domain.Result = ExplorerTelemetrySample.Result(
                null,
                ExplorerTelemetrySample.Series("a", null, 1),
                ExplorerTelemetrySample.Series("b", null, 2)));

        harness.Workspace.SelectTree("b");
        await harness.Workspace.ReevaluateAsync();

        Assert.That(
            harness.Domain.Requests.Select(request => request.TreeId),
            Is.All.Null,
            "narrowing a drawn chart is a presentation choice, not a server-side parameter");
    }

    [Test]
    public async Task Re_selecting_the_current_tree_announces_nothing()
    {
        using var harness = await TelemetryWorkspaceHarness.CreateAsync(domain =>
            domain.Result = ExplorerTelemetrySample.Result(
                null,
                ExplorerTelemetrySample.Series("a", null, 1),
                ExplorerTelemetrySample.Series("b", null, 2)));

        harness.Workspace.SelectTree("a");
        var changes = harness.ChangeCount;
        harness.Workspace.SelectTree("a");

        Assert.That(harness.ChangeCount, Is.EqualTo(changes));
    }

    [Test]
    public async Task Every_series_the_facade_returned_is_charted_including_another_tenants()
    {
        // The panel filters nothing by tenant. Deciding which tenant's series a
        // caller may see is the facade's job, and a head that narrowed locally
        // would be a head that could be edited not to.
        using var harness = await TelemetryWorkspaceHarness.CreateAsync(domain =>
        {
            domain.RequestedVisibility = ExplorerTelemetryVisibility.AllTenants;
            domain.Result = ExplorerTelemetrySample.Result(
                ExplorerTelemetrySample.CrossTenantScope(),
                ExplorerTelemetrySample.Series("t/acme/orders", "acme", 1),
                ExplorerTelemetrySample.Series("t/globex/orders", "globex", 2),
                ExplorerTelemetrySample.Series("sys-audit", TelemetryLabelNames.PlatformTenant, 3));
        });

        Assert.That(harness.Workspace.Chart.Plots, Has.Count.EqualTo(3));
    }
}
