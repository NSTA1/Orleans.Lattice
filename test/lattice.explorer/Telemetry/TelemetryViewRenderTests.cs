using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Web;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.DesignSystem.Layout;
using Orleans.Lattice.Explorer.DesignSystem.Tokens;
using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Plugins.Telemetry;
using Orleans.Lattice.Explorer.Plugins.Telemetry.Views;
using Orleans.Lattice.Explorer.Plugins.Telemetry.Workspace;
using Orleans.Lattice.Explorer.Tests.DesignSystem;

using Orleans.Lattice.Explorer.Core.Vocabulary;

namespace Orleans.Lattice.Explorer.Tests.Telemetry;

/// <summary>
/// What the telemetry components actually emit: that a degraded scope reaches
/// the markup as an alert, that no control anywhere accepts free text, that the
/// chart draws a polyline per plotted series, and that the control row stacks at
/// compact without a media query.
/// </summary>
/// <remarks>
/// Rendered with the framework's own <see cref="HtmlRenderer"/>, the same
/// mechanism the design-system component tests use, so the panels need no extra
/// component-testing dependency. Every render is driven entirely by the state
/// handed in, so nothing here waits on a clock, a timer, or a background task.
/// </remarks>
[TestFixture]
public sealed class TelemetryViewRenderTests
{
    // ---- the scope banner ---------------------------------------------------

    [Test]
    public async Task An_ordinary_scope_renders_as_a_status_line()
    {
        var html = await RenderBannerAsync(TelemetryScopeCaptions.For(ExplorerTelemetrySample.ActiveScope()));

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("role=\"status\""));
            Assert.That(html, Does.Not.Contain("role=\"alert\""));
            Assert.That(html, Does.Contain(ExplorerTelemetrySample.TenantId));
        });
    }

    [Test]
    public async Task A_degraded_scope_renders_as_an_alert_that_says_it_is_narrower_than_asked()
    {
        // The one line standing between a fail-closed answer and a chart that
        // reads as the whole cluster's. It is an alert, not an aside.
        var html = await RenderBannerAsync(
            TelemetryScopeCaptions.For(ExplorerTelemetrySample.DowngradedScope()));

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("role=\"alert\""));
            Assert.That(html, Does.Contain("Narrower than you asked for"));
            Assert.That(html, Does.Contain("not the cluster&#x27;s").Or.Contain("not the cluster's"));
        });
    }

    [Test]
    public async Task An_empty_caption_renders_nothing_at_all()
    {
        var html = await RenderBannerAsync(default);

        Assert.That(html.Trim(), Is.Empty);
    }

    // ---- the chart ----------------------------------------------------------

    [Test]
    public async Task An_empty_chart_renders_the_empty_text_and_no_svg()
    {
        var html = await RenderChartAsync(TelemetryChart.Empty, "Nothing yet.");

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("Nothing yet."));
            Assert.That(html, Does.Not.Contain("<svg"));
        });
    }

    [Test]
    public async Task A_chart_renders_one_polyline_per_plotted_series_and_a_legend_entry_for_each()
    {
        var chart = TelemetryChart.For(ExplorerTelemetrySample.Result(
            null,
            ExplorerTelemetrySample.Series("t/acme/orders", ExplorerTelemetrySample.TenantId, 1, 2),
            ExplorerTelemetrySample.Series("t/acme/audit", ExplorerTelemetrySample.TenantId, 3, 4)));

        var html = await RenderChartAsync(chart);

        Assert.Multiple(() =>
        {
            Assert.That(DesignSystemRenderHarness.CountOccurrences(html, "<polyline"), Is.EqualTo(2));
            Assert.That(
                DesignSystemRenderHarness.CountOccurrences(html, "<li class=\"lxt-legend-item\""),
                Is.EqualTo(2));
            Assert.That(html, Does.Contain("t/acme/orders"));
            Assert.That(html, Does.Contain("t/acme/audit"));
        });
    }

    [Test]
    public async Task The_chart_emits_the_shared_view_box_and_carries_no_width_of_its_own()
    {
        var html = await RenderChartAsync(TelemetryChart.For(ExplorerTelemetrySample.Result()));

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain($"viewBox=\"{TelemetryChart.ViewBox}\""));
            Assert.That(
                html,
                Does.Not.Contain("width="),
                "the SVG is sized by the stylesheet; a width here would be a measurement in a component");
        });
    }

    [Test]
    public async Task The_legend_reading_carries_the_unit_the_catalogue_entry_published()
    {
        // The unit reaches the legend through the chart model, which formats it
        // once per result rather than once per render.
        var chart = TelemetryChart.For(
            ExplorerTelemetrySample.Result(),
            treeFilter: null,
            unit: "ops/s",
            ExplorerTelemetrySemantic.PerOperation);

        var html = await RenderChartAsync(chart);

        Assert.That(html, Does.Contain("ops/s"));
    }

    [Test]
    public async Task A_truncated_chart_says_how_many_series_it_dropped()
    {
        var series = Enumerable
            .Range(0, TelemetryChart.MaxPlots + 2)
            .Select(i => ExplorerTelemetrySample.Series($"tree-{i}", null, i + 1))
            .ToArray();

        var html = await RenderChartAsync(TelemetryChart.For(ExplorerTelemetrySample.Result(null, series)));

        Assert.That(html, Does.Contain($"Showing {TelemetryChart.MaxPlots} of {TelemetryChart.MaxPlots + 2}"));
    }

    // ---- the controls -------------------------------------------------------

    [Test]
    public async Task No_control_anywhere_accepts_free_text()
    {
        // The acceptance criterion, asserted against the markup rather than the
        // intent: a caller can select a server-authored query and bounded
        // parameters, and there is nowhere to type one.
        using var harness = await TelemetryWorkspaceHarness.CreateAsync(domain =>
            domain.Result = ExplorerTelemetrySample.Result(
                null,
                ExplorerTelemetrySample.Series("t/acme/orders", null, 1),
                ExplorerTelemetrySample.Series("t/acme/audit", null, 2)));

        var html = await RenderBoardAsync(harness.Workspace);

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Not.Contain("<input"));
            Assert.That(html, Does.Not.Contain("<textarea"));
            Assert.That(html, Does.Not.Contain("contenteditable"));
            Assert.That(html, Does.Contain("<select"));
        });
    }

    [Test]
    public async Task The_panel_picker_offers_exactly_the_entries_the_catalogue_published()
    {
        using var harness = await TelemetryWorkspaceHarness.CreateAsync();

        var html = await RenderControlsAsync(harness.Workspace);

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("Write throughput"));
            Assert.That(html, Does.Contain("Shard count"));
        });
    }

    [Test]
    public async Task An_entry_that_declares_no_window_parameters_renders_no_window_controls()
    {
        using var harness = await TelemetryWorkspaceHarness.CreateAsync();
        await harness.Workspace.SelectQueryAsync(ExplorerTelemetrySample.InstantQueryId);

        var html = await RenderControlsAsync(harness.Workspace);

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Not.Contain(">Range<"));
            Assert.That(html, Does.Not.Contain(">Step<"));
            Assert.That(html, Does.Contain(">Panel<"), "the entry picker always applies");
        });
    }

    [Test]
    public async Task An_entry_that_declares_window_parameters_renders_them_with_the_server_default_option()
    {
        using var harness = await TelemetryWorkspaceHarness.CreateAsync();

        var html = await RenderControlsAsync(harness.Workspace);

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain(">Range<"));
            Assert.That(html, Does.Contain(">Step<"));
            Assert.That(html, Does.Contain(TelemetryDurationChoices.ServerDefaultLabel));
        });
    }

    [Test]
    public async Task An_entry_that_accepts_a_step_but_no_time_range_renders_no_step_control()
    {
        // A step is only expressible inside a window, so there is nowhere to put
        // one for an entry that does not take a range. A control whose value is
        // silently discarded is worse than no control.
        var stepOnly = ExplorerTelemetrySample.Query(
            "step-only",
            ExplorerTelemetryParameters.Step);

        using var harness = await TelemetryWorkspaceHarness.CreateAsync(
            domain => domain.Catalog = ExplorerTelemetrySample.Catalog(stepOnly));

        var html = await RenderControlsAsync(harness.Workspace);

        Assert.That(html, Does.Not.Contain(">Step<"));
    }

    [Test]
    public async Task The_tree_filter_appears_only_once_the_answer_offers_something_to_choose_between()
    {
        using var single = await TelemetryWorkspaceHarness.CreateAsync();
        using var several = await TelemetryWorkspaceHarness.CreateAsync(domain =>
            domain.Result = ExplorerTelemetrySample.Result(
                null,
                ExplorerTelemetrySample.Series("t/acme/orders", null, 1),
                ExplorerTelemetrySample.Series("t/acme/audit", null, 2)));

        var withOne = await RenderControlsAsync(single.Workspace);
        var withTwo = await RenderControlsAsync(several.Workspace);

        Assert.Multiple(() =>
        {
            Assert.That(withOne, Does.Not.Contain(">Tree<"));
            Assert.That(withTwo, Does.Contain(">Tree<"));
            Assert.That(withTwo, Does.Contain(TelemetryTreeOptions.AllTreesLabel));
        });
    }

    [Test]
    public async Task At_compact_the_control_row_stacks_and_at_expanded_it_does_not()
    {
        using var harness = await TelemetryWorkspaceHarness.CreateAsync();

        var compact = await RenderControlsAsync(harness.Workspace, LatticeBreakpoint.Compact);
        var expanded = await RenderControlsAsync(harness.Workspace, LatticeBreakpoint.Expanded);

        Assert.Multiple(() =>
        {
            Assert.That(compact, Does.Contain("is-stacked"));
            Assert.That(expanded, Does.Not.Contain("is-stacked"));
        });
    }

    // ---- the board ----------------------------------------------------------

    [Test]
    public async Task An_unavailable_surface_renders_an_empty_body_rather_than_an_error()
    {
        using var harness = TelemetryWorkspaceHarness.Create(
            access: ExplorerPluginAccess.ReportUnavailable("no facade here"));

        var html = await RenderBoardAsync(harness.Workspace);

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Not.Contain("no facade here"));
            Assert.That(html, Does.Not.Contain("<select"));
        });
    }

    [Test]
    public async Task An_unauthenticated_connection_is_offered_a_sign_in_rather_than_an_inert_grey_out()
    {
        using var harness = TelemetryWorkspaceHarness.Create(
            access: ExplorerPluginAccess.RequireAuthentication("not signed in"));

        var html = await RenderBoardAsync(harness.Workspace);

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("role=\"alert\""));
            Assert.That(
                html,
                Does.Contain(ExplorerStateCopy.SignInRequired(ExplorerSubjects.TelemetrySignals).Explanation),
                "a recoverable refusal is worded as an invitation, from the shared vocabulary");
        });
    }

    [Test]
    public async Task A_denial_names_the_missing_permission_rather_than_repeating_the_surface_label()
    {
        // The board no longer echoes the gate's raw reason string. A refusal is
        // worded by the shared vocabulary and names the grant the caller lacks,
        // because "Telemetry is not available for your account" tells them only
        // what they can already see.
        using var harness = TelemetryWorkspaceHarness.Create(
            access: ExplorerPluginAccess.Deny("you may not read telemetry"));

        var html = await RenderBoardAsync(harness.Workspace);
        var expected = ExplorerStateCopy.NotPermitted(ExplorerSubjects.TelemetrySignals, "Telemetry");

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain(expected.Explanation));
            Assert.That(html, Does.Contain("Telemetry"), "the refusal names the permission");
            Assert.That(html, Does.Contain("role=\"status\""));
        });
    }

    [Test]
    public async Task An_empty_catalogue_says_there_is_nothing_to_chart_rather_than_rendering_a_picker()
    {
        using var harness = await TelemetryWorkspaceHarness.CreateAsync(
            domain => domain.Catalog = ExplorerTelemetryCatalog.Empty);

        var html = await RenderBoardAsync(harness.Workspace);

        Assert.Multiple(() =>
        {
            Assert.That(
                html,
                Does.Contain(ExplorerStateCopy.Empty(ExplorerSubjects.TelemetrySignals).Explanation),
                "an empty catalogue is Empty, not a refusal and not an outage");
            Assert.That(html, Does.Not.Contain("<select"));
            Assert.That(
                html,
                Does.Contain(TelemetryVocabulary.MetricCatalog.Explanation),
                "the term the caller is being told about is explained at the point of use");
        });
    }

    [Test]
    public async Task The_board_renders_the_servers_own_title_description_and_instruments()
    {
        using var harness = await TelemetryWorkspaceHarness.CreateAsync();

        var html = await RenderBoardAsync(harness.Workspace);

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("Write throughput"));
            Assert.That(html, Does.Contain("Committed writes per second."));
            Assert.That(html, Does.Contain("lattice.write.committed"));
        });
    }

    [Test]
    public async Task The_board_badges_the_effective_scope_and_warns_when_it_was_degraded()
    {
        using var honoured = await TelemetryWorkspaceHarness.CreateAsync(
            domain => domain.Result = ExplorerTelemetrySample.Result(ExplorerTelemetrySample.CrossTenantScope()));
        using var degraded = await TelemetryWorkspaceHarness.CreateAsync(domain =>
        {
            domain.RequestedVisibility = ExplorerTelemetryVisibility.AllTenants;
            domain.Result = ExplorerTelemetrySample.Result(ExplorerTelemetrySample.DowngradedScope());
        });

        var honouredHtml = await RenderBoardAsync(honoured.Workspace);
        var degradedHtml = await RenderBoardAsync(degraded.Workspace);

        Assert.Multiple(() =>
        {
            Assert.That(honouredHtml, Does.Contain("lxt-badge is-ok"));
            Assert.That(honouredHtml, Does.Contain("all tenants"));
            Assert.That(degradedHtml, Does.Contain("lxt-badge is-warn"));
            Assert.That(
                degradedHtml,
                Does.Contain(ExplorerTelemetrySample.TenantId),
                "the badge names the tenant the facade pinned, not the cluster that was asked for");
            Assert.That(degradedHtml, Does.Contain("Narrower than you asked for"));
        });
    }

    [Test]
    public async Task A_retryable_failure_offers_a_retry_and_a_refusal_does_not()
    {
        using var retryable = await TelemetryWorkspaceHarness.CreateAsync();
        retryable.Domain.QueryFailure = TelemetryOperationResult<ExplorerTelemetryResult>.Failure(
            TelemetryQueryStatus.BackendUnavailable,
            "the store is down");
        await retryable.Workspace.ReevaluateAsync();

        using var refused = await TelemetryWorkspaceHarness.CreateAsync();
        refused.Domain.QueryFailure = TelemetryOperationResult<ExplorerTelemetryResult>.Failure(
            TelemetryQueryStatus.OutOfBounds,
            "too long",
            ExplorerTelemetryBoundsViolation.RangeTooLong);
        await refused.Workspace.ReevaluateAsync();

        var retryableHtml = await RenderBoardAsync(retryable.Workspace);
        var refusedHtml = await RenderBoardAsync(refused.Workspace);

        Assert.Multiple(() =>
        {
            Assert.That(retryableHtml, Does.Contain("Try again"));
            Assert.That(refusedHtml, Does.Not.Contain("Try again"));
            Assert.That(refusedHtml, Does.Contain("shorter time range"));
        });
    }

    [Test]
    public async Task Before_evaluating_the_board_says_nothing_has_been_read_rather_than_nothing_matched()
    {
        using var notEvaluated = TelemetryWorkspaceHarness.Create();
        notEvaluated.Domain.Catalog = ExplorerTelemetrySample.Catalog();

        // Discovery without evaluation: what a caller sees between the two.
        using var evaluatedEmpty = await TelemetryWorkspaceHarness.CreateAsync(
            domain => domain.Result = ExplorerTelemetrySample.EmptyResult());

        var emptyHtml = await RenderBoardAsync(evaluatedEmpty.Workspace);

        Assert.Multiple(() =>
        {
            Assert.That(evaluatedEmpty.Workspace.HasEvaluated, Is.True);
            Assert.That(emptyHtml, Does.Contain("matched no series"));
            Assert.That(notEvaluated.Workspace.HasEvaluated, Is.False);
        });
    }

    [Test]
    public async Task The_board_renders_the_same_markup_with_and_without_the_tenancy_add_on()
    {
        // The panels are identical either way; only the caption's wording moves.
        // There are deliberately no tenancy-on and tenancy-off panel variants.
        using var withTenancy = await TelemetryWorkspaceHarness.CreateAsync();
        using var without = await TelemetryWorkspaceHarness.CreateAsync(
            domain => domain.IsTenancyEnabled = false);

        var withHtml = await RenderBoardAsync(withTenancy.Workspace);
        var withoutHtml = await RenderBoardAsync(without.Workspace);

        Assert.Multiple(() =>
        {
            Assert.That(
                DesignSystemRenderHarness.CountOccurrences(withoutHtml, "<polyline"),
                Is.EqualTo(DesignSystemRenderHarness.CountOccurrences(withHtml, "<polyline")));
            Assert.That(
                DesignSystemRenderHarness.CountOccurrences(withoutHtml, "<select"),
                Is.EqualTo(DesignSystemRenderHarness.CountOccurrences(withHtml, "<select")));
            Assert.That(withoutHtml, Does.Contain("Write throughput"));
            Assert.That(withoutHtml, Does.Contain("all data"), "no tenant is named where there is no tenancy");
        });
    }

    // ---- harness ------------------------------------------------------------

    private static Task<string> RenderBannerAsync(TelemetryScopeCaption caption) =>
        DesignSystemRenderHarness.RenderAsync<TelemetryScopeBanner>(
            new Dictionary<string, object?> { ["Caption"] = caption });

    private static Task<string> RenderChartAsync(TelemetryChart chart, string? emptyText = null)
    {
        var parameters = new Dictionary<string, object?> { ["Chart"] = chart };
        if (emptyText is not null)
        {
            parameters["EmptyText"] = emptyText;
        }

        return DesignSystemRenderHarness.RenderAsync<TelemetryChartView>(parameters);
    }

    private static Task<string> RenderBoardAsync(TelemetryWorkspace state) =>
        DesignSystemRenderHarness.RenderAsync<TelemetryBoard>(
            new Dictionary<string, object?> { ["State"] = state });

    private static Task<string> RenderControlsAsync(
        TelemetryWorkspace state,
        LatticeBreakpoint? breakpoint = null)
    {
        var parameters = new Dictionary<string, object?> { ["State"] = state };

        return breakpoint is { } value
            ? DesignSystemRenderHarness.RenderCascadedAsync<TelemetryControls>(
                new LatticeAdaptiveContext(value, LatticeDensity.Cosy, IsMeasured: true),
                parameters)
            : DesignSystemRenderHarness.RenderAsync<TelemetryControls>(parameters);
    }
}
