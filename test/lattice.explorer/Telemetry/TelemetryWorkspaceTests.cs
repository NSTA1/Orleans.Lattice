using Orleans.Lattice.Explorer.Plugins;
using Orleans.Lattice.Explorer.Plugins.Telemetry;
using Orleans.Lattice.Explorer.Plugins.Telemetry.Workspace;

namespace Orleans.Lattice.Explorer.Tests.Telemetry;

/// <summary>
/// The telemetry workspace: catalogue-driven discovery, the bounded controls,
/// and the request they compose.
/// </summary>
/// <remarks>
/// Every test here drives the workspace by calling the method a control calls
/// and asserts on state that is already settled when the call returns. Nothing
/// waits, sleeps, polls, or reads a clock.
/// </remarks>
[TestFixture]
public sealed class TelemetryWorkspaceTests
{
    [Test]
    public void A_null_domain_is_rejected() =>
        Assert.That(
            () => new TelemetryWorkspace(null!, new ExplorerPluginAccessStore()),
            Throws.ArgumentNullException);

    [Test]
    public void A_null_access_store_is_rejected() =>
        Assert.That(
            () => new TelemetryWorkspace(new FakeExplorerTelemetryDomain(), null!),
            Throws.ArgumentNullException);

    [Test]
    public void Before_any_probe_the_surface_is_denied_rather_than_optimistic()
    {
        // The real store is used here on purpose: an unprobed key reads as
        // denied, which is the fail-closed posture a stub could not show.
        var store = new ExplorerPluginAccessStore();
        using var workspace = new TelemetryWorkspace(new FakeExplorerTelemetryDomain(), store);

        Assert.That(workspace.Allowed, Is.False);
    }

    [Test]
    public async Task A_denied_gate_reads_nothing_from_the_cluster()
    {
        using var harness = TelemetryWorkspaceHarness.Create(access: ExplorerPluginAccess.Deny("no"));

        await harness.Workspace.InitializeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(harness.Domain.CatalogReads, Is.Zero);
            Assert.That(harness.Domain.Requests, Is.Empty);
            Assert.That(harness.Workspace.AccessReason, Is.EqualTo("no"));
        });
    }

    [Test]
    public void An_unauthenticated_gate_is_distinguished_from_a_denial()
    {
        using var harness = TelemetryWorkspaceHarness.Create(
            access: ExplorerPluginAccess.RequireAuthentication("sign in"));

        Assert.Multiple(() =>
        {
            Assert.That(harness.Workspace.AuthenticationRequired, Is.True);
            Assert.That(harness.Workspace.Unavailable, Is.False);
        });
    }

    [Test]
    public void An_unavailable_gate_is_distinguished_from_a_denial()
    {
        using var harness = TelemetryWorkspaceHarness.Create(
            access: ExplorerPluginAccess.ReportUnavailable("no facade"));

        Assert.Multiple(() =>
        {
            Assert.That(harness.Workspace.Unavailable, Is.True);
            Assert.That(harness.Workspace.AuthenticationRequired, Is.False);
        });
    }

    [Test]
    public async Task Initialising_discovers_the_catalogue_and_evaluates_the_first_entry()
    {
        using var harness = await TelemetryWorkspaceHarness.CreateAsync();

        Assert.Multiple(() =>
        {
            Assert.That(harness.Domain.CatalogReads, Is.EqualTo(1));
            Assert.That(harness.Workspace.Catalog.Count, Is.EqualTo(2));
            Assert.That(harness.Workspace.Selected?.QueryId, Is.EqualTo(ExplorerTelemetrySample.RangeQueryId));
            Assert.That(harness.Domain.LastRequest?.QueryId, Is.EqualTo(ExplorerTelemetrySample.RangeQueryId));
        });
    }

    [Test]
    public async Task Initialising_twice_discovers_once_so_a_re_render_cannot_re_read_the_cluster()
    {
        using var harness = await TelemetryWorkspaceHarness.CreateAsync();

        await harness.Workspace.InitializeAsync();

        Assert.That(harness.Domain.CatalogReads, Is.EqualTo(1));
    }

    [Test]
    public async Task An_empty_catalogue_selects_nothing_and_sends_nothing()
    {
        using var harness = await TelemetryWorkspaceHarness.CreateAsync(
            domain => domain.Catalog = ExplorerTelemetryCatalog.Empty);

        Assert.Multiple(() =>
        {
            Assert.That(harness.Workspace.Catalog.IsEmpty, Is.True);
            Assert.That(harness.Workspace.Selected, Is.Null);
            Assert.That(harness.Domain.Requests, Is.Empty, "there is no entry to evaluate");
            Assert.That(harness.Workspace.Chart.IsEmpty, Is.True);
        });
    }

    [Test]
    public async Task The_panel_carries_no_query_id_of_its_own_and_can_only_select_what_discovery_returned()
    {
        var offered = ExplorerTelemetrySample.Query("some.server.authored.id");
        using var harness = await TelemetryWorkspaceHarness.CreateAsync(
            domain => domain.Catalog = ExplorerTelemetrySample.Catalog(offered));

        // An id the catalogue does not offer changes nothing: there is no path
        // from a control to a query the server did not publish.
        await harness.Workspace.SelectQueryAsync("something.the.client.made.up");

        Assert.Multiple(() =>
        {
            Assert.That(harness.Workspace.Selected?.QueryId, Is.EqualTo("some.server.authored.id"));
            Assert.That(
                harness.Domain.Requests.Select(request => request.QueryId),
                Is.All.EqualTo("some.server.authored.id"));
        });
    }

    [Test]
    public async Task Selecting_another_offered_entry_evaluates_it()
    {
        using var harness = await TelemetryWorkspaceHarness.CreateAsync();

        await harness.Workspace.SelectQueryAsync(ExplorerTelemetrySample.InstantQueryId);

        Assert.Multiple(() =>
        {
            Assert.That(harness.Workspace.Selected?.QueryId, Is.EqualTo(ExplorerTelemetrySample.InstantQueryId));
            Assert.That(harness.Domain.LastRequest?.QueryId, Is.EqualTo(ExplorerTelemetrySample.InstantQueryId));
        });
    }

    [Test]
    public async Task Re_selecting_the_current_entry_sends_nothing()
    {
        using var harness = await TelemetryWorkspaceHarness.CreateAsync();
        var before = harness.Domain.Requests.Count;

        await harness.Workspace.SelectQueryAsync(ExplorerTelemetrySample.RangeQueryId);

        Assert.That(harness.Domain.Requests, Has.Count.EqualTo(before));
    }

    [Test]
    public async Task Selecting_a_null_query_id_changes_nothing()
    {
        using var harness = await TelemetryWorkspaceHarness.CreateAsync();

        await harness.Workspace.SelectQueryAsync(null);

        Assert.That(harness.Workspace.Selected?.QueryId, Is.EqualTo(ExplorerTelemetrySample.RangeQueryId));
    }

    [Test]
    public async Task The_first_request_leaves_the_window_unset_so_the_facade_supplies_its_own_default()
    {
        // Expanding an unset window client-side to an entry's maximum range
        // overruns its point budget at the default step, which would turn the
        // first request every panel makes into a bounds refusal.
        using var harness = await TelemetryWorkspaceHarness.CreateAsync();

        Assert.That(harness.Domain.LastRequest!.Window.IsUnset, Is.True);
    }

    [Test]
    public async Task Choosing_a_range_makes_the_window_concrete_and_exact()
    {
        using var harness = await TelemetryWorkspaceHarness.CreateAsync();

        await harness.Workspace.SelectRangeAsync("1h");

        var window = harness.Domain.LastRequest!.Window;
        Assert.Multiple(() =>
        {
            Assert.That(window.EndUtc, Is.EqualTo(ExplorerTelemetrySample.Now));
            Assert.That(window.StartUtc, Is.EqualTo(ExplorerTelemetrySample.Now.AddHours(-1)));
            Assert.That(window.Step, Is.EqualTo(TimeSpan.Zero), "no step was chosen, so the facade picks one");
        });
    }

    [Test]
    public async Task Choosing_a_step_puts_it_on_the_window()
    {
        using var harness = await TelemetryWorkspaceHarness.CreateAsync();

        await harness.Workspace.SelectRangeAsync("1h");
        await harness.Workspace.SelectStepAsync("5m");

        Assert.That(harness.Domain.LastRequest!.Window.Step, Is.EqualTo(TimeSpan.FromMinutes(5)));
    }

    [Test]
    public async Task A_range_label_that_was_never_offered_falls_back_to_the_server_default()
    {
        using var harness = await TelemetryWorkspaceHarness.CreateAsync();
        await harness.Workspace.SelectRangeAsync("1h");

        // The sample bounds cap the range at six hours, so "7d" was never in the
        // control. Editing it into the DOM must not reach the facade.
        await harness.Workspace.SelectRangeAsync("7d");

        Assert.Multiple(() =>
        {
            Assert.That(harness.Workspace.Range, Is.EqualTo(TimeSpan.Zero));
            Assert.That(harness.Domain.LastRequest!.Window.IsUnset, Is.True);
        });
    }

    [Test]
    public async Task An_entry_that_declares_no_time_range_never_receives_a_concrete_window()
    {
        using var harness = await TelemetryWorkspaceHarness.CreateAsync();
        await harness.Workspace.SelectRangeAsync("1h");

        await harness.Workspace.SelectQueryAsync(ExplorerTelemetrySample.InstantQueryId);

        Assert.That(
            harness.Domain.LastRequest!.Window.IsUnset,
            Is.True,
            "a window on an entry that does not accept one is a parameter the facade would discard");
    }

    [Test]
    public async Task Switching_to_an_entry_whose_bounds_exclude_the_chosen_step_drops_it_rather_than_sending_it()
    {
        var narrow = ExplorerTelemetrySample.Query(
            "narrow",
            ExplorerTelemetryParameters.TimeRange | ExplorerTelemetryParameters.Step,
            ExplorerTelemetrySample.Bounds(
                minStep: TimeSpan.FromHours(1),
                maxStep: TimeSpan.FromHours(6),
                maxRange: TimeSpan.FromDays(2),
                maxLookback: TimeSpan.FromDays(7)));

        using var harness = await TelemetryWorkspaceHarness.CreateAsync(
            domain => domain.Catalog = ExplorerTelemetrySample.Catalog(ExplorerTelemetrySample.Query(), narrow));

        await harness.Workspace.SelectStepAsync("1m");
        await harness.Workspace.SelectQueryAsync("narrow");

        Assert.Multiple(() =>
        {
            Assert.That(harness.Workspace.Step, Is.EqualTo(TimeSpan.Zero));
            Assert.That(harness.Domain.LastRequest!.Window.Step, Is.EqualTo(TimeSpan.Zero));
        });
    }

    [Test]
    public async Task Choosing_a_step_re_derives_the_legal_ranges()
    {
        var budgeted = ExplorerTelemetrySample.Query(
            "budgeted",
            ExplorerTelemetryParameters.TimeRange | ExplorerTelemetryParameters.Step,
            ExplorerTelemetrySample.Bounds(
                minStep: TimeSpan.FromSeconds(15),
                maxRange: TimeSpan.FromDays(7),
                maxLookback: TimeSpan.FromDays(7),
                maxPoints: 240));

        using var harness = await TelemetryWorkspaceHarness.CreateAsync(
            domain => domain.Catalog = ExplorerTelemetrySample.Catalog(budgeted));

        var atDefault = harness.Workspace.RangeChoices.Count;
        await harness.Workspace.SelectStepAsync("15s");

        Assert.That(harness.Workspace.RangeChoices.Count, Is.LessThan(atDefault));
    }

    [Test]
    public async Task Re_selecting_the_same_range_sends_nothing()
    {
        using var harness = await TelemetryWorkspaceHarness.CreateAsync();
        await harness.Workspace.SelectRangeAsync("1h");
        var before = harness.Domain.Requests.Count;

        await harness.Workspace.SelectRangeAsync("1h");

        Assert.That(harness.Domain.Requests, Has.Count.EqualTo(before));
    }

    [Test]
    public async Task Refreshing_re_reads_the_catalogue_and_re_evaluates()
    {
        using var harness = await TelemetryWorkspaceHarness.CreateAsync();
        var requests = harness.Domain.Requests.Count;

        await harness.Workspace.RefreshAsync();

        Assert.Multiple(() =>
        {
            Assert.That(harness.Domain.CatalogRefreshes, Is.EqualTo(1));
            Assert.That(harness.Domain.Requests, Has.Count.EqualTo(requests + 1));
        });
    }

    [Test]
    public async Task Re_evaluating_does_not_re_read_the_catalogue()
    {
        using var harness = await TelemetryWorkspaceHarness.CreateAsync();

        await harness.Workspace.ReevaluateAsync();

        Assert.Multiple(() =>
        {
            Assert.That(harness.Domain.CatalogRefreshes, Is.Zero);
            Assert.That(harness.Domain.CatalogReads, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task Refreshing_keeps_a_selection_the_cluster_still_offers()
    {
        using var harness = await TelemetryWorkspaceHarness.CreateAsync();
        await harness.Workspace.SelectQueryAsync(ExplorerTelemetrySample.InstantQueryId);

        await harness.Workspace.RefreshAsync();

        Assert.That(harness.Workspace.Selected?.QueryId, Is.EqualTo(ExplorerTelemetrySample.InstantQueryId));
    }

    [Test]
    public async Task Refreshing_re_takes_the_entry_from_the_new_catalogue_so_changed_bounds_apply()
    {
        using var harness = await TelemetryWorkspaceHarness.CreateAsync();

        harness.Domain.Catalog = ExplorerTelemetrySample.Catalog(
            ExplorerTelemetrySample.Query(bounds: ExplorerTelemetrySample.Bounds(maxRange: TimeSpan.FromMinutes(15))));

        await harness.Workspace.RefreshAsync();

        Assert.That(
            harness.Workspace.RangeChoices.All(choice => choice.Duration <= TimeSpan.FromMinutes(15)),
            Is.True);
    }

    [Test]
    public async Task A_selection_the_cluster_stopped_offering_falls_back_to_the_first_entry()
    {
        using var harness = await TelemetryWorkspaceHarness.CreateAsync();
        await harness.Workspace.SelectQueryAsync(ExplorerTelemetrySample.InstantQueryId);

        harness.Domain.Catalog = ExplorerTelemetrySample.Catalog(ExplorerTelemetrySample.Query());
        await harness.Workspace.RefreshAsync();

        Assert.That(harness.Workspace.Selected?.QueryId, Is.EqualTo(ExplorerTelemetrySample.RangeQueryId));
    }

    [Test]
    public async Task A_failed_discovery_leaves_the_previous_catalogue_in_place()
    {
        using var harness = await TelemetryWorkspaceHarness.CreateAsync();

        harness.Domain.CatalogFailure = TelemetryOperationResult<ExplorerTelemetryCatalog>.Failure(
            TelemetryQueryStatus.BackendUnavailable,
            "the store is down");
        await harness.Workspace.RefreshAsync();

        Assert.Multiple(() =>
        {
            Assert.That(harness.Workspace.Catalog.Count, Is.EqualTo(2), "a transient outage must not empty the picker");
            Assert.That(harness.Workspace.Notice, Is.Not.Null);
            Assert.That(harness.Workspace.Notice!.IsRetryable, Is.True);
        });
    }

    [Test]
    public async Task A_failed_evaluation_leaves_the_previous_chart_in_place_and_says_what_happened()
    {
        using var harness = await TelemetryWorkspaceHarness.CreateAsync();
        var plots = harness.Workspace.Chart.Plots.Count;

        harness.Domain.QueryFailure = TelemetryOperationResult<ExplorerTelemetryResult>.Failure(
            TelemetryQueryStatus.BackendUnavailable,
            "the store is down");
        await harness.Workspace.ReevaluateAsync();

        Assert.Multiple(() =>
        {
            Assert.That(harness.Workspace.Chart.Plots, Has.Count.EqualTo(plots));
            Assert.That(harness.Workspace.Notice!.Message, Is.EqualTo("the store is down"));
        });
    }

    [Test]
    public async Task A_successful_evaluation_clears_the_previous_notice()
    {
        using var harness = await TelemetryWorkspaceHarness.CreateAsync();
        harness.Domain.QueryFailure = TelemetryOperationResult<ExplorerTelemetryResult>.Failure(
            TelemetryQueryStatus.BackendUnavailable,
            "down");
        await harness.Workspace.ReevaluateAsync();

        harness.Domain.QueryFailure = null;
        await harness.Workspace.ReevaluateAsync();

        Assert.That(harness.Workspace.Notice, Is.Null);
    }

    [Test]
    public async Task Every_operation_announces_a_change_so_the_view_re_renders()
    {
        using var harness = await TelemetryWorkspaceHarness.CreateAsync();

        Assert.That(harness.ChangeCount, Is.GreaterThan(0));
    }

    [Test]
    public async Task The_busy_flag_is_cleared_once_an_operation_settles()
    {
        using var harness = await TelemetryWorkspaceHarness.CreateAsync();

        await harness.Workspace.RefreshAsync();

        Assert.That(harness.Workspace.Busy, Is.False);
    }

    [Test]
    public async Task A_gate_that_opens_after_the_first_render_loads_without_a_manual_refresh()
    {
        using var harness = TelemetryWorkspaceHarness.Create(access: ExplorerPluginAccess.Denied);

        await harness.Workspace.InitializeAsync();
        harness.Store.Set(TelemetryPluginKeys.PluginId, ExplorerPluginAccess.Allowed);

        Assert.Multiple(() =>
        {
            Assert.That(harness.Workspace.Allowed, Is.True);
            Assert.That(harness.Domain.CatalogReads, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task Another_plugins_gate_changing_does_not_touch_this_surface()
    {
        using var harness = await TelemetryWorkspaceHarness.CreateAsync();
        var changes = harness.ChangeCount;

        harness.Store.Set("orleans.lattice.somethingelse", ExplorerPluginAccess.Denied);

        Assert.That(harness.ChangeCount, Is.EqualTo(changes));
    }

    [Test]
    public async Task Disposing_unsubscribes_so_a_later_gate_change_touches_nothing()
    {
        var harness = await TelemetryWorkspaceHarness.CreateAsync();
        harness.Dispose();
        var changes = harness.ChangeCount;

        harness.Store.Set(TelemetryPluginKeys.PluginId, ExplorerPluginAccess.Denied);

        Assert.That(harness.ChangeCount, Is.EqualTo(changes));
    }

    [Test]
    public async Task A_failed_evaluation_after_a_selection_change_blanks_rather_than_mislabelling_the_old_data()
    {
        // The board captions the chart with the SELECTED entry's title,
        // description, and unit. Keeping the previous query's series under that
        // heading would present bytes as a ratio - a chart unambiguously
        // labelled as data it is not.
        using var harness = await TelemetryWorkspaceHarness.CreateAsync();
        Assert.That(harness.Workspace.Chart.IsEmpty, Is.False);

        harness.Domain.QueryFailure = TelemetryOperationResult<ExplorerTelemetryResult>.Failure(
            TelemetryQueryStatus.BackendUnavailable,
            "the store is down");
        await harness.Workspace.SelectQueryAsync(ExplorerTelemetrySample.InstantQueryId);

        Assert.Multiple(() =>
        {
            Assert.That(harness.Workspace.Chart.IsEmpty, Is.True);
            Assert.That(harness.Workspace.Result, Is.Null);
            Assert.That(harness.Workspace.Scope, Is.EqualTo(ExplorerTelemetryScope.None));
            Assert.That(harness.Workspace.Notice!.Message, Is.EqualTo("the store is down"));
        });
    }

    [Test]
    public async Task A_failed_evaluation_after_a_window_change_blanks_the_previous_windows_chart()
    {
        using var harness = await TelemetryWorkspaceHarness.CreateAsync();

        harness.Domain.QueryFailure = TelemetryOperationResult<ExplorerTelemetryResult>.Failure(
            TelemetryQueryStatus.OutOfBounds,
            "too long",
            ExplorerTelemetryBoundsViolation.RangeTooLong);
        await harness.Workspace.SelectRangeAsync("1h");

        Assert.Multiple(() =>
        {
            Assert.That(
                harness.Workspace.Chart.IsEmpty,
                Is.True,
                "the Range control now reads 1h; the chart below must not still be the default window's");
            Assert.That(harness.Workspace.Notice!.Guidance, Does.Contain("shorter time range"));
        });
    }

    [Test]
    public async Task A_stale_answer_that_lands_after_a_newer_request_is_discarded()
    {
        // Two evaluations overlap when a shell tenant change lands mid-select.
        // Applying them in completion order lets the slower, older one win.
        using var harness = await TelemetryWorkspaceHarness.CreateAsync();

        var gate = new TaskCompletionSource();
        harness.Domain.Gate = gate.Task;
        var slow = harness.Workspace.SelectQueryAsync(ExplorerTelemetrySample.InstantQueryId);

        // The newer request is issued and completes first.
        harness.Domain.Gate = null;
        harness.Domain.Result = ExplorerTelemetrySample.Result(
            null,
            ExplorerTelemetrySample.Series("newest", null, 9));
        await harness.Workspace.ReevaluateAsync();

        var newest = harness.Workspace.Chart.Plots[0].Label;

        // Only now does the older one land.
        gate.SetResult();
        await slow;

        Assert.That(
            harness.Workspace.Chart.Plots[0].Label,
            Is.EqualTo(newest),
            "an answer to a superseded request must not overwrite a newer one");
    }

    [Test]
    public async Task A_gate_that_closes_and_re_opens_after_loading_re_renders_rather_than_wedging_on_the_denial()
    {
        // The token-expiry-then-sign-in path. InitializeAsync is idempotent, so
        // a surface that has already loaded must fall through to a re-render;
        // otherwise the denied message stays on screen with a full catalogue in
        // hand and only a re-navigation clears it.
        using var harness = await TelemetryWorkspaceHarness.CreateAsync();

        harness.Store.Set(TelemetryPluginKeys.PluginId, ExplorerPluginAccess.RequireAuthentication("expired"));
        var afterClose = harness.ChangeCount;
        harness.Store.Set(TelemetryPluginKeys.PluginId, ExplorerPluginAccess.Allowed);

        Assert.Multiple(() =>
        {
            Assert.That(harness.Workspace.Allowed, Is.True);
            Assert.That(harness.Workspace.AuthenticationRequired, Is.False);
            Assert.That(
                harness.ChangeCount,
                Is.GreaterThan(afterClose),
                "the re-open must announce a change or the denied body stays rendered");
        });
    }

    [Test]
    public async Task The_instrument_list_is_composed_once_per_selection_rather_than_per_render()
    {
        using var harness = await TelemetryWorkspaceHarness.CreateAsync();

        Assert.Multiple(() =>
        {
            Assert.That(harness.Workspace.SelectedInstruments, Is.EqualTo("lattice.write.committed"));
            Assert.That(
                harness.Workspace.SelectedInstruments,
                Is.SameAs(harness.Workspace.SelectedInstruments),
                "a joined list that cannot change between renders must not be rebuilt on each one");
        });
    }

    [Test]
    public async Task An_entry_naming_several_instruments_lists_them_all()
    {
        var query = ExplorerTelemetrySample.Query(instruments:
        [
            new ExplorerTelemetryInstrument("a", "m", "u", ExplorerTelemetrySemantic.Level),
            new ExplorerTelemetryInstrument("b", "m", "u", ExplorerTelemetrySemantic.Level),
        ]);

        using var harness = await TelemetryWorkspaceHarness.CreateAsync(
            domain => domain.Catalog = ExplorerTelemetrySample.Catalog(query));

        Assert.That(harness.Workspace.SelectedInstruments, Is.EqualTo("a, b"));
    }

    [Test]
    public async Task An_entry_naming_no_instrument_reports_none()
    {
        using var harness = await TelemetryWorkspaceHarness.CreateAsync(
            domain => domain.Catalog = ExplorerTelemetrySample.Catalog(
                ExplorerTelemetrySample.Query(instruments: [])));

        Assert.That(harness.Workspace.SelectedInstruments, Is.Null);
    }
}
