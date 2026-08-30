namespace Orleans.Lattice.Explorer.Plugins.Telemetry.Workspace;

/// <summary>
/// The workspace's evaluation half: the bounded controls, the request they
/// compose, and the result and scope the facade answered with.
/// </summary>
/// <remarks>
/// <para>
/// <b>There is no field here a caller types into.</b> The range and the step are
/// chosen from the ladders the selected entry's own bounds admitted, and the
/// tree filter from the trees the last answer actually contained. Nothing
/// composes a query, and nothing accepts query text - the request carries a
/// catalogue id and bounded parameters, and there is no other shape it could
/// take.
/// </para>
/// <para>
/// <b>What the facade pinned is what is rendered.</b> The scope on the response
/// is copied straight onto <see cref="Scope"/> and captioned from there. The
/// requested visibility is never rendered as though it were the effective one,
/// which is what stops a fail-closed degrade from being invisible.
/// </para>
/// </remarks>
public sealed partial class TelemetryWorkspace
{
    private ExplorerTelemetryResult? _result;

    /// <summary>
    /// What the chart currently on screen is <em>of</em>: the request that
    /// produced it. Compared against a failed request to decide whether keeping
    /// the chart is honest or misleading.
    /// </summary>
    private (string QueryId, ExplorerTelemetryWindow Window) _shown;

    /// <summary>
    /// Monotonic request number. A result is only applied when it is the newest
    /// one asked for, so two overlapping evaluations - a fast double-select, or
    /// a shell tenant change landing mid-select - apply in request order rather
    /// than in completion order.
    /// </summary>
    private int _requestNumber;

    /// <summary>
    /// The selected time range, or <see cref="TimeSpan.Zero"/> to let the
    /// facade choose the entry's own default window.
    /// </summary>
    public TimeSpan Range { get; private set; }

    /// <summary>
    /// The selected step, or <see cref="TimeSpan.Zero"/> to let the facade
    /// choose the entry's own default resolution.
    /// </summary>
    public TimeSpan Step { get; private set; }

    /// <summary>
    /// The single tree the chart is narrowed to, or <see langword="null"/> to
    /// draw every series the facade returned.
    /// </summary>
    public string? TreeFilter { get; private set; }

    /// <summary>
    /// The last evaluated result, or <see langword="null"/> before the first
    /// evaluation completes.
    /// </summary>
    public ExplorerTelemetryResult? Result => _result;

    /// <summary>
    /// The scope the facade actually applied to the last result. Fail-closed
    /// before anything has been evaluated.
    /// </summary>
    public ExplorerTelemetryScope Scope { get; private set; } = ExplorerTelemetryScope.None;

    /// <summary>
    /// The chart geometry for the last result, recomputed when a result arrives
    /// or the tree filter changes, and never per render.
    /// </summary>
    public TelemetryChart Chart { get; private set; } = TelemetryChart.Empty;

    /// <summary>
    /// The caption reporting the scope the last result was served under,
    /// including the degrade case.
    /// </summary>
    public TelemetryScopeCaption Caption { get; private set; }

    /// <summary>
    /// <see langword="true"/> when the last result was served under a narrower
    /// scope than was requested. A panel must surface this: a chart that
    /// quietly shows one tenant where the cluster was asked for is wrong, not
    /// merely imprecise.
    /// </summary>
    public bool WasDowngraded => Scope.WasDowngraded;

    /// <summary>
    /// <see langword="true"/> when an evaluation has completed and the facade
    /// matched no series. Distinct from "not evaluated yet", so a panel says
    /// "nothing matched" rather than showing an empty frame forever.
    /// </summary>
    public bool HasEvaluated => _result is not null;

    /// <summary>
    /// Chooses the time range by the label the control rendered, and
    /// re-evaluates. A label that was not offered is ignored.
    /// </summary>
    /// <param name="label">The selected range label.</param>
    /// <returns>A task that completes when the new window has been evaluated.</returns>
    public async Task SelectRangeAsync(string? label)
    {
        var range = TelemetryDurationChoices.TryResolve(RangeChoices, label, out var choice)
            ? choice.Duration
            : TimeSpan.Zero;

        if (range == Range)
        {
            return;
        }

        Range = range;
        await EvaluateAsync().ConfigureAwait(false);
        RaiseChanged();
    }

    /// <summary>
    /// Chooses the step by the label the control rendered, and re-evaluates. A
    /// label that was not offered is ignored.
    /// </summary>
    /// <remarks>
    /// Changing the step re-derives the legal ranges, because the same window
    /// costs more points at a finer step and may no longer fit the entry's
    /// budget. A range that stops being legal falls back to the server default
    /// rather than being sent and refused.
    /// </remarks>
    /// <param name="label">The selected step label.</param>
    /// <returns>A task that completes when the new resolution has been evaluated.</returns>
    public async Task SelectStepAsync(string? label)
    {
        var step = TelemetryDurationChoices.TryResolve(StepChoices, label, out var choice)
            ? choice.Duration
            : TimeSpan.Zero;

        if (step == Step)
        {
            return;
        }

        Step = step;

        if (Selected is { } query)
        {
            RangeChoices = TelemetryDurationChoices.RangesFor(query.Bounds, Step);
            if (!TelemetryDurationChoices.IsOffered(RangeChoices, Range))
            {
                Range = TimeSpan.Zero;
            }
        }

        await EvaluateAsync().ConfigureAwait(false);
        RaiseChanged();
    }

    /// <summary>
    /// Narrows the chart to one tree, or to every tree when
    /// <paramref name="tree"/> is empty. A tree the last answer did not contain
    /// is ignored.
    /// </summary>
    /// <remarks>
    /// This redraws from the result already in hand rather than re-querying:
    /// the filter selects among series the facade already returned, so a round
    /// trip would buy nothing and would make a control that feels instant cost
    /// a network call.
    /// </remarks>
    /// <param name="tree">The tree id to narrow to, or empty for all trees.</param>
    public void SelectTree(string? tree)
    {
        var selected = string.IsNullOrEmpty(tree) ? null : tree;
        if (selected is not null && !TelemetryTreeOptions.IsOffered(TreeChoices, selected))
        {
            return;
        }

        if (string.Equals(selected, TreeFilter, StringComparison.Ordinal))
        {
            return;
        }

        TreeFilter = selected;
        Chart = BuildChart();
        RaiseChanged();
    }

    /// <summary>
    /// Evaluates the selected entry and folds the outcome into the view state.
    /// Never throws for a cluster-produced outcome; a caller's own cancellation
    /// still propagates, exactly as the operations seam documents.
    /// </summary>
    private async Task EvaluateAsync()
    {
        if (Selected is not { } query)
        {
            Blank();
            return;
        }

        var request = BuildRequest(query);
        var number = ++_requestNumber;

        Busy = true;
        RaiseChanged();
        try
        {
            var result = await _domain.Queries.QueryAsync(request).ConfigureAwait(false);

            // A newer request was issued while this one was in flight, so this
            // answer is already stale. Applying it would let a slow older
            // evaluation overwrite a fast newer one purely on completion order.
            if (number != _requestNumber)
            {
                return;
            }

            Notice = TelemetryNotice.For(result);

            if (!result.IsSuccess || result.Value is not { } evaluated)
            {
                // A refusal of a re-run of what is already on screen leaves the
                // chart alone: the banner says what happened, and replacing a
                // good answer with an empty frame would lose it for no gain.
                //
                // A refusal of something the caller asked to see *instead* is
                // different, and must blank. The board captions the chart with
                // the SELECTED entry's title, description, and unit, so keeping
                // the previous query's series there would label bytes as a
                // ratio - a chart unambiguously presented as data it is not.
                if (!IsShown(request))
                {
                    Blank();
                }

                return;
            }

            _result = evaluated;
            _shown = (request.QueryId, request.Window);
            Scope = evaluated.Scope;
            Caption = TelemetryScopeCaptions.For(Scope, IsTenancyEnabled);

            TreeChoices = TelemetryTreeOptions.For(evaluated);
            if (!TelemetryTreeOptions.IsOffered(TreeChoices, TreeFilter))
            {
                // The tree the caller was looking at is not in this answer, so
                // keeping the filter would draw an empty chart and imply the
                // tree had gone quiet. Widening to everything shows what the
                // facade did return.
                TreeFilter = null;
            }

            Chart = BuildChart();
        }
        finally
        {
            Busy = false;
        }
    }

    /// <summary>
    /// Whether the chart on screen was produced by <paramref name="request"/>,
    /// so leaving it in place is a re-run of what is already shown rather than
    /// the previous panel's data under a new heading.
    /// </summary>
    private bool IsShown(ExplorerTelemetryRequest request) =>
        _result is not null
        && string.Equals(_shown.QueryId, request.QueryId, StringComparison.Ordinal)
        && _shown.Window == request.Window;

    /// <summary>
    /// Clears everything derived from a result, leaving the notice in place so
    /// the caller is told why the frame is empty.
    /// </summary>
    private void Blank()
    {
        _result = null;
        _shown = default;
        Chart = TelemetryChart.Empty;
        Scope = ExplorerTelemetryScope.None;
        Caption = TelemetryScopeCaptions.For(Scope, IsTenancyEnabled);
        TreeChoices = NoTrees;
        TreeFilter = null;
    }

    /// <summary>
    /// Rebuilds the chart geometry from the result in hand, carrying the
    /// selected entry's unit and semantic so each legend reading is formatted
    /// once here rather than on every render of every legend entry.
    /// </summary>
    private TelemetryChart BuildChart() =>
        TelemetryChart.For(_result, TreeFilter, Selected?.Unit, Selected?.Semantic ?? default);

    /// <summary>
    /// Composes the request for <paramref name="query"/>: its id, the window
    /// the controls chose, the visibility the head is asking for, and nothing
    /// else.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A window is only made concrete once a caller has chosen a range.
    /// Untouched controls send <see cref="ExplorerTelemetryWindow.Unset"/>, so
    /// the facade applies the entry's own default window and step - expanding
    /// an unset window client-side into an entry's maximum range would overrun
    /// its point budget at the default step and turn the first request every
    /// panel makes into a bounds refusal.
    /// </para>
    /// <para>
    /// The tenant filter is <em>not</em> put on the request. It narrows a
    /// drawn chart among series already served, and sending it as a tree filter
    /// would confuse a presentation choice with a server-side parameter.
    /// </para>
    /// </remarks>
    private ExplorerTelemetryRequest BuildRequest(ExplorerTelemetryQuery query)
    {
        var window = ExplorerTelemetryWindow.Unset;
        if (Range > TimeSpan.Zero && query.Accepts(ExplorerTelemetryParameters.TimeRange))
        {
            var end = _clock.GetUtcNow();
            var step = query.Accepts(ExplorerTelemetryParameters.Step) ? Step : TimeSpan.Zero;
            window = ExplorerTelemetryWindow.Between(end - Range, end, step);
        }

        return new ExplorerTelemetryRequest
        {
            QueryId = query.QueryId,
            Window = window,
            RequestedVisibility = RequestedVisibility,
        };
    }

    /// <summary>
    /// The visibility this mount asks for: the shell's, or - for the My Tenant
    /// metrics section - always the caller's own tenant.
    /// </summary>
    /// <remarks>
    /// The pin is a request like any other and grants nothing; the facade
    /// re-derives the tenant from the authenticated caller either way. What the
    /// pin buys is that a section headed "your tenant" cannot inherit a
    /// platform operator's cross-tenant intent from the shell's switcher and
    /// quietly render the whole cluster under that heading.
    /// </remarks>
    private ExplorerTelemetryVisibility RequestedVisibility =>
        _pinnedToOwnTenant ? ExplorerTelemetryVisibility.ActiveTenant : _domain.RequestedVisibility;
}
