using Orleans.Lattice.Api.Telemetry;

namespace Orleans.Lattice.Explorer.UI.Areas.Telemetry;

/// <summary>
/// What one chart asks the facade for under a board's window: the request, or the
/// reason the window cannot be asked of this chart, plus a qualifying note.
/// </summary>
/// <param name="Request">The request to send, or <see langword="null"/> when <paramref name="Problem"/> says why not.</param>
/// <param name="Problem">Why the window does not fit this chart, or <see langword="null"/>.</param>
/// <param name="Note">A qualification the chart shows beside its figure, or <see langword="null"/>.</param>
internal sealed record TelemetryChartRequest(TelemetryQueryRequest? Request, string? Problem, string? Note)
{
    /// <summary>
    /// The number of points an automatic step aims at: enough for a smooth line at
    /// the chart's width, few enough that every entry's point budget admits it.
    /// </summary>
    public const int TargetPoints = 240;

    /// <summary>Composes the request <paramref name="query"/> is asked under <paramref name="window"/>.</summary>
    /// <param name="query">The catalogue entry.</param>
    /// <param name="window">The window the address names.</param>
    /// <param name="now">The current instant.</param>
    /// <param name="tenancyActive">Whether tenancy is on, so the scope in the address applies.</param>
    /// <returns>The chart's request.</returns>
    public static TelemetryChartRequest For(
        TelemetryQueryDescriptor query,
        TelemetryWindow window,
        DateTimeOffset now,
        bool tenancyActive)
    {
        ArgumentNullException.ThrowIfNull(query);
        ArgumentNullException.ThrowIfNull(window);

        var request = new TelemetryQueryRequest
        {
            QueryId = query.QueryId,
            RequestedVisibility = tenancyActive && window.AllTenants
                ? TelemetryTenantVisibility.AllTenants
                : TelemetryTenantVisibility.ActiveTenant,
        };

        string? note = null;
        if (window.Tree is { } tree)
        {
            if (query.Accepts(TelemetryQueryParameters.TreeFilter))
            {
                request = request with { TreeId = tree };
            }
            else
            {
                note = "This chart is not broken down by tree, so the tree filter does not narrow it.";
            }
        }

        if (query.Kind == TelemetryQueryKind.Instant || !query.Accepts(TelemetryQueryParameters.TimeRange))
        {
            if (!window.IsDefault)
            {
                note = Join(note, "This chart is a current reading, so the time range does not apply.");
            }

            return new TelemetryChartRequest(request, null, note);
        }

        if (window.Resolve(now) is not var (start, end))
        {
            return new TelemetryChartRequest(request, null, note);
        }

        var duration = end - start;
        var step = query.Accepts(TelemetryQueryParameters.Step)
            ? query.Bounds.EffectiveStep(window.Step ?? AutomaticStep(duration, query.Bounds))
            : TimeSpan.Zero;
        var range = TelemetryTimeRange.Between(start, end, step);

        var problem = Describe(query.Bounds, query.Bounds.Validate(range, now), range);
        return problem is null
            ? new TelemetryChartRequest(request with { Range = range }, null, note)
            : new TelemetryChartRequest(null, problem, note);
    }

    /// <summary>The finest ladder step that keeps <paramref name="duration"/> within the point target and the entry's budget.</summary>
    /// <param name="duration">The window's length.</param>
    /// <param name="bounds">The entry's bounds.</param>
    /// <returns>The step.</returns>
    public static TimeSpan AutomaticStep(TimeSpan duration, TelemetryQueryBounds bounds)
    {
        var budget = bounds.MaxPoints > 0 ? Math.Min(TargetPoints, bounds.MaxPoints) : TargetPoints;
        foreach (var step in TelemetryDurations.AutomaticSteps)
        {
            if ((duration.Ticks / step.Ticks) + 1 <= budget)
            {
                return step;
            }
        }

        return TelemetryDurations.AutomaticSteps[^1];
    }

    private static string? Describe(TelemetryQueryBounds bounds, TelemetryBoundsViolation violation, TelemetryTimeRange range) =>
        violation switch
        {
            TelemetryBoundsViolation.None => null,
            TelemetryBoundsViolation.RangeTooLong =>
                $"This chart covers at most {TelemetryDurations.Describe(bounds.MaxRange)}. Choose a shorter range.",
            TelemetryBoundsViolation.LookbackTooOld =>
                $"This chart reaches back at most {TelemetryDurations.Describe(bounds.MaxLookback)}. Choose a more recent window.",
            TelemetryBoundsViolation.TooManyPoints =>
                $"At this step the window needs {range.PointCount:N0} points and this chart draws at most {bounds.MaxPoints:N0}. Choose a coarser step or a shorter range.",
            TelemetryBoundsViolation.StepBelowMinimum or TelemetryBoundsViolation.StepAboveMaximum =>
                "This chart cannot be drawn at that step. Choose another step.",
            _ => "This chart cannot be drawn over that window. Choose another range.",
        };

    private static string Join(string? first, string second) => first is null ? second : first + " " + second;
}
