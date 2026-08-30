using System.Diagnostics.CodeAnalysis;

namespace Orleans.Lattice.Explorer.Plugins.Telemetry;

/// <summary>
/// Builds the bounded time-range and step choices a panel's controls offer, by
/// filtering a fixed candidate ladder against the limits the selected catalogue
/// entry declares.
/// </summary>
/// <remarks>
/// <para>
/// <b>The ladder proposes; the entry's bounds dispose.</b> The candidates here
/// are only the granularities a control could sensibly offer. Which of them
/// survive is decided entirely by <see cref="ExplorerTelemetryBounds"/> - the
/// server's own published limits - so a control can never offer a window the
/// facade would refuse, and the client carries no limit of its own that could
/// drift from the server's.
/// </para>
/// <para>
/// <b>Range and step are filtered together, not independently.</b> A range that
/// survives <see cref="ExplorerTelemetryBounds.MaxRange"/> can still overrun the
/// point budget at a fine step, so
/// <see cref="RangesFor(ExplorerTelemetryBounds)"/> keeps only ranges that yield
/// a legal window at the step that will actually apply. Offering a choice that
/// is immediately refused is worse than not offering it.
/// </para>
/// <para>
/// Every result is a materialised array built once per selection change, never
/// per render, and an entry that declares no limits reuses one shared array
/// rather than rebuilding the full ladder each time.
/// </para>
/// </remarks>
public static class TelemetryDurationChoices
{
    private static readonly TelemetryDurationChoice[] RangeLadder =
    [
        new(TimeSpan.FromMinutes(5), "5m"),
        new(TimeSpan.FromMinutes(15), "15m"),
        new(TimeSpan.FromMinutes(30), "30m"),
        new(TimeSpan.FromHours(1), "1h"),
        new(TimeSpan.FromHours(3), "3h"),
        new(TimeSpan.FromHours(6), "6h"),
        new(TimeSpan.FromHours(12), "12h"),
        new(TimeSpan.FromDays(1), "24h"),
        new(TimeSpan.FromDays(2), "2d"),
        new(TimeSpan.FromDays(7), "7d"),
    ];

    private static readonly TelemetryDurationChoice[] StepLadder =
    [
        new(TimeSpan.FromSeconds(15), "15s"),
        new(TimeSpan.FromSeconds(30), "30s"),
        new(TimeSpan.FromMinutes(1), "1m"),
        new(TimeSpan.FromMinutes(5), "5m"),
        new(TimeSpan.FromMinutes(15), "15m"),
        new(TimeSpan.FromMinutes(30), "30m"),
        new(TimeSpan.FromHours(1), "1h"),
        new(TimeSpan.FromHours(6), "6h"),
    ];

    private static readonly TelemetryDurationChoice[] NoChoices = [];

    /// <summary>
    /// The label the range and step controls render for "let the server
    /// choose", which is what an unset window asks for and what a panel sends
    /// before a caller has touched a control.
    /// </summary>
    public const string ServerDefaultLabel = "Server default";

    /// <summary>
    /// The range choices legal for <paramref name="bounds"/>: the ladder
    /// filtered against the longest window, the furthest lookback, and the
    /// point budget at the step that will actually apply.
    /// </summary>
    /// <param name="bounds">The selected entry's published limits.</param>
    /// <returns>
    /// The legal choices in ascending duration order, empty when the entry's
    /// limits rule every candidate out.
    /// </returns>
    public static IReadOnlyList<TelemetryDurationChoice> RangesFor(ExplorerTelemetryBounds bounds) =>
        RangesFor(bounds, TimeSpan.Zero);

    /// <summary>
    /// The range choices legal for <paramref name="bounds"/> at
    /// <paramref name="step"/>: the ladder filtered against the longest window,
    /// the furthest lookback, and the point budget the chosen step implies.
    /// </summary>
    /// <param name="bounds">The selected entry's published limits.</param>
    /// <param name="step">
    /// The step the caller chose, or <see cref="TimeSpan.Zero"/> for the
    /// entry's own default. A finer step admits fewer ranges, because the same
    /// window costs more points.
    /// </param>
    /// <returns>
    /// The legal choices in ascending duration order, empty when the entry's
    /// limits rule every candidate out.
    /// </returns>
    public static IReadOnlyList<TelemetryDurationChoice> RangesFor(
        ExplorerTelemetryBounds bounds,
        TimeSpan step)
    {
        if (bounds.IsUnbounded && step <= TimeSpan.Zero)
        {
            return RangeLadder;
        }

        var effectiveStep = bounds.EffectiveStep(step);
        List<TelemetryDurationChoice>? kept = null;
        for (var i = 0; i < RangeLadder.Length; i++)
        {
            var candidate = RangeLadder[i];
            if (IsRangeLegal(bounds, candidate.Duration, effectiveStep))
            {
                (kept ??= new List<TelemetryDurationChoice>(RangeLadder.Length)).Add(candidate);
            }
        }

        return kept is null ? NoChoices : kept;
    }

    /// <summary>
    /// The step choices legal for <paramref name="bounds"/>: the ladder
    /// filtered against the finest and coarsest step the entry accepts.
    /// </summary>
    /// <param name="bounds">The selected entry's published limits.</param>
    /// <returns>
    /// The legal choices in ascending duration order, empty when the entry's
    /// step range admits no candidate - in which case the control offers only
    /// the server default, which is always legal.
    /// </returns>
    public static IReadOnlyList<TelemetryDurationChoice> StepsFor(ExplorerTelemetryBounds bounds)
    {
        if (bounds.MinStep <= TimeSpan.Zero && bounds.MaxStep <= TimeSpan.Zero)
        {
            return StepLadder;
        }

        List<TelemetryDurationChoice>? kept = null;
        for (var i = 0; i < StepLadder.Length; i++)
        {
            var candidate = StepLadder[i];
            if (bounds.MinStep > TimeSpan.Zero && candidate.Duration < bounds.MinStep)
            {
                continue;
            }

            if (bounds.MaxStep > TimeSpan.Zero && candidate.Duration > bounds.MaxStep)
            {
                continue;
            }

            (kept ??= new List<TelemetryDurationChoice>(StepLadder.Length)).Add(candidate);
        }

        return kept is null ? NoChoices : kept;
    }

    /// <summary>
    /// Finds the choice in <paramref name="choices"/> whose label is
    /// <paramref name="label"/>.
    /// </summary>
    /// <remarks>
    /// This is how a control turns the value a caller selected back into a
    /// duration. It resolves only against the list actually offered, so a value
    /// that was never rendered - including one a caller edited into the DOM -
    /// resolves to nothing and leaves the window as it was, rather than
    /// becoming a window the entry never admitted.
    /// </remarks>
    /// <param name="choices">The choices that were offered.</param>
    /// <param name="label">The selected label, compared ordinally.</param>
    /// <param name="choice">The matching choice when found.</param>
    /// <returns><see langword="true"/> when the label names an offered choice.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="choices"/> is <see langword="null"/>.</exception>
    public static bool TryResolve(
        IReadOnlyList<TelemetryDurationChoice> choices,
        string? label,
        out TelemetryDurationChoice choice)
    {
        ArgumentNullException.ThrowIfNull(choices);

        if (!string.IsNullOrEmpty(label))
        {
            for (var i = 0; i < choices.Count; i++)
            {
                var candidate = choices[i];
                if (string.Equals(candidate.Label, label, StringComparison.Ordinal))
                {
                    choice = candidate;
                    return true;
                }
            }
        }

        choice = default;
        return false;
    }

    /// <summary>
    /// The label for <paramref name="duration"/> among
    /// <paramref name="choices"/>, or <see cref="ServerDefaultLabel"/> when
    /// nothing has been chosen.
    /// </summary>
    /// <param name="choices">The choices that were offered.</param>
    /// <param name="duration">The currently selected duration.</param>
    /// <returns>The label the control shows as selected.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="choices"/> is <see langword="null"/>.</exception>
    public static string LabelFor(IReadOnlyList<TelemetryDurationChoice> choices, TimeSpan duration)
    {
        ArgumentNullException.ThrowIfNull(choices);

        if (duration <= TimeSpan.Zero)
        {
            return ServerDefaultLabel;
        }

        for (var i = 0; i < choices.Count; i++)
        {
            var candidate = choices[i];
            if (candidate.Duration == duration)
            {
                return candidate.Label;
            }
        }

        return ServerDefaultLabel;
    }

    /// <summary>
    /// Whether <paramref name="duration"/> is still one of
    /// <paramref name="choices"/>, so a selection made against one entry's
    /// bounds can be dropped when it is illegal under another's.
    /// </summary>
    /// <param name="choices">The choices currently offered.</param>
    /// <param name="duration">The retained selection.</param>
    /// <returns>
    /// <see langword="true"/> when the selection is unset (the always-legal
    /// server default) or still offered.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="choices"/> is <see langword="null"/>.</exception>
    public static bool IsOffered(IReadOnlyList<TelemetryDurationChoice> choices, TimeSpan duration)
    {
        ArgumentNullException.ThrowIfNull(choices);

        if (duration <= TimeSpan.Zero)
        {
            return true;
        }

        for (var i = 0; i < choices.Count; i++)
        {
            if (choices[i].Duration == duration)
            {
                return true;
            }
        }

        return false;
    }

    [SuppressMessage(
        "Style",
        "IDE0046:Convert to conditional expression",
        Justification = "Three independent limits read as three guards, not as a nested ternary.")]
    private static bool IsRangeLegal(ExplorerTelemetryBounds bounds, TimeSpan range, TimeSpan step)
    {
        if (bounds.MaxRange > TimeSpan.Zero && range > bounds.MaxRange)
        {
            return false;
        }

        if (bounds.MaxLookback > TimeSpan.Zero && range > bounds.MaxLookback)
        {
            return false;
        }

        if (bounds.MaxPoints <= 0 || step <= TimeSpan.Zero)
        {
            return true;
        }

        return (range.Ticks / step.Ticks) + 1 <= bounds.MaxPoints;
    }
}
