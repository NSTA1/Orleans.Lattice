namespace Orleans.Lattice.Api.Telemetry;

/// <summary>
/// Derives the rate window a range query samples over from the resolution step the
/// facade actually evaluated at, and renders it as PromQL duration text.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why the window tracks the step.</b> A rate over a window narrower than the
/// step under-samples: at a one-hour step a hard-coded <c>[5m]</c> reports five
/// minutes in every hour and silently drops the rest. Scaling the window with the
/// step keeps a panel honest at every zoom level, which is why a curated entry
/// declares a step budget rather than baking a window into its template.
/// </para>
/// <para>
/// <b>A fixed ladder, not an arbitrary duration.</b> The window is rounded up to
/// the nearest entry of a small fixed ladder, so it is drawn from a closed set of
/// cached strings. That makes the derivation deterministic (the same step always
/// yields the same window, so a rendered query is reproducible and testable) and
/// keeps the render path free of per-request duration formatting.
/// </para>
/// </remarks>
internal static class TelemetryRateWindow
{
    /// <summary>
    /// The multiple of the resolution step the rate window covers. Four steps is
    /// the conventional floor for a stable rate: it spans enough scrape intervals
    /// to survive a missed sample without smearing a genuine step change.
    /// </summary>
    private const int StepMultiple = 4;

    private static readonly (TimeSpan Window, string Text)[] Ladder =
    [
        (TimeSpan.FromMinutes(1), "1m"),
        (TimeSpan.FromMinutes(2), "2m"),
        (TimeSpan.FromMinutes(5), "5m"),
        (TimeSpan.FromMinutes(10), "10m"),
        (TimeSpan.FromMinutes(15), "15m"),
        (TimeSpan.FromMinutes(30), "30m"),
        (TimeSpan.FromHours(1), "1h"),
        (TimeSpan.FromHours(2), "2h"),
        (TimeSpan.FromHours(6), "6h"),
        (TimeSpan.FromHours(12), "12h"),
        (TimeSpan.FromHours(24), "24h"),
    ];

    /// <summary>
    /// The window used when no step is in play - an instant query, or a range
    /// query whose entry declares no default step.
    /// </summary>
    public static string Default => Ladder[2].Text;

    /// <summary>
    /// Resolves the rate-window text for <paramref name="step"/>: the smallest
    /// ladder entry covering <see cref="StepMultiple"/> steps, clamped to the
    /// ladder's ends. A non-positive step yields <see cref="Default"/>.
    /// </summary>
    /// <param name="step">The resolution step the query is evaluated at.</param>
    /// <returns>PromQL duration text, for example <c>5m</c>.</returns>
    public static string ForStep(TimeSpan step)
    {
        if (step <= TimeSpan.Zero)
        {
            return Default;
        }

        var target = step * StepMultiple;
        for (var i = 0; i < Ladder.Length; i++)
        {
            if (Ladder[i].Window >= target)
            {
                return Ladder[i].Text;
            }
        }

        return Ladder[^1].Text;
    }
}
