namespace Orleans.Lattice.Api.Telemetry;

/// <summary>
/// The range-query guardrails a telemetry binding applies before a range query
/// reaches the backend: the window must be well-ordered, the step strictly
/// positive, both must fit inside the configured
/// <see cref="LatticeTelemetryOptions.MaxRange"/> and
/// <see cref="LatticeTelemetryOptions.MaxStep"/> budgets, and together they must
/// not request more evaluation points than the backend can serve.
/// </summary>
/// <remarks>
/// The checks are evaluated in a fixed order - ordering, step positivity, range
/// budget, step budget, then point budget - so a request that violates more than one is always
/// reported against the same violation, and every binding reports the identical
/// message for the identical request.
/// </remarks>
public static class TelemetryRangeGuardrails
{
    /// <summary>
    /// The most evaluation points a single range query may request per series,
    /// matching the resolution ceiling Prometheus itself enforces.
    /// </summary>
    internal const long MaxEvaluationPoints = 11_000;

    /// <summary>
    /// Tests a range request against the configured guardrails.
    /// </summary>
    /// <param name="options">The telemetry options carrying the range budgets.</param>
    /// <param name="start">The inclusive start of the requested range.</param>
    /// <param name="end">The inclusive end of the requested range.</param>
    /// <param name="step">The requested resolution step.</param>
    /// <param name="violationMessage">
    /// Receives the caller-facing violation message when the request is rejected,
    /// or <see langword="null"/> when it is admitted.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when the request is within budget;
    /// <see langword="false"/> when <paramref name="violationMessage"/> explains
    /// why it was rejected.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is <see langword="null"/>.</exception>
    public static bool TryValidateRange(
        LatticeTelemetryOptions options,
        DateTimeOffset start,
        DateTimeOffset end,
        TimeSpan step,
        out string? violationMessage)
    {
        ArgumentNullException.ThrowIfNull(options);

        if (end < start)
        {
            violationMessage = "The range end must be at or after the range start.";
            return false;
        }

        if (step <= TimeSpan.Zero)
        {
            violationMessage = "The range step must be strictly positive.";
            return false;
        }

        var range = end - start;
        if (range > options.MaxRange)
        {
            violationMessage =
                $"The requested range of {range} exceeds the configured maximum of {options.MaxRange}.";
            return false;
        }

        if (step > options.MaxStep)
        {
            violationMessage =
                $"The requested step of {step} exceeds the configured maximum of {options.MaxStep}.";
            return false;
        }

        // The range and step budgets bound each dimension alone, but not their
        // quotient: a full-budget range at a one-tick step would otherwise ask the
        // backend for billions of evaluation points per series.
        var points = (range.Ticks / step.Ticks) + 1;
        if (points > MaxEvaluationPoints)
        {
            violationMessage =
                $"The requested range and step yield {points} evaluation points, exceeding the maximum of {MaxEvaluationPoints}.";
            return false;
        }

        violationMessage = null;
        return true;
    }
}
