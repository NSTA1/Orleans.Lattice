namespace Orleans.Lattice.Explorer.Plugins.Telemetry;

/// <summary>
/// The window limits a catalogue entry declares: the step range and default, the
/// longest window, the furthest lookback, and the point budget. A panel builds
/// its controls from these rather than from constants of its own, so the limits
/// a user can reach are the ones the server actually enforces.
/// </summary>
/// <remarks>
/// <para>
/// A <see langword="readonly"/> <see langword="record"/>
/// <see langword="struct"/> whose members are pure functions, so a panel may
/// evaluate them per keystroke without allocating.
/// </para>
/// <para>
/// <b>The bounds are advisory here and authoritative at the facade.</b> Checking
/// a window locally saves a round trip and names the specific control at fault;
/// it never grants anything, because the facade re-checks every request it is
/// sent.
/// </para>
/// </remarks>
/// <param name="MinStep">The finest step accepted, or <see cref="TimeSpan.Zero"/> for no minimum.</param>
/// <param name="MaxStep">The coarsest step accepted, or <see cref="TimeSpan.Zero"/> for no maximum.</param>
/// <param name="DefaultStep">The step the facade applies when none is requested.</param>
/// <param name="MaxRange">The longest window accepted, or <see cref="TimeSpan.Zero"/> for no limit.</param>
/// <param name="MaxLookback">The furthest back a window may start, or <see cref="TimeSpan.Zero"/> for no limit.</param>
/// <param name="MaxPoints">The point budget, or zero for no budget.</param>
public readonly record struct ExplorerTelemetryBounds(
    TimeSpan MinStep,
    TimeSpan MaxStep,
    TimeSpan DefaultStep,
    TimeSpan MaxRange,
    TimeSpan MaxLookback,
    int MaxPoints)
{
    /// <summary>The entry declares no limits at all.</summary>
    public static ExplorerTelemetryBounds Unbounded => default;

    /// <summary><see langword="true"/> when the entry declares no limits.</summary>
    public bool IsUnbounded =>
        MinStep <= TimeSpan.Zero
        && MaxStep <= TimeSpan.Zero
        && MaxRange <= TimeSpan.Zero
        && MaxLookback <= TimeSpan.Zero
        && MaxPoints <= 0;

    /// <summary>
    /// The step that will actually apply for <paramref name="requested"/>: the
    /// entry's default when none was requested, clamped into the accepted range.
    /// </summary>
    /// <param name="requested">The requested step, or <see cref="TimeSpan.Zero"/> for the default.</param>
    /// <returns>The step the facade will use.</returns>
    public TimeSpan EffectiveStep(TimeSpan requested)
    {
        var step = requested <= TimeSpan.Zero ? DefaultStep : requested;
        if (MinStep > TimeSpan.Zero && step < MinStep)
        {
            return MinStep;
        }

        return MaxStep > TimeSpan.Zero && step > MaxStep ? MaxStep : step;
    }

    /// <summary>
    /// Checks <paramref name="window"/> against every declared limit, including
    /// the retention limit, which needs the current instant.
    /// </summary>
    /// <param name="window">The window to check.</param>
    /// <param name="nowUtc">
    /// The instant to measure lookback from. Supplied by the caller rather than
    /// read from the clock, so the check stays a pure function.
    /// </param>
    /// <returns>The first violation found, or <see cref="ExplorerTelemetryBoundsViolation.None"/>.</returns>
    public ExplorerTelemetryBoundsViolation Validate(ExplorerTelemetryWindow window, DateTimeOffset nowUtc)
    {
        var clockIndependent = ValidateWithoutClock(window);
        if (clockIndependent != ExplorerTelemetryBoundsViolation.None)
        {
            return clockIndependent;
        }

        return MaxLookback > TimeSpan.Zero && nowUtc - window.StartUtc > MaxLookback
            ? ExplorerTelemetryBoundsViolation.LookbackTooOld
            : ExplorerTelemetryBoundsViolation.None;
    }

    /// <summary>
    /// Checks <paramref name="window"/> against the limits that do not depend on
    /// the current instant: ordering, step range, window length, and the point
    /// budget.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is the check the seam runs before the wire. The retention limit is
    /// deliberately excluded, and not merely as a clock-skew concession: an
    /// <em>unset</em> window starts at the default instant, which is further in
    /// the past than any retention limit, so including that arm would reject
    /// every defaulted request - the exact request a panel sends first.
    /// </para>
    /// <para>
    /// Because the retention arm is skipped, a window that the facade would
    /// refuse as too old may be named here for a different limit it also
    /// violates. Both are genuine refusals of the same window, so the outcome is
    /// never a false acceptance.
    /// </para>
    /// </remarks>
    /// <param name="window">The window to check.</param>
    /// <returns>The first violation found, or <see cref="ExplorerTelemetryBoundsViolation.None"/>.</returns>
    public ExplorerTelemetryBoundsViolation ValidateWithoutClock(ExplorerTelemetryWindow window)
    {
        if (!window.IsAscending)
        {
            return ExplorerTelemetryBoundsViolation.RangeNotAscending;
        }

        if (window.Step < TimeSpan.Zero
            || (MinStep > TimeSpan.Zero && window.Step > TimeSpan.Zero && window.Step < MinStep))
        {
            return ExplorerTelemetryBoundsViolation.StepBelowMinimum;
        }

        if (MaxStep > TimeSpan.Zero && window.Step > MaxStep)
        {
            return ExplorerTelemetryBoundsViolation.StepAboveMaximum;
        }

        if (MaxRange > TimeSpan.Zero && window.Duration > MaxRange)
        {
            return ExplorerTelemetryBoundsViolation.RangeTooLong;
        }

        return MaxPoints > 0 && window.PointCount > MaxPoints
            ? ExplorerTelemetryBoundsViolation.TooManyPoints
            : ExplorerTelemetryBoundsViolation.None;
    }
}
