namespace Orleans.Lattice.Api.Telemetry;

/// <summary>
/// The bounds a curated named query declares for the caller-supplied parameters it
/// accepts: the permitted resolution steps, the longest window, how far back a
/// window may reach, and the sample-point budget. Bounds are authored server-side
/// alongside the query, so a caller can neither widen them nor learn a query it is
/// not offered.
/// </summary>
/// <remarks>
/// <para>
/// This is a value-typed descriptor (a <see langword="readonly"/> record struct),
/// so declaring and checking bounds allocates nothing on the heap. Every dimension
/// is "unbounded when non-positive": a <see langword="default"/> value therefore
/// means <see cref="Unbounded"/>, which is safe because bounds are authored by the
/// server, never supplied by a caller.
/// </para>
/// <para>
/// <see cref="Validate(TelemetryTimeRange, DateTimeOffset)"/> takes the current
/// instant as a parameter rather than reading an ambient clock, so validation is a
/// pure function and is deterministic under test.
/// </para>
/// </remarks>
[GenerateSerializer]
[Alias(ApiTelemetryTypeAliases.TelemetryQueryBounds)]
[Immutable]
public readonly record struct TelemetryQueryBounds
{
    /// <summary>
    /// The smallest resolution step permitted, or non-positive for no minimum.
    /// Guards a caller from requesting a step so fine it overwhelms the backend.
    /// </summary>
    [Id(0)] public TimeSpan MinStep { get; init; }

    /// <summary>The largest resolution step permitted, or non-positive for no maximum.</summary>
    [Id(1)] public TimeSpan MaxStep { get; init; }

    /// <summary>
    /// The step applied when the caller supplies none. Non-positive leaves the
    /// step unset, which the facade resolves against the backend's own default.
    /// </summary>
    [Id(2)] public TimeSpan DefaultStep { get; init; }

    /// <summary>
    /// The longest window permitted between a request's start and end instants, or
    /// non-positive for no limit.
    /// </summary>
    [Id(3)] public TimeSpan MaxRange { get; init; }

    /// <summary>
    /// How far into the past a window may start, measured back from the evaluation
    /// instant, or non-positive for no limit. Keeps a query inside the backend's
    /// retention rather than returning a silently truncated window.
    /// </summary>
    [Id(4)] public TimeSpan MaxLookback { get; init; }

    /// <summary>
    /// The maximum number of sample points a single series may yield, or
    /// non-positive for no limit. Bounds the response size independently of the
    /// window and step, which together can otherwise multiply without limit.
    /// </summary>
    [Id(5)] public int MaxPoints { get; init; }

    /// <summary>
    /// The unbounded bounds: every dimension unset. This is the
    /// <see langword="default"/> value, and the bounds of a query that constrains
    /// nothing.
    /// </summary>
    public static TelemetryQueryBounds Unbounded => default;

    /// <summary>
    /// <see langword="true"/> when no dimension is constrained, so
    /// <see cref="Validate(TelemetryTimeRange, DateTimeOffset)"/> can only reject a
    /// malformed (descending or negative-step) window.
    /// </summary>
    public bool IsUnbounded =>
        MinStep <= TimeSpan.Zero
        && MaxStep <= TimeSpan.Zero
        && MaxRange <= TimeSpan.Zero
        && MaxLookback <= TimeSpan.Zero
        && MaxPoints <= 0;

    /// <summary>
    /// Resolves the step to evaluate at from a caller's requested step: a
    /// non-positive request falls back to <see cref="DefaultStep"/>, a request
    /// below <see cref="MinStep"/> clamps up to it, a request above
    /// <see cref="MaxStep"/> clamps down to it, and an in-range request passes
    /// through unchanged.
    /// </summary>
    /// <param name="requested">The caller's requested step.</param>
    /// <returns>The step to evaluate at.</returns>
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
    /// Checks <paramref name="range"/> against every declared bound and reports the
    /// first violation, or <see cref="TelemetryBoundsViolation.None"/> when the
    /// window is acceptable. The checks are applied in the fixed order declared by
    /// <see cref="TelemetryBoundsViolation"/>, so the reported reason is stable for
    /// a window that violates more than one bound.
    /// </summary>
    /// <param name="range">The window the caller requested.</param>
    /// <param name="now">
    /// The instant to measure <see cref="MaxLookback"/> from. Supplied by the
    /// caller rather than read from an ambient clock, so validation stays a pure,
    /// deterministic function.
    /// </param>
    /// <returns>The first violated bound, or <see cref="TelemetryBoundsViolation.None"/>.</returns>
    public TelemetryBoundsViolation Validate(TelemetryTimeRange range, DateTimeOffset now)
    {
        if (!range.IsAscending)
        {
            return TelemetryBoundsViolation.RangeNotAscending;
        }

        if (range.Step < TimeSpan.Zero
            || (MinStep > TimeSpan.Zero && range.Step > TimeSpan.Zero && range.Step < MinStep))
        {
            return TelemetryBoundsViolation.StepBelowMinimum;
        }

        if (MaxStep > TimeSpan.Zero && range.Step > MaxStep)
        {
            return TelemetryBoundsViolation.StepAboveMaximum;
        }

        if (MaxRange > TimeSpan.Zero && range.Duration > MaxRange)
        {
            return TelemetryBoundsViolation.RangeTooLong;
        }

        if (MaxLookback > TimeSpan.Zero && now - range.StartUtc > MaxLookback)
        {
            return TelemetryBoundsViolation.LookbackTooOld;
        }

        return MaxPoints > 0 && range.PointCount > MaxPoints
            ? TelemetryBoundsViolation.TooManyPoints
            : TelemetryBoundsViolation.None;
    }
}
