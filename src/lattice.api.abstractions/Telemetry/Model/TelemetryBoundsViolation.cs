namespace Orleans.Lattice.Api.Telemetry;

/// <summary>
/// The single reason a requested time range failed a catalogue entry's declared
/// bounds, as reported by
/// <see cref="TelemetryQueryBounds.Validate(TelemetryTimeRange, DateTimeOffset)"/>.
/// A typed reason keeps validation allocation-free on the accepting path and lets
/// a transport binding map a rejection to a distinct status without parsing a
/// message.
/// </summary>
[GenerateSerializer]
[Alias(ApiTelemetryTypeAliases.TelemetryBoundsViolation)]
public enum TelemetryBoundsViolation
{
    /// <summary>The range satisfies every declared bound.</summary>
    None = 0,

    /// <summary>
    /// The range ends before it starts. A descending range is rejected outright
    /// rather than normalised, so a caller never silently gets a window it did not
    /// ask for.
    /// </summary>
    RangeNotAscending = 1,

    /// <summary>The requested step is negative, or below the entry's minimum step.</summary>
    StepBelowMinimum = 2,

    /// <summary>The requested step exceeds the entry's maximum step.</summary>
    StepAboveMaximum = 3,

    /// <summary>The requested window is longer than the entry's maximum range.</summary>
    RangeTooLong = 4,

    /// <summary>
    /// The requested window starts further in the past than the entry's maximum
    /// lookback allows.
    /// </summary>
    LookbackTooOld = 5,

    /// <summary>
    /// The requested window and step together yield more sample points than the
    /// entry's point budget permits.
    /// </summary>
    TooManyPoints = 6,
}
