namespace Orleans.Lattice.Explorer.Telemetry;

/// <summary>
/// Why a catalogue entry refused an evaluation window. A panel renders the
/// specific reason so a user can correct the control that caused it rather than
/// being told only that the request was invalid.
/// </summary>
/// <remarks>
/// The transport carries a bounds refusal as a status and a message, not as a
/// value, so a refusal the facade raised arrives as
/// <see cref="Unspecified"/> with the facade's own explanation. A refusal the
/// seam detected before the wire - by checking the window against the bounds the
/// catalogue already published - arrives named.
/// </remarks>
public enum ExplorerTelemetryBoundsViolation
{
    /// <summary>No violation.</summary>
    None = 0,

    /// <summary>The window ends before it starts.</summary>
    RangeNotAscending = 1,

    /// <summary>The step is finer than the entry's minimum.</summary>
    StepBelowMinimum = 2,

    /// <summary>The step is coarser than the entry's maximum.</summary>
    StepAboveMaximum = 3,

    /// <summary>The window is longer than the entry's maximum range.</summary>
    RangeTooLong = 4,

    /// <summary>The window starts further back than the entry's retention allows.</summary>
    LookbackTooOld = 5,

    /// <summary>The window at this step would yield more points than the entry allows.</summary>
    TooManyPoints = 6,

    /// <summary>
    /// The facade refused the window but the transport carried no machine-readable
    /// reason, so only its message is available.
    /// </summary>
    Unspecified = 7,
}
