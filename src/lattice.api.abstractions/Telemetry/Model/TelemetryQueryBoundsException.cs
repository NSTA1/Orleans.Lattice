namespace Orleans.Lattice.Api.Telemetry;

/// <summary>
/// Thrown when a telemetry query request supplies a window that violates the
/// bounds its catalogue entry declares. The facade rejects rather than silently
/// clamping the window, because a panel rendered over a window the caller did not
/// ask for is a lie an operator cannot see.
/// </summary>
/// <remarks>
/// Carries the typed <see cref="Violation"/> so a transport binding can map the
/// rejection to a distinct status without parsing the message. Derives directly
/// from <see cref="Exception"/>, matching the sibling contract groups.
/// </remarks>
public sealed class TelemetryQueryBoundsException : Exception
{
    /// <summary>
    /// Initialises the exception for <paramref name="queryId"/> and composes a
    /// message from <paramref name="violation"/>.
    /// </summary>
    /// <param name="queryId">The query the request was rejected for.</param>
    /// <param name="violation">The bound that was violated.</param>
    public TelemetryQueryBoundsException(string queryId, TelemetryBoundsViolation violation)
        : base($"Telemetry query '{queryId}' rejected the requested window: {violation}.")
    {
        QueryId = queryId;
        Violation = violation;
    }

    /// <summary>Initialises the exception with a custom <paramref name="message"/>.</summary>
    /// <param name="queryId">The query the request was rejected for.</param>
    /// <param name="violation">The bound that was violated.</param>
    /// <param name="message">The message to report.</param>
    public TelemetryQueryBoundsException(string queryId, TelemetryBoundsViolation violation, string message)
        : base(message)
    {
        QueryId = queryId;
        Violation = violation;
    }

    /// <summary>The query the request was rejected for.</summary>
    public string QueryId { get; }

    /// <summary>The bound the requested window violated.</summary>
    public TelemetryBoundsViolation Violation { get; }
}
