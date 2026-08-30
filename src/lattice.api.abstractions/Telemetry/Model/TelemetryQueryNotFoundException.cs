namespace Orleans.Lattice.Api.Telemetry;

/// <summary>
/// Thrown when a telemetry query is requested by an id that the caller's catalogue
/// does not contain. It deliberately unifies "no such query is registered" with
/// "this query is not offered to you", so no caller can probe for the existence of
/// a query outside its entitlement.
/// </summary>
/// <remarks>
/// Mirrors the sibling <c>TenantNotFoundException</c> shape: a plain exception
/// deriving directly from <see cref="Exception"/>, carrying the offending id.
/// Deriving directly from <see cref="Exception"/> also keeps it safe to mark
/// serializable later, because Orleans registers a same-silo deep copier for
/// <see cref="Exception"/> but not for its subclasses.
/// </remarks>
public sealed class TelemetryQueryNotFoundException : Exception
{
    /// <summary>Initialises the exception for <paramref name="queryId"/>.</summary>
    /// <param name="queryId">The query id the request was rejected for.</param>
    public TelemetryQueryNotFoundException(string queryId)
        : base($"Telemetry query '{queryId}' is not available.")
        => QueryId = queryId;

    /// <summary>Initialises the exception with a custom <paramref name="message"/>.</summary>
    /// <param name="queryId">The query id the request was rejected for.</param>
    /// <param name="message">The message to report.</param>
    public TelemetryQueryNotFoundException(string queryId, string message)
        : base(message)
        => QueryId = queryId;

    /// <summary>The query id the request was rejected for.</summary>
    public string QueryId { get; }
}
