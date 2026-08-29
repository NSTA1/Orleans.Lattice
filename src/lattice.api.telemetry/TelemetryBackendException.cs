namespace Orleans.Lattice.Api.Telemetry;

/// <summary>
/// Thrown when the metrics backend could not answer a curated query: it was
/// unreachable, timed out, returned a non-success status, or returned a payload
/// that could not be read.
/// </summary>
/// <remarks>
/// A backend fault is deliberately distinct from
/// <see cref="TelemetryQueryNotFoundException"/> and
/// <see cref="TelemetryQueryBoundsException"/>, which report a caller error. It is
/// surfaced rather than swallowed as an empty result, because a panel that renders
/// "no data" when the backend is down misreports an outage as a quiet cluster.
/// Derives directly from <see cref="Exception"/>, matching the sibling contract
/// groups, so it stays safe to mark serializable later.
/// </remarks>
public sealed class TelemetryBackendException : Exception
{
    /// <summary>Initialises the exception for <paramref name="queryId"/>.</summary>
    /// <param name="queryId">The query whose evaluation failed.</param>
    /// <param name="message">The message to report.</param>
    public TelemetryBackendException(string queryId, string message)
        : base(message)
        => QueryId = queryId;

    /// <summary>Initialises the exception with the underlying transport fault.</summary>
    /// <param name="queryId">The query whose evaluation failed.</param>
    /// <param name="message">The message to report.</param>
    /// <param name="innerException">The underlying fault.</param>
    public TelemetryBackendException(string queryId, string message, Exception innerException)
        : base(message, innerException)
        => QueryId = queryId;

    /// <summary>The catalogue id of the query whose evaluation failed.</summary>
    public string QueryId { get; }
}
