namespace Orleans.Lattice.Api.Mcp.Telemetry;

/// <summary>
/// Structured result of the <c>lattice_telemetry_query</c> and
/// <c>lattice_telemetry_query_range</c> tools. On success it carries the backend
/// result type (<c>vector</c>, <c>matrix</c>, <c>scalar</c>, or <c>string</c>) and
/// the projected series; on failure it carries a human-readable
/// <see cref="Error"/> and no series, so a backend fault, a guardrail rejection,
/// or a metric-access denial reaches the agent as a clean structured result
/// rather than an unhandled exception.
/// </summary>
/// <remarks>
/// This is an MCP protocol payload projected to JSON by the SDK, not an Orleans
/// grain message, so it carries no Orleans serialization attributes.
/// </remarks>
public sealed record TelemetryQueryResult
{
    /// <summary>
    /// <see langword="true"/> when the query evaluated and its result was
    /// projected; <see langword="false"/> when <see cref="Error"/> explains why it
    /// did not.
    /// </summary>
    public bool Success { get; init; }

    /// <summary>
    /// The failure reason when <see cref="Success"/> is <see langword="false"/>;
    /// otherwise <see langword="null"/>.
    /// </summary>
    public string? Error { get; init; }

    /// <summary>
    /// The backend result type (<c>vector</c>, <c>matrix</c>, <c>scalar</c>, or
    /// <c>string</c>) when <see cref="Success"/> is <see langword="true"/>;
    /// otherwise an empty string.
    /// </summary>
    public string ResultType { get; init; } = string.Empty;

    /// <summary>The projected series when <see cref="Success"/> is <see langword="true"/>; otherwise empty.</summary>
    public IReadOnlyList<TelemetrySeries> Series { get; init; } = [];

    /// <summary>Creates a failed result carrying <paramref name="error"/>.</summary>
    /// <param name="error">The failure reason.</param>
    /// <returns>A failed result.</returns>
    public static TelemetryQueryResult Failure(string error) => new() { Success = false, Error = error };
}
