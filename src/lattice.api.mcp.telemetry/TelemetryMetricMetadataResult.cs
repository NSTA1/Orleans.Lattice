namespace Orleans.Lattice.Api.Mcp.Telemetry;

/// <summary>
/// Structured result of the <c>lattice_telemetry_metric_metadata</c> tool: the
/// backend metadata entries the caller may see. In the deny-all metric-access
/// posture the entries are filtered to admitted metric names only; on a backend
/// fault <see cref="Success"/> is <see langword="false"/> and <see cref="Error"/>
/// explains why.
/// </summary>
/// <remarks>
/// This is an MCP protocol payload projected to JSON by the SDK, not an Orleans
/// grain message, so it carries no Orleans serialization attributes.
/// </remarks>
public sealed record TelemetryMetricMetadataResult
{
    /// <summary>
    /// <see langword="true"/> when the metadata was read; <see langword="false"/>
    /// when <see cref="Error"/> explains the backend fault or a metric-access
    /// denial.
    /// </summary>
    public bool Success { get; init; }

    /// <summary>
    /// The failure reason when <see cref="Success"/> is <see langword="false"/>;
    /// otherwise <see langword="null"/>.
    /// </summary>
    public string? Error { get; init; }

    /// <summary>The admitted metadata entries.</summary>
    public IReadOnlyList<TelemetryMetricMetadata> Metrics { get; init; } = [];

    /// <summary>Creates a failed result carrying <paramref name="error"/>.</summary>
    /// <param name="error">The failure reason.</param>
    /// <returns>A failed result.</returns>
    public static TelemetryMetricMetadataResult Failure(string error) => new() { Success = false, Error = error };

    /// <summary>
    /// Creates a successful result with no metadata entries. Returned when the
    /// backend has no metadata surface to serve (an empty or unwired metadata
    /// endpoint), so the tool degrades gracefully to an empty typed result rather
    /// than surfacing a raw backend fault - consistent with how
    /// <c>lattice_telemetry_list_metrics</c> and the query tools return empty on
    /// an unpopulated backend (issue #1339).
    /// </summary>
    /// <returns>A successful result carrying no metadata entries.</returns>
    public static TelemetryMetricMetadataResult Empty() => new() { Success = true, Metrics = [] };
}
