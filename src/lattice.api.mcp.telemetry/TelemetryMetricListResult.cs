namespace Orleans.Lattice.Api.Mcp.Telemetry;

/// <summary>
/// Structured result of the <c>lattice_telemetry_list_metrics</c> tool: the
/// backend metric names the caller may see. In the deny-all metric-access posture
/// the list is filtered to the admitted names only; on a backend fault
/// <see cref="Success"/> is <see langword="false"/> and <see cref="Error"/>
/// explains why.
/// </summary>
/// <remarks>
/// This is an MCP protocol payload projected to JSON by the SDK, not an Orleans
/// grain message, so it carries no Orleans serialization attributes.
/// </remarks>
public sealed record TelemetryMetricListResult
{
    /// <summary>
    /// <see langword="true"/> when the metric names were listed;
    /// <see langword="false"/> when <see cref="Error"/> explains the backend fault.
    /// </summary>
    public bool Success { get; init; }

    /// <summary>
    /// The failure reason when <see cref="Success"/> is <see langword="false"/>;
    /// otherwise <see langword="null"/>.
    /// </summary>
    public string? Error { get; init; }

    /// <summary>The admitted metric names, in the order the backend returned them.</summary>
    public IReadOnlyList<string> Metrics { get; init; } = [];

    /// <summary>Creates a failed result carrying <paramref name="error"/>.</summary>
    /// <param name="error">The failure reason.</param>
    /// <returns>A failed result.</returns>
    public static TelemetryMetricListResult Failure(string error) => new() { Success = false, Error = error };
}
