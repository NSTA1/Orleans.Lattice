namespace Orleans.Lattice.Api.Mcp.Telemetry;

/// <summary>
/// One series in a Prometheus query result: its label set and its samples. An
/// instant (vector) series carries a single sample; a range (matrix) series
/// carries one sample per resolution step; a scalar or string result is modelled
/// as a single series with an empty label set and one sample.
/// </summary>
/// <remarks>
/// This is an MCP protocol payload projected to JSON by the SDK, not an Orleans
/// grain message, so it carries no Orleans serialization attributes.
/// </remarks>
public sealed record TelemetrySeries
{
    /// <summary>
    /// The series' label set (including the <c>__name__</c> metric-name label when
    /// the backend supplies it). Empty for a scalar or string result.
    /// </summary>
    public IReadOnlyDictionary<string, string> Labels { get; init; }
        = new Dictionary<string, string>(StringComparer.Ordinal);

    /// <summary>The series' samples, in the order the backend returned them.</summary>
    public IReadOnlyList<TelemetrySample> Samples { get; init; } = [];
}
