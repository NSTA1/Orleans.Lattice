namespace Orleans.Lattice.Api.Mcp.Telemetry;

/// <summary>
/// A single (timestamp, value) sample from a Prometheus query result. The value
/// is carried as its raw backend string form (Prometheus returns sample values as
/// strings, including special forms such as <c>NaN</c> and <c>+Inf</c>) so no
/// precision is lost in projection.
/// </summary>
/// <remarks>
/// This is an MCP protocol payload projected to JSON by the SDK, not an Orleans
/// grain message, so it carries no Orleans serialization attributes.
/// </remarks>
public sealed record TelemetrySample
{
    /// <summary>The sample timestamp, in unix seconds (may carry a fractional part).</summary>
    public double Timestamp { get; init; }

    /// <summary>The sample value in its raw backend string form.</summary>
    public string Value { get; init; } = string.Empty;
}
