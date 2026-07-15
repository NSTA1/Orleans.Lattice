namespace Orleans.Lattice.Api.Mcp.Telemetry;

/// <summary>
/// One metric's backend metadata entry: its name, sample type, help text, and
/// unit. A metric may have more than one entry when it is exported with differing
/// metadata across targets, so the metadata result carries a flat list keyed by
/// <see cref="Metric"/>.
/// </summary>
/// <remarks>
/// This is an MCP protocol payload projected to JSON by the SDK, not an Orleans
/// grain message, so it carries no Orleans serialization attributes.
/// </remarks>
public sealed record TelemetryMetricMetadata
{
    /// <summary>The metric name this metadata entry describes.</summary>
    public required string Metric { get; init; }

    /// <summary>The metric sample type (for example <c>counter</c> or <c>gauge</c>), or an empty string.</summary>
    public string Type { get; init; } = string.Empty;

    /// <summary>The metric help text, or an empty string.</summary>
    public string Help { get; init; } = string.Empty;

    /// <summary>The metric unit, or an empty string.</summary>
    public string Unit { get; init; } = string.Empty;
}
