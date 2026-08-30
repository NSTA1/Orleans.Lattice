namespace Orleans.Lattice.Explorer.Plugins.Telemetry;

/// <summary>
/// One sample in a series: a timestamp and the value observed at it.
/// </summary>
/// <param name="TimestampUtc">When the value was observed.</param>
/// <param name="Value">
/// The observed value, which may be non-finite when the backend reported a gap
/// or a division by zero.
/// </param>
public readonly record struct ExplorerTelemetryPoint(DateTimeOffset TimestampUtc, double Value)
{
    /// <summary>
    /// <see langword="true"/> when the value is a real number a chart can plot,
    /// so a panel can skip a gap rather than render it as zero.
    /// </summary>
    public bool IsFinite => double.IsFinite(Value);
}
