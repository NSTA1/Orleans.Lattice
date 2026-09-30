namespace Orleans.Lattice.Explorer.UI.Areas.Telemetry;

/// <summary>One hairline tick on a telemetry chart's value axis.</summary>
/// <param name="Y">Its vertical position.</param>
/// <param name="Label">The value it marks, with its unit.</param>
internal sealed record TelemetryAxisTick(double Y, string Label);
