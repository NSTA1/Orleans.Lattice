namespace Orleans.Lattice.Explorer.Plugins.Telemetry;

/// <summary>
/// One label on a returned series, such as the silo, tree, or tenant it belongs
/// to. Labels are reported exactly as the backend produced them and are never
/// used by the seam to include or exclude a series.
/// </summary>
/// <param name="Name">The label name.</param>
/// <param name="Value">The label value.</param>
public readonly record struct ExplorerTelemetryLabel(string Name, string Value);
