namespace Orleans.Lattice.Explorer.UI.Areas.Telemetry;

/// <summary>One row of a chart's table alternative: a time and every series' reading at it.</summary>
/// <param name="Time">The time.</param>
/// <param name="Values">Each series' reading at <paramref name="Time"/>, in the chart's series order.</param>
internal sealed record TelemetryValuesRow(DateTimeOffset Time, IReadOnlyList<double> Values);
