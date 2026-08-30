namespace Orleans.Lattice.Explorer.Plugins.Telemetry;

/// <summary>
/// One instrument a curated query reads, named so a panel can attribute a series
/// to the meter it came from and label it with the instrument's own unit and
/// semantic rather than a guess.
/// </summary>
/// <param name="Name">The instrument name.</param>
/// <param name="Meter">The meter that publishes it.</param>
/// <param name="Unit">The instrument's unit.</param>
/// <param name="Semantic">What one measurement of it counts.</param>
public readonly record struct ExplorerTelemetryInstrument(
    string Name,
    string Meter,
    string Unit,
    ExplorerTelemetrySemantic Semantic);
