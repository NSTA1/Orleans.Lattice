namespace Orleans.Lattice.Explorer.Plugins.Telemetry;

/// <summary>
/// The shape of a curated query: a single value read at an instant, or a series
/// evaluated across a window at a step.
/// </summary>
public enum ExplorerTelemetryQueryKind
{
    /// <summary>
    /// Evaluated at one instant. Only the window's end is used; its start and
    /// step are ignored.
    /// </summary>
    Instant = 0,

    /// <summary>
    /// Evaluated across a window at a step, producing one point per step.
    /// </summary>
    Range = 1,
}
