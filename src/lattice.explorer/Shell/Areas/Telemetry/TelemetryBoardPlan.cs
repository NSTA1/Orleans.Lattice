using Orleans.Lattice.Api.Telemetry;

namespace Orleans.Lattice.Explorer.Shell.Areas.Telemetry;

/// <summary>
/// A board resolved against the catalogue the caller was served: the charts it can
/// draw and the queries it names that the catalogue left out.
/// </summary>
/// <param name="Board">The board.</param>
/// <param name="Charts">The catalogue entries the board draws, in board order.</param>
/// <param name="Omitted">The titles of the board's queries the catalogue does not offer this caller.</param>
internal sealed record TelemetryBoardPlan(
    TelemetryBoard Board,
    IReadOnlyList<TelemetryQueryDescriptor> Charts,
    IReadOnlyList<string> Omitted)
{
    /// <summary>Whether the board has at least one chart the caller can read.</summary>
    public bool HasCharts => Charts.Count > 0;

    /// <summary>How many charts the board names in total.</summary>
    public int Total => Charts.Count + Omitted.Count;
}
