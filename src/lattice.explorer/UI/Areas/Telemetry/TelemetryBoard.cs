namespace Orleans.Lattice.Explorer.UI.Areas.Telemetry;

/// <summary>
/// One curated board of the Telemetry area: a titled group of catalogue queries
/// drawn together, addressed as <c>/telemetry/{Key}</c>.
/// </summary>
/// <param name="Key">The board's lower-case address segment.</param>
/// <param name="Title">The board's heading.</param>
/// <param name="Summary">One sentence saying what the board shows.</param>
/// <param name="Queries">The catalogue queries the board draws, in order, each with the title used when the catalogue omits it.</param>
/// <param name="TenancyOnly">Whether the board exists only while tenancy is on.</param>
internal sealed record TelemetryBoard(
    string Key,
    string Title,
    string Summary,
    IReadOnlyList<TelemetryBoardQuery> Queries,
    bool TenancyOnly = false);
