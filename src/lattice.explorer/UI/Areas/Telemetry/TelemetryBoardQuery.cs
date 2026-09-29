namespace Orleans.Lattice.Explorer.UI.Areas.Telemetry;

/// <summary>A catalogue query a board draws, by id.</summary>
/// <param name="QueryId">The server-authored catalogue query id.</param>
/// <param name="Title">
/// The title the area names the query by when the catalogue omits it (the caller's
/// grant or the metric allow-list does not admit it), so the note can say what is missing.
/// </param>
internal sealed record TelemetryBoardQuery(string QueryId, string Title);
