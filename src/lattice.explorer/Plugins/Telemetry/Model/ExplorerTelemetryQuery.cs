namespace Orleans.Lattice.Explorer.Plugins.Telemetry;

/// <summary>
/// One curated query the cluster offers, described in the server's own words:
/// its title, its unit, what a measurement of it counts, the parameters it
/// accepts, the window limits it enforces, and the instruments it reads.
/// </summary>
/// <remarks>
/// <para>
/// <b>This is what a panel is built from.</b> A panel renders the
/// <see cref="Title"/>, <see cref="Unit"/>, and <see cref="Semantic"/> the
/// server published rather than strings of its own next to a hard-coded query
/// id, so a panel's label cannot drift from the instrument behind it when the
/// catalogue changes.
/// </para>
/// </remarks>
public sealed record ExplorerTelemetryQuery
{
    private static readonly ExplorerTelemetryInstrument[] NoInstruments = [];

    /// <summary>The catalogue-stable id, and the only thing a request selects by.</summary>
    public required string QueryId { get; init; }

    /// <summary>The server-authored title a panel renders.</summary>
    public required string Title { get; init; }

    /// <summary>The server-authored description a panel renders as help text.</summary>
    public required string Description { get; init; }

    /// <summary>The unit the values are in.</summary>
    public required string Unit { get; init; }

    /// <summary>Whether the query is evaluated at an instant or across a window.</summary>
    public ExplorerTelemetryQueryKind Kind { get; init; }

    /// <summary>What one measurement counts.</summary>
    public ExplorerTelemetrySemantic Semantic { get; init; }

    /// <summary>The bounded inputs the entry accepts.</summary>
    public ExplorerTelemetryParameters Parameters { get; init; }

    /// <summary>The window limits the entry enforces.</summary>
    public ExplorerTelemetryBounds Bounds { get; init; }

    /// <summary>The instruments the query reads.</summary>
    public required IReadOnlyList<ExplorerTelemetryInstrument> Instruments { get; init; }

    /// <summary>The shared empty instrument list, for an entry that names none.</summary>
    public static IReadOnlyList<ExplorerTelemetryInstrument> NoInstrumentsDeclared => NoInstruments;

    /// <summary>
    /// Whether the entry accepts <paramref name="parameter"/>, so a panel can
    /// enable or hide the control that supplies it.
    /// </summary>
    /// <param name="parameter">The parameter to test. <see cref="ExplorerTelemetryParameters.None"/> is never accepted.</param>
    /// <returns><see langword="true"/> when the entry declares it.</returns>
    public bool Accepts(ExplorerTelemetryParameters parameter) =>
        parameter != ExplorerTelemetryParameters.None && (Parameters & parameter) == parameter;

    /// <summary>Whether the query reads the instrument named <paramref name="instrumentName"/>.</summary>
    /// <param name="instrumentName">The instrument name, compared ordinally.</param>
    /// <returns><see langword="true"/> when the entry names it.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="instrumentName"/> is <see langword="null"/>.</exception>
    public bool ReadsInstrument(string instrumentName)
    {
        ArgumentNullException.ThrowIfNull(instrumentName);

        var instruments = Instruments;
        for (var i = 0; i < instruments.Count; i++)
        {
            if (string.Equals(instruments[i].Name, instrumentName, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }
}
