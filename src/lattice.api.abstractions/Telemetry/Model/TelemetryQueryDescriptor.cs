namespace Orleans.Lattice.Api.Telemetry;

/// <summary>
/// One entry in the curated named-query catalogue: everything a client needs to
/// render and parameterise a panel, and nothing that lets it author a query. The
/// entry declares its stable id, its display title and unit, the kind of
/// evaluation it performs, what it measures, the bounded parameters it accepts,
/// and the instruments it reads.
/// </summary>
/// <remarks>
/// <para>
/// <b>No query text.</b> The descriptor deliberately carries no query expression.
/// The facade exposes server-authored queries selected by
/// <see cref="QueryId"/> and never accepts query text from any caller, so the
/// expression is not part of the client-facing contract at all; it lives on the
/// server-side <see cref="TelemetryQueryDefinition"/> that wraps this descriptor.
/// Structuring it this way makes the rule mechanical rather than documentary.
/// </para>
/// <para>
/// <b>Identity is stable.</b> <see cref="QueryId"/> names what the entry measures.
/// Changing what a query measures is a new id, never a mutation of an existing
/// one, so a saved dashboard, a bookmark, or a cached client cannot silently start
/// showing a different quantity under the same name.
/// </para>
/// <para>
/// <b>Title honesty.</b> <see cref="Semantic"/> and <see cref="Instruments"/>
/// together make a panel title checkable: a title implying a record rate over an
/// instrument that records one observation per operation is a declared mismatch a
/// guard can catch, rather than a discrepancy an operator discovers during an
/// incident.
/// </para>
/// </remarks>
[GenerateSerializer]
[Alias(ApiTelemetryTypeAliases.TelemetryQueryDescriptor)]
[Immutable]
public sealed record TelemetryQueryDescriptor
{
    /// <summary>
    /// The catalogue-stable id a caller selects this query by. Compared
    /// ordinally; it is an identifier, not display text.
    /// </summary>
    [Id(0)] public required string QueryId { get; init; }

    /// <summary>
    /// The human-readable panel title. It must be honest about
    /// <see cref="Semantic"/>: a title reading as a record rate over a
    /// <see cref="TelemetryMeasurementSemantic.PerOperation"/> query is drift.
    /// </summary>
    [Id(1)] public required string Title { get; init; }

    /// <summary>
    /// A longer description of what the query reports and how to read it,
    /// including any path it under-counts. May be empty, never
    /// <see langword="null"/> on a facade-produced entry.
    /// </summary>
    [Id(2)] public required string Description { get; init; }

    /// <summary>
    /// The unit of the values this query yields, for example <c>{op}/s</c>,
    /// <c>ms</c>, <c>By</c>, or <c>1</c> for a dimensionless ratio. This is the
    /// query's unit, which may differ from the units of the instruments it reads.
    /// </summary>
    [Id(3)] public required string Unit { get; init; }

    /// <summary>
    /// Whether the query is evaluated at an instant or across a range. Declared by
    /// the entry, never chosen by the caller.
    /// </summary>
    [Id(4)] public TelemetryQueryKind Kind { get; init; }

    /// <summary>
    /// What the query reports, in the per-operation versus per-record sense. A
    /// derived query over instruments of differing semantics reports
    /// <see cref="TelemetryMeasurementSemantic.Ratio"/>.
    /// </summary>
    [Id(5)] public TelemetryMeasurementSemantic Semantic { get; init; }

    /// <summary>
    /// The bounded parameters this query accepts. A value supplied for an
    /// undeclared parameter is ignored rather than widening the query.
    /// </summary>
    [Id(6)] public TelemetryQueryParameters Parameters { get; init; }

    /// <summary>
    /// The bounds applied to the accepted parameters. Defaults to
    /// <see cref="TelemetryQueryBounds.Unbounded"/>.
    /// </summary>
    [Id(7)] public TelemetryQueryBounds Bounds { get; init; }

    /// <summary>
    /// The instruments this query reads, each carrying its own unit and measured
    /// semantic. Empty only for a query that reads no instrument, which the
    /// catalogue does not otherwise produce.
    /// </summary>
    [Id(8)] public required IReadOnlyList<TelemetryInstrumentReference> Instruments { get; init; }

    /// <summary>
    /// <see langword="true"/> when this entry declares <paramref name="parameter"/>
    /// (every flag of it, for a combined value). <see cref="TelemetryQueryParameters.None"/>
    /// is never accepted, so a caller cannot probe with an empty flag set.
    /// </summary>
    /// <param name="parameter">The parameter, or combination of parameters, to test for.</param>
    /// <returns><see langword="true"/> when every requested flag is declared.</returns>
    public bool Accepts(TelemetryQueryParameters parameter) =>
        parameter != TelemetryQueryParameters.None && (Parameters & parameter) == parameter;

    /// <summary>
    /// <see langword="true"/> when this query reads an instrument named
    /// <paramref name="instrumentName"/>, compared ordinally. Scans the declared
    /// instruments by index, so the check allocates nothing.
    /// </summary>
    /// <param name="instrumentName">The fully qualified instrument name to look for.</param>
    /// <returns><see langword="true"/> when the instrument is declared.</returns>
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
