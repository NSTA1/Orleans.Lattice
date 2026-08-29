namespace Orleans.Lattice.Api.Telemetry;

/// <summary>
/// The closed set of bounded parameters a curated named query may accept, declared
/// per catalogue entry. A query honours exactly the flags it declares; a value
/// supplied for a parameter the entry does not declare is ignored rather than
/// widening the query.
/// </summary>
/// <remarks>
/// <para>
/// The set is deliberately closed. The facade exposes server-authored queries
/// selected by id and never accepts raw query text from any caller, so the only
/// caller-supplied inputs are these structural parameters, each clamped by the
/// entry's <see cref="TelemetryQueryBounds"/>. Adding a parameter is a considered
/// contract change here, not something a catalogue author can invent.
/// </para>
/// <para>
/// Tenant scoping is deliberately absent from this set. The effective tenant is
/// derived server-side from the authenticated caller and is never a query
/// parameter; a caller may only request a visibility, which the facade
/// re-validates and pins itself.
/// </para>
/// </remarks>
[Flags]
[GenerateSerializer]
[Alias(ApiTelemetryTypeAliases.TelemetryQueryParameters)]
public enum TelemetryQueryParameters
{
    /// <summary>The query accepts no caller-supplied parameters at all.</summary>
    None = 0,

    /// <summary>
    /// The query accepts a start and end instant. Declared by every
    /// <see cref="TelemetryQueryKind.Range"/> entry; an
    /// <see cref="TelemetryQueryKind.Instant"/> entry uses only the end instant
    /// and need not declare it.
    /// </summary>
    TimeRange = 1,

    /// <summary>
    /// The query accepts a resolution step. Only meaningful together with
    /// <see cref="TimeRange"/> on a <see cref="TelemetryQueryKind.Range"/> entry.
    /// </summary>
    Step = 2,

    /// <summary>
    /// The query accepts a single logical tree id to narrow the result to. The
    /// filter narrows within the effective tenant scope; it can never widen it,
    /// so naming another tenant's tree yields no series rather than that tenant's
    /// data.
    /// </summary>
    TreeFilter = 4,
}
