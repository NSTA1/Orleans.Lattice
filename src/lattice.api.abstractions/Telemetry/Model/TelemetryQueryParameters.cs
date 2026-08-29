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
/// Tenant scoping is deliberately absent from this set. It is not a query
/// parameter at all: the effective tenant is derived server-side from the
/// authenticated caller, and a caller may only <em>request</em> a visibility (and,
/// for an operator's single-tenant request, name the tenant it wants), which the
/// facade re-validates and may refuse. Those fields therefore live on the request
/// itself rather than being declared per catalogue entry, so no entry can opt out
/// of tenant scoping by failing to declare it.
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
