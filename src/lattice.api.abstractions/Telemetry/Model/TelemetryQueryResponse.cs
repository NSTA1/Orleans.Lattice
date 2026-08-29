namespace Orleans.Lattice.Api.Telemetry;

/// <summary>
/// The result of evaluating one curated named query: the series the backend
/// returned, the shape they take, the window actually evaluated, and - always -
/// the tenant scope the facade pinned.
/// </summary>
/// <remarks>
/// <para>
/// <b>The scope is not optional.</b> Every response reports the
/// <see cref="TelemetryTenantScope"/> it was served under, including whether the
/// caller's requested visibility was degraded, so a client can never present a
/// tenant-scoped panel as a cluster-wide one. There is no response shape that
/// omits it.
/// </para>
/// <para>
/// <b>The window is echoed, not assumed.</b> <see cref="Range"/> reports the
/// window and step the facade actually evaluated after clamping the request
/// against the entry's bounds, so a client renders the axis it really received
/// rather than the one it asked for.
/// </para>
/// <para>
/// A rejected request does not produce a response: an unknown or unoffered query
/// id raises <see cref="TelemetryQueryNotFoundException"/> and an out-of-bounds
/// window raises <see cref="TelemetryQueryBoundsException"/>, matching the typed,
/// fail-closed convention the sibling contract groups use.
/// </para>
/// </remarks>
[GenerateSerializer]
[Alias(ApiTelemetryTypeAliases.TelemetryQueryResponse)]
[Immutable]
public sealed record TelemetryQueryResponse
{
    /// <summary>
    /// The catalogue id of the query that was evaluated, echoed so a client
    /// multiplexing several panels can attribute a response without tracking call
    /// order.
    /// </summary>
    [Id(0)] public required string QueryId { get; init; }

    /// <summary>
    /// The tenant scope the query was actually evaluated under, including any
    /// fail-closed degradation of a requested cross-tenant visibility.
    /// </summary>
    [Id(1)] public required TelemetryTenantScope Scope { get; init; }

    /// <summary>The shape of the returned result.</summary>
    [Id(2)] public TelemetryResultKind ResultKind { get; init; }

    /// <summary>
    /// The series returned, in the order the backend produced them. Empty when
    /// <see cref="ResultKind"/> is <see cref="TelemetryResultKind.Empty"/>.
    /// </summary>
    [Id(3)] public required IReadOnlyList<TelemetryTimeSeries> Series { get; init; }

    /// <summary>
    /// The window and step actually evaluated, after the request was clamped
    /// against the catalogue entry's bounds.
    /// </summary>
    [Id(4)] public TelemetryTimeRange Range { get; init; }

    /// <summary>The number of series returned.</summary>
    public int SeriesCount => Series.Count;

    /// <summary>
    /// <see langword="true"/> when the query matched no series, whether reported
    /// as <see cref="TelemetryResultKind.Empty"/> or as an otherwise-shaped result
    /// carrying none.
    /// </summary>
    public bool IsEmpty => Series.Count == 0;
}
