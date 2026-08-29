namespace Orleans.Lattice.Api.Telemetry;

/// <summary>
/// A request to evaluate one curated named query: the query's catalogue id plus
/// the bounded parameters that entry declares. There is no field for query text,
/// and none for a tenant id, so neither can be supplied by a caller.
/// </summary>
/// <remarks>
/// <para>
/// <b>Selection, not composition.</b> <see cref="QueryId"/> selects a
/// server-authored entry from <see cref="TelemetryQueryCatalog"/>. An id absent
/// from the caller's catalogue is rejected with
/// <see cref="TelemetryQueryNotFoundException"/>, which unifies "no such query"
/// with "not offered to you" so a caller cannot probe for queries outside its
/// entitlement.
/// </para>
/// <para>
/// <b>Tenant scoping is requested, not asserted.</b>
/// <see cref="RequestedVisibility"/> is the only tenancy input, and it is a
/// request rather than an assertion: the facade derives the effective tenant from
/// the authenticated caller, validates any cross-tenant request server-side, and
/// reports what it actually applied on
/// <see cref="TelemetryQueryResponse.Scope"/>. An unvalidated cross-tenant request
/// degrades to the caller's active tenant.
/// </para>
/// <para>
/// <b>Undeclared parameters are ignored.</b> A value set for a parameter the
/// selected entry does not declare in
/// <see cref="TelemetryQueryDescriptor.Parameters"/> has no effect; it can never
/// widen the query.
/// </para>
/// </remarks>
[GenerateSerializer]
[Alias(ApiTelemetryTypeAliases.TelemetryQueryRequest)]
[Immutable]
public sealed record TelemetryQueryRequest
{
    /// <summary>
    /// The catalogue-stable id of the query to evaluate, compared ordinally.
    /// </summary>
    [Id(0)] public required string QueryId { get; init; }

    /// <summary>
    /// The evaluation window. A <see cref="TelemetryQueryKind.Range"/> entry uses
    /// its start, end, and step; an <see cref="TelemetryQueryKind.Instant"/> entry
    /// uses only its end as the evaluation instant. Checked against the entry's
    /// <see cref="TelemetryQueryDescriptor.Bounds"/> before evaluation.
    /// </summary>
    [Id(1)] public TelemetryTimeRange Range { get; init; }

    /// <summary>
    /// Optional single logical tree id to narrow the result to, honoured only when
    /// the entry declares <see cref="TelemetryQueryParameters.TreeFilter"/>. The
    /// filter narrows within the effective tenant scope and can never widen it, so
    /// naming a tree outside that scope yields no series rather than another
    /// tenant's data. <see langword="null"/> (the default) applies no tree filter.
    /// </summary>
    [Id(2)] public string? TreeId { get; init; }

    /// <summary>
    /// The tenant visibility the caller would like. Defaults, fail-closed, to
    /// <see cref="TelemetryTenantVisibility.ActiveTenant"/>. Requesting
    /// <see cref="TelemetryTenantVisibility.AllTenants"/> is honoured only after
    /// the facade validates the caller as a platform operator; otherwise the query
    /// is served at active-tenant scope and the response reports the degradation.
    /// </summary>
    [Id(3)] public TelemetryTenantVisibility RequestedVisibility { get; init; }
}
