namespace Orleans.Lattice.Api.Telemetry;

/// <summary>
/// A request to evaluate one curated named query: the query's catalogue id plus
/// the bounded parameters that entry declares. There is no field for query text,
/// so a caller can only select a server-authored query, never compose one. The
/// two tenancy fields are requests the facade re-validates and may refuse, not
/// assertions it acts on.
/// </summary>
/// <remarks>
/// <para>
/// <b>Selection, not composition.</b> <see cref="QueryId"/> selects a
/// server-authored entry from <see cref="TelemetryQueryCatalog"/>. An id absent
/// from the caller's catalogue is rejected with
/// <see cref="TelemetryQueryNotFoundException"/>, which unifies "no such query"
/// with "not offered to you" so a caller cannot probe for queries outside its
/// entitlement. There is no field on this request that carries query text.
/// </para>
/// <para>
/// <b>Tenant scoping is requested, not asserted.</b>
/// <see cref="RequestedVisibility"/> and <see cref="RequestedTenantId"/> are the
/// only tenancy inputs, and both are requests rather than assertions: the facade
/// derives the effective tenant from the authenticated caller, validates any
/// widened request server-side, and reports what it actually applied on
/// <see cref="TelemetryQueryResponse.Scope"/>. An unvalidated request degrades to
/// the caller's active tenant.
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
    /// <see cref="TelemetryTenantVisibility.AllTenants"/> or
    /// <see cref="TelemetryTenantVisibility.SingleTenant"/> is honoured only after
    /// the facade validates the caller as a platform operator; otherwise the query
    /// is served at active-tenant scope and the response reports the degradation.
    /// </summary>
    [Id(3)] public TelemetryTenantVisibility RequestedVisibility { get; init; }

    /// <summary>
    /// The tenant a platform operator would like the query evaluated against, used
    /// only when <see cref="RequestedVisibility"/> is
    /// <see cref="TelemetryTenantVisibility.SingleTenant"/>. Ignored entirely for
    /// every other visibility.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>This is a request, never an assertion.</b> It is the exact counterpart of
    /// <see cref="RequestedVisibility"/>: the wire carries what the caller would
    /// like, and the facade decides. The facade honours it only after validating
    /// the caller as a platform operator server-side; for any other caller it is
    /// ignored in full and the effective scope fails closed to
    /// <see cref="TelemetryTenantVisibility.ActiveTenant"/> pinned to the tenant the
    /// facade derived from the authenticated caller.
    /// </para>
    /// <para>
    /// The effective tenant is therefore still derived server-side and is never
    /// trusted from the wire. A caller cannot read another tenant's series by
    /// naming it here any more than it can by asking for
    /// <see cref="TelemetryTenantVisibility.AllTenants"/>; what actually applied is
    /// always reported on <see cref="TelemetryQueryResponse.Scope"/>, and a refused
    /// request is visible through
    /// <see cref="TelemetryTenantScope.WasDowngraded"/>.
    /// </para>
    /// <para>
    /// It exists so a platform-operator surface can fetch one tenant directly
    /// instead of fetching every tenant and discarding all but one. This is the
    /// single property on this request permitted to name a tenant, and the contract
    /// guard fails the build if a second one appears.
    /// </para>
    /// </remarks>
    [Id(4)] public string? RequestedTenantId { get; init; }
}
