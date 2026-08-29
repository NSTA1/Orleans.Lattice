namespace Orleans.Lattice.Api.Telemetry;

/// <summary>
/// The tenant visibility a telemetry query is evaluated under. It appears in two
/// distinct roles: as the visibility a caller <em>requests</em> on a
/// <see cref="TelemetryQueryRequest"/>, and as the visibility the facade
/// <em>pinned</em> on the resulting <see cref="TelemetryTenantScope"/>. The two
/// can differ, and the response's value is the authoritative one.
/// </summary>
/// <remarks>
/// <para>
/// There is no ambient view beyond the caller's own tenant. The default,
/// <see cref="ActiveTenant"/>, resolves to the tenant the facade derives from the
/// authenticated caller, so a tenant - and a platform operator that has not
/// asserted otherwise - sees only its own series. A caller must explicitly request
/// <see cref="AllTenants"/> or <see cref="SingleTenant"/> to see anything wider or
/// elsewhere, and the facade honours either request only after validating the
/// caller as a platform operator server-side.
/// </para>
/// <para>
/// An unvalidated widening request degrades, fail-closed, to
/// <see cref="ActiveTenant"/> rather than failing loudly, and the degradation is
/// reported through <see cref="TelemetryTenantScope.WasDowngraded"/> so the client
/// can label the view honestly instead of implying it is wider than it is.
/// </para>
/// <para>
/// This extends the Explorer's existing two-valued tenant-visibility contract with
/// the operator-only single-tenant selection, keeping the same fail-closed rule: a
/// request is only ever a request, and the effective scope is decided server-side.
/// </para>
/// </remarks>
[GenerateSerializer]
[Alias(ApiTelemetryTypeAliases.TelemetryTenantVisibility)]
public enum TelemetryTenantVisibility
{
    /// <summary>
    /// The default scope: evaluate only against the caller's own active tenant's
    /// series. Never reveals another tenant's telemetry.
    /// </summary>
    ActiveTenant = 0,

    /// <summary>
    /// The explicit cross-tenant scope: evaluate against every tenant's series.
    /// Honoured only when the facade validates the caller as a platform operator;
    /// otherwise the query falls back, fail-closed, to
    /// <see cref="ActiveTenant"/>.
    /// </summary>
    AllTenants = 1,

    /// <summary>
    /// The explicit single-tenant scope: evaluate against the series of the one
    /// tenant named by <see cref="TelemetryQueryRequest.RequestedTenantId"/>, which
    /// need not be the caller's own. Honoured only when the facade validates the
    /// caller as a platform operator; otherwise the query falls back, fail-closed,
    /// to <see cref="ActiveTenant"/> and the requested tenant id is ignored
    /// entirely.
    /// </summary>
    /// <remarks>
    /// This exists so a platform-operator surface can render one tenant without
    /// fetching every tenant's series and filtering client-side, which costs data
    /// proportional to the tenant count in order to display one of them. It is not
    /// a widening of authority: a caller the facade validates for this is already
    /// validated for <see cref="AllTenants"/>, so it narrows what is fetched
    /// rather than broadening what is reachable.
    /// </remarks>
    SingleTenant = 2,
}
