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
/// There is no ambient cross-tenant view. The default,
/// <see cref="ActiveTenant"/>, resolves to the tenant the facade derives from the
/// authenticated caller, so a tenant - and a platform operator that has not
/// asserted otherwise - sees only its own series. A caller must explicitly request
/// <see cref="AllTenants"/> to see across tenants, and the facade honours that
/// request only after validating the caller as a platform operator server-side.
/// </para>
/// <para>
/// An unvalidated cross-tenant request degrades, fail-closed, to
/// <see cref="ActiveTenant"/> rather than failing loudly, and the degradation is
/// reported through <see cref="TelemetryTenantScope.WasDowngraded"/> so the client
/// can label the view honestly instead of implying it is cross-tenant.
/// </para>
/// <para>
/// This mirrors the Explorer's existing tenant-visibility contract so a client
/// switching between the two surfaces gets one, consistent fail-closed rule.
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
}
