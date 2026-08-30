namespace Orleans.Lattice.Explorer.Telemetry;

/// <summary>
/// The tenant scope a telemetry query was <em>actually</em> evaluated under, as
/// the facade reported it: the visibility that was requested, the visibility that
/// was applied, and - when the applied visibility names a single tenant - the
/// tenant the facade derived.
/// </summary>
/// <remarks>
/// <para>
/// <b>Every field here is server-decided output.</b> The seam copies the facade's
/// answer across without reinterpreting it: it never derives a tenant, never
/// substitutes one, and never filters a series by one. A panel renders the scope
/// this type carries, never the one it asked for.
/// </para>
/// <para>
/// <b>The six outcomes stay six.</b> An honoured single-tenant view and a refused
/// one differ only in the requested visibility they record, and that difference
/// is exactly what <see cref="WasDowngraded"/> is computed from - so an honoured
/// operator view reports <see cref="WasDowngraded"/> as
/// <see langword="false"/> while a refused one reports <see langword="true"/>,
/// even though both name exactly one tenant.
/// </para>
/// <para>
/// A <see langword="readonly"/> <see langword="record"/>
/// <see langword="struct"/>, so a polling panel reading it per response
/// allocates nothing beyond the tenant id string the response already carries.
/// </para>
/// </remarks>
/// <param name="RequestedVisibility">The visibility the caller asked for.</param>
/// <param name="EffectiveVisibility">
/// The visibility the facade applied after validating the request server-side.
/// This is the authoritative value.
/// </param>
/// <param name="TenantId">
/// The tenant the query was pinned to whenever
/// <paramref name="EffectiveVisibility"/> names a single tenant, or
/// <see langword="null"/> for a validated cross-tenant evaluation.
/// </param>
public readonly record struct ExplorerTelemetryScope(
    ExplorerTelemetryVisibility RequestedVisibility,
    ExplorerTelemetryVisibility EffectiveVisibility,
    string? TenantId)
{
    /// <summary>
    /// The scope an unevaluated or absent result reports: the fail-closed
    /// active-tenant view with no tenant established.
    /// </summary>
    public static ExplorerTelemetryScope None { get; } = new(
        ExplorerTelemetryVisibility.ActiveTenant,
        ExplorerTelemetryVisibility.ActiveTenant,
        TenantId: null);

    /// <summary>
    /// <see langword="true"/> when the facade served a narrower visibility than
    /// the caller requested - the fail-closed degradation of an unvalidated
    /// widening request. A panel that ignores this labels a single tenant's data
    /// as though it were the whole cluster's.
    /// </summary>
    public bool WasDowngraded => RequestedVisibility != EffectiveVisibility;

    /// <summary>
    /// <see langword="true"/> when the query was evaluated across <em>every</em>
    /// tenant's series. An honoured single-tenant evaluation is
    /// <see langword="false"/> here: it is scoped to exactly one tenant, even
    /// when that tenant is not the caller's own.
    /// </summary>
    public bool IsCrossTenant => EffectiveVisibility == ExplorerTelemetryVisibility.AllTenants;
}
