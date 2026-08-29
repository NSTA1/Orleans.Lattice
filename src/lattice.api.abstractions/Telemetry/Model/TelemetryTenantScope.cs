namespace Orleans.Lattice.Api.Telemetry;

/// <summary>
/// The tenant scope a telemetry query was <em>actually</em> evaluated under,
/// reported on every response. It carries the visibility the caller requested, the
/// visibility the facade pinned, and - when the pinned visibility is
/// per-tenant - the tenant id the facade derived. The two visibilities can differ,
/// and this type is how that difference reaches the client.
/// </summary>
/// <remarks>
/// <para>
/// <b>The effective tenant is never trusted from the request.</b> A request
/// carries a requested <see cref="TelemetryTenantVisibility"/> and, for an
/// operator's single-tenant request, a requested tenant id - both of which are
/// requests the facade may refuse. The facade derives the effective tenant from
/// the authenticated caller, validates any widened request server-side, and pins
/// the result here. The reported <see cref="TenantId"/> is therefore always
/// server-decided output; it equals a caller's requested tenant only when the
/// facade validated that caller and chose to honour it.
/// </para>
/// <para>
/// <b>Degradation is visible, not silent.</b> An unvalidated
/// <see cref="TelemetryTenantVisibility.AllTenants"/> or
/// <see cref="TelemetryTenantVisibility.SingleTenant"/> request is served at
/// <see cref="TelemetryTenantVisibility.ActiveTenant"/> scope rather than refused,
/// and <see cref="WasDowngraded"/> reports it, so a client labels the panel for
/// the scope it actually got instead of the one it asked for.
/// </para>
/// <para>
/// This is a value-typed descriptor (a <see langword="readonly"/> record struct),
/// so attaching a scope to every response costs no heap allocation beyond the
/// tenant id string the facade already holds.
/// </para>
/// </remarks>
[GenerateSerializer]
[Alias(ApiTelemetryTypeAliases.TelemetryTenantScope)]
[Immutable]
public readonly record struct TelemetryTenantScope
{
    /// <summary>The visibility the caller asked for, echoed so a degradation is attributable.</summary>
    [Id(0)] public TelemetryTenantVisibility RequestedVisibility { get; init; }

    /// <summary>
    /// The visibility the facade actually applied after server-side validation.
    /// This is the authoritative value; the requested one is informational.
    /// </summary>
    [Id(1)] public TelemetryTenantVisibility EffectiveVisibility { get; init; }

    /// <summary>
    /// The tenant the query was pinned to whenever
    /// <see cref="EffectiveVisibility"/> names a single tenant - either
    /// <see cref="TelemetryTenantVisibility.ActiveTenant"/> (the caller's own,
    /// derived from its credential) or
    /// <see cref="TelemetryTenantVisibility.SingleTenant"/> (an operator's request
    /// the facade validated and honoured). <see langword="null"/> for a validated
    /// cross-tenant evaluation. Always decided server-side.
    /// </summary>
    [Id(2)] public string? TenantId { get; init; }

    /// <summary>
    /// <see langword="true"/> when the facade served a narrower visibility than the
    /// caller requested - the fail-closed degradation of an unvalidated widening
    /// request, whether that request was
    /// <see cref="TelemetryTenantVisibility.AllTenants"/> or
    /// <see cref="TelemetryTenantVisibility.SingleTenant"/>.
    /// </summary>
    public bool WasDowngraded => RequestedVisibility != EffectiveVisibility;

    /// <summary>
    /// <see langword="true"/> when the query was evaluated across <em>every</em>
    /// tenant's series, which happens only after the facade validated the caller as
    /// a platform operator. An honoured
    /// <see cref="TelemetryTenantVisibility.SingleTenant"/> evaluation is
    /// <see langword="false"/> here: it is scoped to exactly one tenant, even
    /// though that tenant need not be the caller's own.
    /// </summary>
    public bool IsCrossTenant => EffectiveVisibility == TelemetryTenantVisibility.AllTenants;

    /// <summary>
    /// Creates a scope pinned, fail-closed, to the caller's own active tenant while
    /// recording what the caller had requested. Passing
    /// <see cref="TelemetryTenantVisibility.AllTenants"/> or
    /// <see cref="TelemetryTenantVisibility.SingleTenant"/> as
    /// <paramref name="requestedVisibility"/> therefore records a refused widening
    /// request, which <see cref="WasDowngraded"/> then reports.
    /// </summary>
    /// <param name="tenantId">The server-derived effective tenant. Must be non-empty.</param>
    /// <param name="requestedVisibility">The visibility the caller asked for.</param>
    /// <returns>An active-tenant scope.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="tenantId"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException"><paramref name="tenantId"/> is empty or white space.</exception>
    public static TelemetryTenantScope PinnedTo(string tenantId, TelemetryTenantVisibility requestedVisibility)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tenantId);

        return new TelemetryTenantScope
        {
            RequestedVisibility = requestedVisibility,
            EffectiveVisibility = TelemetryTenantVisibility.ActiveTenant,
            TenantId = tenantId,
        };
    }

    /// <summary>
    /// Creates the honoured single-tenant scope: requested and effective are both
    /// <see cref="TelemetryTenantVisibility.SingleTenant"/> and the query is pinned
    /// to <paramref name="tenantId"/>. Produced only after the facade has validated
    /// the caller as a platform operator and accepted its requested tenant; a
    /// refused request uses <see cref="PinnedTo"/> instead, which reports the
    /// degradation.
    /// </summary>
    /// <param name="tenantId">The requested tenant the facade validated and honoured. Must be non-empty.</param>
    /// <returns>An honoured single-tenant scope.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="tenantId"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException"><paramref name="tenantId"/> is empty or white space.</exception>
    public static TelemetryTenantScope AtRequestedTenant(string tenantId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tenantId);

        return new TelemetryTenantScope
        {
            RequestedVisibility = TelemetryTenantVisibility.SingleTenant,
            EffectiveVisibility = TelemetryTenantVisibility.SingleTenant,
            TenantId = tenantId,
        };
    }

    /// <summary>
    /// Creates the validated cross-tenant scope: requested and effective are both
    /// <see cref="TelemetryTenantVisibility.AllTenants"/> and no single tenant is
    /// pinned. Produced only after the facade has validated the caller as a
    /// platform operator.
    /// </summary>
    /// <returns>A cross-tenant scope.</returns>
    public static TelemetryTenantScope AcrossAllTenants() => new()
    {
        RequestedVisibility = TelemetryTenantVisibility.AllTenants,
        EffectiveVisibility = TelemetryTenantVisibility.AllTenants,
        TenantId = null,
    };
}
