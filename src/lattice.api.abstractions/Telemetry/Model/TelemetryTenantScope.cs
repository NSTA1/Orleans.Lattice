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
/// <b>The tenant is never read from the request.</b> A request carries only a
/// requested <see cref="TelemetryTenantVisibility"/>; it has no tenant-id field at
/// all. The facade derives the effective tenant from the authenticated caller,
/// re-validates any cross-tenant request server-side, and pins the result here. A
/// caller therefore cannot name a tenant, and this reported
/// <see cref="TenantId"/> is server-derived output, never echoed input.
/// </para>
/// <para>
/// <b>Degradation is visible, not silent.</b> An unvalidated
/// <see cref="TelemetryTenantVisibility.AllTenants"/> request is served as
/// <see cref="TelemetryTenantVisibility.ActiveTenant"/> rather than refused, and
/// <see cref="WasDowngraded"/> reports it, so a client labels the panel as
/// single-tenant instead of presenting a scoped view as if it were cluster-wide.
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
    /// The tenant the query was pinned to when
    /// <see cref="EffectiveVisibility"/> is
    /// <see cref="TelemetryTenantVisibility.ActiveTenant"/>; <see langword="null"/>
    /// for a validated cross-tenant evaluation. Derived server-side from the
    /// authenticated caller, never supplied by one.
    /// </summary>
    [Id(2)] public string? TenantId { get; init; }

    /// <summary>
    /// <see langword="true"/> when the facade served a narrower visibility than the
    /// caller requested - the fail-closed degradation of an unvalidated
    /// cross-tenant request.
    /// </summary>
    public bool WasDowngraded => RequestedVisibility != EffectiveVisibility;

    /// <summary>
    /// <see langword="true"/> when the query was evaluated across every tenant's
    /// series, which happens only after the facade validated the caller as a
    /// platform operator.
    /// </summary>
    public bool IsCrossTenant => EffectiveVisibility == TelemetryTenantVisibility.AllTenants;

    /// <summary>
    /// Creates a scope pinned to one tenant, recording what the caller had
    /// requested. Passing <see cref="TelemetryTenantVisibility.AllTenants"/> as
    /// <paramref name="requestedVisibility"/> records a fail-closed degradation.
    /// </summary>
    /// <param name="tenantId">The server-derived effective tenant. Must be non-empty.</param>
    /// <param name="requestedVisibility">The visibility the caller asked for.</param>
    /// <returns>A per-tenant scope.</returns>
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
