using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.Telemetry;

/// <summary>
/// What a caller's <see cref="LatticeOperation.Telemetry"/> capability admits it
/// to, as <see cref="TelemetryTenantScopeResolver.AuthorizeAsync"/> decided it:
/// the whole cluster (the grant <see cref="LatticeScope.ClusterWide"/> authors), or
/// one tenant (the grant <see cref="LatticeScope.TenantCapability(TenantId)"/>
/// authors).
/// </summary>
/// <remarks>
/// In-process authorization vocabulary only: it is never persisted or sent on the
/// wire, so it carries no Orleans serialization attributes. A tenant-confined grant
/// pins every query to <see cref="TenantId"/>, whatever visibility the request asks
/// for (issue #5582).
/// </remarks>
public readonly record struct TelemetryAccessGrant
{
    private TelemetryAccessGrant(string? tenantId)
    {
        TenantId = tenantId;
    }

    /// <summary>The grant admitting the caller to the whole cluster's telemetry.</summary>
    public static TelemetryAccessGrant ClusterWide => default;

    /// <summary>
    /// <see langword="true"/> when the caller holds the cluster-wide capability, so
    /// the tenant scope is resolved as before (its own tenant, widened only for a
    /// validated platform operator).
    /// </summary>
    public bool IsClusterWide => TenantId is null;

    /// <summary>
    /// The one tenant a tenant-confined grant admits the caller to, taken from the
    /// caller's validated active tenant; <see langword="null"/> for a cluster-wide
    /// grant.
    /// </summary>
    public string? TenantId { get; }

    /// <summary>Creates the grant confined to <paramref name="tenantId"/>.</summary>
    /// <param name="tenantId">The validated tenant id. Must not be <see langword="null"/> or empty.</param>
    /// <returns>A tenant-confined grant.</returns>
    /// <exception cref="ArgumentException"><paramref name="tenantId"/> is <see langword="null"/> or empty.</exception>
    public static TelemetryAccessGrant ForTenant(string tenantId)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        return new TelemetryAccessGrant(tenantId);
    }
}
