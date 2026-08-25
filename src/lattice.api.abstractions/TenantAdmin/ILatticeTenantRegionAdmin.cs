namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// Transport-agnostic <b>per-tenant region-residency</b> control facade: one
/// coherent, discoverable, fail-closed surface for the three region-residency
/// operations - an operator authorizing a tenant's allowed region set, a tenant
/// admin setting the tenant's residency within that allowed set, and reading a
/// tenant's per-region status. It is a sibling of
/// <see cref="ILatticeTenantAdmin"/>, added append-only alongside it so the tenant
/// lifecycle surface is unchanged. Every transport binding (gRPC, MCP) is a thin
/// adapter over this single surface.
/// </summary>
/// <remarks>
/// <para>
/// <b>Two-tier, fail-closed authorization.</b> Authorizing the allowed region set
/// is an <i>operator</i> action, authorized as cluster-wide
/// <see cref="Orleans.Lattice.LatticeOperation.Admin"/> on the reserved auth policy
/// tree, which the auth gate's control-plane isolation grants only to a platform
/// operator and denies to every other caller <b>regardless of the data-plane
/// default effect</b>. Setting residency and reading status are <i>tenant-admin</i>
/// actions, authorized when the caller is that operator <b>or</b> a live admin
/// subject on the tenant record. Both tiers are independent of the data-plane
/// <c>DefaultEffect</c>, so an unmatched request always resolves to deny.
/// </para>
/// <para>
/// <b>Invariants.</b> Residency is always a subset of the allowed set; the last
/// resident region can never be removed (an unbypassable guard); the residency set
/// is runtime-mutable. Replication of a tenant's data is scoped to its resident
/// regions, while tenant definitions still converge everywhere.
/// </para>
/// </remarks>
public interface ILatticeTenantRegionAdmin
{
    /// <summary>
    /// Authorizes a tenant's allowed region set (an <b>operator</b> action). Sets
    /// the allowed set to exactly <paramref name="allowedRegions"/>: regions not
    /// currently allowed are authorized, and currently-allowed regions absent from
    /// the set are revoked. Revoking a region the tenant is still resident in is
    /// refused fail-closed (residency must stay a subset of the allowed set).
    /// </summary>
    /// <param name="tenantId">The tenant id. Must be a valid, non-empty tenant id.</param>
    /// <param name="allowedRegions">The complete desired allowed region set. Must not be <c>null</c>; each id must be non-empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The authorization result with the resulting allowed region ids.</returns>
    /// <exception cref="ArgumentException"><paramref name="tenantId"/> is invalid, or an entry of <paramref name="allowedRegions"/> is <c>null</c> or empty.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="allowedRegions"/> is <c>null</c>.</exception>
    /// <exception cref="TenantNotFoundException">No tenant with that id is registered.</exception>
    /// <exception cref="TenantRegionNotAllowedException">A revoked region is still resident.</exception>
    /// <exception cref="Orleans.Lattice.LatticeAuthorizationDeniedException">The caller is not a platform operator.</exception>
    Task<TenantRegionAuthorizationResult> AuthorizeAllowedRegionsAsync(
        string tenantId, IReadOnlyCollection<string> allowedRegions, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sets a tenant's residency set within its allowed regions (a <b>tenant-admin</b>
    /// action). Regions newly in the set begin adding (transition to
    /// <see cref="TenantRegionLifecycleStatus.Provisioning"/>); currently-resident
    /// regions absent from the set begin removing (transition to
    /// <see cref="TenantRegionLifecycleStatus.Draining"/>). Every region in the set
    /// must be allowed, and the change may never remove the last resident region -
    /// both are enforced fail-closed.
    /// </summary>
    /// <param name="tenantId">The tenant id. Must be a valid, non-empty tenant id.</param>
    /// <param name="residencyRegions">The complete desired residency set. Must not be <c>null</c>; each id must be non-empty; must not be empty (a tenant must stay resident somewhere).</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The residency-change result with the added, removed, and resulting regions.</returns>
    /// <exception cref="ArgumentException"><paramref name="tenantId"/> is invalid, or an entry of <paramref name="residencyRegions"/> is <c>null</c> or empty.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="residencyRegions"/> is <c>null</c>.</exception>
    /// <exception cref="TenantNotFoundException">No tenant with that id is registered.</exception>
    /// <exception cref="TenantRegionNotAllowedException">A requested residency region is not in the allowed set.</exception>
    /// <exception cref="TenantLastRegionException">The change would remove the last resident region.</exception>
    /// <exception cref="Orleans.Lattice.LatticeAuthorizationDeniedException">The caller is neither a platform operator nor a tenant admin.</exception>
    Task<TenantResidencyChangeResult> SetResidencyAsync(
        string tenantId, IReadOnlyCollection<string> residencyRegions, CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads a tenant's per-region residency status (a <b>tenant-admin</b> action):
    /// one row per region that is either allowed or carries a non-<c>None</c>
    /// status, ordered by region id.
    /// </summary>
    /// <param name="tenantId">The tenant id. Must be a valid, non-empty tenant id.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The per-region status report.</returns>
    /// <exception cref="ArgumentException"><paramref name="tenantId"/> is <c>null</c>, empty, or not a valid tenant id.</exception>
    /// <exception cref="TenantNotFoundException">No tenant with that id is registered.</exception>
    /// <exception cref="Orleans.Lattice.LatticeAuthorizationDeniedException">The caller is neither a platform operator nor a tenant admin.</exception>
    Task<TenantRegionStatusReport> GetTenantRegionStatusAsync(
        string tenantId, CancellationToken cancellationToken = default);
}
