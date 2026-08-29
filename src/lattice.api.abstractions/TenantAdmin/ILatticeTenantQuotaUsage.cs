namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// Transport-agnostic <b>tenant usage-against-quota</b> read facade: the one
/// surface that reports what a tenant is actually consuming next to what it is
/// allowed to consume. It is a sibling of <see cref="ILatticeTenantAdmin"/>,
/// <see cref="ILatticeTenantRegionAdmin"/>, and
/// <see cref="ILatticeTenantSelfService"/>, added append-only so those surfaces
/// are unchanged, and every transport binding (gRPC, MCP) is a thin adapter over
/// this one surface.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why it exists.</b> <see cref="TenantStatusReport.Quotas"/> reports a
/// tenant's ceilings but no consumption, so a quota panel can render a limit but
/// not a bar. This facade closes that gap by projecting the tenancy engine's warm
/// per-tenant usage index - the same aggregate admission control enforces against
/// - onto the control-API contract.
/// </para>
/// <para>
/// <b>Two-tier, fail-closed authorization.</b> A platform operator may read any
/// tenant; a live tenant-admin subject may read only its own tenant; every other
/// caller is refused. Both tiers are independent of the data-plane
/// <c>DefaultEffect</c>, so an unmatched request always resolves to deny.
/// </para>
/// <para>
/// <b>No existence probe.</b> Exactly as
/// <see cref="ILatticeTenantSelfService.GetTenantAsync"/> does, an absent tenant
/// and an unauthorized tenant are unified into a single
/// <see cref="TenantNotFoundException"/>, so a caller can never learn whether a
/// tenant outside its authority exists.
/// </para>
/// <para>
/// <b>Read-only.</b> This facade never mutates a tenant; authoring quotas remains
/// the operator-only <see cref="ILatticeTenantAdmin.SetTenantQuotasAsync"/>
/// surface.
/// </para>
/// </remarks>
public interface ILatticeTenantQuotaUsage
{
    /// <summary>
    /// Reads one tenant's current usage against its quota ceilings: per dimension
    /// the consumption, the steady-state ceiling, the burst-adjusted admission
    /// ceiling, and the live and accrued overage, together with the
    /// <see cref="TenantQuotaUsageReport.EnforcementScope"/> the figures were read
    /// under. Fails closed with a <see cref="TenantNotFoundException"/> when the
    /// tenant does not exist <em>or</em> the caller is not authorized to read it -
    /// the two cases are deliberately indistinguishable.
    /// </summary>
    /// <param name="tenantId">The tenant id to read. Must be a valid, non-empty tenant id.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The tenant's usage-against-quota report.</returns>
    /// <exception cref="ArgumentException"><paramref name="tenantId"/> is <c>null</c>, empty, or not a valid tenant id.</exception>
    /// <exception cref="TenantNotFoundException">No such tenant is registered, or the caller is not authorized to read it.</exception>
    Task<TenantQuotaUsageReport> GetQuotaUsageAsync(
        string tenantId, CancellationToken cancellationToken = default);
}
