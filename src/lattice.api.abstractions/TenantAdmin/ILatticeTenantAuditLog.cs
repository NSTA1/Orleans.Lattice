using Orleans.Lattice.Api.Auth;

namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// Tenant-admin reader of the durable admin audit trail: the records of one
/// tenant's own partition (<c>t/{tenant}</c>), filtered and paged newest first.
/// </summary>
/// <remarks>
/// <para>
/// <b>Fail-closed, tenant-admin tier.</b> A call is authorized when the caller is a
/// platform operator or a live admin subject of the named, active tenant - the same
/// tier as <see cref="ILatticeTenantAccessAdmin"/>. Anyone else is told
/// <i>denied</i> (an <see cref="UnauthorizedAccessException"/>) whether or not the
/// tenant exists, and the refusal is recorded as a
/// <see cref="LatticeAdminAuditOutcome.Denied"/> <see cref="LatticeAdminAuditAction.Read"/>.
/// </para>
/// <para>
/// <b>One partition only.</b> The reader scans the named tenant's partition and
/// nothing else; <see cref="AdminAuditQuery.Scope"/> is ignored, so a tenant admin can
/// never read the cluster partition or another tenant's.
/// </para>
/// <para>
/// <b>Final records only.</b> Pending (in-flight) intents are never returned; they
/// are visible to the operator reader (<see cref="ILatticeAdminAuditLog"/>) alone.
/// </para>
/// </remarks>
public interface ILatticeTenantAuditLog
{
    /// <summary>Lists the final admin audit records of <paramref name="tenantId"/> matching <paramref name="query"/>, newest first.</summary>
    /// <param name="tenantId">The tenant whose partition to read.</param>
    /// <param name="query">The filters and paging cursor; <see cref="AdminAuditQuery.Scope"/> is ignored.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>One page of records and the cursor for the next page.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="tenantId"/> or <paramref name="query"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException"><paramref name="tenantId"/> is not a valid tenant id, or the page token is malformed.</exception>
    /// <exception cref="UnauthorizedAccessException">The caller is neither a platform operator nor an admin subject of the tenant.</exception>
    Task<AdminAuditPage> ListAuditAsync(string tenantId, AdminAuditQuery query, CancellationToken cancellationToken = default);
}
