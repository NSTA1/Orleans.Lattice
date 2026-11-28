namespace Orleans.Lattice.Api.Auth;

/// <summary>
/// Operator reader of the durable admin audit trail (the reserved
/// <c>sys-admin-audit</c> tree): every partition, the cluster partition and every
/// tenant's, filtered and paged newest first.
/// </summary>
/// <remarks>
/// <para>
/// <b>Fail-closed, operator only.</b> Every call is gated exactly like every
/// <see cref="ILatticeAuthAdmin"/> call: cluster-wide
/// <see cref="Orleans.Lattice.LatticeOperation.Admin"/> on the reserved auth policy
/// tree. A refused call throws <see cref="UnauthorizedAccessException"/> and is
/// itself recorded as a <see cref="LatticeAdminAuditOutcome.Denied"/>
/// <see cref="LatticeAdminAuditAction.Read"/>; a successful read is not recorded.
/// </para>
/// <para>
/// <b>In-flight calls.</b> A call whose intent has not been finalized yet is
/// returned as a <see cref="LatticeAdminAuditOutcome.Pending"/> record, meaning in
/// progress. Only this operator reader shows pending records; the tenant reader
/// never does.
/// </para>
/// </remarks>
public interface ILatticeAdminAuditLog
{
    /// <summary>Lists admin audit records matching <paramref name="query"/>, newest first.</summary>
    /// <param name="query">The filters and paging cursor.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>One page of records and the cursor for the next page.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="query"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">The page token is malformed.</exception>
    /// <exception cref="UnauthorizedAccessException">The caller is not a platform operator.</exception>
    Task<AdminAuditPage> ListAuditAsync(AdminAuditQuery query, CancellationToken cancellationToken = default);
}
