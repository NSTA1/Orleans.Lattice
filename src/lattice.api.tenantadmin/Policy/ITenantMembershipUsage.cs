namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// The membership-usage seam the tenant posture reads: how many tenant groups a
/// tenant has, and how many membership edges hang off them. The production
/// implementation, <see cref="ScopedStoreTenantMembershipUsage"/>, counts through
/// the membership directory's tenant-scoped store; tests substitute fixed counts.
/// </summary>
internal interface ITenantMembershipUsage
{
    /// <summary>Counts the tenant's groups, or returns <see langword="null"/> when the count cannot be measured.</summary>
    /// <param name="tenant">The tenant. Must not be the reserved default tenant.</param>
    /// <param name="cancellationToken">Cancels the count.</param>
    /// <returns>The group count, or <see langword="null"/> when unmeasured.</returns>
    Task<long?> CountGroupsAsync(TenantId tenant, CancellationToken cancellationToken);

    /// <summary>Counts the membership edges under the tenant's groups, or returns <see langword="null"/> when the count cannot be measured.</summary>
    /// <param name="tenant">The tenant. Must not be the reserved default tenant.</param>
    /// <param name="cancellationToken">Cancels the count.</param>
    /// <returns>The edge count, or <see langword="null"/> when unmeasured.</returns>
    Task<long?> CountEdgesAsync(TenantId tenant, CancellationToken cancellationToken);
}
