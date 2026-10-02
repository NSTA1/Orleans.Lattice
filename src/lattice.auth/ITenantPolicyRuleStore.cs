namespace Orleans.Lattice.Auth;

/// <summary>
/// The internal tenant-tier maintenance surface of the authorization policy store,
/// for the tenancy add-on's tenant deletion pipeline. Registered by
/// <c>AddLatticeAuth</c> and resolved from the container by the tenancy package
/// (through <c>InternalsVisibleTo</c>); it is not part of the public
/// <see cref="ILatticeAuthorizationPolicyStore"/> contract, which stays unchanged.
/// </summary>
internal interface ITenantPolicyRuleStore
{
    /// <summary>
    /// Removes every tenant-tier rule owned by <paramref name="tenant"/> - every rule
    /// whose id is <c>tenant:{tenant}:...</c> - wherever it is scoped. Idempotent and
    /// resumable: it re-scans the policy tree on every call, so a purge interrupted
    /// part-way finishes on the next call, and a call after a completed purge removes
    /// nothing. Runs under system origin, so it is not subject to the tenant-tier
    /// write guard; the caller authorizes the purge.
    /// </summary>
    /// <param name="tenant">The tenant whose rules to remove. Must be an initialised tenant other than <see cref="TenantId.Default"/>.</param>
    /// <param name="cancellationToken">Cancels the purge between deletions.</param>
    /// <returns>The number of rules this call removed.</returns>
    /// <exception cref="ArgumentException">
    /// <paramref name="tenant"/> is the uninitialised "no tenant" value or the reserved
    /// <see cref="TenantId.Default"/> tenant, which has no tenant-tier rules.
    /// </exception>
    Task<int> PurgeTenantRulesAsync(TenantId tenant, CancellationToken cancellationToken = default);
}
