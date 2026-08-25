namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The real per-tenant <see cref="ITenantAdmissionController"/> contributed by the
/// tenancy package: it enforces each tenant's aggregate resource quota on the
/// write-admission path by admitting the write against the tenant's compiled usage
/// view. It reports itself <see cref="IsActive"/> unconditionally (registering the
/// tenancy add-on turns enforcement on) and overrides the core
/// <see cref="NullTenantAdmissionController"/>.
/// </summary>
/// <remarks>
/// <para>
/// A breach is signalled by <em>throwing</em> <see cref="LatticeQuotaExceededException"/>
/// (carrying the tenant id and breached dimension), which the T4 write-admission
/// seam propagates directly to the caller; the method never returns <c>false</c>,
/// so the seam's <see cref="LatticeTenantAccessDeniedException"/> refusal path is
/// reserved for a genuine access denial rather than a quota breach.
/// </para>
/// <para>
/// The decision is a warm, allocation-free in-memory read: a frozen-dictionary
/// lookup for the tenant's view, an enum scope resolve, a struct select of the
/// local or global usage sample, and the branch-only
/// <see cref="TenantQuotaEvaluator"/> check. Only a refusal allocates (the
/// exception). It fails open - admits - for an unknown or not-yet-warm tenant, so a
/// tenant with no landed usage sample is never spuriously refused.
/// </para>
/// </remarks>
internal sealed class LatticeTenantAdmissionController(
    ITenantUsageIndex index,
    ITenantEnforcementScopeResolver scopeResolver) : ITenantAdmissionController
{
    private readonly ITenantUsageIndex _index = index ?? throw new ArgumentNullException(nameof(index));
    private readonly ITenantEnforcementScopeResolver _scopeResolver =
        scopeResolver ?? throw new ArgumentNullException(nameof(scopeResolver));

    /// <inheritdoc />
    /// <remarks>Always active: registering the tenancy package turns quota enforcement on.</remarks>
    public bool IsActive => true;

    /// <inheritdoc />
    /// <remarks>
    /// Admits against the usage aggregate selected by the tenant's enforcement
    /// scope - the cross-cluster global fold under
    /// <see cref="TenantEnforcementScope.GlobalConverged"/>, or this cluster's local
    /// sample under <see cref="TenantEnforcementScope.PerCluster"/> - and throws
    /// <see cref="LatticeQuotaExceededException"/> on the first breached dimension.
    /// </remarks>
    public ValueTask<bool> IsAdmittedAsync(TenantId tenant, string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(treeId);

        // Fail open until the tenant has a warm view: an unknown tenant (not yet
        // compiled) or one with no landed sample is admitted. This is the documented
        // "enforcement fails open until the first sample lands" behaviour and keeps
        // the reserved default tenant (unbounded) on the fast path.
        if (!_index.TryGetView(tenant, out var view))
        {
            return new ValueTask<bool>(true);
        }

        var scope = _scopeResolver.Resolve(tenant);
        var usage = view.UsageFor(scope);

        // Throws LatticeQuotaExceededException (carrying the tenant id) on a breach;
        // returns normally when every bounded dimension is within its ceiling.
        TenantQuotaEvaluator.Admit(tenant, view.Quotas, usage, treeId);
        return new ValueTask<bool>(true);
    }
}
