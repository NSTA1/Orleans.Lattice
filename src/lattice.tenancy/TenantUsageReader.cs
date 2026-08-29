namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The implementation of <see cref="ITenantUsageReader"/>. Joins the warm
/// per-tenant usage index (<see cref="ITenantUsageIndex"/>), the tenant's
/// enforcement scope (<see cref="ITenantEnforcementScopeResolver"/>), and the
/// durable metered-overage billing seam (<see cref="ITenantOverageBilling"/>)
/// into a single <see cref="TenantUsageReading"/>.
/// </summary>
/// <remarks>
/// <para>
/// Deliberately selects the usage aggregate the tenant's own enforcement scope
/// admits against (<see cref="TenantUsageView.UsageFor"/>) rather than always
/// folding globally, so the returned
/// <see cref="TenantObservabilitySnapshot.InstantaneousOverage"/> is the overage
/// the tenant is actually being admitted on, and the reported
/// <see cref="TenantUsageReading.Scope"/> names the aggregate it was derived
/// from. The sibling <c>TenantObservabilitySource</c> keeps its unconditional
/// global fold, because the per-tenant gauges it feeds are cluster-fabric series.
/// </para>
/// <para>
/// Allocation: a warm frozen-dictionary probe and a value-typed projection, so
/// the only per-call heap traffic is the awaited metered-overage read and the
/// async state machine. The reading itself is a struct.
/// </para>
/// </remarks>
internal sealed class TenantUsageReader(
    ITenantUsageIndex usageIndex,
    ITenantOverageBilling overageBilling,
    ITenantEnforcementScopeResolver scopeResolver) : ITenantUsageReader
{
    private readonly ITenantUsageIndex _usageIndex =
        usageIndex ?? throw new ArgumentNullException(nameof(usageIndex));

    private readonly ITenantOverageBilling _overageBilling =
        overageBilling ?? throw new ArgumentNullException(nameof(overageBilling));

    private readonly ITenantEnforcementScopeResolver _scopeResolver =
        scopeResolver ?? throw new ArgumentNullException(nameof(scopeResolver));

    /// <inheritdoc />
    public TenantEnforcementScope ResolveScope(TenantId tenant) => _scopeResolver.Resolve(tenant);

    /// <inheritdoc />
    public async Task<TenantUsageReading?> ReadAsync(
        TenantId tenant,
        CancellationToken cancellationToken = default)
    {
        if (tenant.Value is null)
        {
            return null;
        }

        await _usageIndex.EnsureWarmAsync(cancellationToken).ConfigureAwait(false);
        if (!_usageIndex.TryGetView(tenant, out var view))
        {
            return null;
        }

        var scope = _scopeResolver.Resolve(tenant);

        var overage = await _overageBilling
            .GetMeteredOverageAsync(tenant, cancellationToken)
            .ConfigureAwait(false);

        var snapshot = new TenantObservabilitySnapshot(
            tenant, view.UsageFor(scope), view.Quotas, overage);

        return new TenantUsageReading(snapshot, scope);
    }
}
