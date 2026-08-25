namespace Orleans.Lattice.Tenancy;

/// <summary>
/// One tenant's converged metered overage: its identity paired with the
/// cross-cluster fold of its grow-only overage counters. The unit streamed by
/// <see cref="ITenantOverageBilling.ListMeteredOverageAsync"/> so a billing consumer
/// can enumerate every tenant's billable overage in one pass.
/// </summary>
/// <remarks>
/// A transient, derived read result: it is never persisted and never crosses a
/// grain boundary as a payload (the durable state is the per-tenant
/// <see cref="TenantOverageRecord"/>), so it carries no Orleans serialization
/// attributes.
/// </remarks>
public readonly record struct TenantMeteredOverage
{
    /// <summary>Initializes a tenant's metered-overage projection.</summary>
    /// <param name="tenant">The tenant the overage is metered for.</param>
    /// <param name="overage">The tenant's converged cross-cluster metered overage.</param>
    public TenantMeteredOverage(TenantId tenant, TenantOverageSample overage)
    {
        Tenant = tenant;
        Overage = overage;
    }

    /// <summary>The tenant the overage is metered for.</summary>
    public TenantId Tenant { get; init; }

    /// <summary>The tenant's converged cross-cluster metered overage.</summary>
    public TenantOverageSample Overage { get; init; }
}
