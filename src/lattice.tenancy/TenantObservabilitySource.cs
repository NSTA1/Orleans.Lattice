namespace Orleans.Lattice.Tenancy;

/// <summary>
/// Composes the warm per-tenant usage index (<see cref="ITenantUsageIndex"/>) and
/// the durable metered-overage billing seam (<see cref="ITenantOverageBilling"/>)
/// into <see cref="TenantObservabilitySnapshot"/> projections for the per-tenant
/// observable gauges and the <see cref="ITenantObservabilityView"/> read surface.
/// </summary>
/// <remarks>
/// Every read here is off the warm admission hot path: the publisher samples on
/// its own timer and the view is a low-frequency operator/tenant read. The bulk
/// <see cref="SnapshotAllAsync"/> pass performs one overage-tree enumeration and
/// joins it against the warm usage snapshot, so its allocations (a join
/// dictionary and the result list) are bounded by the tenant count and paid off
/// the hot path.
/// </remarks>
internal sealed class TenantObservabilitySource(
    ITenantUsageIndex usageIndex,
    ITenantOverageBilling overageBilling)
{
    private readonly ITenantUsageIndex _usageIndex =
        usageIndex ?? throw new ArgumentNullException(nameof(usageIndex));

    private readonly ITenantOverageBilling _overageBilling =
        overageBilling ?? throw new ArgumentNullException(nameof(overageBilling));

    /// <summary>
    /// Projects one tenant's observability snapshot from the warm usage view and
    /// its durable metered overage, or <c>null</c> when the tenant is uninitialised
    /// or absent from the warm usage index.
    /// </summary>
    /// <param name="tenant">The tenant to project.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The tenant's snapshot, or <c>null</c> when it has no usage view.</returns>
    public async Task<TenantObservabilitySnapshot?> SnapshotOneAsync(
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

        var overage = await _overageBilling
            .GetMeteredOverageAsync(tenant, cancellationToken)
            .ConfigureAwait(false);

        return new TenantObservabilitySnapshot(tenant, view.GlobalUsage, view.Quotas, overage);
    }

    /// <summary>
    /// Projects every registered tenant's observability snapshot: the warm usage
    /// index supplies the tenant set (with quotas and the global usage fold) and a
    /// single <see cref="ITenantOverageBilling.ListMeteredOverageAsync"/> pass
    /// supplies the durable metered overage, joined by tenant (a tenant with no
    /// metered overage folds in <see cref="TenantOverageSample.Empty"/>).
    /// </summary>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>A snapshot per registered tenant; empty when no tenants are registered.</returns>
    public async Task<IReadOnlyList<TenantObservabilitySnapshot>> SnapshotAllAsync(
        CancellationToken cancellationToken = default)
    {
        await _usageIndex.EnsureWarmAsync(cancellationToken).ConfigureAwait(false);
        var views = _usageIndex.EnumerateViews();
        if (views.Count == 0)
        {
            return Array.Empty<TenantObservabilitySnapshot>();
        }

        var overageByTenant = new Dictionary<string, TenantOverageSample>(StringComparer.Ordinal);
        await foreach (var metered in _overageBilling
            .ListMeteredOverageAsync(cancellationToken)
            .ConfigureAwait(false))
        {
            if (metered.Tenant.Value is { } id)
            {
                overageByTenant[id] = metered.Overage;
            }
        }

        var snapshots = new List<TenantObservabilitySnapshot>(views.Count);
        foreach (var (id, view) in views)
        {
            if (!TenantId.TryParse(id, out var tenant))
            {
                continue;
            }

            var overage = overageByTenant.TryGetValue(id, out var metered)
                ? metered
                : TenantOverageSample.Empty;

            snapshots.Add(new TenantObservabilitySnapshot(tenant, view.GlobalUsage, view.Quotas, overage));
        }

        return snapshots;
    }
}
