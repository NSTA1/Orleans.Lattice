namespace Orleans.Lattice.Explorer.Tenancy;

/// <summary>
/// The read-only status of one tenant: its lifecycle state, its per-region
/// residency, and the quota ceilings authored for it.
/// </summary>
public sealed record ExplorerTenantDetail
{
    /// <summary>The tenant id.</summary>
    public required string TenantId { get; init; }

    /// <summary>The tenant's lifecycle state.</summary>
    public ExplorerTenantLifecycle Status { get; init; }

    /// <summary><see langword="true"/> when this is the reserved default tenant.</summary>
    public bool IsDefault { get; init; }

    /// <summary>
    /// The tenant's per-region residency, ordered by region id. Never
    /// <see langword="null"/>; empty when the cluster is single-region.
    /// </summary>
    public required IReadOnlyList<ExplorerTenantRegion> Regions { get; init; }

    /// <summary>
    /// The quota ceilings authored for the tenant. This is the authoritative
    /// descriptor and can be fresher than the ceilings carried on a
    /// <see cref="ExplorerTenantQuotaUsage"/> reading, which are deliberately
    /// taken from the same snapshot as its usage figures.
    /// </summary>
    public ExplorerTenantQuotaLimits Quotas { get; init; }
}
