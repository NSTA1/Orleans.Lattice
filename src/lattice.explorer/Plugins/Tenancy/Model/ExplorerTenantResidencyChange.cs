namespace Orleans.Lattice.Explorer.Plugins.Tenancy;

/// <summary>
/// The outcome of a residency change: which regions began adding, which began
/// draining, and the tenant's resulting per-region status.
/// <para>
/// The added and removed sets are what a panel should report back to the
/// operator, because residency changes are not instant: a newly-listed region
/// starts provisioning and back-filling, and a removed one starts draining, so
/// <see cref="Regions"/> will not yet show either as settled.
/// </para>
/// </summary>
public sealed record ExplorerTenantResidencyChange
{
    /// <summary>The tenant whose residency was authored.</summary>
    public required string TenantId { get; init; }

    /// <summary>
    /// The regions the call added to residency, which begin provisioning. Never
    /// <see langword="null"/>.
    /// </summary>
    public required IReadOnlyList<string> AddedRegions { get; init; }

    /// <summary>
    /// The regions the call removed from residency, which begin draining. Never
    /// <see langword="null"/>.
    /// </summary>
    public required IReadOnlyList<string> RemovedRegions { get; init; }

    /// <summary>
    /// The tenant's resulting per-region status, ordered by region id. Never
    /// <see langword="null"/>.
    /// </summary>
    public required IReadOnlyList<ExplorerTenantRegion> Regions { get; init; }
}
