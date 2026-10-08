namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>Observable receiver-side progress for one tenant region's backfill.</summary>
[GenerateSerializer]
[Alias(ApiTenantAdminTypeAliases.TenantRegionBackfillProgress)]
[Immutable]
public sealed record TenantRegionBackfillProgress
{
    /// <summary>The current overall backfill phase.</summary>
    [Id(0)] public required string Phase { get; init; }

    /// <summary>A safe, actionable reason when progress is blocked or unavailable.</summary>
    [Id(1)] public string? StallReason { get; init; }

    /// <summary>Per-tree bootstrap progress, ordered by tree id.</summary>
    [Id(2)] public required IReadOnlyList<TenantRegionBackfillTreeProgress> Trees { get; init; }
}
