namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// One page of the rules governing a tenant, ordered by tenant-local tree name
/// (tenant-wide rules first) and then rule id. It holds the tenant's tenant-tier
/// rules (<see cref="TenantRuleLayer.Tenant"/>, editable) and the operator rules
/// scoped to the tenant's own trees (<see cref="TenantRuleLayer.Platform"/>,
/// read-only). Cluster-wide <c>Tree:*</c> rules and app role rules are never
/// listed. <see cref="NextPageToken"/> is the cursor to pass back in the next
/// <see cref="TenantAccessPageRequest"/>; it is <see langword="null"/> on the
/// final page.
/// </summary>
[GenerateSerializer]
[Alias(ApiTenantAdminTypeAliases.TenantRulePage)]
[Immutable]
public sealed record TenantRulePage
{
    /// <summary>The rules on this page.</summary>
    [Id(0)] public IReadOnlyList<TenantRuleView> Entries { get; init; } = Array.Empty<TenantRuleView>();

    /// <summary>The continuation cursor for the next page, or <see langword="null"/> when this is the last page.</summary>
    [Id(1)] public string? NextPageToken { get; init; }
}
