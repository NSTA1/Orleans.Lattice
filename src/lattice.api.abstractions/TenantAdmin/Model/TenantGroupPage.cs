namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// One page of a tenant's groups, in ascending ordinal order of their local
/// names. <see cref="NextPageToken"/> is the cursor to pass back in the next
/// <see cref="TenantAccessPageRequest"/>; it is <see langword="null"/> on the
/// final page. Only the named tenant's own groups are ever listed.
/// </summary>
[GenerateSerializer]
[Alias(ApiTenantAdminTypeAliases.TenantGroupPage)]
[Immutable]
public sealed record TenantGroupPage
{
    /// <summary>The groups on this page, ordered by local name.</summary>
    [Id(0)] public IReadOnlyList<TenantGroupDescriptor> Entries { get; init; } = Array.Empty<TenantGroupDescriptor>();

    /// <summary>The continuation cursor for the next page, or <see langword="null"/> when this is the last page.</summary>
    [Id(1)] public string? NextPageToken { get; init; }
}
