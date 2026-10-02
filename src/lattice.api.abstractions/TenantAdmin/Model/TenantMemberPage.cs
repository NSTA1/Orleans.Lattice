namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// One page of a tenant's member set, in ascending ordinal order of entry id.
/// <see cref="NextPageToken"/> is the cursor to pass back in the next
/// <see cref="TenantAccessPageRequest"/>; it is <see langword="null"/> on the
/// final page.
/// </summary>
/// <remarks>
/// The member set does not repeat the admin set: admins are implicitly members,
/// so only the entries added to the member set itself are listed here.
/// </remarks>
[GenerateSerializer]
[Alias(ApiTenantAdminTypeAliases.TenantMemberPage)]
[Immutable]
public sealed record TenantMemberPage
{
    /// <summary>The member-set entries on this page.</summary>
    [Id(0)] public IReadOnlyList<TenantMemberEntry> Entries { get; init; } = Array.Empty<TenantMemberEntry>();

    /// <summary>The continuation cursor for the next page, or <see langword="null"/> when this is the last page.</summary>
    [Id(1)] public string? NextPageToken { get; init; }
}
