namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// A tenant group as surfaced by <see cref="ILatticeTenantDirectoryAdmin"/>: its
/// tenant-<b>local</b> <see cref="Name"/> and an optional human-readable
/// <see cref="DisplayName"/>. The group's membership edges are administered
/// separately through the group-member operations.
/// </summary>
/// <remarks>
/// <see cref="Name"/> is the <c>{name}</c> part of the reserved
/// <c>t/{tenant}/{name}</c> group id: 1 to 63 characters of lower-case ASCII
/// letters, digits, <c>-</c>, <c>_</c> and <c>.</c>. The facade composes the full
/// id under the tenant the call names, so a descriptor can never name another
/// tenant's group.
/// </remarks>
[GenerateSerializer]
[Alias(ApiTenantAdminTypeAliases.TenantGroupDescriptor)]
[Immutable]
public sealed record TenantGroupDescriptor
{
    /// <summary>The group's tenant-local name.</summary>
    [Id(0)] public required string Name { get; init; }

    /// <summary>An optional human-readable display name.</summary>
    [Id(1)] public string? DisplayName { get; init; }
}
