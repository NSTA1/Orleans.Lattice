namespace Orleans.Lattice.Api.TenantAdmin.Grpc;

/// <summary>
/// Wire request for the paged delegated tenant-access listings
/// (<c>ListTenantGroups</c>, <c>ListTenantMembers</c>, <c>ListTenantRules</c>),
/// carrying the tenant the listing is scoped to and the page to read.
/// </summary>
[GenerateSerializer]
[Alias(GrpcTenantAdminTypeAliases.TenantAdminAccessListRequest)]
[Immutable]
public sealed record TenantAdminAccessListRequest
{
    /// <summary>The tenant id the listing is scoped to.</summary>
    [Id(0)] public required string TenantId { get; init; }

    /// <summary>The paging request (page size and continuation cursor).</summary>
    [Id(1)] public required TenantAccessPageRequest Page { get; init; }
}
