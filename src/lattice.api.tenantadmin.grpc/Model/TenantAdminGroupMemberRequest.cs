namespace Orleans.Lattice.Api.TenantAdmin.Grpc;

/// <summary>
/// Wire request for the <c>AddTenantGroupMember</c> and
/// <c>RemoveTenantGroupMember</c> RPCs: one direct-membership edge of one of a
/// tenant's groups.
/// </summary>
[GenerateSerializer]
[Alias(GrpcTenantAdminTypeAliases.TenantAdminGroupMemberRequest)]
[Immutable]
public sealed record TenantAdminGroupMemberRequest
{
    /// <summary>The tenant id that owns the group.</summary>
    [Id(0)] public required string TenantId { get; init; }

    /// <summary>The group's tenant-local name.</summary>
    [Id(1)] public required string GroupName { get; init; }

    /// <summary>The member's id, read per <see cref="MemberKind"/>.</summary>
    [Id(2)] public required string MemberId { get; init; }

    /// <summary>The kind of principal <see cref="MemberId"/> names.</summary>
    [Id(3)] public TenantSubjectKind MemberKind { get; init; }
}
