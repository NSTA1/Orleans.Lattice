namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// One direct member of a tenant group: a user, another group of the same
/// tenant, or a cluster group. <see cref="Kind"/> says which, and so how
/// <see cref="MemberId"/> reads.
/// </summary>
/// <remarks>
/// A <see cref="TenantSubjectKind.TenantGroup"/> member carries the nested
/// group's local name; a <see cref="TenantSubjectKind.ClusterGroup"/> member
/// carries the cluster group id; a <see cref="TenantSubjectKind.User"/> member
/// carries the user's subject id. A tenant group can never be a member of a
/// cluster group or of another tenant's group, so the reverse edge never appears
/// here.
/// </remarks>
[GenerateSerializer]
[Alias(ApiTenantAdminTypeAliases.TenantGroupMember)]
[Immutable]
public sealed record TenantGroupMember
{
    /// <summary>The member's id: a user id, a local tenant group name, or a cluster group id, per <see cref="Kind"/>.</summary>
    [Id(0)] public required string MemberId { get; init; }

    /// <summary>The kind of principal <see cref="MemberId"/> names.</summary>
    [Id(1)] public TenantSubjectKind Kind { get; init; }
}
