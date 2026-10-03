namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// One entry of a tenant's member set or admin set: a user, one of the tenant's
/// own groups, or a cluster group. A subject may act as the tenant when its id,
/// or any of its resolved transitive groups, matches an entry of the admin set
/// or the member set; admins are implicitly members.
/// </summary>
/// <remarks>
/// A <see cref="TenantSubjectKind.TenantGroup"/> entry carries the group's local
/// name. An entry can never name another tenant's group.
/// </remarks>
[GenerateSerializer]
[Alias(ApiTenantAdminTypeAliases.TenantMemberEntry)]
[Immutable]
public sealed record TenantMemberEntry
{
    /// <summary>The entry's id: a user id, a local tenant group name, or a cluster group id, per <see cref="Kind"/>.</summary>
    [Id(0)] public required string SubjectId { get; init; }

    /// <summary>The kind of principal <see cref="SubjectId"/> names.</summary>
    [Id(1)] public TenantSubjectKind Kind { get; init; }
}
