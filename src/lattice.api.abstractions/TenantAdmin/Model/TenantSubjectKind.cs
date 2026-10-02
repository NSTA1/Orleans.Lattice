namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// The kind of principal a delegated tenant-access entry names: a group member,
/// a tenant member-set or admin-set entry, or the subject of a tenant-tier rule.
/// It distinguishes a tenant's own group from a cluster group explicitly, so a
/// tenant-local group name and a cluster group id that happen to be spelled the
/// same can never be confused.
/// </summary>
/// <remarks>
/// <para>
/// A <see cref="TenantGroup"/> entry carries the group's <b>local</b> name (the
/// <c>{name}</c> part of the reserved <c>t/{tenant}/{name}</c> group id); the
/// facade composes the full id under the tenant the call names, so a caller can
/// never name another tenant's group through this kind. A
/// <see cref="ClusterGroup"/> entry carries the cluster group id verbatim (for
/// example an identity-provider group), and may never be in the reserved
/// <c>t/</c> grammar.
/// </para>
/// <para>
/// The zero value is <see cref="User"/>, matching the default of the cluster
/// facade's member and subject kinds.
/// </para>
/// </remarks>
[GenerateSerializer]
[Alias(ApiTenantAdminTypeAliases.TenantSubjectKind)]
public enum TenantSubjectKind
{
    /// <summary>An individual user, named by its subject id.</summary>
    User = 0,

    /// <summary>A group owned by the tenant the call names, named by its local group name.</summary>
    TenantGroup = 1,

    /// <summary>A cluster-wide group (for example an identity-provider group), named by its cluster group id.</summary>
    ClusterGroup = 2,
}
