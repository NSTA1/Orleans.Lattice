using System.Collections.Frozen;

namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The compiled, immutable per-tenant slice of a <see cref="CompiledTenantPolicy"/>
/// snapshot: a tenant's identity and status, its frozen admin-subject set, its
/// frozen member-subject set when delegated tenant access administration is
/// enabled, and its cross-tenant grants indexed by grantee tenant id. Every lookup
/// here is allocation-free, so it sits on the engine's warm decision path.
/// </summary>
/// <remarks>
/// A tenant compiled without a member set (<paramref name="members"/> is
/// <c>null</c>) is <b>not group-aware</b>: <see cref="IsAdmin(string, IReadOnlyCollection{string})"/>
/// and <see cref="IsMember"/> then reduce to the exact-subject-id admin check and
/// never look at groups. That is the shape every tenant takes while the feature is
/// off, and the reserved <see cref="TenantId.Default"/> tenant always takes.
/// </remarks>
/// <param name="id">The tenant's identity.</param>
/// <param name="status">The tenant's resolved lifecycle status.</param>
/// <param name="admins">The tenant's admin-subject set.</param>
/// <param name="tenantGrants">The tenant's tenant-grantee grants, indexed by grantee tenant id.</param>
/// <param name="members">The tenant's member-subject set, or <c>null</c> when the tenant is not group-aware.</param>
internal sealed class CompiledTenant(
    TenantId id,
    TenantStatus status,
    FrozenSet<string> admins,
    FrozenDictionary<string, CrossTenantGrant[]> tenantGrants,
    FrozenSet<string>? members = null)
{
    /// <summary>The tenant's identity.</summary>
    public TenantId Id => id;

    /// <summary>The tenant's resolved lifecycle status.</summary>
    public TenantStatus Status => status;

    /// <summary>The tenant's admin-subject set. Exposed for tests.</summary>
    internal FrozenSet<string> Admins => admins;

    /// <summary>
    /// The tenant's member-subject set: empty when the tenant is not group-aware.
    /// Exposed for tests and the subject index.
    /// </summary>
    internal FrozenSet<string> Members => members ?? FrozenSet<string>.Empty;

    /// <summary>
    /// <c>true</c> when the tenant was compiled with delegated tenant access
    /// administration enabled, so its member set and group entries count.
    /// </summary>
    public bool IsGroupAware => members is not null;

    /// <summary>
    /// Returns <c>true</c> when <paramref name="subjectId"/> is one of the tenant's
    /// admin subjects. A zero-allocation exact-id membership probe.
    /// </summary>
    /// <param name="subjectId">The subject id to test. Must not be <c>null</c>.</param>
    /// <returns><c>true</c> when the subject administers the tenant.</returns>
    public bool IsAdmin(string subjectId) => admins.Contains(subjectId);

    /// <summary>
    /// Returns <c>true</c> when the subject administers the tenant: its id is an
    /// admin entry, or - when the tenant is group-aware - any of its resolved groups
    /// is. A zero-allocation probe for the group shapes a resolved subject carries.
    /// </summary>
    /// <param name="subjectId">The subject id to test. Must not be <c>null</c>.</param>
    /// <param name="groupIds">The subject's resolved transitive group ids. Must not be <c>null</c>.</param>
    /// <returns><c>true</c> when the subject, or one of its groups, is an admin entry.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="subjectId"/> or <paramref name="groupIds"/> is <c>null</c>.</exception>
    public bool IsAdmin(string subjectId, IReadOnlyCollection<string> groupIds)
    {
        ArgumentNullException.ThrowIfNull(subjectId);
        ArgumentNullException.ThrowIfNull(groupIds);

        if (admins.Contains(subjectId))
        {
            return true;
        }

        return members is not null && ContainsAny(admins, null, groupIds);
    }

    /// <summary>
    /// Returns <c>true</c> when the subject may act as the tenant. When the tenant
    /// is group-aware: its id or any of its resolved groups is an admin entry
    /// (admins are implicitly members) or a member entry. When it is not: exactly
    /// <see cref="IsAdmin(string)"/>, so member entries and groups are ignored. A
    /// zero-allocation probe for the group shapes a resolved subject carries.
    /// </summary>
    /// <param name="subjectId">The subject id to test. Must not be <c>null</c>.</param>
    /// <param name="groupIds">The subject's resolved transitive group ids. Must not be <c>null</c>.</param>
    /// <returns><c>true</c> when the subject may act as the tenant.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="subjectId"/> or <paramref name="groupIds"/> is <c>null</c>.</exception>
    public bool IsMember(string subjectId, IReadOnlyCollection<string> groupIds)
    {
        ArgumentNullException.ThrowIfNull(subjectId);
        ArgumentNullException.ThrowIfNull(groupIds);

        if (admins.Contains(subjectId))
        {
            return true;
        }

        if (members is null)
        {
            return false;
        }

        return members.Contains(subjectId) || ContainsAny(admins, members, groupIds);
    }

    /// <summary>
    /// Attempts to get the cross-tenant grants this tenant issued to the grantee
    /// tenant <paramref name="granteeTenantId"/>. A zero-allocation lookup.
    /// </summary>
    /// <param name="granteeTenantId">The grantee tenant's id text.</param>
    /// <param name="grants">The grants when present; otherwise <c>null</c>.</param>
    /// <returns><c>true</c> when at least one tenant-grantee grant exists for the grantee.</returns>
    public bool TryGetTenantGrants(string granteeTenantId, out CrossTenantGrant[]? grants) =>
        tenantGrants.TryGetValue(granteeTenantId, out grants);

    /// <summary>
    /// <c>true</c> when any id in <paramref name="groupIds"/> is in
    /// <paramref name="first"/> or (when supplied) <paramref name="second"/>.
    /// Allocation-free for a <see cref="HashSet{T}"/> (the shape membership
    /// resolution produces), an array or a list; any other collection is enumerated
    /// through its interface.
    /// </summary>
    private static bool ContainsAny(FrozenSet<string> first, FrozenSet<string>? second, IReadOnlyCollection<string> groupIds)
    {
        if (groupIds.Count == 0 || (first.Count == 0 && (second is null || second.Count == 0)))
        {
            return false;
        }

        switch (groupIds)
        {
            case HashSet<string> set:
                foreach (var group in set)
                {
                    if (Contains(first, second, group))
                    {
                        return true;
                    }
                }

                return false;
            case IReadOnlyList<string> list:
                for (var i = 0; i < list.Count; i++)
                {
                    if (Contains(first, second, list[i]))
                    {
                        return true;
                    }
                }

                return false;
            default:
                foreach (var group in groupIds)
                {
                    if (Contains(first, second, group))
                    {
                        return true;
                    }
                }

                return false;
        }
    }

    private static bool Contains(FrozenSet<string> first, FrozenSet<string>? second, string? group) =>
        group is not null && (first.Contains(group) || (second is not null && second.Contains(group)));
}
