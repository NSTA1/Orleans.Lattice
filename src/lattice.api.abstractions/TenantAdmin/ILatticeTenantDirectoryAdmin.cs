namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// Transport-agnostic <b>tenant directory</b> facade for delegated tenant access
/// administration: a tenant's own groups, their direct members, and the tenant's
/// member set - the subjects who may act as the tenant on the data plane. It lets a
/// tenant's administrators decide who belongs to their tenant without a platform
/// operator in the loop and without ever reaching outside the tenant. Every
/// transport binding (gRPC, MCP) is a thin adapter over this one surface.
/// </summary>
/// <remarks>
/// <para>
/// <b>Opt-in.</b> The surface is inert unless the cluster enables delegated tenant
/// access administration (<c>LatticeTenancyOptions.DelegatedAccessAdministrationEnabled</c>).
/// While it is off every operation is refused with
/// <see cref="TenantAccessAdministrationDisabledException"/> before anything is read
/// or written; <see cref="ILatticeTenantPolicyAdmin.GetPostureAsync"/> reports the flag.
/// </para>
/// <para>
/// <b>Tenant-tier, fail-closed authorization.</b> Every operation names its tenant
/// explicitly and is authorized against the caller: a platform operator, or an admin
/// of that tenant directly or through a group. Any other caller is refused
/// <see cref="Orleans.Lattice.LatticeAuthorizationDeniedException"/>, whether or not
/// the tenant exists, so the surface cannot be used to probe for tenants. The
/// reserved default tenant (<see cref="Orleans.Lattice.TenantId.DefaultId"/>) has no
/// tenant groups or members and is refused with
/// <see cref="ReservedTenantOperationException"/>.
/// </para>
/// <para>
/// <b>Tenant-local names.</b> Groups are named by their tenant-local name; the
/// facade composes the reserved <c>t/{tenant}/{name}</c> id under the tenant the
/// call names, so a caller can never name, list, or discover another tenant's group
/// (it reads as not found). <see cref="TenantSubjectKind"/> distinguishes a local
/// group from a cluster group spelled the same way.
/// </para>
/// <para>
/// <b>Confinement.</b> A tenant group may contain users, groups of the same tenant,
/// and cluster groups. It may never become a member of a cluster group or of another
/// tenant's group, and no entry may name another tenant's group; both are refused
/// with <see cref="TenantAccessConfinementException"/> for every caller.
/// </para>
/// <para>
/// <b>Caps.</b> The tenant's <c>MaxGroups</c>, <c>MaxMembershipEdges</c> and
/// <c>MaxMemberSubjects</c> quota dimensions bound its footprint in the shared
/// membership trees; a write that would exceed one is refused with
/// <see cref="Orleans.Lattice.LatticeQuotaExceededException"/>.
/// </para>
/// <para>
/// Every mutation is idempotent, and every listing is in ascending ordinal order.
/// </para>
/// </remarks>
public interface ILatticeTenantDirectoryAdmin
{
    // ----- Groups -----

    /// <summary>Reads one page of the tenant's groups, in ascending order of local name.</summary>
    /// <param name="tenantId">The tenant whose groups to list. Must be a valid, non-empty tenant id.</param>
    /// <param name="page">Paging request (page size and continuation cursor). Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the scan.</param>
    /// <returns>One page of the tenant's groups.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="page"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentException"><paramref name="tenantId"/> is <c>null</c>, empty, or not a valid tenant id.</exception>
    /// <exception cref="TenantAccessAdministrationDisabledException">Delegated tenant access administration is disabled.</exception>
    /// <exception cref="ReservedTenantOperationException"><paramref name="tenantId"/> is the reserved default tenant.</exception>
    /// <exception cref="Orleans.Lattice.LatticeAuthorizationDeniedException">The caller is neither a platform operator nor an admin of the tenant.</exception>
    Task<TenantGroupPage> ListGroupsAsync(
        string tenantId, TenantAccessPageRequest page, CancellationToken cancellationToken = default);

    /// <summary>Reads one of the tenant's groups, or <c>null</c> when the tenant has no group of that name.</summary>
    /// <param name="tenantId">The tenant that owns the group. Must be a valid, non-empty tenant id.</param>
    /// <param name="groupName">The group's tenant-local name. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The group, or <c>null</c> when it does not exist.</returns>
    /// <exception cref="ArgumentException"><paramref name="tenantId"/> is not a valid tenant id, or <paramref name="groupName"/> is <c>null</c>, empty, or not a valid local group name.</exception>
    /// <exception cref="TenantAccessAdministrationDisabledException">Delegated tenant access administration is disabled.</exception>
    /// <exception cref="ReservedTenantOperationException"><paramref name="tenantId"/> is the reserved default tenant.</exception>
    /// <exception cref="Orleans.Lattice.LatticeAuthorizationDeniedException">The caller is neither a platform operator nor an admin of the tenant.</exception>
    Task<TenantGroupDescriptor?> GetGroupAsync(
        string tenantId, string groupName, CancellationToken cancellationToken = default);

    /// <summary>Creates or replaces one of the tenant's groups.</summary>
    /// <param name="tenantId">The tenant that owns the group. Must be a valid, non-empty tenant id.</param>
    /// <param name="group">The group to upsert. Must not be <c>null</c>, and its name must be a valid local group name.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    /// <returns>The group as stored.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="group"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentException"><paramref name="tenantId"/> is not a valid tenant id, or the group's name is not a valid local group name.</exception>
    /// <exception cref="TenantAccessAdministrationDisabledException">Delegated tenant access administration is disabled.</exception>
    /// <exception cref="ReservedTenantOperationException"><paramref name="tenantId"/> is the reserved default tenant.</exception>
    /// <exception cref="Orleans.Lattice.LatticeQuotaExceededException">Creating the group would exceed the tenant's <c>MaxGroups</c> cap.</exception>
    /// <exception cref="Orleans.Lattice.LatticeAuthorizationDeniedException">The caller is neither a platform operator nor an admin of the tenant.</exception>
    Task<TenantGroupDescriptor> UpsertGroupAsync(
        string tenantId, TenantGroupDescriptor group, CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes one of the tenant's groups, together with its membership edges in both
    /// directions and its entries in the tenant's member set, admin set, and
    /// tenant-tier rules. Idempotent: removing a group that does not exist cascades
    /// nothing and reports <see cref="TenantGroupRemovalResult.Removed"/>
    /// <see langword="false"/>.
    /// </summary>
    /// <param name="tenantId">The tenant that owns the group. Must be a valid, non-empty tenant id.</param>
    /// <param name="groupName">The group's tenant-local name. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    /// <returns>What the removal cascaded to.</returns>
    /// <exception cref="ArgumentException"><paramref name="tenantId"/> is not a valid tenant id, or <paramref name="groupName"/> is not a valid local group name.</exception>
    /// <exception cref="TenantAccessAdministrationDisabledException">Delegated tenant access administration is disabled.</exception>
    /// <exception cref="ReservedTenantOperationException"><paramref name="tenantId"/> is the reserved default tenant.</exception>
    /// <exception cref="TenantLastAdminSubjectException">The group is the tenant's last admin-set entry.</exception>
    /// <exception cref="Orleans.Lattice.LatticeAuthorizationDeniedException">The caller is neither a platform operator nor an admin of the tenant.</exception>
    Task<TenantGroupRemovalResult> RemoveGroupAsync(
        string tenantId, string groupName, CancellationToken cancellationToken = default);

    // ----- Group members -----

    /// <summary>
    /// Returns the <b>direct</b> members of one of the tenant's groups, in ascending
    /// ordinal order of member id. A group that does not exist has no members.
    /// </summary>
    /// <param name="tenantId">The tenant that owns the group. Must be a valid, non-empty tenant id.</param>
    /// <param name="groupName">The group's tenant-local name. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancels the scan.</param>
    /// <returns>The group's direct members.</returns>
    /// <exception cref="ArgumentException"><paramref name="tenantId"/> is not a valid tenant id, or <paramref name="groupName"/> is not a valid local group name.</exception>
    /// <exception cref="TenantAccessAdministrationDisabledException">Delegated tenant access administration is disabled.</exception>
    /// <exception cref="ReservedTenantOperationException"><paramref name="tenantId"/> is the reserved default tenant.</exception>
    /// <exception cref="Orleans.Lattice.LatticeAuthorizationDeniedException">The caller is neither a platform operator nor an admin of the tenant.</exception>
    Task<IReadOnlyList<TenantGroupMember>> ListGroupMembersAsync(
        string tenantId, string groupName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Makes <paramref name="memberId"/> a direct member of one of the tenant's
    /// groups. The member is a user, another group of the same tenant, or a cluster
    /// group. Idempotent.
    /// </summary>
    /// <param name="tenantId">The tenant that owns the group. Must be a valid, non-empty tenant id.</param>
    /// <param name="groupName">The tenant-local name of the group to add to. Must name an existing group.</param>
    /// <param name="memberId">The member's id, read per <paramref name="memberKind"/>. Must not be <c>null</c> or empty.</param>
    /// <param name="memberKind">The kind of principal <paramref name="memberId"/> names.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    /// <returns>The change result.</returns>
    /// <exception cref="ArgumentException">An argument is <c>null</c>, empty, or malformed, or the group does not exist.</exception>
    /// <exception cref="TenantAccessConfinementException">The edge would nest a tenant group outside the tenant, or names another tenant's group.</exception>
    /// <exception cref="TenantAccessAdministrationDisabledException">Delegated tenant access administration is disabled.</exception>
    /// <exception cref="ReservedTenantOperationException"><paramref name="tenantId"/> is the reserved default tenant.</exception>
    /// <exception cref="Orleans.Lattice.LatticeQuotaExceededException">The edge would exceed the tenant's <c>MaxMembershipEdges</c> cap.</exception>
    /// <exception cref="Orleans.Lattice.LatticeAuthorizationDeniedException">The caller is neither a platform operator nor an admin of the tenant.</exception>
    Task<TenantMembershipChangeResult> AddGroupMemberAsync(
        string tenantId,
        string groupName,
        string memberId,
        TenantSubjectKind memberKind = TenantSubjectKind.User,
        CancellationToken cancellationToken = default);

    /// <summary>Removes a direct member from one of the tenant's groups. Idempotent.</summary>
    /// <param name="tenantId">The tenant that owns the group. Must be a valid, non-empty tenant id.</param>
    /// <param name="groupName">The group's tenant-local name. Must not be <c>null</c> or empty.</param>
    /// <param name="memberId">The member's id, read per <paramref name="memberKind"/>. Must not be <c>null</c> or empty.</param>
    /// <param name="memberKind">The kind of principal <paramref name="memberId"/> names.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    /// <returns>The change result.</returns>
    /// <exception cref="ArgumentException">An argument is <c>null</c>, empty, or malformed.</exception>
    /// <exception cref="TenantAccessAdministrationDisabledException">Delegated tenant access administration is disabled.</exception>
    /// <exception cref="ReservedTenantOperationException"><paramref name="tenantId"/> is the reserved default tenant.</exception>
    /// <exception cref="Orleans.Lattice.LatticeAuthorizationDeniedException">The caller is neither a platform operator nor an admin of the tenant.</exception>
    Task<TenantMembershipChangeResult> RemoveGroupMemberAsync(
        string tenantId,
        string groupName,
        string memberId,
        TenantSubjectKind memberKind = TenantSubjectKind.User,
        CancellationToken cancellationToken = default);

    // ----- Tenant member set -----

    /// <summary>
    /// Reads one page of the tenant's member set - the entries whose subjects may
    /// act as the tenant - in ascending ordinal order of entry id. Admins are
    /// implicitly members and are not repeated here.
    /// </summary>
    /// <param name="tenantId">The tenant whose member set to list. Must be a valid, non-empty tenant id.</param>
    /// <param name="page">Paging request (page size and continuation cursor). Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the scan.</param>
    /// <returns>One page of the member set.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="page"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentException"><paramref name="tenantId"/> is <c>null</c>, empty, or not a valid tenant id.</exception>
    /// <exception cref="TenantAccessAdministrationDisabledException">Delegated tenant access administration is disabled.</exception>
    /// <exception cref="ReservedTenantOperationException"><paramref name="tenantId"/> is the reserved default tenant.</exception>
    /// <exception cref="Orleans.Lattice.LatticeAuthorizationDeniedException">The caller is neither a platform operator nor an admin of the tenant.</exception>
    Task<TenantMemberPage> ListMembersAsync(
        string tenantId, TenantAccessPageRequest page, CancellationToken cancellationToken = default);

    /// <summary>
    /// Adds a user, one of the tenant's groups, or a cluster group to the tenant's
    /// member set. Membership only lets the subject act as the tenant; what it may
    /// then do is decided by rules, default-deny. Idempotent.
    /// </summary>
    /// <param name="tenantId">The tenant whose member set to change. Must be a valid, non-empty tenant id.</param>
    /// <param name="subjectId">The entry's id, read per <paramref name="subjectKind"/>. Must not be <c>null</c> or empty.</param>
    /// <param name="subjectKind">The kind of principal <paramref name="subjectId"/> names.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    /// <returns>The change result.</returns>
    /// <exception cref="ArgumentException">An argument is <c>null</c>, empty, or malformed.</exception>
    /// <exception cref="TenantAccessConfinementException">The entry names another tenant's group.</exception>
    /// <exception cref="TenantAccessAdministrationDisabledException">Delegated tenant access administration is disabled.</exception>
    /// <exception cref="ReservedTenantOperationException"><paramref name="tenantId"/> is the reserved default tenant.</exception>
    /// <exception cref="Orleans.Lattice.LatticeQuotaExceededException">The entry would exceed the tenant's <c>MaxMemberSubjects</c> cap.</exception>
    /// <exception cref="Orleans.Lattice.LatticeAuthorizationDeniedException">The caller is neither a platform operator nor an admin of the tenant.</exception>
    Task<TenantMembershipChangeResult> AddMemberAsync(
        string tenantId,
        string subjectId,
        TenantSubjectKind subjectKind = TenantSubjectKind.User,
        CancellationToken cancellationToken = default);

    /// <summary>Removes an entry from the tenant's member set. Idempotent.</summary>
    /// <param name="tenantId">The tenant whose member set to change. Must be a valid, non-empty tenant id.</param>
    /// <param name="subjectId">The entry's id, read per <paramref name="subjectKind"/>. Must not be <c>null</c> or empty.</param>
    /// <param name="subjectKind">The kind of principal <paramref name="subjectId"/> names.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    /// <returns>The change result.</returns>
    /// <exception cref="ArgumentException">An argument is <c>null</c>, empty, or malformed.</exception>
    /// <exception cref="TenantAccessAdministrationDisabledException">Delegated tenant access administration is disabled.</exception>
    /// <exception cref="ReservedTenantOperationException"><paramref name="tenantId"/> is the reserved default tenant.</exception>
    /// <exception cref="Orleans.Lattice.LatticeAuthorizationDeniedException">The caller is neither a platform operator nor an admin of the tenant.</exception>
    Task<TenantMembershipChangeResult> RemoveMemberAsync(
        string tenantId,
        string subjectId,
        TenantSubjectKind subjectKind = TenantSubjectKind.User,
        CancellationToken cancellationToken = default);

    // ----- Resolution -----

    /// <summary>
    /// Resolves whether a subject is an admin or a member of the tenant, and through
    /// which admin-set and member-set entries, expanding the subject's transitive
    /// groups from the membership directory.
    /// </summary>
    /// <remarks>
    /// The subject may be any principal, not only one already associated with the
    /// tenant, so a tenant admin who has added a cluster group to the tenant's member
    /// or admin set can learn whether any given user is a (transitive) member of that
    /// cluster group. This is by design and not a cross-tenant disclosure: adding a
    /// cluster group grants its members the ability to act as the tenant, so who
    /// those members are is the tenant admin's legitimate knowledge of who can act as
    /// their tenant. A tenant admin learns nothing about a cluster group the tenant
    /// has not admitted, because only entries of the tenant's own sets are reported.
    /// </remarks>
    /// <param name="tenantId">The tenant to resolve against. Must be a valid, non-empty tenant id.</param>
    /// <param name="subjectId">The subject id to resolve, read per <paramref name="subjectKind"/>. Must not be <c>null</c> or empty.</param>
    /// <param name="subjectKind">The kind of principal <paramref name="subjectId"/> names.</param>
    /// <param name="cancellationToken">Cancels the resolution.</param>
    /// <returns>The subject's standing in the tenant.</returns>
    /// <exception cref="ArgumentException">An argument is <c>null</c>, empty, or malformed.</exception>
    /// <exception cref="TenantAccessAdministrationDisabledException">Delegated tenant access administration is disabled.</exception>
    /// <exception cref="ReservedTenantOperationException"><paramref name="tenantId"/> is the reserved default tenant.</exception>
    /// <exception cref="Orleans.Lattice.LatticeAuthorizationDeniedException">The caller is neither a platform operator nor an admin of the tenant.</exception>
    Task<TenantSubjectResolution> ResolveSubjectAsync(
        string tenantId,
        string subjectId,
        TenantSubjectKind subjectKind = TenantSubjectKind.User,
        CancellationToken cancellationToken = default);
}
