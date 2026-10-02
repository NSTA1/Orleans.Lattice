using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// The membership underlay the tenant directory facade drives once it has
/// authorized a caller: the shared membership directory plus membership's internal
/// tenant-scoped store (counts, the tenant group page, and the group removal
/// cascade). It performs <b>no</b> authorization of its own; every call runs under
/// system origin, so the facade's tenant-admin check is the single enforcement
/// point. Kept as a seam so the facade is unit-testable without a cluster.
/// </summary>
internal interface ITenantDirectoryStore
{
    /// <summary>Reads a group record by its full id, or <c>null</c> when none is stored.</summary>
    /// <param name="groupId">The full group id.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The group, or <c>null</c>.</returns>
    Task<MembershipGroup?> GetGroupAsync(string groupId, CancellationToken cancellationToken);

    /// <summary>Creates or replaces a group record.</summary>
    /// <param name="group">The group to write.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    /// <returns>A task that completes when the group is stored.</returns>
    Task UpsertGroupAsync(MembershipGroup group, CancellationToken cancellationToken);

    /// <summary>Counts the tenant's groups (the <c>MaxGroups</c> usage).</summary>
    /// <param name="tenant">The tenant.</param>
    /// <param name="cancellationToken">Cancels the count.</param>
    /// <returns>The number of the tenant's groups.</returns>
    Task<int> CountTenantGroupsAsync(TenantId tenant, CancellationToken cancellationToken);

    /// <summary>Counts the membership edges whose group is one of the tenant's groups (the <c>MaxMembershipEdges</c> usage).</summary>
    /// <param name="tenant">The tenant.</param>
    /// <param name="cancellationToken">Cancels the count.</param>
    /// <returns>The number of the tenant's membership edges.</returns>
    Task<int> CountTenantEdgesAsync(TenantId tenant, CancellationToken cancellationToken);

    /// <summary>Reads one page of the tenant's groups in ascending ordinal order of full id.</summary>
    /// <param name="tenant">The tenant.</param>
    /// <param name="afterGroupId">The exclusive full-id cursor, or <c>null</c> to start at the beginning.</param>
    /// <param name="pageSize">The page size, 1 to 1000.</param>
    /// <param name="cancellationToken">Cancels the scan.</param>
    /// <returns>The page, and the cursor of its last entry when more remain.</returns>
    Task<DirectoryGroupSlice> ListTenantGroupsAsync(
        TenantId tenant, string? afterGroupId, int pageSize, CancellationToken cancellationToken);

    /// <summary>
    /// Removes a group's membership edges in both directions, then its record.
    /// Idempotent and resumable.
    /// </summary>
    /// <param name="groupId">The full group id.</param>
    /// <param name="cancellationToken">Cancels the removal.</param>
    /// <returns>The number of edges removed.</returns>
    Task<int> RemoveGroupCascadeAsync(string groupId, CancellationToken cancellationToken);

    /// <summary>Adds a direct membership edge. Enforces membership's tenant group nesting invariant.</summary>
    /// <param name="groupId">The full parent group id.</param>
    /// <param name="memberId">The member id.</param>
    /// <param name="memberKind">Whether the member is a user or a group.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    /// <returns>A task that completes when the edge is stored.</returns>
    /// <exception cref="LatticeTenantGroupNestingException">The edge would violate the nesting invariant.</exception>
    Task AddMemberAsync(string groupId, string memberId, MembershipMemberKind memberKind, CancellationToken cancellationToken);

    /// <summary>Removes a direct membership edge. Idempotent.</summary>
    /// <param name="groupId">The full parent group id.</param>
    /// <param name="memberId">The member id.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    /// <returns>A task that completes when the edge is gone.</returns>
    Task RemoveMemberAsync(string groupId, string memberId, CancellationToken cancellationToken);

    /// <summary>Returns a group's direct member ids.</summary>
    /// <param name="groupId">The full group id.</param>
    /// <param name="cancellationToken">Cancels the scan.</param>
    /// <returns>The direct member ids, in no particular order.</returns>
    Task<IReadOnlyCollection<string>> MembersOfAsync(string groupId, CancellationToken cancellationToken);

    /// <summary>Returns the transitive groups a member belongs to (not including the member itself).</summary>
    /// <param name="memberId">The member id.</param>
    /// <param name="cancellationToken">Cancels the walk.</param>
    /// <returns>The transitive group closure.</returns>
    Task<IReadOnlyCollection<string>> GroupsOfAsync(string memberId, CancellationToken cancellationToken);

    /// <summary>Returns the seeds together with every group they transitively belong to.</summary>
    /// <param name="seedGroups">The seed group ids.</param>
    /// <param name="cancellationToken">Cancels the walk.</param>
    /// <returns>The seeds and their transitive closure.</returns>
    Task<IReadOnlyCollection<string>> ExpandGroupsAsync(IReadOnlyCollection<string> seedGroups, CancellationToken cancellationToken);
}
