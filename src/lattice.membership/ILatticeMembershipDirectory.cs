namespace Orleans.Lattice.Membership;

/// <summary>
/// The introspectable group directory backing subject resolution. Groups and
/// membership edges are stored in reserved, dogfooded <c>sys-membership-*</c>
/// <c>ILattice</c> trees, so every record is readable through the ordinary read /
/// scan / change-feed surface and every mutation is durably auditable via the
/// per-key history view.
/// </summary>
public interface ILatticeMembershipDirectory
{
    /// <summary>Creates or replaces a group record.</summary>
    /// <param name="group">The group to upsert. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    Task UpsertGroupAsync(MembershipGroup group, CancellationToken cancellationToken = default);

    /// <summary>Reads a group record, or <c>null</c> when no such group exists.</summary>
    /// <param name="groupId">The group id. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    Task<MembershipGroup?> GetGroupAsync(string groupId, CancellationToken cancellationToken = default);

    /// <summary>Enumerates every group record in id order.</summary>
    /// <param name="cancellationToken">Cancels the scan.</param>
    IAsyncEnumerable<MembershipGroup> ListGroupsAsync(CancellationToken cancellationToken = default);

    /// <summary>Removes a group record. A no-op when no such group exists. Does not remove the group's membership edges.</summary>
    /// <param name="groupId">The group id. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    Task RemoveGroupAsync(string groupId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Adds a membership edge making <paramref name="memberId"/> a direct member
    /// of <paramref name="groupId"/>. Idempotent.
    /// </summary>
    /// <param name="groupId">The parent group id. Must not be <c>null</c>.</param>
    /// <param name="memberId">The member id (a user or nested group). Must not be <c>null</c>.</param>
    /// <param name="memberKind">Whether the member is a user or a nested group.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    Task AddMemberAsync(string groupId, string memberId, MembershipMemberKind memberKind = MembershipMemberKind.User, CancellationToken cancellationToken = default);

    /// <summary>Removes a membership edge. A no-op when the edge does not exist.</summary>
    /// <param name="groupId">The parent group id. Must not be <c>null</c>.</param>
    /// <param name="memberId">The member id. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    Task RemoveMemberAsync(string groupId, string memberId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the full <b>transitive</b> set of group ids
    /// <paramref name="memberId"/> belongs to (walking nested groups, with cycle
    /// detection). Does not include <paramref name="memberId"/> itself.
    /// </summary>
    /// <param name="memberId">The member id (a user or group). Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the scan.</param>
    Task<IReadOnlyCollection<string>> GroupsOfAsync(string memberId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Expands a set of seed group ids into its full <b>transitive</b> closure
    /// over the directory graph (walking nested-group parents, with cycle
    /// detection). The returned set <b>includes the seeds themselves</b> plus
    /// every group reachable from them; a seed that is not a known directory
    /// node contributes only itself. Used to expand token-asserted / claim-derived
    /// group ids so downstream policy sees a single uniformly-expanded group set.
    /// </summary>
    /// <param name="seedGroups">The seed group ids to expand. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the scan.</param>
    Task<IReadOnlyCollection<string>> ExpandGroupsAsync(IReadOnlyCollection<string> seedGroups, CancellationToken cancellationToken = default);

    /// <summary>Returns the <b>direct</b> members of a group (users and nested groups).</summary>
    /// <param name="groupId">The group id. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the scan.</param>
    Task<IReadOnlyCollection<string>> MembersOfAsync(string groupId, CancellationToken cancellationToken = default);
}
