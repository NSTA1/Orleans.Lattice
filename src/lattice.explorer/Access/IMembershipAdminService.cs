using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Explorer.Access;

/// <summary>
/// The membership-administration surface the Access area drives over the
/// auth-admin control plane: listing, creating, editing, and deleting users and
/// groups; adding and removing membership edges (including nested groups); and
/// browsing a group's direct members plus a subject's transitive group closure.
/// Every read folds a denial / transport failure into a non-success
/// <see cref="AccessListView{T}"/>, and every mutation into an
/// <see cref="AccessOperationResult"/>, so the UI degrades cleanly and never
/// leaks an exception.
/// </summary>
public interface IMembershipAdminService
{
    /// <summary>Lists one page of the user catalog in ascending user-id order.</summary>
    /// <param name="pageSize">The page size, or <c>0</c> for the facade default.</param>
    /// <param name="pageToken">The continuation cursor, or <see langword="null"/> to start from the beginning.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<AccessListView<AuthUser>> ListUsersAsync(int pageSize = 0, string? pageToken = null, CancellationToken cancellationToken = default);

    /// <summary>Reads a single user, or <see langword="null"/> when it does not exist or the read is denied / fails.</summary>
    /// <param name="userId">The user id. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<AuthUser?> GetUserAsync(string userId, CancellationToken cancellationToken = default);

    /// <summary>Creates or replaces a user record.</summary>
    /// <param name="user">The user to upsert. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<AccessOperationResult> UpsertUserAsync(AuthUser user, CancellationToken cancellationToken = default);

    /// <summary>Deletes a user record.</summary>
    /// <param name="userId">The user id. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<AccessOperationResult> DeleteUserAsync(string userId, CancellationToken cancellationToken = default);

    /// <summary>Lists one page of the group catalog in ascending group-id order.</summary>
    /// <param name="pageSize">The page size, or <c>0</c> for the facade default.</param>
    /// <param name="pageToken">The continuation cursor, or <see langword="null"/> to start from the beginning.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<AccessListView<AuthGroup>> ListGroupsAsync(int pageSize = 0, string? pageToken = null, CancellationToken cancellationToken = default);

    /// <summary>Reads a single group, or <see langword="null"/> when it does not exist or the read is denied / fails.</summary>
    /// <param name="groupId">The group id. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<AuthGroup?> GetGroupAsync(string groupId, CancellationToken cancellationToken = default);

    /// <summary>Creates or replaces a group record.</summary>
    /// <param name="group">The group to upsert. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<AccessOperationResult> UpsertGroupAsync(AuthGroup group, CancellationToken cancellationToken = default);

    /// <summary>Deletes a group record.</summary>
    /// <param name="groupId">The group id. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<AccessOperationResult> DeleteGroupAsync(string groupId, CancellationToken cancellationToken = default);

    /// <summary>Adds a membership edge making <paramref name="memberId"/> a direct member of <paramref name="groupId"/>.</summary>
    /// <param name="groupId">The parent group id. Must not be <see langword="null"/> or empty.</param>
    /// <param name="memberId">The member id (a user or nested group). Must not be <see langword="null"/> or empty.</param>
    /// <param name="memberKind">Whether the member is a user or a nested group.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<AccessOperationResult> AddMemberAsync(
        string groupId,
        string memberId,
        MembershipMemberKind memberKind = MembershipMemberKind.User,
        CancellationToken cancellationToken = default);

    /// <summary>Removes a membership edge.</summary>
    /// <param name="groupId">The parent group id. Must not be <see langword="null"/> or empty.</param>
    /// <param name="memberId">The member id. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<AccessOperationResult> RemoveMemberAsync(string groupId, string memberId, CancellationToken cancellationToken = default);

    /// <summary>Lists a group's <b>direct</b> members (users and nested groups), in ascending ordinal order.</summary>
    /// <param name="groupId">The group id. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<AccessListView<string>> ListDirectMembersAsync(string groupId, CancellationToken cancellationToken = default);

    /// <summary>Lists a subject's full <b>transitive</b> group closure, in ascending ordinal order.</summary>
    /// <param name="memberId">The member id (a user or group). Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<AccessListView<string>> ListSubjectGroupsAsync(string memberId, CancellationToken cancellationToken = default);
}
