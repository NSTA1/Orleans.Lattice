using Grpc.Core;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Explorer.Access;

/// <summary>
/// The default <see cref="IMembershipAdminService"/> over an
/// <see cref="IAuthAdminClient"/>. Every action catches a
/// <see cref="LatticeAuthorizationDeniedException"/> (the translated server
/// denial) and a residual <see cref="RpcException"/> and returns a non-success
/// result, so the Access UI degrades cleanly and never leaks an exception even
/// when the advisory capability map believed an action was allowed.
/// </summary>
public sealed class MembershipAdminService(IAuthAdminClient client) : IMembershipAdminService
{
    private readonly IAuthAdminClient _client = client ?? throw new ArgumentNullException(nameof(client));

    /// <inheritdoc />
    public async Task<AccessListView<AuthUser>> ListUsersAsync(int pageSize = 0, string? pageToken = null, CancellationToken cancellationToken = default)
    {
        var request = new AuthPageRequest { PageSize = pageSize, PageToken = string.IsNullOrEmpty(pageToken) ? null : pageToken };
        try
        {
            var page = await _client.ListUsersAsync(request, cancellationToken).ConfigureAwait(false);
            return new AccessListView<AuthUser>
            {
                Status = AccessOperationStatus.Succeeded,
                Entries = page.Entries,
                NextPageToken = page.NextPageToken,
            };
        }
        catch (LatticeAuthorizationDeniedException ex)
        {
            return Denied<AuthUser>(ex);
        }
        catch (RpcException ex)
        {
            return Failed<AuthUser>(ex);
        }
    }

    /// <inheritdoc />
    public async Task<AuthUser?> GetUserAsync(string userId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(userId);
        try
        {
            return await _client.GetUserAsync(userId, cancellationToken).ConfigureAwait(false);
        }
        catch (LatticeAuthorizationDeniedException)
        {
            return null;
        }
        catch (RpcException)
        {
            return null;
        }
    }

    /// <inheritdoc />
    public Task<AccessOperationResult> UpsertUserAsync(AuthUser user, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(user);
        return RunAsync(
            async () =>
            {
                await _client.UpsertUserAsync(user, cancellationToken).ConfigureAwait(false);
                return AccessOperationResult.Success($"Saved user '{user.UserId}'.");
            });
    }

    /// <inheritdoc />
    public Task<AccessOperationResult> DeleteUserAsync(string userId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(userId);
        return RunAsync(
            async () =>
            {
                await _client.RemoveUserAsync(userId, cancellationToken).ConfigureAwait(false);
                return AccessOperationResult.Success($"Deleted user '{userId}'.");
            });
    }

    /// <inheritdoc />
    public async Task<AccessListView<AuthGroup>> ListGroupsAsync(int pageSize = 0, string? pageToken = null, CancellationToken cancellationToken = default)
    {
        var request = new AuthPageRequest { PageSize = pageSize, PageToken = string.IsNullOrEmpty(pageToken) ? null : pageToken };
        try
        {
            var page = await _client.ListGroupsAsync(request, cancellationToken).ConfigureAwait(false);
            return new AccessListView<AuthGroup>
            {
                Status = AccessOperationStatus.Succeeded,
                Entries = page.Entries,
                NextPageToken = page.NextPageToken,
            };
        }
        catch (LatticeAuthorizationDeniedException ex)
        {
            return Denied<AuthGroup>(ex);
        }
        catch (RpcException ex)
        {
            return Failed<AuthGroup>(ex);
        }
    }

    /// <inheritdoc />
    public async Task<AuthGroup?> GetGroupAsync(string groupId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(groupId);
        try
        {
            return await _client.GetGroupAsync(groupId, cancellationToken).ConfigureAwait(false);
        }
        catch (LatticeAuthorizationDeniedException)
        {
            return null;
        }
        catch (RpcException)
        {
            return null;
        }
    }

    /// <inheritdoc />
    public Task<AccessOperationResult> UpsertGroupAsync(AuthGroup group, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(group);
        return RunAsync(
            async () =>
            {
                await _client.UpsertGroupAsync(group, cancellationToken).ConfigureAwait(false);
                return AccessOperationResult.Success($"Saved group '{group.GroupId}'.");
            });
    }

    /// <inheritdoc />
    public Task<AccessOperationResult> DeleteGroupAsync(string groupId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(groupId);
        return RunAsync(
            async () =>
            {
                await _client.RemoveGroupAsync(groupId, cancellationToken).ConfigureAwait(false);
                return AccessOperationResult.Success($"Deleted group '{groupId}'.");
            });
    }

    /// <inheritdoc />
    public Task<AccessOperationResult> AddMemberAsync(
        string groupId,
        string memberId,
        MembershipMemberKind memberKind = MembershipMemberKind.User,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(groupId);
        ArgumentException.ThrowIfNullOrEmpty(memberId);
        return RunAsync(
            async () =>
            {
                await _client.AddMemberAsync(groupId, memberId, memberKind, cancellationToken).ConfigureAwait(false);
                var kind = memberKind == MembershipMemberKind.Group ? "group" : "user";
                return AccessOperationResult.Success($"Added {kind} '{memberId}' to group '{groupId}'.");
            });
    }

    /// <inheritdoc />
    public Task<AccessOperationResult> RemoveMemberAsync(string groupId, string memberId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(groupId);
        ArgumentException.ThrowIfNullOrEmpty(memberId);
        return RunAsync(
            async () =>
            {
                await _client.RemoveMemberAsync(groupId, memberId, cancellationToken).ConfigureAwait(false);
                return AccessOperationResult.Success($"Removed '{memberId}' from group '{groupId}'.");
            });
    }

    /// <inheritdoc />
    public async Task<AccessListView<string>> ListDirectMembersAsync(string groupId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(groupId);
        try
        {
            var members = await _client.ListGroupMembersAsync(groupId, cancellationToken).ConfigureAwait(false);
            return new AccessListView<string> { Status = AccessOperationStatus.Succeeded, Entries = members };
        }
        catch (LatticeAuthorizationDeniedException ex)
        {
            return Denied<string>(ex);
        }
        catch (RpcException ex)
        {
            return Failed<string>(ex);
        }
    }

    /// <inheritdoc />
    public async Task<AccessListView<string>> ListSubjectGroupsAsync(string memberId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(memberId);
        try
        {
            var groups = await _client.ListSubjectGroupsAsync(memberId, cancellationToken).ConfigureAwait(false);
            return new AccessListView<string> { Status = AccessOperationStatus.Succeeded, Entries = groups };
        }
        catch (LatticeAuthorizationDeniedException ex)
        {
            return Denied<string>(ex);
        }
        catch (RpcException ex)
        {
            return Failed<string>(ex);
        }
    }

    private static AccessListView<T> Denied<T>(LatticeAuthorizationDeniedException ex) =>
        new() { Status = AccessOperationStatus.Denied, Message = AccessFailure.DenialMessage(ex) };

    private static AccessListView<T> Failed<T>(RpcException ex) =>
        new() { Status = AccessOperationStatus.Failed, Message = AccessFailure.FailureMessage(ex) };

    private static async Task<AccessOperationResult> RunAsync(Func<Task<AccessOperationResult>> action)
    {
        try
        {
            return await action().ConfigureAwait(false);
        }
        catch (LatticeAuthorizationDeniedException ex)
        {
            return AccessOperationResult.Denied(AccessFailure.DenialMessage(ex));
        }
        catch (RpcException ex)
        {
            return AccessOperationResult.Failure(AccessFailure.FailureMessage(ex));
        }
    }
}
