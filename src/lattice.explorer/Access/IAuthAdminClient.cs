using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Explorer.Access;

/// <summary>
/// The explorer's transport-facing view of the auth-admin control plane: the
/// membership, policy, and introspection surface the Access area drives, over a
/// gRPC channel built from the current endpoint and sign-in. Shaped like the
/// <c>ILatticeAuthAdmin</c> facade (returning the same facade model records)
/// rather than the raw gRPC envelopes, so the membership and policy services can
/// be unit-tested against a fake without any transport dependency.
/// </summary>
/// <remarks>
/// Every call may surface a <see cref="LatticeAuthorizationDeniedException"/>
/// when the server denies the caller: the control plane is administrator-gated,
/// so a non-administrator (or anonymous) caller is refused fail-closed. The
/// production client translates the gRPC <c>PermissionDenied</c> /
/// <c>Unauthenticated</c> status back to this typed exception, so callers handle
/// a single denial shape even when an advisory capability flag suggested the
/// action was allowed.
/// </remarks>
public interface IAuthAdminClient
{
    // ----- Membership administration -----

    /// <summary>Reads one page of the user catalog in ascending user-id order.</summary>
    /// <param name="request">The paging request. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<AuthUserPage> ListUsersAsync(AuthPageRequest request, CancellationToken cancellationToken = default);

    /// <summary>Reads a user record, or <see langword="null"/> when no such user exists.</summary>
    /// <param name="userId">The user id. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<AuthUser?> GetUserAsync(string userId, CancellationToken cancellationToken = default);

    /// <summary>Creates or replaces a user record.</summary>
    /// <param name="user">The user to upsert. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task UpsertUserAsync(AuthUser user, CancellationToken cancellationToken = default);

    /// <summary>Removes a user record. A no-op when no such user exists.</summary>
    /// <param name="userId">The user id. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task RemoveUserAsync(string userId, CancellationToken cancellationToken = default);

    /// <summary>Reads one page of the group catalog in ascending group-id order.</summary>
    /// <param name="request">The paging request. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<AuthGroupPage> ListGroupsAsync(AuthPageRequest request, CancellationToken cancellationToken = default);

    /// <summary>Reads a group record, or <see langword="null"/> when no such group exists.</summary>
    /// <param name="groupId">The group id. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<AuthGroup?> GetGroupAsync(string groupId, CancellationToken cancellationToken = default);

    /// <summary>Creates or replaces a group record.</summary>
    /// <param name="group">The group to upsert. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task UpsertGroupAsync(AuthGroup group, CancellationToken cancellationToken = default);

    /// <summary>Removes a group record. A no-op when no such group exists.</summary>
    /// <param name="groupId">The group id. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task RemoveGroupAsync(string groupId, CancellationToken cancellationToken = default);

    /// <summary>Adds a membership edge making <paramref name="memberId"/> a direct member of <paramref name="groupId"/>. Idempotent.</summary>
    /// <param name="groupId">The parent group id. Must not be <see langword="null"/> or empty.</param>
    /// <param name="memberId">The member id (a user or nested group). Must not be <see langword="null"/> or empty.</param>
    /// <param name="memberKind">Whether the member is a user or a nested group.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task AddMemberAsync(
        string groupId,
        string memberId,
        MembershipMemberKind memberKind = MembershipMemberKind.User,
        CancellationToken cancellationToken = default);

    /// <summary>Removes a membership edge. A no-op when the edge does not exist.</summary>
    /// <param name="groupId">The parent group id. Must not be <see langword="null"/> or empty.</param>
    /// <param name="memberId">The member id. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task RemoveMemberAsync(string groupId, string memberId, CancellationToken cancellationToken = default);

    /// <summary>Returns the <b>direct</b> members of a group (users and nested groups), in ascending ordinal order.</summary>
    /// <param name="groupId">The group id. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<IReadOnlyList<string>> ListGroupMembersAsync(string groupId, CancellationToken cancellationToken = default);

    /// <summary>Returns the full <b>transitive</b> set of group ids <paramref name="memberId"/> belongs to, in ascending ordinal order.</summary>
    /// <param name="memberId">The member id (a user or group). Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<IReadOnlyList<string>> ListSubjectGroupsAsync(string memberId, CancellationToken cancellationToken = default);

    // ----- Policy administration -----

    /// <summary>Creates or replaces an authorization rule.</summary>
    /// <param name="rule">The rule to persist. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task PutRuleAsync(LatticeAuthorizationRule rule, CancellationToken cancellationToken = default);

    /// <summary>Reads a single rule by its governed tree id and rule id, or <see langword="null"/> when none exists.</summary>
    /// <param name="treeId">The rule's governed tree id. Must not be <see langword="null"/> or empty.</param>
    /// <param name="ruleId">The rule id. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<LatticeAuthorizationRule?> GetRuleAsync(string treeId, string ruleId, CancellationToken cancellationToken = default);

    /// <summary>Removes a rule by its governed tree id and rule id. Returns <see langword="true"/> when a rule was removed.</summary>
    /// <param name="treeId">The rule's governed tree id. Must not be <see langword="null"/> or empty.</param>
    /// <param name="ruleId">The rule id. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<bool> RemoveRuleAsync(string treeId, string ruleId, CancellationToken cancellationToken = default);

    /// <summary>Reads one page of every rule in the store, ordered by <c>(governed tree id, rule id)</c>.</summary>
    /// <param name="request">The paging request. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<AuthRulePage> ListRulesAsync(AuthPageRequest request, CancellationToken cancellationToken = default);

    /// <summary>Reads one page of the rules governing a single tree, ordered by rule id.</summary>
    /// <param name="treeId">The governed tree id. Must not be <see langword="null"/> or empty.</param>
    /// <param name="request">The paging request. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<AuthRulePage> ListRulesForTreeAsync(string treeId, AuthPageRequest request, CancellationToken cancellationToken = default);

    // ----- Policy introspection -----

    /// <summary>Explains whether <paramref name="subjectId"/> may perform <paramref name="operation"/> over <paramref name="scope"/>, with the applying rules.</summary>
    /// <param name="subjectId">The subject to explain the decision for. Must not be <see langword="null"/> or empty.</param>
    /// <param name="operation">The operation to evaluate.</param>
    /// <param name="scope">The keyspace scope to evaluate. Must not be <see langword="null"/>.</param>
    /// <param name="subjectKind">Whether <paramref name="subjectId"/> names a user or a group. Defaults to <see cref="LatticeSubjectSelectorKind.User"/>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<AuthExplanation> ExplainAsync(
        string subjectId,
        LatticeOperation operation,
        LatticeScope scope,
        LatticeSubjectSelectorKind subjectKind = LatticeSubjectSelectorKind.User,
        CancellationToken cancellationToken = default);

    /// <summary>Returns the authorization rules currently in effect for <paramref name="subjectId"/>.</summary>
    /// <param name="subjectId">The subject to resolve permissions for. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<AuthEffectivePermissions> EffectivePermissionsAsync(string subjectId, CancellationToken cancellationToken = default);

    // ----- Identity directory -----

    /// <summary>
    /// Searches or browses the configured identity directory. When no directory
    /// is configured the result is <see cref="DirectorySearchResult.Unavailable"/>
    /// (empty and <see cref="DirectorySearchResult.Available"/> <see langword="false"/>).
    /// </summary>
    /// <param name="request">The typeahead / browse request. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<DirectorySearchResult> SearchDirectoryAsync(DirectorySearchRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Resolves a single directory principal by its exact id, or
    /// <see langword="null"/> when no such principal exists (or no directory is
    /// configured).
    /// </summary>
    /// <param name="principalId">The exact principal id to resolve. Must not be <see langword="null"/> or empty.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<DirectoryPrincipalDescriptor?> ResolveDirectoryPrincipalAsync(string principalId, CancellationToken cancellationToken = default);

    /// <summary>Reads the cluster's best-effort access model (authentication mode, rule enforcement, directory availability).</summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<AccessModelDescriptor> GetAccessModelAsync(CancellationToken cancellationToken = default);
}
