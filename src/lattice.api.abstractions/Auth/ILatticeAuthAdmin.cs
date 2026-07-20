using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Api.Auth;

/// <summary>
/// Transport-agnostic configuration and control facade over a cluster's
/// authorization system: membership administration, policy administration, and
/// policy introspection behind a single surface. Every transport binding (a
/// future gRPC surface) is a thin adapter over this one facade, so the admin
/// semantics are written and tested once and no transport concern leaks into
/// the control logic.
/// </summary>
/// <remarks>
/// <para>
/// <b>Administrator-gated.</b> This is a control plane, so <b>every</b>
/// operation - read or write - first authorizes the caller as an administrator
/// through the <b>same enforcement primitive the in-cluster data path uses</b>:
/// the caller identity is resolved from the ambient
/// <see cref="LatticeCredentialContext"/> and required to satisfy an
/// administrator verdict from the access gate before any membership or policy
/// work runs. A non-administrator (or anonymous) caller is refused
/// <see cref="LatticeAuthorizationDeniedException"/>, fail-closed. The facade
/// adds no bespoke, un-authorized write path to the membership or policy trees.
/// </para>
/// <para>
/// <b>Zero cost when unregistered.</b> The add-on is opt-in and absent by
/// default; when the authorization package is not registered the gate is the
/// core no-op and the administrator check short-circuits, so the facade behaves
/// exactly as an un-secured control surface would.
/// </para>
/// </remarks>
public interface ILatticeAuthAdmin
{
    // ----- Membership administration -----

    /// <summary>Creates or replaces a group record.</summary>
    /// <param name="group">The group to upsert. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    Task UpsertGroupAsync(AuthGroup group, CancellationToken cancellationToken = default);

    /// <summary>Reads a group record, or <c>null</c> when no such group exists.</summary>
    /// <param name="groupId">The group id. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    Task<AuthGroup?> GetGroupAsync(string groupId, CancellationToken cancellationToken = default);

    /// <summary>Removes a group record. A no-op when no such group exists.</summary>
    /// <param name="groupId">The group id. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    Task RemoveGroupAsync(string groupId, CancellationToken cancellationToken = default);

    /// <summary>Reads one page of the group catalog in ascending group-id order.</summary>
    /// <param name="request">Paging request (page size and continuation cursor). Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the scan.</param>
    Task<AuthGroupPage> ListGroupsAsync(AuthPageRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Adds a membership edge making <paramref name="memberId"/> a direct member
    /// of <paramref name="groupId"/>. Idempotent.
    /// </summary>
    /// <param name="groupId">The parent group id. Must not be <c>null</c> or empty.</param>
    /// <param name="memberId">The member id (a user or nested group). Must not be <c>null</c> or empty.</param>
    /// <param name="memberKind">Whether the member is a user or a nested group.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    Task AddMemberAsync(
        string groupId,
        string memberId,
        MembershipMemberKind memberKind = MembershipMemberKind.User,
        CancellationToken cancellationToken = default);

    /// <summary>Removes a membership edge. A no-op when the edge does not exist.</summary>
    /// <param name="groupId">The parent group id. Must not be <c>null</c> or empty.</param>
    /// <param name="memberId">The member id. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    Task RemoveMemberAsync(string groupId, string memberId, CancellationToken cancellationToken = default);

    /// <summary>Returns the <b>direct</b> members of a group (users and nested groups), in ascending ordinal order.</summary>
    /// <param name="groupId">The group id. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancels the scan.</param>
    Task<IReadOnlyList<string>> ListGroupMembersAsync(string groupId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the full <b>transitive</b> set of group ids
    /// <paramref name="memberId"/> belongs to (walking nested groups), in
    /// ascending ordinal order.
    /// </summary>
    /// <param name="memberId">The member id (a user or group). Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancels the scan.</param>
    Task<IReadOnlyList<string>> ListSubjectGroupsAsync(string memberId, CancellationToken cancellationToken = default);

    // ----- Policy administration -----

    /// <summary>Creates or replaces an authorization rule.</summary>
    /// <param name="rule">The rule to persist. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    Task PutRuleAsync(LatticeAuthorizationRule rule, CancellationToken cancellationToken = default);

    /// <summary>Reads a single rule by its governed tree id and rule id, or <c>null</c> when none exists.</summary>
    /// <param name="treeId">The rule's governed tree id. Must not be <c>null</c> or empty.</param>
    /// <param name="ruleId">The rule id. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    Task<LatticeAuthorizationRule?> GetRuleAsync(string treeId, string ruleId, CancellationToken cancellationToken = default);

    /// <summary>Removes a rule by its governed tree id and rule id. Returns <c>true</c> when a rule was removed.</summary>
    /// <param name="treeId">The rule's governed tree id. Must not be <c>null</c> or empty.</param>
    /// <param name="ruleId">The rule id. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    Task<bool> RemoveRuleAsync(string treeId, string ruleId, CancellationToken cancellationToken = default);

    /// <summary>Reads one page of every rule in the store, ordered by <c>(governed tree id, rule id)</c>.</summary>
    /// <param name="request">Paging request (page size and continuation cursor). Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the scan.</param>
    Task<AuthRulePage> ListRulesAsync(AuthPageRequest request, CancellationToken cancellationToken = default);

    /// <summary>Reads one page of the rules governing a single tree, ordered by rule id.</summary>
    /// <param name="treeId">The governed tree id. Must not be <c>null</c> or empty.</param>
    /// <param name="request">Paging request (page size and continuation cursor). Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the scan.</param>
    Task<AuthRulePage> ListRulesForTreeAsync(string treeId, AuthPageRequest request, CancellationToken cancellationToken = default);

    // ----- Policy introspection -----

    /// <summary>
    /// Explains whether <paramref name="subjectId"/> may perform
    /// <paramref name="operation"/> over <paramref name="scope"/>, returning the
    /// gate's verdict plus the authored rules that apply. The verdict is
    /// produced by the same access gate the data plane consults, so it can never
    /// disagree with the enforced decision.
    /// </summary>
    /// <param name="subjectId">The subject to explain the decision for. Must not be <c>null</c> or empty.</param>
    /// <param name="operation">The operation to evaluate.</param>
    /// <param name="scope">The keyspace scope to evaluate (whole tree, a key, or a prefix). Must not be <c>null</c>.</param>
    /// <param name="subjectKind">
    /// Whether <paramref name="subjectId"/> names a user or a group. When
    /// <see cref="LatticeSubjectSelectorKind.Group"/>, the decision is evaluated
    /// for a principal that is a member of the named group (and its ancestor
    /// groups), so <c>group</c>-scoped rules targeting it match exactly as they
    /// would for a real member. Defaults to
    /// <see cref="LatticeSubjectSelectorKind.User"/>.
    /// </param>
    /// <param name="cancellationToken">Cancels the evaluation.</param>
    Task<AuthExplanation> ExplainAsync(
        string subjectId,
        LatticeOperation operation,
        LatticeScope scope,
        LatticeSubjectSelectorKind subjectKind = LatticeSubjectSelectorKind.User,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the authorization rules currently in effect for
    /// <paramref name="subjectId"/> - the grants and denies whose subject
    /// selector matches the subject directly or through one of its groups -
    /// computed from the live policy store.
    /// </summary>
    /// <param name="subjectId">The subject to resolve permissions for. Must not be <c>null</c> or empty.</param>
    /// <param name="subjectKind">
    /// Whether <paramref name="subjectId"/> names a user or a group. When
    /// <see cref="LatticeSubjectSelectorKind.Group"/>, the resolved set is the
    /// rules matching the named group (and its ancestor groups). Defaults to
    /// <see cref="LatticeSubjectSelectorKind.User"/>.
    /// </param>
    /// <param name="cancellationToken">Cancels the scan.</param>
    Task<AuthEffectivePermissions> EffectivePermissionsAsync(
        string subjectId,
        LatticeSubjectSelectorKind subjectKind = LatticeSubjectSelectorKind.User,
        CancellationToken cancellationToken = default);

    // ----- Identity directory -----

    /// <summary>
    /// Searches or browses the configured external identity directory for
    /// principals matching <paramref name="request"/>, so an operator can pick a
    /// real user or group before granting it access. When no directory is
    /// configured the result is an explicit
    /// <see cref="DirectorySearchResult.Unavailable"/> (empty and
    /// <see cref="DirectorySearchResult.Available"/> <see langword="false"/>)
    /// rather than an error.
    /// </summary>
    /// <param name="request">The typeahead / browse request. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the search.</param>
    Task<DirectorySearchResult> SearchDirectoryAsync(DirectorySearchRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Resolves a single principal by its exact id against the configured
    /// external identity directory, confirming it exists, or returns
    /// <c>null</c> when no such principal exists (or when no directory is
    /// configured).
    /// </summary>
    /// <param name="principalId">The exact principal id to resolve. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancels the resolve.</param>
    Task<DirectoryPrincipalDescriptor?> ResolveDirectoryPrincipalAsync(string principalId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Reports the cluster's best-effort access model: the active authentication
    /// mode, whether authorization rules are actually enforced, and whether an
    /// identity directory is available for validating candidate ids (with its
    /// provider id and operator-facing explanation), so a UI can render the right
    /// create-form guidance.
    /// </summary>
    /// <param name="cancellationToken">Cancels the read.</param>
    Task<AccessModelDescriptor> GetAccessModelAsync(CancellationToken cancellationToken = default);
}
