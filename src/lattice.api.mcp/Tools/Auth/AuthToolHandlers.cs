using System.ComponentModel;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The thin adapter methods the auth tool module exposes as MCP tools. Every
/// method is a stateless, static shim over the internal
/// <see cref="ILatticeAuthAdmin"/> facade: it resolves the facade from the tool
/// invocation's request service provider (bound by the MCP SDK from
/// <c>RequestContext.Services</c>), marshals the tool-call arguments into the
/// facade's model types, and returns the facade result verbatim. No
/// authorization, read, write, or introspection logic lives here - the facade
/// owns it, and its administrator gate refuses a non-administrator caller
/// fail-closed even if one somehow reaches an invocation.
/// </summary>
/// <remarks>
/// The methods are grouped into <b>introspection</b> reads (advertised
/// read-only) and <b>administration</b> writes (advertised destructive). They
/// are held as static method groups so the tool module materialises each tool's
/// delegate exactly once when it builds its tool list, never per
/// <c>tools/call</c>.
/// </remarks>
internal static class AuthToolHandlers
{
    // ----- Introspection (read-only) -----

    /// <summary>Explains whether a subject may perform an operation over a keyspace scope.</summary>
    public static Task<AuthExplanation> ExplainAsync(
        ILatticeAuthAdmin admin,
        string subjectId,
        LatticeOperation operation,
        LatticeScopeKind scopeKind,
        string treeId,
        string? keyOrPrefix = null,
        [Description("Whether subjectId names a user or a group. Set to Group to explain a group subject (evaluated as a member of that group and its ancestors); defaults to User.")]
        LatticeSubjectSelectorKind subjectKind = LatticeSubjectSelectorKind.User,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(admin);
        var scope = new LatticeScope(scopeKind, treeId, keyOrPrefix);
        return admin.ExplainAsync(subjectId, operation, scope, subjectKind, cancellationToken);
    }

    /// <summary>Returns the authorization rules currently in effect for a subject.</summary>
    public static Task<AuthEffectivePermissions> EffectivePermissionsAsync(
        ILatticeAuthAdmin admin,
        string subjectId,
        [Description("Whether subjectId names a user or a group. Set to Group to resolve a group subject's rules (the group and its ancestors); defaults to User.")]
        LatticeSubjectSelectorKind subjectKind = LatticeSubjectSelectorKind.User,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(admin);
        return admin.EffectivePermissionsAsync(subjectId, subjectKind, cancellationToken);
    }

    /// <summary>Reads a single group record, or <c>null</c> when no such group exists.</summary>
    public static Task<AuthGroup?> GetGroupAsync(
        ILatticeAuthAdmin admin,
        string groupId,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(admin);
        return admin.GetGroupAsync(groupId, cancellationToken);
    }

    /// <summary>Reads one page of the group catalog in ascending group-id order.</summary>
    public static Task<AuthGroupPage> ListGroupsAsync(
        ILatticeAuthAdmin admin,
        int pageSize = 0,
        string? pageToken = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(admin);
        return admin.ListGroupsAsync(new AuthPageRequest { PageSize = pageSize, PageToken = pageToken }, cancellationToken);
    }

    /// <summary>Returns the direct members (users and nested groups) of a group.</summary>
    public static async Task<AuthGroupMembersResult> ListGroupMembersAsync(
        ILatticeAuthAdmin admin,
        string groupId,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(admin);
        var members = await admin.ListGroupMembersAsync(groupId, cancellationToken).ConfigureAwait(false);
        return new AuthGroupMembersResult { GroupId = groupId, Members = members };
    }

    /// <summary>Returns the full transitive set of group ids a member belongs to.</summary>
    public static async Task<AuthSubjectGroupsResult> ListSubjectGroupsAsync(
        ILatticeAuthAdmin admin,
        string memberId,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(admin);
        var groups = await admin.ListSubjectGroupsAsync(memberId, cancellationToken).ConfigureAwait(false);
        return new AuthSubjectGroupsResult { MemberId = memberId, Groups = groups };
    }

    /// <summary>Reads a single rule by its governed tree id and rule id, or <c>null</c> when none exists.</summary>
    public static Task<LatticeAuthorizationRule?> GetRuleAsync(
        ILatticeAuthAdmin admin,
        string treeId,
        string ruleId,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(admin);
        return admin.GetRuleAsync(treeId, ruleId, cancellationToken);
    }

    /// <summary>Reads one page of every rule in the store, ordered by (governed tree id, rule id).</summary>
    public static Task<AuthRulePage> ListRulesAsync(
        ILatticeAuthAdmin admin,
        int pageSize = 0,
        string? pageToken = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(admin);
        return admin.ListRulesAsync(new AuthPageRequest { PageSize = pageSize, PageToken = pageToken }, cancellationToken);
    }

    /// <summary>Reads one page of the rules governing a single tree, ordered by rule id.</summary>
    public static Task<AuthRulePage> ListRulesForTreeAsync(
        ILatticeAuthAdmin admin,
        string treeId,
        int pageSize = 0,
        string? pageToken = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(admin);
        return admin.ListRulesForTreeAsync(treeId, new AuthPageRequest { PageSize = pageSize, PageToken = pageToken }, cancellationToken);
    }

    // ----- Administration (destructive) -----

    /// <summary>Creates or replaces a group record, returning the written record.</summary>
    public static async Task<AuthGroup> UpsertGroupAsync(
        ILatticeAuthAdmin admin,
        string groupId,
        string? displayName = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(admin);
        var group = new AuthGroup { GroupId = groupId, DisplayName = displayName };
        await admin.UpsertGroupAsync(group, cancellationToken).ConfigureAwait(false);
        return group;
    }

    /// <summary>Removes a group record. A no-op when no such group exists.</summary>
    public static Task RemoveGroupAsync(
        ILatticeAuthAdmin admin,
        string groupId,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(admin);
        return admin.RemoveGroupAsync(groupId, cancellationToken);
    }

    /// <summary>Adds a membership edge making a member a direct member of a group. Idempotent.</summary>
    public static Task AddMemberAsync(
        ILatticeAuthAdmin admin,
        string groupId,
        string memberId,
        MembershipMemberKind memberKind = MembershipMemberKind.User,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(admin);
        return admin.AddMemberAsync(groupId, memberId, memberKind, cancellationToken);
    }

    /// <summary>Removes a membership edge. A no-op when the edge does not exist.</summary>
    public static Task RemoveMemberAsync(
        ILatticeAuthAdmin admin,
        string groupId,
        string memberId,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(admin);
        return admin.RemoveMemberAsync(groupId, memberId, cancellationToken);
    }

    /// <summary>Creates or replaces an authorization rule, returning the persisted rule.</summary>
    public static async Task<LatticeAuthorizationRule> PutRuleAsync(
        ILatticeAuthAdmin admin,
        string ruleId,
        LatticeSubjectSelectorKind subjectKind,
        string subjectId,
        LatticeScopeKind scopeKind,
        string treeId,
        LatticeOperation operations,
        LatticeEffect effect,
        string? keyOrPrefix = null,
        string? condition = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(admin);
        var subject = new LatticeSubjectSelector(subjectKind, subjectId);
        var scope = new LatticeScope(scopeKind, treeId, keyOrPrefix);
        var rule = new LatticeAuthorizationRule(ruleId, subject, scope, operations, effect, condition);
        await admin.PutRuleAsync(rule, cancellationToken).ConfigureAwait(false);
        return rule;
    }

    /// <summary>Removes a rule by its governed tree id and rule id. Returns <c>true</c> when a rule was removed.</summary>
    public static Task<bool> RemoveRuleAsync(
        ILatticeAuthAdmin admin,
        string treeId,
        string ruleId,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(admin);
        return admin.RemoveRuleAsync(treeId, ruleId, cancellationToken);
    }
}
