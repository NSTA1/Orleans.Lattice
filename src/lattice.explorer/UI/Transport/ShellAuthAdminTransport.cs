using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Api.Auth.Grpc;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Explorer.UI.Transport;

/// <summary>
/// The Shell's <see cref="ILatticeAuthAdmin"/> over gRPC: a per-circuit adapter
/// over <see cref="LatticeAuthApiGrpcClient"/>, ported from the Access plugin's
/// <c>GrpcAuthAdminClient</c>. Faults map through <see cref="ShellTransportFaults"/>.
/// </summary>
/// <param name="channel">The circuit's transport channel.</param>
internal sealed class ShellAuthAdminTransport(ShellTransportChannel channel)
    : ShellTransportAdapter<LatticeAuthApiGrpcClient>(channel, LatticeAuthApiGrpcClient.Create), ILatticeAuthAdmin
{
    /// <inheritdoc />
    public Task UpsertGroupAsync(AuthGroup group, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(group);
        return CallAsync(group, static (client, state, ct) => client.UpsertGroupAsync(state, ct), null, cancellationToken);
    }

    /// <inheritdoc />
    public Task<AuthGroup?> GetGroupAsync(string groupId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(groupId);
        return CallAsync(
            groupId,
            static async (client, state, ct) =>
                (await client.GetGroupAsync(new AuthGroupRef { GroupId = state }, ct).ConfigureAwait(false)).Group,
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task RemoveGroupAsync(string groupId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(groupId);
        return CallAsync(
            groupId,
            static (client, state, ct) => client.RemoveGroupAsync(new AuthGroupRef { GroupId = state }, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<AuthGroupPage> ListGroupsAsync(AuthPageRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        return CallAsync(request, static (client, state, ct) => client.ListGroupsAsync(state, ct), null, cancellationToken);
    }

    /// <inheritdoc />
    public Task AddMemberAsync(
        string groupId,
        string memberId,
        MembershipMemberKind memberKind = MembershipMemberKind.User,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(groupId);
        ArgumentException.ThrowIfNullOrEmpty(memberId);
        var edge = new AuthMemberEdge { GroupId = groupId, MemberId = memberId, MemberKind = memberKind };
        return CallAsync(edge, static (client, state, ct) => client.AddMemberAsync(state, ct), null, cancellationToken);
    }

    /// <inheritdoc />
    public Task RemoveMemberAsync(string groupId, string memberId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(groupId);
        ArgumentException.ThrowIfNullOrEmpty(memberId);
        var edge = new AuthMemberEdge { GroupId = groupId, MemberId = memberId };
        return CallAsync(edge, static (client, state, ct) => client.RemoveMemberAsync(state, ct), null, cancellationToken);
    }

    /// <inheritdoc />
    public Task<IReadOnlyList<string>> ListGroupMembersAsync(string groupId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(groupId);
        return CallAsync(
            groupId,
            static async (client, state, ct) =>
                (await client.ListGroupMembersAsync(new AuthGroupRef { GroupId = state }, ct).ConfigureAwait(false)).Values,
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<IReadOnlyList<string>> ListSubjectGroupsAsync(string memberId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(memberId);
        return CallAsync(
            memberId,
            static async (client, state, ct) =>
                (await client.ListSubjectGroupsAsync(new AuthMemberRef { MemberId = state }, ct).ConfigureAwait(false)).Values,
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task PutRuleAsync(LatticeAuthorizationRule rule, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(rule);
        return CallAsync(
            new AuthPutRule { Rule = rule },
            static (client, state, ct) => client.PutRuleAsync(state, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<LatticeAuthorizationRule?> GetRuleAsync(string treeId, string ruleId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentException.ThrowIfNullOrEmpty(ruleId);
        return CallAsync(
            new AuthRuleRef { TreeId = treeId, RuleId = ruleId },
            static async (client, state, ct) => (await client.GetRuleAsync(state, ct).ConfigureAwait(false)).Rule,
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<bool> RemoveRuleAsync(string treeId, string ruleId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentException.ThrowIfNullOrEmpty(ruleId);
        return CallAsync(
            new AuthRuleRef { TreeId = treeId, RuleId = ruleId },
            static async (client, state, ct) => (await client.RemoveRuleAsync(state, ct).ConfigureAwait(false)).Removed,
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<AuthRulePage> ListRulesAsync(AuthPageRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        return CallAsync(request, static (client, state, ct) => client.ListRulesAsync(state, ct), null, cancellationToken);
    }

    /// <inheritdoc />
    public Task<AuthRulePage> ListRulesForTreeAsync(string treeId, AuthPageRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentNullException.ThrowIfNull(request);
        return CallAsync(
            new AuthTreeRulesPage { TreeId = treeId, Page = request },
            static (client, state, ct) => client.ListRulesForTreeAsync(state, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<AuthExplanation> ExplainAsync(
        string subjectId,
        LatticeOperation operation,
        LatticeScope scope,
        LatticeSubjectSelectorKind subjectKind = LatticeSubjectSelectorKind.User,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(subjectId);
        ArgumentNullException.ThrowIfNull(scope);
        return CallAsync(
            new AuthExplainQuery { SubjectId = subjectId, Operation = operation, Scope = scope, SubjectKind = subjectKind },
            static (client, state, ct) => client.ExplainAsync(state, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<AuthEffectivePermissions> EffectivePermissionsAsync(
        string subjectId,
        LatticeSubjectSelectorKind subjectKind = LatticeSubjectSelectorKind.User,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(subjectId);
        return CallAsync(
            new AuthSubjectRef { SubjectId = subjectId, SubjectKind = subjectKind },
            static (client, state, ct) => client.EffectivePermissionsAsync(state, ct),
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<DirectorySearchResult> SearchDirectoryAsync(DirectorySearchRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        return CallAsync(request, static (client, state, ct) => client.SearchDirectoryAsync(state, ct), null, cancellationToken);
    }

    /// <inheritdoc />
    public Task<DirectoryPrincipalDescriptor?> ResolveDirectoryPrincipalAsync(string principalId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(principalId);
        return CallAsync(
            principalId,
            static async (client, state, ct) =>
                (await client.ResolveDirectoryPrincipalAsync(new AuthPrincipalRef { PrincipalId = state }, ct).ConfigureAwait(false)).Principal,
            null,
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<AccessModelDescriptor> GetAccessModelAsync(CancellationToken cancellationToken = default) =>
        CallAsync(
            (object?)null,
            static (client, _, ct) => client.GetAccessModelAsync(new AuthAccessModelQuery(), ct),
            null,
            cancellationToken);
}
