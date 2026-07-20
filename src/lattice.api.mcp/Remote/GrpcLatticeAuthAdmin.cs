using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Api.Auth.Grpc;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Remote-host adapter that implements the auth-admin control facade
/// (<see cref="ILatticeAuthAdmin"/>) by delegating to the auth-API gRPC client
/// (<see cref="LatticeAuthApiGrpcClient"/>), so the topology-agnostic auth tool
/// module - and the discovery core's permission resolver - work unchanged
/// against a cluster reached over gRPC. Every member has full gRPC parity;
/// scalar arguments are wrapped in their wire request records and unit / wrapper
/// responses are unwrapped to the facade's scalar return shape.
/// </summary>
internal sealed class GrpcLatticeAuthAdmin : ILatticeAuthAdmin
{
    private readonly LatticeAuthApiGrpcClient _client;

    /// <summary>Initialises the adapter over the supplied auth-API gRPC client.</summary>
    public GrpcLatticeAuthAdmin(LatticeAuthApiGrpcClient client)
    {
        ArgumentNullException.ThrowIfNull(client);
        _client = client;
    }

    /// <inheritdoc />
    public async Task UpsertUserAsync(AuthUser user, CancellationToken cancellationToken = default)
        => await _client.UpsertUserAsync(user, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc />
    public async Task<AuthUser?> GetUserAsync(string userId, CancellationToken cancellationToken = default)
    {
        var result = await _client.GetUserAsync(new AuthUserRef { UserId = userId }, cancellationToken).ConfigureAwait(false);
        return result.User;
    }

    /// <inheritdoc />
    public async Task RemoveUserAsync(string userId, CancellationToken cancellationToken = default)
        => await _client.RemoveUserAsync(new AuthUserRef { UserId = userId }, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc />
    public Task<AuthUserPage> ListUsersAsync(AuthPageRequest request, CancellationToken cancellationToken = default)
        => _client.ListUsersAsync(request, cancellationToken);

    /// <inheritdoc />
    public async Task UpsertGroupAsync(AuthGroup group, CancellationToken cancellationToken = default)
        => await _client.UpsertGroupAsync(group, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc />
    public async Task<AuthGroup?> GetGroupAsync(string groupId, CancellationToken cancellationToken = default)
    {
        var result = await _client.GetGroupAsync(new AuthGroupRef { GroupId = groupId }, cancellationToken).ConfigureAwait(false);
        return result.Group;
    }

    /// <inheritdoc />
    public async Task RemoveGroupAsync(string groupId, CancellationToken cancellationToken = default)
        => await _client.RemoveGroupAsync(new AuthGroupRef { GroupId = groupId }, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc />
    public Task<AuthGroupPage> ListGroupsAsync(AuthPageRequest request, CancellationToken cancellationToken = default)
        => _client.ListGroupsAsync(request, cancellationToken);

    /// <inheritdoc />
    public async Task AddMemberAsync(
        string groupId,
        string memberId,
        MembershipMemberKind memberKind = MembershipMemberKind.User,
        CancellationToken cancellationToken = default)
        => await _client.AddMemberAsync(
            new AuthMemberEdge { GroupId = groupId, MemberId = memberId, MemberKind = memberKind },
            cancellationToken).ConfigureAwait(false);

    /// <inheritdoc />
    public async Task RemoveMemberAsync(string groupId, string memberId, CancellationToken cancellationToken = default)
        => await _client.RemoveMemberAsync(
            new AuthMemberEdge { GroupId = groupId, MemberId = memberId },
            cancellationToken).ConfigureAwait(false);

    /// <inheritdoc />
    public async Task<IReadOnlyList<string>> ListGroupMembersAsync(string groupId, CancellationToken cancellationToken = default)
    {
        var result = await _client.ListGroupMembersAsync(new AuthGroupRef { GroupId = groupId }, cancellationToken).ConfigureAwait(false);
        return result.Values;
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<string>> ListSubjectGroupsAsync(string memberId, CancellationToken cancellationToken = default)
    {
        var result = await _client.ListSubjectGroupsAsync(new AuthMemberRef { MemberId = memberId }, cancellationToken).ConfigureAwait(false);
        return result.Values;
    }

    /// <inheritdoc />
    public async Task PutRuleAsync(LatticeAuthorizationRule rule, CancellationToken cancellationToken = default)
        => await _client.PutRuleAsync(new AuthPutRule { Rule = rule }, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc />
    public async Task<LatticeAuthorizationRule?> GetRuleAsync(string treeId, string ruleId, CancellationToken cancellationToken = default)
    {
        var result = await _client.GetRuleAsync(new AuthRuleRef { TreeId = treeId, RuleId = ruleId }, cancellationToken).ConfigureAwait(false);
        return result.Rule;
    }

    /// <inheritdoc />
    public async Task<bool> RemoveRuleAsync(string treeId, string ruleId, CancellationToken cancellationToken = default)
    {
        var result = await _client.RemoveRuleAsync(new AuthRuleRef { TreeId = treeId, RuleId = ruleId }, cancellationToken).ConfigureAwait(false);
        return result.Removed;
    }

    /// <inheritdoc />
    public Task<AuthRulePage> ListRulesAsync(AuthPageRequest request, CancellationToken cancellationToken = default)
        => _client.ListRulesAsync(request, cancellationToken);

    /// <inheritdoc />
    public Task<AuthRulePage> ListRulesForTreeAsync(string treeId, AuthPageRequest request, CancellationToken cancellationToken = default)
        => _client.ListRulesForTreeAsync(new AuthTreeRulesPage { TreeId = treeId, Page = request }, cancellationToken);

    /// <inheritdoc />
    public Task<AuthExplanation> ExplainAsync(
        string subjectId,
        LatticeOperation operation,
        LatticeScope scope,
        LatticeSubjectSelectorKind subjectKind = LatticeSubjectSelectorKind.User,
        CancellationToken cancellationToken = default)
        => _client.ExplainAsync(
            new AuthExplainQuery { SubjectId = subjectId, Operation = operation, Scope = scope, SubjectKind = subjectKind },
            cancellationToken);

    /// <inheritdoc />
    public Task<AuthEffectivePermissions> EffectivePermissionsAsync(
        string subjectId,
        LatticeSubjectSelectorKind subjectKind = LatticeSubjectSelectorKind.User,
        CancellationToken cancellationToken = default)
        => _client.EffectivePermissionsAsync(
            new AuthSubjectRef { SubjectId = subjectId, SubjectKind = subjectKind },
            cancellationToken);

    // ----- Identity directory (issues #1248 / #1249) -----
    // The identity-directory and access-model facade operations ride the gRPC
    // binding added in #1249: scalar arguments are wrapped in their wire request
    // records and the nullable-principal response is unwrapped to the facade's
    // scalar return shape, exactly as the membership and policy members above.

    /// <inheritdoc />
    public Task<DirectorySearchResult> SearchDirectoryAsync(DirectorySearchRequest request, CancellationToken cancellationToken = default)
        => _client.SearchDirectoryAsync(request, cancellationToken);

    /// <inheritdoc />
    public async Task<DirectoryPrincipalDescriptor?> ResolveDirectoryPrincipalAsync(string principalId, CancellationToken cancellationToken = default)
    {
        var result = await _client
            .ResolveDirectoryPrincipalAsync(new AuthPrincipalRef { PrincipalId = principalId }, cancellationToken)
            .ConfigureAwait(false);
        return result.Principal;
    }

    /// <inheritdoc />
    public Task<AccessModelDescriptor> GetAccessModelAsync(CancellationToken cancellationToken = default)
        => _client.GetAccessModelAsync(new AuthAccessModelQuery(), cancellationToken);
}
