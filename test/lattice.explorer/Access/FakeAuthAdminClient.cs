using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.Access;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Explorer.Tests.Access;

/// <summary>
/// A hand-rolled <see cref="IAuthAdminClient"/> fake that lets a test script the
/// outcome of each call: a canned value, a translated
/// <see cref="LatticeAuthorizationDeniedException"/> (a server denial), or a
/// residual <see cref="Grpc.Core.RpcException"/> (a transport failure). It also
/// records the last inputs so a test can assert the service forwarded them.
/// </summary>
internal sealed class FakeAuthAdminClient : IAuthAdminClient
{
    public AuthUserPage UsersResult { get; set; } = new();
    public AuthGroupPage GroupsResult { get; set; } = new();
    public AuthRulePage RulesResult { get; set; } = new();
    public AuthUser? UserResult { get; set; }
    public AuthGroup? GroupResult { get; set; }
    public LatticeAuthorizationRule? RuleResult { get; set; }
    public IReadOnlyList<string> MembersResult { get; set; } = Array.Empty<string>();
    public IReadOnlyList<string> SubjectGroupsResult { get; set; } = Array.Empty<string>();
    public bool RemoveRuleResult { get; set; } = true;
    public AuthExplanation? ExplanationResult { get; set; }
    public AuthEffectivePermissions? EffectiveResult { get; set; }

    public DirectorySearchResult DirectorySearchResult { get; set; } = DirectorySearchResult.Unavailable;
    public DirectoryPrincipalDescriptor? DirectoryPrincipalResult { get; set; }
    public AccessModelDescriptor AccessModelResult { get; set; }
        = new() { DirectoryProviderId = "null", DirectoryExplanation = string.Empty };

    public Exception? ListUsersThrows { get; set; }
    public Exception? MutationThrows { get; set; }
    public Exception? ListThrows { get; set; }
    public Exception? ExplainThrows { get; set; }
    public Exception? DirectoryThrows { get; set; }
    public Exception? AccessModelThrows { get; set; }

    public int ListUsersCallCount { get; private set; }
    public AuthPageRequest? LastUsersRequest { get; private set; }
    public AuthUser? LastUpsertedUser { get; private set; }
    public AuthGroup? LastUpsertedGroup { get; private set; }
    public string? LastAddedGroupId { get; private set; }
    public string? LastAddedMemberId { get; private set; }
    public MembershipMemberKind? LastAddedMemberKind { get; private set; }
    public LatticeAuthorizationRule? LastPutRule { get; private set; }
    public string? LastExplainSubjectId { get; private set; }
    public LatticeOperation? LastExplainOperation { get; private set; }
    public LatticeScope? LastExplainScope { get; private set; }
    public LatticeSubjectSelectorKind? LastExplainSubjectKind { get; private set; }
    public DirectorySearchRequest? LastDirectorySearchRequest { get; private set; }
    public string? LastResolvedPrincipalId { get; private set; }
    public int GetAccessModelCallCount { get; private set; }

    public Task<AuthUserPage> ListUsersAsync(AuthPageRequest request, CancellationToken cancellationToken = default)
    {
        ListUsersCallCount++;
        LastUsersRequest = request;
        if (ListUsersThrows is not null)
        {
            throw ListUsersThrows;
        }

        return Task.FromResult(UsersResult);
    }

    public Task<AuthUser?> GetUserAsync(string userId, CancellationToken cancellationToken = default) =>
        Task.FromResult(UserResult);

    public Task UpsertUserAsync(AuthUser user, CancellationToken cancellationToken = default)
    {
        LastUpsertedUser = user;
        if (MutationThrows is not null)
        {
            throw MutationThrows;
        }

        return Task.CompletedTask;
    }

    public Task RemoveUserAsync(string userId, CancellationToken cancellationToken = default)
    {
        if (MutationThrows is not null)
        {
            throw MutationThrows;
        }

        return Task.CompletedTask;
    }

    public Task<AuthGroupPage> ListGroupsAsync(AuthPageRequest request, CancellationToken cancellationToken = default)
    {
        if (ListThrows is not null)
        {
            throw ListThrows;
        }

        return Task.FromResult(GroupsResult);
    }

    public Task<AuthGroup?> GetGroupAsync(string groupId, CancellationToken cancellationToken = default) =>
        Task.FromResult(GroupResult);

    public Task UpsertGroupAsync(AuthGroup group, CancellationToken cancellationToken = default)
    {
        LastUpsertedGroup = group;
        if (MutationThrows is not null)
        {
            throw MutationThrows;
        }

        return Task.CompletedTask;
    }

    public Task RemoveGroupAsync(string groupId, CancellationToken cancellationToken = default)
    {
        if (MutationThrows is not null)
        {
            throw MutationThrows;
        }

        return Task.CompletedTask;
    }

    public Task AddMemberAsync(
        string groupId,
        string memberId,
        MembershipMemberKind memberKind = MembershipMemberKind.User,
        CancellationToken cancellationToken = default)
    {
        LastAddedGroupId = groupId;
        LastAddedMemberId = memberId;
        LastAddedMemberKind = memberKind;
        if (MutationThrows is not null)
        {
            throw MutationThrows;
        }

        return Task.CompletedTask;
    }

    public Task RemoveMemberAsync(string groupId, string memberId, CancellationToken cancellationToken = default)
    {
        if (MutationThrows is not null)
        {
            throw MutationThrows;
        }

        return Task.CompletedTask;
    }

    public Task<IReadOnlyList<string>> ListGroupMembersAsync(string groupId, CancellationToken cancellationToken = default)
    {
        if (ListThrows is not null)
        {
            throw ListThrows;
        }

        return Task.FromResult(MembersResult);
    }

    public Task<IReadOnlyList<string>> ListSubjectGroupsAsync(string memberId, CancellationToken cancellationToken = default)
    {
        if (ListThrows is not null)
        {
            throw ListThrows;
        }

        return Task.FromResult(SubjectGroupsResult);
    }

    public Task PutRuleAsync(LatticeAuthorizationRule rule, CancellationToken cancellationToken = default)
    {
        LastPutRule = rule;
        if (MutationThrows is not null)
        {
            throw MutationThrows;
        }

        return Task.CompletedTask;
    }

    public Task<LatticeAuthorizationRule?> GetRuleAsync(string treeId, string ruleId, CancellationToken cancellationToken = default) =>
        Task.FromResult(RuleResult);

    public Task<bool> RemoveRuleAsync(string treeId, string ruleId, CancellationToken cancellationToken = default)
    {
        if (MutationThrows is not null)
        {
            throw MutationThrows;
        }

        return Task.FromResult(RemoveRuleResult);
    }

    public Task<AuthRulePage> ListRulesAsync(AuthPageRequest request, CancellationToken cancellationToken = default)
    {
        if (ListThrows is not null)
        {
            throw ListThrows;
        }

        return Task.FromResult(RulesResult);
    }

    public Task<AuthRulePage> ListRulesForTreeAsync(string treeId, AuthPageRequest request, CancellationToken cancellationToken = default)
    {
        if (ListThrows is not null)
        {
            throw ListThrows;
        }

        return Task.FromResult(RulesResult);
    }

    public Task<AuthExplanation> ExplainAsync(
        string subjectId,
        LatticeOperation operation,
        LatticeScope scope,
        LatticeSubjectSelectorKind subjectKind = LatticeSubjectSelectorKind.User,
        CancellationToken cancellationToken = default)
    {
        LastExplainSubjectId = subjectId;
        LastExplainOperation = operation;
        LastExplainScope = scope;
        LastExplainSubjectKind = subjectKind;
        if (ExplainThrows is not null)
        {
            throw ExplainThrows;
        }

        return Task.FromResult(ExplanationResult ?? new AuthExplanation
        {
            SubjectId = subjectId,
            Operation = operation,
            Scope = scope,
            Allowed = true,
        });
    }

    public Task<AuthEffectivePermissions> EffectivePermissionsAsync(string subjectId, CancellationToken cancellationToken = default)
    {
        if (ExplainThrows is not null)
        {
            throw ExplainThrows;
        }

        return Task.FromResult(EffectiveResult ?? new AuthEffectivePermissions { SubjectId = subjectId });
    }

    public Task<DirectorySearchResult> SearchDirectoryAsync(DirectorySearchRequest request, CancellationToken cancellationToken = default)
    {
        LastDirectorySearchRequest = request;
        if (DirectoryThrows is not null)
        {
            throw DirectoryThrows;
        }

        return Task.FromResult(DirectorySearchResult);
    }

    public Task<DirectoryPrincipalDescriptor?> ResolveDirectoryPrincipalAsync(string principalId, CancellationToken cancellationToken = default)
    {
        LastResolvedPrincipalId = principalId;
        if (DirectoryThrows is not null)
        {
            throw DirectoryThrows;
        }

        return Task.FromResult(DirectoryPrincipalResult);
    }

    public Task<AccessModelDescriptor> GetAccessModelAsync(CancellationToken cancellationToken = default)
    {
        GetAccessModelCallCount++;
        if (AccessModelThrows is not null)
        {
            throw AccessModelThrows;
        }

        return Task.FromResult(AccessModelResult);
    }
}
