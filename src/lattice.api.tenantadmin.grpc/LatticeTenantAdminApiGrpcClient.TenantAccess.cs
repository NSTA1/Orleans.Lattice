using Grpc.Core;
using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Api.TenantAdmin.Grpc;

/// <summary>
/// The delegated tenant access administration half of the client: a direct
/// implementation of <see cref="ILatticeTenantDirectoryAdmin"/> and
/// <see cref="ILatticeTenantPolicyAdmin"/> over the binding's RPCs.
/// </summary>
/// <remarks>
/// Every member validates only what it can check locally (non-null, non-empty
/// arguments); tenant-id syntax, authorization, confinement and caps are decided by
/// the server's facade, whose typed failures reach the caller as an
/// <see cref="RpcException"/>: <see cref="StatusCode.FailedPrecondition"/> for a
/// disabled feature, the reserved default tenant or the last admin entry;
/// <see cref="StatusCode.InvalidArgument"/> for a malformed or confined request;
/// <see cref="StatusCode.ResourceExhausted"/> for a reached cap; and
/// <see cref="StatusCode.PermissionDenied"/> for a denied caller. A server whose
/// host does not register a facade answers its RPCs
/// <see cref="StatusCode.Unimplemented"/>.
/// </remarks>
public sealed partial class LatticeTenantAdminApiGrpcClient : ILatticeTenantDirectoryAdmin, ILatticeTenantPolicyAdmin
{
    /// <inheritdoc />
    public Task<TenantGroupPage> ListGroupsAsync(
        string tenantId, TenantAccessPageRequest page, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        ArgumentNullException.ThrowIfNull(page);
        return UnaryAsync(
            _methods.ListTenantGroups,
            new TenantAdminAccessListRequest { TenantId = tenantId, Page = page },
            cancellationToken);
    }

    /// <inheritdoc />
    public async Task<TenantGroupDescriptor?> GetGroupAsync(
        string tenantId, string groupName, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        ArgumentException.ThrowIfNullOrEmpty(groupName);
        var lookup = await UnaryAsync(
            _methods.GetTenantGroup,
            new TenantAdminGroupRequest { TenantId = tenantId, GroupName = groupName },
            cancellationToken).ConfigureAwait(false);
        return lookup.Group;
    }

    /// <inheritdoc />
    public Task<TenantGroupDescriptor> UpsertGroupAsync(
        string tenantId, TenantGroupDescriptor group, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        ArgumentNullException.ThrowIfNull(group);
        return UnaryAsync(
            _methods.UpsertTenantGroup,
            new TenantAdminGroupUpsertRequest { TenantId = tenantId, Group = group },
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<TenantGroupRemovalResult> RemoveGroupAsync(
        string tenantId, string groupName, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        ArgumentException.ThrowIfNullOrEmpty(groupName);
        return UnaryAsync(
            _methods.RemoveTenantGroup,
            new TenantAdminGroupRequest { TenantId = tenantId, GroupName = groupName },
            cancellationToken);
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<TenantGroupMember>> ListGroupMembersAsync(
        string tenantId, string groupName, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        ArgumentException.ThrowIfNullOrEmpty(groupName);
        var list = await UnaryAsync(
            _methods.ListTenantGroupMembers,
            new TenantAdminGroupRequest { TenantId = tenantId, GroupName = groupName },
            cancellationToken).ConfigureAwait(false);
        return list.Members;
    }

    /// <inheritdoc />
    public Task<TenantMembershipChangeResult> AddGroupMemberAsync(
        string tenantId,
        string groupName,
        string memberId,
        TenantSubjectKind memberKind = TenantSubjectKind.User,
        CancellationToken cancellationToken = default)
        => GroupMemberAsync(_methods.AddTenantGroupMember, tenantId, groupName, memberId, memberKind, cancellationToken);

    /// <inheritdoc />
    public Task<TenantMembershipChangeResult> RemoveGroupMemberAsync(
        string tenantId,
        string groupName,
        string memberId,
        TenantSubjectKind memberKind = TenantSubjectKind.User,
        CancellationToken cancellationToken = default)
        => GroupMemberAsync(_methods.RemoveTenantGroupMember, tenantId, groupName, memberId, memberKind, cancellationToken);

    /// <inheritdoc />
    public Task<TenantMemberPage> ListMembersAsync(
        string tenantId, TenantAccessPageRequest page, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        ArgumentNullException.ThrowIfNull(page);
        return UnaryAsync(
            _methods.ListTenantMembers,
            new TenantAdminAccessListRequest { TenantId = tenantId, Page = page },
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<TenantMembershipChangeResult> AddMemberAsync(
        string tenantId,
        string subjectId,
        TenantSubjectKind subjectKind = TenantSubjectKind.User,
        CancellationToken cancellationToken = default)
        => MemberAsync(_methods.AddTenantMember, tenantId, subjectId, subjectKind, cancellationToken);

    /// <inheritdoc />
    public Task<TenantMembershipChangeResult> RemoveMemberAsync(
        string tenantId,
        string subjectId,
        TenantSubjectKind subjectKind = TenantSubjectKind.User,
        CancellationToken cancellationToken = default)
        => MemberAsync(_methods.RemoveTenantMember, tenantId, subjectId, subjectKind, cancellationToken);

    /// <inheritdoc />
    public Task<TenantSubjectResolution> ResolveSubjectAsync(
        string tenantId,
        string subjectId,
        TenantSubjectKind subjectKind = TenantSubjectKind.User,
        CancellationToken cancellationToken = default)
        => MemberAsync(_methods.ResolveTenantSubject, tenantId, subjectId, subjectKind, cancellationToken);

    /// <inheritdoc />
    public Task<TenantRuleView> PutRuleAsync(
        string tenantId, TenantRuleDraft rule, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        ArgumentNullException.ThrowIfNull(rule);
        return UnaryAsync(
            _methods.PutTenantRule,
            new TenantAdminRulePutRequest { TenantId = tenantId, Rule = rule },
            cancellationToken);
    }

    /// <inheritdoc />
    public async Task<TenantRuleView?> GetRuleAsync(
        string tenantId, string ruleId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        ArgumentException.ThrowIfNullOrEmpty(ruleId);
        var lookup = await UnaryAsync(
            _methods.GetTenantRule,
            new TenantAdminRuleRequest { TenantId = tenantId, RuleId = ruleId },
            cancellationToken).ConfigureAwait(false);
        return lookup.Rule;
    }

    /// <inheritdoc />
    public async Task<bool> RemoveRuleAsync(
        string tenantId, string ruleId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        ArgumentException.ThrowIfNullOrEmpty(ruleId);
        var removal = await UnaryAsync(
            _methods.RemoveTenantRule,
            new TenantAdminRuleRequest { TenantId = tenantId, RuleId = ruleId },
            cancellationToken).ConfigureAwait(false);
        return removal.Removed;
    }

    /// <inheritdoc />
    public Task<TenantRulePage> ListRulesAsync(
        string tenantId, TenantAccessPageRequest page, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        ArgumentNullException.ThrowIfNull(page);
        return UnaryAsync(
            _methods.ListTenantRules,
            new TenantAdminAccessListRequest { TenantId = tenantId, Page = page },
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<TenantExplanation> ExplainAsync(
        string tenantId,
        string subjectId,
        string treeName,
        string? key,
        LatticeOperation operation,
        TenantSubjectKind subjectKind = TenantSubjectKind.User,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        ArgumentException.ThrowIfNullOrEmpty(subjectId);
        ArgumentException.ThrowIfNullOrEmpty(treeName);
        return UnaryAsync(
            _methods.ExplainTenantAccess,
            new TenantAdminExplainRequest
            {
                TenantId = tenantId,
                SubjectId = subjectId,
                TreeName = treeName,
                Key = key,
                Operation = operation,
                SubjectKind = subjectKind,
            },
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<TenantEffectivePermissions> EffectivePermissionsAsync(
        string tenantId,
        string subjectId,
        string? treeName = null,
        TenantSubjectKind subjectKind = TenantSubjectKind.User,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        ArgumentException.ThrowIfNullOrEmpty(subjectId);
        return UnaryAsync(
            _methods.GetTenantEffectivePermissions,
            new TenantAdminEffectivePermissionsRequest
            {
                TenantId = tenantId,
                SubjectId = subjectId,
                TreeName = treeName,
                SubjectKind = subjectKind,
            },
            cancellationToken);
    }

    /// <inheritdoc />
    public Task<TenantAccessPosture> GetPostureAsync(string tenantId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        return UnaryAsync(
            _methods.GetTenantAccessPosture,
            new TenantAdminTenantRequest { TenantId = tenantId },
            cancellationToken);
    }

    /// <summary>Validates and issues one of the two group-member edge mutations, which share a request shape.</summary>
    private Task<TenantMembershipChangeResult> GroupMemberAsync(
        Method<TenantAdminGroupMemberRequest, TenantMembershipChangeResult> method,
        string tenantId,
        string groupName,
        string memberId,
        TenantSubjectKind memberKind,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        ArgumentException.ThrowIfNullOrEmpty(groupName);
        ArgumentException.ThrowIfNullOrEmpty(memberId);
        return UnaryAsync(
            method,
            new TenantAdminGroupMemberRequest
            {
                TenantId = tenantId,
                GroupName = groupName,
                MemberId = memberId,
                MemberKind = memberKind,
            },
            cancellationToken);
    }

    /// <summary>Validates and issues one of the three single-subject RPCs, which share a request shape.</summary>
    private Task<TResponse> MemberAsync<TResponse>(
        Method<TenantAdminMemberRequest, TResponse> method,
        string tenantId,
        string subjectId,
        TenantSubjectKind subjectKind,
        CancellationToken cancellationToken)
        where TResponse : class
    {
        ArgumentException.ThrowIfNullOrEmpty(tenantId);
        ArgumentException.ThrowIfNullOrEmpty(subjectId);
        return UnaryAsync(
            method,
            new TenantAdminMemberRequest { TenantId = tenantId, SubjectId = subjectId, SubjectKind = subjectKind },
            cancellationToken);
    }
}
