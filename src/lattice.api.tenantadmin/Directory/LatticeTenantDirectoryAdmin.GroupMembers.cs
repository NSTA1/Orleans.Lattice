using Orleans.Lattice.Membership;
using Orleans.Lattice.Tenancy;

namespace Orleans.Lattice.Api.TenantAdmin;

internal sealed partial class LatticeTenantDirectoryAdmin
{
    /// <inheritdoc />
    public async Task<IReadOnlyList<TenantGroupMember>> ListGroupMembersAsync(
        string tenantId, string groupName, CancellationToken cancellationToken = default)
    {
        var tenant = TenantAdminArguments.ParseTenantId(tenantId);
        ValidateGroupName(groupName, nameof(groupName));

        await AuthorizeAsync(tenant, "list-group-members", cancellationToken).ConfigureAwait(false);

        var groupId = LatticeTenantGroupId.Compose(tenant, groupName).Value;
        var members = await _store.MembersOfAsync(groupId, cancellationToken).ConfigureAwait(false);
        if (members.Count == 0)
        {
            return Array.Empty<TenantGroupMember>();
        }

        var ordered = new List<string>(members);
        ordered.Sort(OrdinalStringOrder.Comparison);

        var result = new List<TenantGroupMember>(ordered.Count);
        foreach (var memberId in ordered)
        {
            var kind = await ClassifyAsync(memberId, cancellationToken).ConfigureAwait(false);
            result.Add(new TenantGroupMember { MemberId = ToCallerId(memberId, kind), Kind = kind });
        }

        return result;
    }

    /// <inheritdoc />
    public async Task<TenantMembershipChangeResult> AddGroupMemberAsync(
        string tenantId,
        string groupName,
        string memberId,
        TenantSubjectKind memberKind = TenantSubjectKind.User,
        CancellationToken cancellationToken = default)
    {
        var tenant = TenantAdminArguments.ParseTenantId(tenantId);
        ValidateGroupName(groupName, nameof(groupName));
        ValidateEntry(memberId, memberKind, nameof(memberId), nameof(memberKind));

        var record = await AuthorizeAsync(tenant, "add-group-member", cancellationToken).ConfigureAwait(false);

        var groupId = LatticeTenantGroupId.Compose(tenant, groupName).Value;
        var storedMemberId = ToStoredId(tenant, memberId, memberKind, nameof(memberId));
        await RequireGroupAsync(tenant, groupId, groupName, nameof(groupName), cancellationToken).ConfigureAwait(false);

        var members = await _store.MembersOfAsync(groupId, cancellationToken).ConfigureAwait(false);
        if (Contains(members, storedMemberId))
        {
            return GroupChange(tenant, groupName, memberId, memberKind, changed: false);
        }

        if (memberKind == TenantSubjectKind.TenantGroup)
        {
            // D3: a nested group must be one of this tenant's own groups, and exist.
            await RequireGroupAsync(tenant, storedMemberId, memberId, nameof(memberId), cancellationToken)
                .ConfigureAwait(false);
        }
        else
        {
            await ValidateDirectoryPrincipalAsync(memberId, memberKind, nameof(memberId), cancellationToken)
                .ConfigureAwait(false);
        }

        var edgeCap = record.Quotas.EffectiveMaxMembershipEdges;
        var edges = await _store.CountTenantEdgesAsync(tenant, cancellationToken).ConfigureAwait(false);
        TenantAccessCaps.AdmitAddition(
            tenant,
            MembershipConstants.EdgesTree,
            TenantAccessCaps.MembershipEdgesDimension,
            edges,
            edgeCap);

        var membershipKind = memberKind == TenantSubjectKind.User ? MembershipMemberKind.User : MembershipMemberKind.Group;
        try
        {
            await _store.AddMemberAsync(groupId, storedMemberId, membershipKind, cancellationToken).ConfigureAwait(false);
        }
        catch (LatticeTenantGroupNestingException ex)
        {
            throw new TenantAccessConfinementException(
                tenant.Value, TenantAccessConfinementRule.GroupNesting, ex.Message, nameof(memberId));
        }

        // Verify-and-compensate (see the type remarks): concurrent adds can all pass
        // the check above, so re-count and withdraw this edge if over the cap.
        if (await _store.CountTenantEdgesAsync(tenant, cancellationToken).ConfigureAwait(false) is var edgeCount
            && edgeCount > edgeCap)
        {
            await TenantCapCompensation.WithdrawAndRefuseAsync(
                ct => _store.RemoveMemberAsync(groupId, storedMemberId, ct),
                _logger,
                tenant,
                MembershipConstants.EdgesTree,
                TenantAccessCaps.MembershipEdgesDimension,
                edgeCap,
                edgeCount).ConfigureAwait(false);
        }

        return GroupChange(tenant, groupName, memberId, memberKind, changed: true);
    }

    /// <inheritdoc />
    public async Task<TenantMembershipChangeResult> RemoveGroupMemberAsync(
        string tenantId,
        string groupName,
        string memberId,
        TenantSubjectKind memberKind = TenantSubjectKind.User,
        CancellationToken cancellationToken = default)
    {
        var tenant = TenantAdminArguments.ParseTenantId(tenantId);
        ValidateGroupName(groupName, nameof(groupName));
        ValidateEntry(memberId, memberKind, nameof(memberId), nameof(memberKind));

        await AuthorizeAsync(tenant, "remove-group-member", cancellationToken).ConfigureAwait(false);

        var groupId = LatticeTenantGroupId.Compose(tenant, groupName).Value;
        var storedMemberId = ToStoredId(tenant, memberId, memberKind, nameof(memberId));

        var members = await _store.MembersOfAsync(groupId, cancellationToken).ConfigureAwait(false);
        if (!Contains(members, storedMemberId))
        {
            return GroupChange(tenant, groupName, memberId, memberKind, changed: false);
        }

        await _store.RemoveMemberAsync(groupId, storedMemberId, cancellationToken).ConfigureAwait(false);
        return GroupChange(tenant, groupName, memberId, memberKind, changed: true);
    }

    private static bool Contains(IReadOnlyCollection<string> ids, string id)
    {
        foreach (var candidate in ids)
        {
            if (string.Equals(candidate, id, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    private static TenantMembershipChangeResult GroupChange(
        TenantId tenant, string groupName, string memberId, TenantSubjectKind kind, bool changed) =>
        new()
        {
            TenantId = tenant.Value,
            GroupName = groupName,
            SubjectId = memberId,
            SubjectKind = kind,
            Changed = changed,
        };
}
