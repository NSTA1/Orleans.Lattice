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

        var audit = GroupMemberRequest(LatticeAdminAuditAction.GroupMemberAdd, tenant, groupName, memberId, memberKind);
        var record = await AuthorizeAsync(audit, tenant, "add-group-member", cancellationToken).ConfigureAwait(false);

        return await LatticeAdminAudit
            .RunAsync(
                _audit,
                audit,
                async ct =>
                {
                    var groupId = LatticeTenantGroupId.Compose(tenant, groupName).Value;
                    var storedMemberId = ToStoredId(tenant, memberId, memberKind, nameof(memberId));
                    await RequireGroupAsync(tenant, groupId, groupName, nameof(groupName), ct).ConfigureAwait(false);

                    var members = await _store.MembersOfAsync(groupId, ct).ConfigureAwait(false);
                    if (Contains(members, storedMemberId))
                    {
                        return GroupChange(tenant, groupName, memberId, memberKind, changed: false);
                    }

                    if (memberKind == TenantSubjectKind.TenantGroup)
                    {
                        // D3: a nested group must be one of this tenant's own groups, and exist.
                        await RequireGroupAsync(tenant, storedMemberId, memberId, nameof(memberId), ct)
                            .ConfigureAwait(false);
                    }
                    else
                    {
                        await ValidateDirectoryPrincipalAsync(memberId, memberKind, nameof(memberId), ct)
                            .ConfigureAwait(false);
                    }

                    var edgeCap = record.Quotas.EffectiveMaxMembershipEdges;
                    var edges = await _store.CountTenantEdgesAsync(tenant, ct).ConfigureAwait(false);
                    TenantAccessCaps.AdmitAddition(
                        tenant,
                        MembershipConstants.EdgesTree,
                        TenantAccessCaps.MembershipEdgesDimension,
                        edges,
                        edgeCap);

                    var membershipKind = memberKind == TenantSubjectKind.User ? MembershipMemberKind.User : MembershipMemberKind.Group;
                    try
                    {
                        await _store.AddMemberAsync(groupId, storedMemberId, membershipKind, ct).ConfigureAwait(false);
                    }
                    catch (LatticeTenantGroupNestingException ex)
                    {
                        throw new TenantAccessConfinementException(
                            tenant.Value, TenantAccessConfinementRule.GroupNesting, ex.Message, nameof(memberId));
                    }

                    // Verify-and-compensate (see the type remarks): concurrent adds can all pass
                    // the check above, so re-count and withdraw this edge if over the cap.
                    if (await _store.CountTenantEdgesAsync(tenant, ct).ConfigureAwait(false) is var edgeCount
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
                },
                static r => r.Changed,
                cancellationToken)
            .ConfigureAwait(false);
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

        var audit = GroupMemberRequest(LatticeAdminAuditAction.GroupMemberRemove, tenant, groupName, memberId, memberKind);
        await AuthorizeAsync(audit, tenant, "remove-group-member", cancellationToken).ConfigureAwait(false);

        return await LatticeAdminAudit
            .RunAsync(
                _audit,
                audit,
                async ct =>
                {
                    var groupId = LatticeTenantGroupId.Compose(tenant, groupName).Value;
                    var storedMemberId = ToStoredId(tenant, memberId, memberKind, nameof(memberId));

                    var members = await _store.MembersOfAsync(groupId, ct).ConfigureAwait(false);
                    if (!Contains(members, storedMemberId))
                    {
                        return GroupChange(tenant, groupName, memberId, memberKind, changed: false);
                    }

                    var removed = await _store.RemoveMemberAsync(groupId, storedMemberId, ct).ConfigureAwait(false);
                    return GroupChange(tenant, groupName, memberId, memberKind, changed: removed);
                },
                static r => r.Changed,
                cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// The admin audit request for a tenant group membership change, anchored at the
    /// membership edge the change writes last; the reserved default tenant has no tenant
    /// groups, so its (refused) call carries no anchor.
    /// </summary>
    private static LatticeAdminAuditRequest GroupMemberRequest(
        LatticeAdminAuditAction action, TenantId tenant, string groupName, string memberId, TenantSubjectKind memberKind)
    {
        var groupId = TenantAdminAudit.TenantGroupId(tenant, groupName);
        var storedMemberId = memberKind == TenantSubjectKind.TenantGroup
            ? TenantAdminAudit.TenantGroupId(tenant, memberId)
            : memberId;
        return TenantAdminAudit.Anchored(
            action,
            tenant,
            LatticeAdminAuditTargetKind.GroupMember,
            string.Concat(groupName, "/", memberId),
            groupId is not null && storedMemberId is not null ? MembershipAuditAnchors.Edge(groupId, storedMemberId) : null,
            new Dictionary<string, string>(1, StringComparer.Ordinal) { ["memberKind"] = memberKind.ToString() });
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
