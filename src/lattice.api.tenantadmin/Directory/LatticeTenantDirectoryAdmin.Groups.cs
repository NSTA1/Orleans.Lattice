using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;
using Orleans.Lattice.Tenancy;

namespace Orleans.Lattice.Api.TenantAdmin;

internal sealed partial class LatticeTenantDirectoryAdmin
{
    /// <inheritdoc />
    public async Task<TenantGroupPage> ListGroupsAsync(
        string tenantId, TenantAccessPageRequest page, CancellationToken cancellationToken = default)
    {
        var tenant = ParseTenant(tenantId);
        ArgumentNullException.ThrowIfNull(page);

        // The page token is the local name of the previous page's last group, so it
        // can only ever resume inside the tenant the call names.
        if (page.PageToken is { } token)
        {
            ValidateGroupName(token, nameof(page));
        }

        await AuthorizeAsync(tenant, "list-groups", cancellationToken).ConfigureAwait(false);

        var after = page.PageToken is null ? null : LatticeTenantGroupId.Compose(tenant, page.PageToken).Value;
        var slice = await _store
            .ListTenantGroupsAsync(tenant, after, page.EffectivePageSize, cancellationToken)
            .ConfigureAwait(false);

        var entries = new List<TenantGroupDescriptor>(slice.Groups.Count);
        foreach (var group in slice.Groups)
        {
            if (LatticeTenantGroupId.TryParse(group.GroupId, out var id))
            {
                entries.Add(new TenantGroupDescriptor { Name = id.Name, DisplayName = group.DisplayName });
            }
        }

        string? next = null;
        if (slice.ContinuationAfter is { } continuation && LatticeTenantGroupId.TryParse(continuation, out var last))
        {
            next = last.Name;
        }

        return new TenantGroupPage { Entries = entries, NextPageToken = next };
    }

    /// <inheritdoc />
    public async Task<TenantGroupDescriptor?> GetGroupAsync(
        string tenantId, string groupName, CancellationToken cancellationToken = default)
    {
        var tenant = ParseTenant(tenantId);
        ValidateGroupName(groupName, nameof(groupName));

        await AuthorizeAsync(tenant, "get-group", cancellationToken).ConfigureAwait(false);

        var group = await _store
            .GetGroupAsync(LatticeTenantGroupId.Compose(tenant, groupName).Value, cancellationToken)
            .ConfigureAwait(false);

        return group is null ? null : new TenantGroupDescriptor { Name = groupName, DisplayName = group.DisplayName };
    }

    /// <inheritdoc />
    public async Task<TenantGroupDescriptor> UpsertGroupAsync(
        string tenantId, TenantGroupDescriptor group, CancellationToken cancellationToken = default)
    {
        var tenant = ParseTenant(tenantId);
        ArgumentNullException.ThrowIfNull(group);
        ValidateGroupName(group.Name, nameof(group));

        var record = await AuthorizeAsync(tenant, "upsert-group", cancellationToken).ConfigureAwait(false);

        var groupId = LatticeTenantGroupId.Compose(tenant, group.Name).Value;
        var cap = record.Quotas.EffectiveMaxGroups;

        // Only a new group counts against the cap; replacing an existing group's
        // display name never does.
        var isNew = await _store.GetGroupAsync(groupId, cancellationToken).ConfigureAwait(false) is null;
        if (isNew)
        {
            var count = await _store.CountTenantGroupsAsync(tenant, cancellationToken).ConfigureAwait(false);
            TenantAccessCaps.AdmitAddition(
                tenant, MembershipConstants.GroupsTree, TenantAccessCaps.GroupsDimension, count, cap);
        }

        await _store
            .UpsertGroupAsync(new MembershipGroup(groupId, group.DisplayName), cancellationToken)
            .ConfigureAwait(false);

        // Verify-and-compensate (see the type remarks): concurrent creators can all
        // pass the check above, so re-count after the write and withdraw this
        // group if the tenant is now over its cap.
        if (isNew
            && await _store.CountTenantGroupsAsync(tenant, cancellationToken).ConfigureAwait(false) is var groups
            && groups > cap)
        {
            await TenantCapCompensation.WithdrawAndRefuseAsync(
                ct => _store.RemoveGroupCascadeAsync(groupId, ct),
                _logger,
                tenant,
                MembershipConstants.GroupsTree,
                TenantAccessCaps.GroupsDimension,
                cap,
                groups).ConfigureAwait(false);
        }

        return new TenantGroupDescriptor { Name = group.Name, DisplayName = group.DisplayName };
    }

    /// <inheritdoc />
    public async Task<TenantGroupRemovalResult> RemoveGroupAsync(
        string tenantId, string groupName, CancellationToken cancellationToken = default)
    {
        var tenant = ParseTenant(tenantId);
        ValidateGroupName(groupName, nameof(groupName));

        var record = await AuthorizeAsync(tenant, "remove-group", cancellationToken).ConfigureAwait(false);

        var groupId = LatticeTenantGroupId.Compose(tenant, groupName).Value;
        if (await _store.GetGroupAsync(groupId, cancellationToken).ConfigureAwait(false) is null)
        {
            return new TenantGroupRemovalResult { TenantId = tenant.Value, GroupName = groupName };
        }

        // Last-admin protection (D6): a group entry counts as one admin entry, and a
        // tenant may never be left with none. Checked before anything is written.
        var inAdminSet = record.HasAdminSubject(groupId);
        if (inAdminSet && record.AdminSubjectCount <= 1)
        {
            throw new TenantLastAdminSubjectException(tenant.Value, groupId);
        }

        var inMemberSet = record.HasMemberSubject(groupId);

        // The cascade runs while the group record still exists and removes the
        // record last, so an interrupted removal is completed by a re-run (which
        // still finds the group) rather than leaving dangling references behind.
        if (inAdminSet || inMemberSet)
        {
            await RemoveRegistryEntriesAsync(record, groupId, inAdminSet, inMemberSet, cancellationToken)
                .ConfigureAwait(false);
        }

        var removedRules = await _rules
            .RemoveRulesNamingGroupAsync(tenant, groupId, cancellationToken)
            .ConfigureAwait(false);

        var edgesRemoved = await _store.RemoveGroupCascadeAsync(groupId, cancellationToken).ConfigureAwait(false);

        return new TenantGroupRemovalResult
        {
            TenantId = tenant.Value,
            GroupName = groupName,
            Removed = true,
            EdgesRemoved = edgesRemoved,
            RemovedFromMemberSet = inMemberSet,
            RemovedFromAdminSet = inAdminSet,
            RemovedRuleIds = ToLocalRuleIds(tenant, removedRules),
        };
    }

    /// <summary>
    /// Removes a group's member-set and admin-set entries through the registry's
    /// optimistic, CRDT-merging put. Two concurrent removals of different admin
    /// entries can each pass the guard above and together empty the admin set, so
    /// the guard is re-applied to the merged record before it is committed
    /// (<see cref="TenantAdminSetCommit"/>): a removal that would leave no admin
    /// entry is refused with nothing written, never written and then repaired.
    /// </summary>
    private async Task RemoveRegistryEntriesAsync(
        TenantRecord record,
        string groupId,
        bool inAdminSet,
        bool inMemberSet,
        CancellationToken cancellationToken)
    {
        if (inMemberSet)
        {
            record.RemoveMemberSubject(groupId, TenantRemovalStamp.ForMemberEntry(_clock, record, groupId), _writerId);
        }

        if (!inAdminSet)
        {
            await _registry.PutAsync(record, cancellationToken).ConfigureAwait(false);
            return;
        }

        record.RemoveAdminSubject(groupId, TenantRemovalStamp.ForAdminEntry(_clock, record, groupId), _writerId);
        await TenantAdminSetCommit
            .CommitAsync(_registry, record, groupId, _clock, _writerId, cancellationToken)
            .ConfigureAwait(false);
    }

    private static IReadOnlyList<string> ToLocalRuleIds(TenantId tenant, IReadOnlyList<string> ruleIds)
    {
        if (ruleIds.Count == 0)
        {
            return Array.Empty<string>();
        }

        var prefixLength = LatticeTenantRuleIds.Prefix.Length + tenant.Value.Length + 1;
        var local = new List<string>(ruleIds.Count);
        foreach (var ruleId in ruleIds)
        {
            local.Add(ruleId.Length > prefixLength ? ruleId[prefixLength..] : ruleId);
        }

        local.Sort(OrdinalStringOrder.Comparison);
        return local;
    }
}
