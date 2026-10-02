using Orleans.Lattice.Tenancy;

namespace Orleans.Lattice.Api.TenantAdmin;

internal sealed partial class LatticeTenantDirectoryAdmin
{
    /// <inheritdoc />
    public async Task<TenantMemberPage> ListMembersAsync(
        string tenantId, TenantAccessPageRequest page, CancellationToken cancellationToken = default)
    {
        var tenant = ParseTenant(tenantId);
        ArgumentNullException.ThrowIfNull(page);

        var record = await AuthorizeAsync(tenant, "list-members", cancellationToken).ConfigureAwait(false);

        // The page token is the stored id of the previous page's last entry. It is
        // only ever compared against this tenant's own member set, so it cannot widen
        // a listing to another tenant.
        var after = page.PageToken;
        var size = page.EffectivePageSize;
        var entries = new List<TenantMemberEntry>(Math.Min(size, record.MemberSubjectCount));
        string? next = null;

        foreach (var storedId in record.MemberSubjects)
        {
            // A foreign or malformed tenant-group entry (only reachable by replication
            // or restore) never counts and is never shown.
            if (!TenantAccessEntries.IsAdmissible(storedId, tenant)
                || (after is not null && string.CompareOrdinal(storedId, after) <= 0))
            {
                continue;
            }

            if (entries.Count == size)
            {
                next = LastStoredId(tenant, entries[^1]);
                break;
            }

            var kind = await ClassifyAsync(storedId, cancellationToken).ConfigureAwait(false);
            entries.Add(new TenantMemberEntry { SubjectId = ToCallerId(storedId, kind), Kind = kind });
        }

        return new TenantMemberPage { Entries = entries, NextPageToken = next };
    }

    /// <inheritdoc />
    public async Task<TenantMembershipChangeResult> AddMemberAsync(
        string tenantId,
        string subjectId,
        TenantSubjectKind subjectKind = TenantSubjectKind.User,
        CancellationToken cancellationToken = default)
    {
        var tenant = ParseTenant(tenantId);
        ValidateEntry(subjectId, subjectKind, nameof(subjectId), nameof(subjectKind));

        var record = await AuthorizeAsync(tenant, "add-member", cancellationToken).ConfigureAwait(false);

        var storedId = ToStoredId(tenant, subjectId, subjectKind, nameof(subjectId));
        if (record.HasMemberSubject(storedId))
        {
            return MemberChange(tenant, subjectId, subjectKind, changed: false);
        }

        if (subjectKind == TenantSubjectKind.TenantGroup)
        {
            await RequireGroupAsync(tenant, storedId, subjectId, nameof(subjectId), cancellationToken)
                .ConfigureAwait(false);
        }
        else
        {
            await ValidateDirectoryPrincipalAsync(subjectId, subjectKind, nameof(subjectId), cancellationToken)
                .ConfigureAwait(false);
        }

        var memberCap = record.Quotas.EffectiveMaxMemberSubjects;
        TenantAccessCaps.AdmitAddition(
            tenant,
            TenantTreeNames.RegistryTree,
            TenantAccessCaps.MemberSubjectsDimension,
            record.MemberSubjectCount,
            memberCap);

        record.AddMemberSubject(storedId, _clock.Next(), _writerId);
        var merged = await _registry.PutAsync(record, cancellationToken).ConfigureAwait(false);

        // Verify-and-compensate on the committed join (see the type remarks):
        // concurrent adds from stale reads can each pass the check above, and the
        // registry's merge is the first point where the combined count is visible.
        // Withdraw this entry at a later stamp, which supersedes only this call's add.
        if (merged.MemberSubjectCount > memberCap)
        {
            merged.RemoveMemberSubject(storedId, _clock.Next(), _writerId);
            await _registry.PutAsync(merged, cancellationToken).ConfigureAwait(false);
            ThrowCapExceeded(tenant, TenantTreeNames.RegistryTree, TenantAccessCaps.MemberSubjectsDimension, memberCap);
        }

        return MemberChange(tenant, subjectId, subjectKind, changed: true);
    }

    /// <inheritdoc />
    public async Task<TenantMembershipChangeResult> RemoveMemberAsync(
        string tenantId,
        string subjectId,
        TenantSubjectKind subjectKind = TenantSubjectKind.User,
        CancellationToken cancellationToken = default)
    {
        var tenant = ParseTenant(tenantId);
        ValidateEntry(subjectId, subjectKind, nameof(subjectId), nameof(subjectKind));

        var record = await AuthorizeAsync(tenant, "remove-member", cancellationToken).ConfigureAwait(false);

        var storedId = ToStoredId(tenant, subjectId, subjectKind, nameof(subjectId));
        if (!record.HasMemberSubject(storedId))
        {
            return MemberChange(tenant, subjectId, subjectKind, changed: false);
        }

        record.RemoveMemberSubject(storedId, _clock.Next(), _writerId);
        await _registry.PutAsync(record, cancellationToken).ConfigureAwait(false);

        return MemberChange(tenant, subjectId, subjectKind, changed: true);
    }

    /// <inheritdoc />
    public async Task<TenantSubjectResolution> ResolveSubjectAsync(
        string tenantId,
        string subjectId,
        TenantSubjectKind subjectKind = TenantSubjectKind.User,
        CancellationToken cancellationToken = default)
    {
        var tenant = ParseTenant(tenantId);
        ValidateEntry(subjectId, subjectKind, nameof(subjectId), nameof(subjectKind));

        var record = await AuthorizeAsync(tenant, "resolve-subject", cancellationToken).ConfigureAwait(false);

        var storedId = ToStoredId(tenant, subjectId, subjectKind, nameof(subjectId));

        // A user's groups are its transitive closure; a group stands for itself and
        // every group it is nested in.
        var groups = subjectKind == TenantSubjectKind.User
            ? await _store.GroupsOfAsync(storedId, cancellationToken).ConfigureAwait(false)
            : await _store.ExpandGroupsAsync(new[] { storedId }, cancellationToken).ConfigureAwait(false);
        var closure = new HashSet<string>(groups, StringComparer.Ordinal) { storedId };

        var adminEntries = await MatchingEntriesAsync(tenant, record.AdminSubjects, closure, cancellationToken)
            .ConfigureAwait(false);
        var memberEntries = await MatchingEntriesAsync(tenant, record.MemberSubjects, closure, cancellationToken)
            .ConfigureAwait(false);

        return new TenantSubjectResolution
        {
            TenantId = tenant.Value,
            SubjectId = subjectId,
            SubjectKind = subjectKind,
            IsAdmin = adminEntries.Count > 0,
            IsMember = adminEntries.Count > 0 || memberEntries.Count > 0,
            AdminEntries = adminEntries,
            MemberEntries = memberEntries,
        };
    }

    private async Task<IReadOnlyList<TenantMemberEntry>> MatchingEntriesAsync(
        TenantId tenant, IReadOnlyList<string> storedIds, HashSet<string> closure, CancellationToken cancellationToken)
    {
        List<TenantMemberEntry>? matches = null;
        foreach (var storedId in storedIds)
        {
            if (!closure.Contains(storedId) || !TenantAccessEntries.IsAdmissible(storedId, tenant))
            {
                continue;
            }

            var kind = await ClassifyAsync(storedId, cancellationToken).ConfigureAwait(false);
            (matches ??= []).Add(new TenantMemberEntry { SubjectId = ToCallerId(storedId, kind), Kind = kind });
        }

        return matches is null ? Array.Empty<TenantMemberEntry>() : matches;
    }

    /// <summary>The stored id a listed entry came from, used as the next page's cursor.</summary>
    private static string LastStoredId(TenantId tenant, TenantMemberEntry entry) =>
        entry.Kind == TenantSubjectKind.TenantGroup
            ? LatticeTenantGroupId.Compose(tenant, entry.SubjectId).Value
            : entry.SubjectId;

    private static TenantMembershipChangeResult MemberChange(
        TenantId tenant, string subjectId, TenantSubjectKind kind, bool changed) =>
        new()
        {
            TenantId = tenant.Value,
            SubjectId = subjectId,
            SubjectKind = kind,
            Changed = changed,
        };
}
