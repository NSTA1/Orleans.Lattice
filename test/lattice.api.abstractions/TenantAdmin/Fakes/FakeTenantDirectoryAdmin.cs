namespace Orleans.Lattice.Api.TenantAdmin.Fakes;

/// <summary>
/// An in-memory <see cref="ILatticeTenantDirectoryAdmin"/> for tests that bind to
/// the delegated tenant-access contract without a cluster (the Explorer's tenant
/// Access pages, transport bindings). It keeps per-tenant groups, direct members,
/// member sets and admin sets, honours the contract's guards (default tenant,
/// feature flag, scripted denial, foreign tenant groups), idempotency, ordinal
/// ordering and paging, and cascades a group removal to edges, the member and
/// admin sets, and - when linked to a <see cref="FakeTenantPolicyAdmin"/> - the
/// tenant-tier rules naming the group.
/// </summary>
/// <param name="gate">The shared guard state, or <c>null</c> for a fresh one.</param>
/// <param name="policy">The policy fake whose rules a group removal cascades to, or <c>null</c>.</param>
internal sealed class FakeTenantDirectoryAdmin(FakeTenantAccessGate? gate = null, FakeTenantPolicyAdmin? policy = null)
    : ILatticeTenantDirectoryAdmin
{
    private readonly Dictionary<string, SortedDictionary<string, TenantGroupDescriptor>> _groups = new(StringComparer.Ordinal);
    private readonly Dictionary<(string Tenant, string Group), List<TenantGroupMember>> _edges = [];
    private readonly Dictionary<string, List<TenantMemberEntry>> _members = new(StringComparer.Ordinal);
    private readonly Dictionary<string, List<TenantMemberEntry>> _admins = new(StringComparer.Ordinal);

    /// <summary>The guard state: feature flag, scripted denial, and the call log.</summary>
    public FakeTenantAccessGate Gate { get; } = gate ?? new FakeTenantAccessGate();

    /// <summary>Seeds an admin-set entry for <paramref name="tenantId"/>, which <see cref="ResolveSubjectAsync"/> and group removal read.</summary>
    /// <param name="tenantId">The tenant.</param>
    /// <param name="entry">The admin-set entry.</param>
    public void SeedAdmin(string tenantId, TenantMemberEntry entry) => Add(_admins, tenantId, entry);

    /// <summary>Returns the admin-set entries of <paramref name="tenantId"/>.</summary>
    /// <param name="tenantId">The tenant.</param>
    /// <returns>The admin-set entries, in ordinal order.</returns>
    public IReadOnlyList<TenantMemberEntry> AdminEntries(string tenantId) =>
        _admins.TryGetValue(tenantId, out var admins) ? [.. admins] : [];

    /// <inheritdoc />
    public Task<TenantGroupPage> ListGroupsAsync(
        string tenantId, TenantAccessPageRequest page, CancellationToken cancellationToken = default)
    {
        Gate.Check(tenantId, nameof(ListGroupsAsync));
        List<TenantGroupDescriptor> groups = _groups.TryGetValue(tenantId, out var map) ? [.. map.Values] : [];
        var (entries, next) = FakeTenantAccessGate.Page(groups, g => g.Name, page);
        return Task.FromResult(new TenantGroupPage { Entries = entries, NextPageToken = next });
    }

    /// <inheritdoc />
    public Task<TenantGroupDescriptor?> GetGroupAsync(
        string tenantId, string groupName, CancellationToken cancellationToken = default)
    {
        Gate.Check(tenantId, nameof(GetGroupAsync));
        ArgumentException.ThrowIfNullOrEmpty(groupName);
        return Task.FromResult(FindGroup(tenantId, groupName));
    }

    /// <inheritdoc />
    public Task<TenantGroupDescriptor> UpsertGroupAsync(
        string tenantId, TenantGroupDescriptor group, CancellationToken cancellationToken = default)
    {
        Gate.Check(tenantId, nameof(UpsertGroupAsync));
        ArgumentNullException.ThrowIfNull(group);
        ArgumentException.ThrowIfNullOrEmpty(group.Name, nameof(group));
        if (!_groups.TryGetValue(tenantId, out var map))
        {
            map = new SortedDictionary<string, TenantGroupDescriptor>(StringComparer.Ordinal);
            _groups[tenantId] = map;
        }

        map[group.Name] = group;
        return Task.FromResult(group);
    }

    /// <inheritdoc />
    public Task<TenantGroupRemovalResult> RemoveGroupAsync(
        string tenantId, string groupName, CancellationToken cancellationToken = default)
    {
        Gate.Check(tenantId, nameof(RemoveGroupAsync));
        ArgumentException.ThrowIfNullOrEmpty(groupName);
        if (FindGroup(tenantId, groupName) is null)
        {
            return Task.FromResult(new TenantGroupRemovalResult { TenantId = tenantId, GroupName = groupName });
        }

        var asEntry = new TenantMemberEntry { SubjectId = groupName, Kind = TenantSubjectKind.TenantGroup };
        _admins.TryGetValue(tenantId, out var admins);
        if (admins is [var onlyAdmin] && onlyAdmin == asEntry)
        {
            throw new TenantLastAdminSubjectException(tenantId, groupName);
        }

        var edgesRemoved = _edges.Remove((tenantId, groupName), out var own) ? own.Count : 0;
        foreach (var (key, groupMembers) in _edges)
        {
            if (key.Tenant == tenantId)
            {
                edgesRemoved += groupMembers.RemoveAll(m => m.Kind == TenantSubjectKind.TenantGroup && m.MemberId == groupName);
            }
        }

        _groups[tenantId].Remove(groupName);
        _members.TryGetValue(tenantId, out var memberSet);
        return Task.FromResult(new TenantGroupRemovalResult
        {
            TenantId = tenantId,
            GroupName = groupName,
            Removed = true,
            EdgesRemoved = edgesRemoved,
            RemovedFromMemberSet = memberSet is not null && memberSet.Remove(asEntry),
            RemovedFromAdminSet = admins is not null && admins.Remove(asEntry),
            RemovedRuleIds = policy?.RemoveRulesNaming(tenantId, groupName) ?? [],
        });
    }

    /// <inheritdoc />
    public Task<IReadOnlyList<TenantGroupMember>> ListGroupMembersAsync(
        string tenantId, string groupName, CancellationToken cancellationToken = default)
    {
        Gate.Check(tenantId, nameof(ListGroupMembersAsync));
        ArgumentException.ThrowIfNullOrEmpty(groupName);
        IReadOnlyList<TenantGroupMember> members = _edges.TryGetValue((tenantId, groupName), out var list) ? [.. list] : [];
        return Task.FromResult(members);
    }

    /// <inheritdoc />
    public Task<TenantMembershipChangeResult> AddGroupMemberAsync(
        string tenantId,
        string groupName,
        string memberId,
        TenantSubjectKind memberKind = TenantSubjectKind.User,
        CancellationToken cancellationToken = default)
    {
        Gate.Check(tenantId, nameof(AddGroupMemberAsync));
        ArgumentException.ThrowIfNullOrEmpty(groupName);
        ArgumentException.ThrowIfNullOrEmpty(memberId);
        FakeTenantAccessGate.RejectForeignTenantGroup(tenantId, memberId, memberKind, nameof(memberId));
        if (FindGroup(tenantId, groupName) is null)
        {
            throw new ArgumentException($"Tenant '{tenantId}' has no group '{groupName}'.", nameof(groupName));
        }

        if (memberKind == TenantSubjectKind.TenantGroup && FindGroup(tenantId, memberId) is null)
        {
            throw new ArgumentException($"Tenant '{tenantId}' has no group '{memberId}'.", nameof(memberId));
        }

        if (!_edges.TryGetValue((tenantId, groupName), out var members))
        {
            members = [];
            _edges[(tenantId, groupName)] = members;
        }

        var member = new TenantGroupMember { MemberId = memberId, Kind = memberKind };
        var changed = !members.Contains(member);
        if (changed)
        {
            members.Add(member);
            members.Sort(static (a, b) => string.CompareOrdinal(a.MemberId, b.MemberId));
        }

        return Task.FromResult(Change(tenantId, groupName, memberId, memberKind, changed));
    }

    /// <inheritdoc />
    public Task<TenantMembershipChangeResult> RemoveGroupMemberAsync(
        string tenantId,
        string groupName,
        string memberId,
        TenantSubjectKind memberKind = TenantSubjectKind.User,
        CancellationToken cancellationToken = default)
    {
        Gate.Check(tenantId, nameof(RemoveGroupMemberAsync));
        ArgumentException.ThrowIfNullOrEmpty(groupName);
        ArgumentException.ThrowIfNullOrEmpty(memberId);
        var changed = _edges.TryGetValue((tenantId, groupName), out var members)
            && members.Remove(new TenantGroupMember { MemberId = memberId, Kind = memberKind });
        return Task.FromResult(Change(tenantId, groupName, memberId, memberKind, changed));
    }

    /// <inheritdoc />
    public Task<TenantMemberPage> ListMembersAsync(
        string tenantId, TenantAccessPageRequest page, CancellationToken cancellationToken = default)
    {
        Gate.Check(tenantId, nameof(ListMembersAsync));
        List<TenantMemberEntry> members = _members.TryGetValue(tenantId, out var list) ? list : [];
        var (entries, next) = FakeTenantAccessGate.Page(members, m => m.SubjectId, page);
        return Task.FromResult(new TenantMemberPage { Entries = entries, NextPageToken = next });
    }

    /// <inheritdoc />
    public Task<TenantMembershipChangeResult> AddMemberAsync(
        string tenantId,
        string subjectId,
        TenantSubjectKind subjectKind = TenantSubjectKind.User,
        CancellationToken cancellationToken = default)
    {
        Gate.Check(tenantId, nameof(AddMemberAsync));
        ArgumentException.ThrowIfNullOrEmpty(subjectId);
        FakeTenantAccessGate.RejectForeignTenantGroup(tenantId, subjectId, subjectKind, nameof(subjectId));
        var changed = Add(_members, tenantId, new TenantMemberEntry { SubjectId = subjectId, Kind = subjectKind });
        return Task.FromResult(Change(tenantId, null, subjectId, subjectKind, changed));
    }

    /// <inheritdoc />
    public Task<TenantMembershipChangeResult> RemoveMemberAsync(
        string tenantId,
        string subjectId,
        TenantSubjectKind subjectKind = TenantSubjectKind.User,
        CancellationToken cancellationToken = default)
    {
        Gate.Check(tenantId, nameof(RemoveMemberAsync));
        ArgumentException.ThrowIfNullOrEmpty(subjectId);
        var changed = _members.TryGetValue(tenantId, out var members)
            && members.Remove(new TenantMemberEntry { SubjectId = subjectId, Kind = subjectKind });
        return Task.FromResult(Change(tenantId, null, subjectId, subjectKind, changed));
    }

    /// <inheritdoc />
    public Task<TenantSubjectResolution> ResolveSubjectAsync(
        string tenantId,
        string subjectId,
        TenantSubjectKind subjectKind = TenantSubjectKind.User,
        CancellationToken cancellationToken = default)
    {
        Gate.Check(tenantId, nameof(ResolveSubjectAsync));
        ArgumentException.ThrowIfNullOrEmpty(subjectId);
        var adminEntries = Matching(_admins, tenantId, subjectId, subjectKind);
        var memberEntries = Matching(_members, tenantId, subjectId, subjectKind);
        return Task.FromResult(new TenantSubjectResolution
        {
            TenantId = tenantId,
            SubjectId = subjectId,
            SubjectKind = subjectKind,
            IsAdmin = adminEntries.Count > 0,
            IsMember = adminEntries.Count > 0 || memberEntries.Count > 0,
            AdminEntries = adminEntries,
            MemberEntries = memberEntries,
        });
    }

    private TenantGroupDescriptor? FindGroup(string tenantId, string groupName) =>
        _groups.TryGetValue(tenantId, out var map) && map.TryGetValue(groupName, out var group) ? group : null;

    private List<TenantMemberEntry> Matching(
        Dictionary<string, List<TenantMemberEntry>> sets, string tenantId, string subjectId, TenantSubjectKind kind)
    {
        var matched = new List<TenantMemberEntry>();
        if (!sets.TryGetValue(tenantId, out var entries))
        {
            return matched;
        }

        foreach (var entry in entries)
        {
            var direct = entry.SubjectId == subjectId && entry.Kind == kind;
            var viaGroup = entry.Kind == TenantSubjectKind.TenantGroup
                && Contains(tenantId, entry.SubjectId, subjectId, kind, depth: 0);
            if (direct || viaGroup)
            {
                matched.Add(entry);
            }
        }

        return matched;
    }

    // Walks the tenant's own nested groups; the depth bound stops a cyclic fixture from recursing forever.
    private bool Contains(string tenantId, string groupName, string subjectId, TenantSubjectKind kind, int depth)
    {
        if (depth > 32 || !_edges.TryGetValue((tenantId, groupName), out var members))
        {
            return false;
        }

        foreach (var member in members)
        {
            if ((member.MemberId == subjectId && member.Kind == kind)
                || (member.Kind == TenantSubjectKind.TenantGroup
                    && Contains(tenantId, member.MemberId, subjectId, kind, depth + 1)))
            {
                return true;
            }
        }

        return false;
    }

    private static bool Add(Dictionary<string, List<TenantMemberEntry>> sets, string tenantId, TenantMemberEntry entry)
    {
        if (!sets.TryGetValue(tenantId, out var entries))
        {
            entries = [];
            sets[tenantId] = entries;
        }

        if (entries.Contains(entry))
        {
            return false;
        }

        entries.Add(entry);
        entries.Sort(static (a, b) => string.CompareOrdinal(a.SubjectId, b.SubjectId));
        return true;
    }

    private static TenantMembershipChangeResult Change(
        string tenantId, string? groupName, string subjectId, TenantSubjectKind kind, bool changed) => new()
        {
            TenantId = tenantId,
            GroupName = groupName,
            SubjectId = subjectId,
            SubjectKind = kind,
            Changed = changed,
        };
}
