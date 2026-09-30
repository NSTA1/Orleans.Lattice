using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.UI.Areas.Access;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Access;

/// <summary>
/// An in-memory <see cref="ILatticeAuthAdmin"/>: groups, direct members, rules,
/// a directory and an access model a test sets up directly. A test makes any
/// call fail with <see cref="Fail"/>, or holds it open with <see cref="Hold"/>
/// (a <see cref="TaskCompletionSource"/> the test completes), so loading and
/// error states are reached without timers.
/// </summary>
internal sealed class FakeAuthAdmin : ILatticeAuthAdmin
{
    private readonly Dictionary<string, Exception> _failures = new(StringComparer.Ordinal);
    private readonly Dictionary<string, TaskCompletionSource> _holds = new(StringComparer.Ordinal);

    /// <summary>The groups, by id.</summary>
    public SortedDictionary<string, AuthGroup> Groups { get; } = new(StringComparer.Ordinal);

    /// <summary>The direct members of each group.</summary>
    public Dictionary<string, SortedSet<string>> Members { get; } = new(StringComparer.Ordinal);

    /// <summary>The kind each member was added as.</summary>
    public Dictionary<(string Group, string Member), MembershipMemberKind> MemberKinds { get; } = [];

    /// <summary>The rules, in store order.</summary>
    public List<LatticeAuthorizationRule> Rules { get; } = [];

    /// <summary>The identity directory's principals.</summary>
    public List<DirectoryPrincipalDescriptor> Directory { get; } = [];

    /// <summary>The access model; the directory is available when its flag is set.</summary>
    public AccessModelDescriptor Model { get; set; } = new()
    {
        AuthenticationMode = AccessAuthenticationMode.Claims,
        RulesEnforced = true,
        DirectoryAvailable = false,
        DirectoryProviderId = "none",
        DirectoryExplanation = string.Empty,
        LocalMembershipEffective = true,
    };

    /// <summary>The page size the list calls use, whatever the caller asked for; 0 honours the request.</summary>
    public int ForcedPageSize { get; set; }

    /// <summary>What <see cref="ExplainAsync"/> answers; by default, denied with the subject's rules matched.</summary>
    public Func<string, LatticeOperation, LatticeScope, LatticeSubjectSelectorKind, AuthExplanation>? Explain { get; set; }

    /// <summary>Every call made, by method name.</summary>
    public List<string> Calls { get; } = [];

    /// <summary>Makes every later call to <paramref name="method"/> throw <paramref name="exception"/>.</summary>
    public void Fail(string method, Exception exception) => _failures[method] = exception;

    /// <summary>Lets <paramref name="method"/> succeed again.</summary>
    public void Heal(string method) => _failures.Remove(method);

    /// <summary>Holds every later call to <paramref name="method"/> until the returned source completes.</summary>
    public TaskCompletionSource Hold(string method)
    {
        var hold = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _holds[method] = hold;
        return hold;
    }

    /// <summary>Adds a group with optional members.</summary>
    public FakeAuthAdmin WithGroup(string groupId, string? displayName = null, params string[] members)
    {
        Groups[groupId] = new AuthGroup { GroupId = groupId, DisplayName = displayName };
        var set = Members.TryGetValue(groupId, out var existing) ? existing : Members[groupId] = new SortedSet<string>(StringComparer.Ordinal);
        foreach (var member in members)
        {
            set.Add(member);
        }

        return this;
    }

    /// <summary>Adds a rule.</summary>
    public FakeAuthAdmin WithRule(LatticeAuthorizationRule rule)
    {
        Rules.Add(rule);
        return this;
    }

    /// <summary>Adds a directory principal and turns the directory on.</summary>
    public FakeAuthAdmin WithPrincipal(string id, string displayName, DirectoryPrincipalKind kind)
    {
        Directory.Add(new DirectoryPrincipalDescriptor { Id = id, DisplayName = displayName, Kind = kind });
        Model = Model with { DirectoryAvailable = true, DirectoryProviderId = "entra", DirectoryExplanation = "An object id from the directory." };
        return this;
    }

    /// <inheritdoc />
    public async Task UpsertGroupAsync(AuthGroup group, CancellationToken cancellationToken = default)
    {
        await EnterAsync(nameof(UpsertGroupAsync));
        Groups[group.GroupId] = group;
        if (!Members.ContainsKey(group.GroupId))
        {
            Members[group.GroupId] = new SortedSet<string>(StringComparer.Ordinal);
        }
    }

    /// <inheritdoc />
    public async Task<AuthGroup?> GetGroupAsync(string groupId, CancellationToken cancellationToken = default)
    {
        await EnterAsync(nameof(GetGroupAsync));
        return Groups.GetValueOrDefault(groupId);
    }

    /// <inheritdoc />
    public async Task RemoveGroupAsync(string groupId, CancellationToken cancellationToken = default)
    {
        await EnterAsync(nameof(RemoveGroupAsync));
        Groups.Remove(groupId);
        Members.Remove(groupId);
    }

    /// <inheritdoc />
    public async Task<AuthGroupPage> ListGroupsAsync(AuthPageRequest request, CancellationToken cancellationToken = default)
    {
        await EnterAsync(nameof(ListGroupsAsync));
        var (entries, next) = Page(Groups.Values.ToList(), request);
        return new AuthGroupPage { Entries = entries, NextPageToken = next };
    }

    /// <inheritdoc />
    public async Task AddMemberAsync(string groupId, string memberId, MembershipMemberKind memberKind = MembershipMemberKind.User, CancellationToken cancellationToken = default)
    {
        await EnterAsync(nameof(AddMemberAsync));
        if (!Members.TryGetValue(groupId, out var set))
        {
            set = Members[groupId] = new SortedSet<string>(StringComparer.Ordinal);
        }

        set.Add(memberId);
        MemberKinds[(groupId, memberId)] = memberKind;
    }

    /// <inheritdoc />
    public async Task RemoveMemberAsync(string groupId, string memberId, CancellationToken cancellationToken = default)
    {
        await EnterAsync(nameof(RemoveMemberAsync));
        if (Members.TryGetValue(groupId, out var set))
        {
            set.Remove(memberId);
        }
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<string>> ListGroupMembersAsync(string groupId, CancellationToken cancellationToken = default)
    {
        await EnterAsync(nameof(ListGroupMembersAsync));
        return Members.TryGetValue(groupId, out var set) ? [.. set] : [];
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<string>> ListSubjectGroupsAsync(string memberId, CancellationToken cancellationToken = default)
    {
        await EnterAsync(nameof(ListSubjectGroupsAsync));
        return GroupsOf(memberId);
    }

    /// <inheritdoc />
    public async Task PutRuleAsync(LatticeAuthorizationRule rule, CancellationToken cancellationToken = default)
    {
        await EnterAsync(nameof(PutRuleAsync));
        Rules.RemoveAll(existing => existing.RuleId == rule.RuleId && existing.Scope.TreeId == rule.Scope.TreeId);
        Rules.Add(rule);
    }

    /// <inheritdoc />
    public async Task<LatticeAuthorizationRule?> GetRuleAsync(string treeId, string ruleId, CancellationToken cancellationToken = default)
    {
        await EnterAsync(nameof(GetRuleAsync));
        return Rules.FirstOrDefault(rule => rule.Scope.TreeId == treeId && rule.RuleId == ruleId);
    }

    /// <inheritdoc />
    public async Task<bool> RemoveRuleAsync(string treeId, string ruleId, CancellationToken cancellationToken = default)
    {
        await EnterAsync(nameof(RemoveRuleAsync));
        return Rules.RemoveAll(rule => rule.Scope.TreeId == treeId && rule.RuleId == ruleId) > 0;
    }

    /// <inheritdoc />
    public async Task<AuthRulePage> ListRulesAsync(AuthPageRequest request, CancellationToken cancellationToken = default)
    {
        await EnterAsync(nameof(ListRulesAsync));
        RuleRequests.Add(request);
        if (request.ActiveTenantOnly && NarrowsTo is { } tenant)
        {
            var owned = Rules.Where(rule => AccessCatalog.IsOwnedBy(rule, TenantId.Parse(tenant))).ToList();
            var (narrowed, after) = Page(owned, request);
            return new AuthRulePage { Entries = narrowed, NextPageToken = after, Tenant = tenant };
        }

        var (entries, next) = Page(Rules, request);
        return new AuthRulePage { Entries = entries, NextPageToken = next };
    }

    /// <summary>Every rule-listing request, in order.</summary>
    public List<AuthPageRequest> RuleRequests { get; } = [];

    /// <summary>
    /// The tenant a narrowed rule listing is narrowed to, as a current cluster
    /// does; <see langword="null"/> (the default) ignores the narrowing, as a
    /// cluster that predates it does.
    /// </summary>
    public string? NarrowsTo { get; set; }

    /// <inheritdoc />
    public async Task<AuthRulePage> ListRulesForTreeAsync(string treeId, AuthPageRequest request, CancellationToken cancellationToken = default)
    {
        await EnterAsync(nameof(ListRulesForTreeAsync));
        var (entries, next) = Page(Rules.Where(rule => rule.Scope.TreeId == treeId || rule.Scope.TreeId == LatticeScope.ClusterWideTreeId).ToList(), request);
        return new AuthRulePage { Entries = entries, NextPageToken = next };
    }

    /// <inheritdoc />
    public async Task<AuthExplanation> ExplainAsync(string subjectId, LatticeOperation operation, LatticeScope scope, LatticeSubjectSelectorKind subjectKind = LatticeSubjectSelectorKind.User, CancellationToken cancellationToken = default)
    {
        await EnterAsync(nameof(ExplainAsync));
        return Explain?.Invoke(subjectId, operation, scope, subjectKind) ?? new AuthExplanation
        {
            SubjectId = subjectId,
            GroupIds = GroupsOf(subjectId),
            Operation = operation,
            Scope = scope,
            Allowed = false,
            Reason = "No rule grants it.",
            DefaultEffect = LatticeEffect.Deny,
            MatchedRules = RulesFor(subjectId, subjectKind),
        };
    }

    /// <inheritdoc />
    public async Task<AuthEffectivePermissions> EffectivePermissionsAsync(string subjectId, LatticeSubjectSelectorKind subjectKind = LatticeSubjectSelectorKind.User, CancellationToken cancellationToken = default)
    {
        await EnterAsync(nameof(EffectivePermissionsAsync));
        return new AuthEffectivePermissions
        {
            SubjectId = subjectId,
            GroupIds = GroupsOf(subjectId),
            Rules = RulesFor(subjectId, subjectKind),
        };
    }

    /// <inheritdoc />
    public async Task<DirectorySearchResult> SearchDirectoryAsync(DirectorySearchRequest request, CancellationToken cancellationToken = default)
    {
        await EnterAsync(nameof(SearchDirectoryAsync));
        if (!Model.DirectoryAvailable)
        {
            return DirectorySearchResult.Unavailable;
        }

        var matches = Directory
            .Where(principal => request.Kind is null || principal.Kind == request.Kind)
            .Where(principal => principal.Id.Contains(request.Term, StringComparison.OrdinalIgnoreCase)
                || principal.DisplayName.Contains(request.Term, StringComparison.OrdinalIgnoreCase))
            .ToList();
        var offset = request.ContinuationToken is null ? 0 : int.Parse(request.ContinuationToken, System.Globalization.CultureInfo.InvariantCulture);
        var size = request.PageSize < 1 ? 20 : request.PageSize;
        var page = matches.Skip(offset).Take(size).ToList();
        var next = offset + size < matches.Count ? (offset + size).ToString(System.Globalization.CultureInfo.InvariantCulture) : null;
        return new DirectorySearchResult { Principals = page, ContinuationToken = next, Available = true };
    }

    /// <inheritdoc />
    public async Task<DirectoryPrincipalDescriptor?> ResolveDirectoryPrincipalAsync(string principalId, CancellationToken cancellationToken = default)
    {
        await EnterAsync(nameof(ResolveDirectoryPrincipalAsync));
        return Model.DirectoryAvailable ? Directory.FirstOrDefault(principal => principal.Id == principalId) : null;
    }

    /// <inheritdoc />
    public async Task<AccessModelDescriptor> GetAccessModelAsync(CancellationToken cancellationToken = default)
    {
        await EnterAsync(nameof(GetAccessModelAsync));
        return Model;
    }

    private IReadOnlyList<string> GroupsOf(string memberId) =>
        [.. Members.Where(pair => pair.Value.Contains(memberId)).Select(pair => pair.Key).Order(StringComparer.Ordinal)];

    private IReadOnlyList<LatticeAuthorizationRule> RulesFor(string subjectId, LatticeSubjectSelectorKind kind)
    {
        var groups = GroupsOf(subjectId).ToHashSet(StringComparer.Ordinal);
        return
        [
            .. Rules.Where(rule =>
                (rule.Subject.Kind == kind && rule.Subject.Id == subjectId)
                || (rule.Subject.Kind == LatticeSubjectSelectorKind.Group && groups.Contains(rule.Subject.Id))),
        ];
    }

    private (IReadOnlyList<T> Entries, string? Next) Page<T>(IReadOnlyList<T> all, AuthPageRequest request)
    {
        var size = ForcedPageSize > 0 ? ForcedPageSize : request.EffectivePageSize;
        var offset = request.PageToken is null ? 0 : int.Parse(request.PageToken, System.Globalization.CultureInfo.InvariantCulture);
        var entries = all.Skip(offset).Take(size).ToList();
        var next = offset + size < all.Count ? (offset + size).ToString(System.Globalization.CultureInfo.InvariantCulture) : null;
        return (entries, next);
    }

    private async Task EnterAsync(string method)
    {
        Calls.Add(method);
        if (_holds.TryGetValue(method, out var hold))
        {
            await hold.Task;
        }

        if (_failures.TryGetValue(method, out var failure))
        {
            throw failure;
        }
    }
}
