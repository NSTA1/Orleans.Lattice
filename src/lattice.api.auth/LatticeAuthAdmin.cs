using System.Diagnostics;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;

namespace Orleans.Lattice.Api.Auth;

/// <summary>
/// Default <see cref="ILatticeAuthAdmin"/> implementation. Registered as a silo
/// singleton by <c>AddLatticeAuthApi</c>. It administers the membership
/// directory (<see cref="ILatticeMembershipDirectory"/>) and the authorization
/// policy store (<see cref="ILatticeAuthorizationPolicyStore"/>), and introspects
/// policy through the same access gate the data plane consults.
/// </summary>
/// <remarks>
/// <para>
/// <b>Administrator gate.</b> Every operation begins with
/// <see cref="AuthorizeAdminAsync"/>, which routes through
/// <see cref="LatticeAccessGateEnforcement.EnforceWholeTreeAsync"/> - the exact
/// enforcement primitive the in-cluster mutation choke points use - requiring an
/// <see cref="LatticeOperation.Admin"/> verdict on the authorization policy tree.
/// A bootstrap administrator (the root of trust) satisfies it; every other
/// subject is default-denied fail-closed. Only after that check passes does the
/// facade touch the directory or the store.
/// </para>
/// <para>
/// <b>System-origin underlay.</b> Once the administrator check has authorized the
/// caller, the underlying membership and policy tree operations run under
/// <see cref="LatticeAccessGateContext.EnterSystemOrigin"/> so the facade's
/// administrator check is the single, authoritative enforcement point and the
/// directory's own dogfooded reads cannot re-enter the gate. This mirrors the way
/// the policy store already runs its own tree operations system-origin while
/// delegating "who may edit policy" to a higher layer - here, this facade.
/// </para>
/// </remarks>
internal sealed class LatticeAuthAdmin(
    ILatticeAuthorizationPolicyStore store,
    ILatticeMembershipDirectory directory,
    ILatticeAccessGate gate,
    ILatticeMembershipContext membership,
    ILatticeIdentityDirectory identityDirectory,
    IEnumerable<ILatticeCredentialAuthenticator> authenticators,
    IOptions<LatticeApiAuthOptions> apiOptions,
    IOptionsMonitor<LatticeAuthOptions> authOptions,
    IOptionsMonitor<LatticeMembershipOptions> membershipOptions,
    IOptionsMonitor<LatticeIdentityDirectoryOptions> identityDirectoryOptions) : ILatticeAuthAdmin
{
    private const string RuleKeySeparator = "\u001f";

    /// <summary>
    /// The control-plane tree whose <see cref="LatticeOperation.Admin"/>
    /// capability defines "authorization administrator": the reserved policy
    /// tree. Because a rule can never be scoped at the reserved
    /// <c>sys-auth-*</c> namespace, only a bootstrap administrator satisfies this
    /// under the recommended deny-by-default posture.
    /// </summary>
    private static readonly string AdminScopeTreeId = LatticeAuthReservedTrees.PolicyTreeId;

    private readonly ILatticeAuthorizationPolicyStore _store = store ?? throw new ArgumentNullException(nameof(store));
    private readonly ILatticeMembershipDirectory _directory = directory ?? throw new ArgumentNullException(nameof(directory));
    private readonly ILatticeAccessGate _gate = gate ?? throw new ArgumentNullException(nameof(gate));
    private readonly ILatticeMembershipContext _membership = membership ?? throw new ArgumentNullException(nameof(membership));
    private readonly ILatticeIdentityDirectory _identityDirectory = identityDirectory ?? throw new ArgumentNullException(nameof(identityDirectory));
    private readonly ILatticeCredentialAuthenticator[] _authenticators = (authenticators ?? throw new ArgumentNullException(nameof(authenticators))).ToArray();
    private readonly LatticeApiAuthOptions _apiOptions = (apiOptions ?? throw new ArgumentNullException(nameof(apiOptions))).Value;
    private readonly IOptionsMonitor<LatticeAuthOptions> _authOptions = authOptions ?? throw new ArgumentNullException(nameof(authOptions));
    private readonly IOptionsMonitor<LatticeMembershipOptions> _membershipOptions = membershipOptions ?? throw new ArgumentNullException(nameof(membershipOptions));
    private readonly IOptionsMonitor<LatticeIdentityDirectoryOptions> _identityDirectoryOptions = identityDirectoryOptions ?? throw new ArgumentNullException(nameof(identityDirectoryOptions));

    // ----- Membership administration -----

    /// <inheritdoc />
    public async Task UpsertGroupAsync(AuthGroup group, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(group);
        ArgumentException.ThrowIfNullOrEmpty(group.GroupId);
        await AuthorizeAdminAsync(cancellationToken).ConfigureAwait(false);

        await ValidateDirectoryPrincipalAsync(
            group.GroupId, DirectoryPrincipalKind.Group, nameof(group), cancellationToken).ConfigureAwait(false);

        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await _directory
                .UpsertGroupAsync(new MembershipGroup(group.GroupId, group.DisplayName), cancellationToken)
                .ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async Task<AuthGroup?> GetGroupAsync(string groupId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(groupId);
        await AuthorizeAdminAsync(cancellationToken).ConfigureAwait(false);

        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            var group = await _directory.GetGroupAsync(groupId, cancellationToken).ConfigureAwait(false);
            return group is null ? null : ToAuthGroup(group);
        }
    }

    /// <inheritdoc />
    public async Task RemoveGroupAsync(string groupId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(groupId);
        await AuthorizeAdminAsync(cancellationToken).ConfigureAwait(false);

        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await _directory.RemoveGroupAsync(groupId, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async Task<AuthGroupPage> ListGroupsAsync(AuthPageRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        await AuthorizeAdminAsync(cancellationToken).ConfigureAwait(false);

        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            var (page, next) = await PageAsync(
                _directory.ListGroupsAsync(cancellationToken),
                request.PageToken,
                request.EffectivePageSize,
                static g => g.GroupId,
                cancellationToken).ConfigureAwait(false);

            var entries = new List<AuthGroup>(page.Count);
            foreach (var group in page)
            {
                entries.Add(ToAuthGroup(group));
            }

            return new AuthGroupPage { Entries = entries, NextPageToken = next };
        }
    }

    /// <inheritdoc />
    public async Task AddMemberAsync(
        string groupId,
        string memberId,
        MembershipMemberKind memberKind = MembershipMemberKind.User,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(groupId);
        ArgumentException.ThrowIfNullOrEmpty(memberId);
        await AuthorizeAdminAsync(cancellationToken).ConfigureAwait(false);

        await ValidateDirectoryPrincipalAsync(
            memberId, ToDirectoryPrincipalKind(memberKind), nameof(memberId), cancellationToken).ConfigureAwait(false);

        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await _directory.AddMemberAsync(groupId, memberId, memberKind, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async Task RemoveMemberAsync(string groupId, string memberId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(groupId);
        ArgumentException.ThrowIfNullOrEmpty(memberId);
        await AuthorizeAdminAsync(cancellationToken).ConfigureAwait(false);

        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await _directory.RemoveMemberAsync(groupId, memberId, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<string>> ListGroupMembersAsync(string groupId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(groupId);
        await AuthorizeAdminAsync(cancellationToken).ConfigureAwait(false);

        IReadOnlyCollection<string> members;
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            members = await _directory.MembersOfAsync(groupId, cancellationToken).ConfigureAwait(false);
        }

        return Sorted(members);
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<string>> ListSubjectGroupsAsync(string memberId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(memberId);
        await AuthorizeAdminAsync(cancellationToken).ConfigureAwait(false);

        IReadOnlyCollection<string> groups;
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            groups = await _directory.GroupsOfAsync(memberId, cancellationToken).ConfigureAwait(false);
        }

        return Sorted(groups);
    }

    // ----- Policy administration -----

    /// <inheritdoc />
    public async Task PutRuleAsync(LatticeAuthorizationRule rule, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(rule);
        await AuthorizeAdminAsync(cancellationToken).ConfigureAwait(false);

        // The store runs its own write system-origin (it edits the reserved policy
        // tree that feeds the gate); the administrator check above is the caller
        // authorization the store deliberately delegates upward.
        await _store.PutRuleAsync(rule, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<LatticeAuthorizationRule?> GetRuleAsync(string treeId, string ruleId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentException.ThrowIfNullOrEmpty(ruleId);
        await AuthorizeAdminAsync(cancellationToken).ConfigureAwait(false);

        return await _store.GetRuleAsync(treeId, ruleId, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<bool> RemoveRuleAsync(string treeId, string ruleId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentException.ThrowIfNullOrEmpty(ruleId);
        await AuthorizeAdminAsync(cancellationToken).ConfigureAwait(false);

        return await _store.RemoveRuleAsync(treeId, ruleId, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<AuthRulePage> ListRulesAsync(AuthPageRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        await AuthorizeAdminAsync(cancellationToken).ConfigureAwait(false);

        // The store scans the policy tree system-origin internally.
        var (page, next) = await PageAsync(
            _store.ListRulesAsync(cancellationToken),
            request.PageToken,
            request.EffectivePageSize,
            RuleCatalogKey,
            cancellationToken).ConfigureAwait(false);

        return new AuthRulePage { Entries = page, NextPageToken = next };
    }

    /// <inheritdoc />
    public async Task<AuthRulePage> ListRulesForTreeAsync(string treeId, AuthPageRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ArgumentNullException.ThrowIfNull(request);
        await AuthorizeAdminAsync(cancellationToken).ConfigureAwait(false);

        var (page, next) = await PageAsync(
            _store.ListRulesForTreeAsync(treeId, cancellationToken),
            request.PageToken,
            request.EffectivePageSize,
            static r => r.RuleId,
            cancellationToken).ConfigureAwait(false);

        return new AuthRulePage { Entries = page, NextPageToken = next };
    }

    // ----- Policy introspection -----

    /// <inheritdoc />
    public async Task<AuthExplanation> ExplainAsync(
        string subjectId,
        LatticeOperation operation,
        LatticeScope scope,
        LatticeSubjectSelectorKind subjectKind = LatticeSubjectSelectorKind.User,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(subjectId);
        ArgumentNullException.ThrowIfNull(scope);
        await AuthorizeAdminAsync(cancellationToken).ConfigureAwait(false);

        var subject = await ResolveNamedSubjectAsync(subjectId, subjectKind, cancellationToken).ConfigureAwait(false);

        // Evaluate against the SAME access gate the data plane consults, so the
        // explained verdict is identical to the enforced decision by construction.
        var (key, rangeStart, rangeEnd) = TranslateScope(scope);
        var request = new LatticeAccessRequest(scope.TreeId, operation, subject, key, rangeStart, rangeEnd);
        var decision = await _gate.AuthorizeAsync(in request, cancellationToken).ConfigureAwait(false);

        var matched = await CollectMatchedRulesAsync(subject, operation, scope, cancellationToken).ConfigureAwait(false);

        return new AuthExplanation
        {
            SubjectId = subjectId,
            GroupIds = Sorted(subject.GroupIds),
            Operation = operation,
            Scope = scope,
            Allowed = decision.Allowed,
            Filtered = decision.KeyFilter is not null,
            Reason = decision.Reason,
            DefaultEffect = _authOptions.CurrentValue.DefaultEffect,
            MatchedRules = matched,
        };
    }

    /// <inheritdoc />
    public async Task<AuthEffectivePermissions> EffectivePermissionsAsync(
        string subjectId,
        LatticeSubjectSelectorKind subjectKind = LatticeSubjectSelectorKind.User,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(subjectId);
        await AuthorizeAdminAsync(cancellationToken).ConfigureAwait(false);

        var subject = await ResolveNamedSubjectAsync(subjectId, subjectKind, cancellationToken).ConfigureAwait(false);
        var groupSet = ToGroupSet(subject.GroupIds);

        var rules = new List<LatticeAuthorizationRule>();
        var cap = _apiOptions.MaxExplanationRules;

        // The store scans the policy tree system-origin internally.
        await foreach (var rule in _store.ListRulesAsync(cancellationToken).ConfigureAwait(false))
        {
            if (rules.Count >= cap)
            {
                break;
            }

            if (SelectorMatches(rule.Subject, subjectId, groupSet))
            {
                rules.Add(rule);
            }
        }

        rules.Sort(CompareRuleCatalogOrder);

        return new AuthEffectivePermissions
        {
            SubjectId = subjectId,
            GroupIds = Sorted(subject.GroupIds),
            Rules = rules,
        };
    }

    // ----- Identity directory -----

    /// <inheritdoc />
    public async Task<DirectorySearchResult> SearchDirectoryAsync(DirectorySearchRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        await AuthorizeAdminAsync(cancellationToken).ConfigureAwait(false);

        // No configured directory: fold to an explicit unavailable result rather
        // than calling the no-op provider and hiding the distinction between
        // "nothing matched" and "validation is off".
        if (!DirectoryAvailable)
        {
            return DirectorySearchResult.Unavailable;
        }

        var query = new DirectorySearchQuery(
            request.Term ?? string.Empty,
            request.Kind,
            request.PageSize,
            request.ContinuationToken);
        var startTimestamp = Stopwatch.GetTimestamp();
        var page = await _identityDirectory.SearchAsync(query, cancellationToken).ConfigureAwait(false);
        LatticeMembershipMetrics.RecordDirectorySearch(
            Stopwatch.GetElapsedTime(startTimestamp).TotalMilliseconds,
            matched: page.Principals.Count > 0);

        var descriptors = new List<DirectoryPrincipalDescriptor>(page.Principals.Count);
        foreach (var principal in page.Principals)
        {
            descriptors.Add(ToDescriptor(principal));
        }

        return new DirectorySearchResult
        {
            Principals = descriptors,
            ContinuationToken = page.ContinuationToken,
            Available = true,
        };
    }

    /// <inheritdoc />
    public async Task<DirectoryPrincipalDescriptor?> ResolveDirectoryPrincipalAsync(string principalId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(principalId);
        await AuthorizeAdminAsync(cancellationToken).ConfigureAwait(false);

        // With no directory configured the resolve is unresolvable: a null result
        // (the same "not found" answer) rather than an error, matching the no-op
        // provider's own behaviour.
        if (!DirectoryAvailable)
        {
            return null;
        }

        var principal = await _identityDirectory.ResolveAsync(principalId, cancellationToken).ConfigureAwait(false);
        return principal is null ? null : ToDescriptor(principal);
    }

    /// <inheritdoc />
    public async Task<AccessModelDescriptor> GetAccessModelAsync(CancellationToken cancellationToken = default)
    {
        await AuthorizeAdminAsync(cancellationToken).ConfigureAwait(false);

        return new AccessModelDescriptor
        {
            AuthenticationMode = DetermineAuthenticationMode(),
            RulesEnforced = _gate is not NullLatticeAccessGate,
            DirectoryAvailable = DirectoryAvailable,
            DirectoryProviderId = _identityDirectory.ProviderId,
            // The Explorer's create form is the group form, so surface the
            // group-scoped guidance; the seam stays kind-aware.
            DirectoryExplanation = _identityDirectory.DescribeEntry(DirectoryPrincipalKind.Group),
            // Locally-defined membership is inert when the cluster resolves groups
            // solely from the identity-provider token (TokenOnly merge mode).
            LocalMembershipEffective =
                _membershipOptions.CurrentValue.GroupMergeMode != SubjectGroupMergeMode.TokenOnly,
        };
    }

    // ----- Internals -----

    /// <summary>
    /// Authorizes the ambient caller as an administrator through the same
    /// enforcement primitive the in-cluster data path uses. Throws
    /// <see cref="LatticeAuthorizationDeniedException"/> when the caller is not an
    /// administrator (an anonymous caller included). Short-circuits at zero cost
    /// when no real access gate is registered.
    /// </summary>
    private ValueTask AuthorizeAdminAsync(CancellationToken cancellationToken) =>
        LatticeAccessGateEnforcement.EnforceWholeTreeAsync(
            _gate, _membership, AdminScopeTreeId, LatticeOperation.Admin, cancellationToken);

    /// <summary>
    /// Resolves a named subject id into a <see cref="LatticeSubject"/> carrying
    /// its full transitively-expanded group closure. The directory read runs
    /// system-origin so it bypasses the gate exactly as the in-cluster subject
    /// resolver does.
    /// </summary>
    /// <remarks>
    /// A <see cref="LatticeSubjectSelectorKind.User"/> subject carries the groups
    /// it belongs to (its own id is not a group and is excluded). A
    /// <see cref="LatticeSubjectSelectorKind.Group"/> subject is evaluated as a
    /// principal that is a member of the named group: its closure is the group
    /// itself plus every ancestor group (via
    /// <see cref="ILatticeMembershipDirectory.ExpandGroupsAsync"/>), so a
    /// <c>group</c>-scoped rule targeting it (or a parent) matches exactly as it
    /// would for any real member.
    /// </remarks>
    private async ValueTask<LatticeSubject> ResolveNamedSubjectAsync(
        string subjectId,
        LatticeSubjectSelectorKind subjectKind,
        CancellationToken cancellationToken)
    {
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            var groups = subjectKind == LatticeSubjectSelectorKind.Group
                ? await _directory.ExpandGroupsAsync(new[] { subjectId }, cancellationToken).ConfigureAwait(false)
                : await _directory.GroupsOfAsync(subjectId, cancellationToken).ConfigureAwait(false);
            return new LatticeSubject(subjectId, groups);
        }
    }

    private async Task<IReadOnlyList<LatticeAuthorizationRule>> CollectMatchedRulesAsync(
        LatticeSubject subject,
        LatticeOperation operation,
        LatticeScope scope,
        CancellationToken cancellationToken)
    {
        var matched = new List<LatticeAuthorizationRule>();
        var seen = new HashSet<string>(StringComparer.Ordinal);
        var cap = _apiOptions.MaxExplanationRules;
        var groupSet = ToGroupSet(subject.GroupIds);

        // Scan the target tree's own rules first, then the cluster-wide "*" bucket
        // so a Tree:* wildcard rule that grants (or denies) the request is cited in
        // the explanation rather than silently omitted (issue #1339). The gate's
        // verdict is authoritative; this only assembles the human-facing rule list.
        await CollectFromTreeAsync(scope.TreeId, subject, operation, scope, groupSet, matched, seen, cap, cancellationToken)
            .ConfigureAwait(false);

        if (!string.Equals(scope.TreeId, LatticeScope.ClusterWideTreeId, StringComparison.Ordinal))
        {
            await CollectFromTreeAsync(
                LatticeScope.ClusterWideTreeId, subject, operation, scope, groupSet, matched, seen, cap, cancellationToken)
                .ConfigureAwait(false);
        }

        matched.Sort(static (a, b) => string.CompareOrdinal(a.RuleId, b.RuleId));
        return matched;
    }

    /// <summary>
    /// Appends every rule stored under <paramref name="treeId"/> that governs the
    /// explain request (operation, subject, and scope overlap) to
    /// <paramref name="matched"/>, deduplicating by rule id and honouring the
    /// explanation cap. Used to fold both the target tree's exact rules and the
    /// cluster-wide "*" wildcard bucket into a single citation list.
    /// </summary>
    private async Task CollectFromTreeAsync(
        string treeId,
        LatticeSubject subject,
        LatticeOperation operation,
        LatticeScope scope,
        HashSet<string> groupSet,
        List<LatticeAuthorizationRule> matched,
        HashSet<string> seen,
        int cap,
        CancellationToken cancellationToken)
    {
        // ListRulesForTreeAsync scans the policy tree system-origin internally.
        await foreach (var rule in _store.ListRulesForTreeAsync(treeId, cancellationToken).ConfigureAwait(false))
        {
            if (matched.Count >= cap)
            {
                break;
            }

            if ((rule.Operations & operation) == 0)
            {
                continue;
            }

            if (!SelectorMatches(rule.Subject, subject.SubjectId, groupSet))
            {
                continue;
            }

            if (!ScopeOverlaps(rule.Scope, scope))
            {
                continue;
            }

            if (!seen.Add(rule.RuleId))
            {
                continue;
            }

            matched.Add(rule);
        }
    }

    private static (string? Key, string? RangeStart, string? RangeEnd) TranslateScope(LatticeScope scope) =>
        scope.Kind switch
        {
            LatticeScopeKind.Key => (scope.KeyOrPrefix, (string?)null, (string?)null),
            LatticeScopeKind.Prefix => ((string?)null, scope.KeyOrPrefix, PrefixUpperBound(scope.KeyOrPrefix!)),
            _ => ((string?)null, (string?)null, (string?)null),
        };

    private static bool SelectorMatches(LatticeSubjectSelector selector, string subjectId, HashSet<string> groups) =>
        selector.Kind switch
        {
            LatticeSubjectSelectorKind.User => string.Equals(selector.Id, subjectId, StringComparison.Ordinal),
            LatticeSubjectSelectorKind.Group => groups.Contains(selector.Id),
            _ => false,
        };

    /// <summary>
    /// Materializes a subject's group closure into an ordinal <see cref="HashSet{T}"/>
    /// once per introspection call so the per-rule group-membership test is an
    /// O(1) lookup rather than a linear scan with a boxed enumerator per rule.
    /// </summary>
    private static HashSet<string> ToGroupSet(IReadOnlyCollection<string> groupIds) =>
        new(groupIds, StringComparer.Ordinal);

    /// <summary>
    /// <see langword="true"/> when a rule scope and an explain-target scope over
    /// the same tree can govern a common key. Advisory containment used only to
    /// surface debugging detail; the authoritative verdict is the gate's.
    /// </summary>
    private static bool ScopeOverlaps(LatticeScope rule, LatticeScope target) =>
        rule.Kind switch
        {
            LatticeScopeKind.Tree => true,
            LatticeScopeKind.Key => target.Kind switch
            {
                LatticeScopeKind.Tree => true,
                LatticeScopeKind.Key => string.Equals(rule.KeyOrPrefix, target.KeyOrPrefix, StringComparison.Ordinal),
                LatticeScopeKind.Prefix => rule.KeyOrPrefix!.StartsWith(target.KeyOrPrefix!, StringComparison.Ordinal),
                _ => false,
            },
            LatticeScopeKind.Prefix => target.Kind switch
            {
                LatticeScopeKind.Tree => true,
                LatticeScopeKind.Key => target.KeyOrPrefix!.StartsWith(rule.KeyOrPrefix!, StringComparison.Ordinal),
                LatticeScopeKind.Prefix =>
                    target.KeyOrPrefix!.StartsWith(rule.KeyOrPrefix!, StringComparison.Ordinal)
                    || rule.KeyOrPrefix!.StartsWith(target.KeyOrPrefix!, StringComparison.Ordinal),
                _ => false,
            },
            _ => false,
        };

    /// <summary>
    /// Reads one page from an ascending, id-ordered source: skips every element at
    /// or before <paramref name="token"/>, collects up to <paramref name="size"/>
    /// elements, and sets the continuation token to the last collected element's
    /// key when at least one further element remains.
    /// </summary>
    private static async Task<(List<T> Page, string? Next)> PageAsync<T>(
        IAsyncEnumerable<T> source,
        string? token,
        int size,
        Func<T, string> keyOf,
        CancellationToken cancellationToken)
    {
        var page = new List<T>(Math.Min(size, 32));
        string? next = null;

        await foreach (var item in source.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            if (token is not null && string.CompareOrdinal(keyOf(item), token) <= 0)
            {
                continue;
            }

            if (page.Count == size)
            {
                // A further element beyond this page exists: resume after the last
                // element we included.
                next = keyOf(page[^1]);
                break;
            }

            page.Add(item);
        }

        return (page, next);
    }

    private static string RuleCatalogKey(LatticeAuthorizationRule rule) =>
        string.Concat(rule.Scope.TreeId, RuleKeySeparator, rule.RuleId);

    private static int CompareRuleCatalogOrder(LatticeAuthorizationRule a, LatticeAuthorizationRule b)
    {
        var byTree = string.CompareOrdinal(a.Scope.TreeId, b.Scope.TreeId);
        return byTree != 0 ? byTree : string.CompareOrdinal(a.RuleId, b.RuleId);
    }

    private static string PrefixUpperBound(string prefix)
    {
        var chars = prefix.ToCharArray();
        chars[^1] = (char)(chars[^1] + 1);
        return new string(chars);
    }

    private static IReadOnlyList<string> Sorted(IReadOnlyCollection<string> values)
    {
        var list = new List<string>(values);
        list.Sort(StringComparer.Ordinal);
        return list;
    }

    private static AuthGroup ToAuthGroup(MembershipGroup group) =>
        new() { GroupId = group.GroupId, DisplayName = group.DisplayName };

    private static DirectoryPrincipalDescriptor ToDescriptor(DirectoryPrincipal principal) =>
        new()
        {
            Id = principal.Id,
            DisplayName = principal.DisplayName,
            Kind = principal.Kind,
            Claims = principal.Claims,
        };

    /// <summary>
    /// <see langword="true"/> when a real, searchable identity directory is
    /// configured; <see langword="false"/> when the default no-op
    /// <see cref="NullIdentityDirectory"/> is in force (ids are accepted without
    /// validation).
    /// </summary>
    private bool DirectoryAvailable => _identityDirectory is not NullIdentityDirectory;

    /// <summary>
    /// Enforces the fail-closed identity-directory validation contract on a
    /// membership-reference create path. When
    /// <see cref="LatticeIdentityDirectoryOptions.ValidationRequired"/> is set and a
    /// real provider is active (<see cref="DirectoryAvailable"/>), the supplied
    /// <paramref name="principalId"/> is resolved through
    /// <see cref="ILatticeIdentityDirectory.ResolveAsync"/> and the create is
    /// rejected when it resolves to nothing or to a principal whose
    /// <see cref="DirectoryPrincipal.Kind"/> does not match
    /// <paramref name="expectedKind"/>. Validation is skipped entirely when
    /// validation is not required or when the no-op
    /// <see cref="NullIdentityDirectory"/> is in force, matching the documented
    /// contract. Runs outside the system-origin write scope: the identity source is
    /// a separate seam from the membership tree, mirroring
    /// <see cref="ResolveDirectoryPrincipalAsync"/>.
    /// </summary>
    /// <param name="principalId">The candidate principal id being referenced.</param>
    /// <param name="expectedKind">The kind the id must resolve to.</param>
    /// <param name="paramName">The offending create-path parameter name for the rejection.</param>
    /// <param name="cancellationToken">Cancels the resolve.</param>
    /// <exception cref="LatticeDirectoryValidationException">
    /// The id does not resolve, or resolves to the wrong <see cref="DirectoryPrincipalKind"/>.
    /// </exception>
    private async Task ValidateDirectoryPrincipalAsync(
        string principalId,
        DirectoryPrincipalKind expectedKind,
        string paramName,
        CancellationToken cancellationToken)
    {
        if (!DirectoryAvailable || !_identityDirectoryOptions.CurrentValue.ValidationRequired)
        {
            return;
        }

        var principal = await _identityDirectory.ResolveAsync(principalId, cancellationToken).ConfigureAwait(false);
        if (principal is null)
        {
            throw LatticeDirectoryValidationException.Unresolved(principalId, expectedKind, paramName);
        }

        if (principal.Kind != expectedKind)
        {
            throw LatticeDirectoryValidationException.KindMismatch(principalId, expectedKind, principal.Kind, paramName);
        }
    }

    /// <summary>
    /// Maps a local <see cref="MembershipMemberKind"/> to the upstream-directory
    /// <see cref="DirectoryPrincipalKind"/> that a member reference of that kind
    /// must resolve to.
    /// </summary>
    private static DirectoryPrincipalKind ToDirectoryPrincipalKind(MembershipMemberKind memberKind) =>
        memberKind == MembershipMemberKind.Group ? DirectoryPrincipalKind.Group : DirectoryPrincipalKind.User;

    /// <summary>
    /// Best-effort authentication mode from the silo's registered credential
    /// authenticators: any real authenticator beyond the anonymous fallback means
    /// the silo can authenticate a caller from claims; only the anonymous
    /// fallback means no caller is ever authenticated; no authenticator at all is
    /// indeterminate. A transport-specific authorizer (for example the flat-Basic
    /// authorizer at the gRPC state layer) is not registered here and so is not
    /// visible - <see cref="AccessAuthenticationMode.Basic"/> is left to the
    /// transport capability probe layered above this facade.
    /// </summary>
    private AccessAuthenticationMode DetermineAuthenticationMode()
    {
        var hasAnyAuthenticator = false;
        foreach (var authenticator in _authenticators)
        {
            hasAnyAuthenticator = true;
            if (authenticator is not AnonymousCredentialAuthenticator)
            {
                return AccessAuthenticationMode.Claims;
            }
        }

        return hasAnyAuthenticator ? AccessAuthenticationMode.Anonymous : AccessAuthenticationMode.Unknown;
    }
}
