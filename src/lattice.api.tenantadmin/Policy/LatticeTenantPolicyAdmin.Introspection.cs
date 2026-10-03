using Orleans.Lattice.Auth;
using Orleans.Lattice.Tenancy;

namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>Layer-aware explain and effective permissions.</summary>
internal sealed partial class LatticeTenantPolicyAdmin
{
    /// <inheritdoc />
    public async Task<TenantExplanation> ExplainAsync(
        string tenantId,
        string subjectId,
        string treeName,
        string? key,
        LatticeOperation operation,
        TenantSubjectKind subjectKind = TenantSubjectKind.User,
        CancellationToken cancellationToken = default)
    {
        var tenant = ParseTenant(tenantId);
        ArgumentException.ThrowIfNullOrEmpty(subjectId);
        ArgumentException.ThrowIfNullOrEmpty(treeName);
        if (key is { Length: 0 })
        {
            throw new ArgumentException("A key must be non-empty, or null for a whole-tree request.", nameof(key));
        }

        var record = await AuthorizeAsync(tenant, nameof(ExplainAsync), answersWhileDisabled: false, cancellationToken)
            .ConfigureAwait(false);
        var scope = TenantPolicyScope.For(tenant);

        // Restricted to the tenant's own trees by construction: the name is local and
        // composed under t/{T}/, and a reserved, system or sentinel name is refused.
        var treeId = scope.ComposeTreeId(treeName, admitAppTrees: true, nameof(treeName));
        var principalId = scope.ComposeSubjectId(subjectId, subjectKind, nameof(subjectId));

        var explanation = new TenantExplanation
        {
            TenantId = tenant.Value,
            SubjectId = subjectId,
            SubjectKind = subjectKind,
            TreeName = treeName,
            Key = key,
            Operation = operation,
            DefaultEffect = _decisions.DefaultEffect,
        };

        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            var subject = await ResolveNamedSubjectAsync(principalId, subjectKind, cancellationToken).ConfigureAwait(false);

            // The tenant gate decides first: a subject that cannot act as the tenant is
            // refused on every one of its trees, whatever the rules say.
            var tenantGate = ValidateActingAs(record, subject);
            if (!tenantGate.Allowed)
            {
                return explanation with
                {
                    Allowed = false,
                    Reason = $"Subject '{subjectId}' cannot act as tenant '{tenant.Value}': {tenantGate.Reason}",
                };
            }

            var verdict = _decisions.Evaluate(subject, treeId, operation, key);
            var matched = await CollectMatchedRulesAsync(scope, subject, treeId, operation, key, cancellationToken)
                .ConfigureAwait(false);
            var deciding = await ResolveDecidingRuleAsync(scope, verdict, treeId, matched, cancellationToken)
                .ConfigureAwait(false);

            var views = new List<TenantRuleView>(matched.Count);
            foreach (var rule in matched)
            {
                views.Add(scope.ToView(rule, scope.Classify(rule)));
            }

            views.Sort(CompareIntrospectionOrder);
            return explanation with
            {
                Allowed = verdict.Allowed,
                Filtered = verdict.Filtered,
                Reason = verdict.Reason,
                DecidingLayer = verdict.DecidingLayer,
                DecidingRule = deciding,
                MatchedRules = views,
            };
        }
    }

    /// <inheritdoc />
    public async Task<TenantEffectivePermissions> EffectivePermissionsAsync(
        string tenantId,
        string subjectId,
        string? treeName = null,
        TenantSubjectKind subjectKind = TenantSubjectKind.User,
        CancellationToken cancellationToken = default)
    {
        var tenant = ParseTenant(tenantId);
        ArgumentException.ThrowIfNullOrEmpty(subjectId);
        if (treeName is { Length: 0 })
        {
            throw new ArgumentException("A tree name must be non-empty, or null for every tree.", nameof(treeName));
        }

        var record = await AuthorizeAsync(tenant, nameof(EffectivePermissionsAsync), answersWhileDisabled: false, cancellationToken)
            .ConfigureAwait(false);
        var scope = TenantPolicyScope.For(tenant);
        var treeId = treeName is null ? null : scope.ComposeTreeId(treeName, admitAppTrees: true, nameof(treeName));
        var principalId = scope.ComposeSubjectId(subjectId, subjectKind, nameof(subjectId));

        var result = new TenantEffectivePermissions
        {
            TenantId = tenant.Value,
            SubjectId = subjectId,
            SubjectKind = subjectKind,
            TreeName = treeName,
        };

        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            var subject = await ResolveNamedSubjectAsync(principalId, subjectKind, cancellationToken).ConfigureAwait(false);

            // A subject that cannot act as the tenant holds no permission on its trees,
            // so no rule applies to it there.
            if (!ValidateActingAs(record, subject).Allowed)
            {
                return result;
            }

            var groups = ToGroupSet(subject.GroupIds);
            var targetIsTenantLayer = treeId is not null && TenantRuleConfinement.TryGetTenantLayerTree(treeId, out _);
            var views = new List<TenantRuleView>();
            await foreach (var rule in _store.ListRulesAsync(cancellationToken).ConfigureAwait(false))
            {
                if (views.Count >= MaxIntrospectionRules)
                {
                    break;
                }

                var visibility = scope.Classify(rule);
                if (visibility == TenantRuleVisibility.Hidden
                    || !SelectorMatches(rule.Subject, subject.SubjectId, groups)
                    || (treeId is not null && !Governs(rule, visibility, treeId, targetIsTenantLayer)))
                {
                    continue;
                }

                views.Add(scope.ToView(rule, visibility));
            }

            views.Sort(CompareIntrospectionOrder);
            return result with { Rules = views };
        }
    }

    /// <summary>
    /// Resolves a named principal into a subject carrying its transitive group
    /// closure. A user carries the groups it belongs to; a group is evaluated as a
    /// member of it, so its closure is the group and every ancestor. Runs inside the
    /// caller's system-origin scope.
    /// </summary>
    private async Task<LatticeSubject> ResolveNamedSubjectAsync(
        string principalId, TenantSubjectKind kind, CancellationToken cancellationToken)
    {
        var groups = kind == TenantSubjectKind.User
            ? await _directory.GroupsOfAsync(principalId, cancellationToken).ConfigureAwait(false)
            : await _directory.ExpandGroupsAsync([principalId], cancellationToken).ConfigureAwait(false);
        return new LatticeSubject(principalId, groups);
    }

    /// <summary>
    /// Collects the rules the tenant may see in full that govern the request: those
    /// on the tree itself and, for a tree the tenant layer governs, the tenant's
    /// tenant-wide rules - matching the operation, the subject and the key. Two
    /// targeted per-tree scans. Runs inside the caller's system-origin scope.
    /// </summary>
    private async Task<List<LatticeAuthorizationRule>> CollectMatchedRulesAsync(
        TenantPolicyScope scope,
        LatticeSubject subject,
        string treeId,
        LatticeOperation operation,
        string? key,
        CancellationToken cancellationToken)
    {
        var matched = new List<LatticeAuthorizationRule>();
        var groups = ToGroupSet(subject.GroupIds);
        await CollectFromTreeAsync(treeId).ConfigureAwait(false);
        if (TenantRuleConfinement.TryGetTenantLayerTree(treeId, out _))
        {
            await CollectFromTreeAsync(scope.TenantWideTreeId).ConfigureAwait(false);
        }

        return matched;

        async Task CollectFromTreeAsync(string bucket)
        {
            await foreach (var rule in _store.ListRulesForTreeAsync(bucket, cancellationToken).ConfigureAwait(false))
            {
                if (matched.Count >= MaxIntrospectionRules)
                {
                    return;
                }

                var visibility = scope.Classify(rule);
                if (visibility is (TenantRuleVisibility.Tenant or TenantRuleVisibility.PlatformTree)
                    && (rule.Operations & operation) != LatticeOperation.None
                    && SelectorMatches(rule.Subject, subject.SubjectId, groups)
                    && KeyMatches(rule.Scope, key))
                {
                    matched.Add(rule);
                }
            }
        }
    }

    /// <summary>
    /// Builds the view of the rule the engine's trace names. A platform-wide or app
    /// role rule is reported by id and effect only; any other is reported in full,
    /// read from the matched set or, failing that, the store.
    /// </summary>
    private async Task<TenantRuleView?> ResolveDecidingRuleAsync(
        TenantPolicyScope scope,
        TenantPolicyVerdict verdict,
        string treeId,
        List<LatticeAuthorizationRule> matched,
        CancellationToken cancellationToken)
    {
        if (verdict.RuleId is not { } ruleId)
        {
            return null;
        }

        if (verdict.AllTrees)
        {
            return TenantPolicyScope.Withheld(ruleId, TenantRuleOrigin.PlatformWide, verdict.Effect);
        }

        if (LatticeAppRuleIds.IsAppOwned(ruleId))
        {
            return TenantPolicyScope.Withheld(ruleId, TenantRuleOrigin.AppRole, verdict.Effect);
        }

        var ruleTreeId = verdict.TenantWide ? scope.TenantWideTreeId : treeId;
        LatticeAuthorizationRule? rule = null;
        foreach (var candidate in matched)
        {
            if (string.Equals(candidate.RuleId, ruleId, StringComparison.Ordinal)
                && string.Equals(candidate.Scope.TreeId, ruleTreeId, StringComparison.Ordinal))
            {
                rule = candidate;
                break;
            }
        }

        rule ??= await _store.GetRuleAsync(ruleTreeId, ruleId, cancellationToken).ConfigureAwait(false);
        var visibility = rule is null ? TenantRuleVisibility.Hidden : scope.Classify(rule);
        if (visibility is TenantRuleVisibility.Tenant or TenantRuleVisibility.PlatformTree)
        {
            return scope.ToView(rule!, visibility);
        }

        // The snapshot decided on a rule the store no longer holds (it was edited or
        // removed since): name it by id and effect alone, in the layer that decided.
        var isTenant = verdict.DecidingLayer == TenantRuleLayer.Tenant && scope.OwnsRuleId(ruleId);
        return new TenantRuleView
        {
            RuleId = isTenant ? ruleId[scope.RuleIdPrefix.Length..] : ruleId,
            Layer = isTenant ? TenantRuleLayer.Tenant : TenantRuleLayer.Platform,
            Origin = isTenant ? TenantRuleOrigin.Tenant : TenantRuleOrigin.PlatformTree,
            Editable = isTenant,
            Effect = verdict.Effect,
        };
    }

    /// <summary>
    /// <see langword="true"/> when a rule the tenant may see governs
    /// <paramref name="treeId"/>: a rule on that tree, a tenant-wide rule over a tree
    /// the tenant layer governs, or a cluster-wide rule.
    /// </summary>
    private static bool Governs(
        LatticeAuthorizationRule rule, TenantRuleVisibility visibility, string treeId, bool targetIsTenantLayer) =>
        visibility switch
        {
            TenantRuleVisibility.PlatformWide => true,
            TenantRuleVisibility.Tenant when rule.Scope.IsTenantWide() => targetIsTenantLayer,
            _ => string.Equals(rule.Scope.TreeId, treeId, StringComparison.Ordinal),
        };

    private static bool KeyMatches(LatticeScope scope, string? key) =>
        key is null
        || scope.Kind switch
        {
            LatticeScopeKind.Key => string.Equals(scope.KeyOrPrefix, key, StringComparison.Ordinal),
            LatticeScopeKind.Prefix => key.StartsWith(scope.KeyOrPrefix!, StringComparison.Ordinal),
            _ => true,
        };

    private static bool SelectorMatches(LatticeSubjectSelector selector, string subjectId, HashSet<string> groups) =>
        selector.Kind switch
        {
            LatticeSubjectSelectorKind.User => string.Equals(selector.Id, subjectId, StringComparison.Ordinal),
            LatticeSubjectSelectorKind.Group => groups.Contains(selector.Id),
            _ => false,
        };

    private static HashSet<string> ToGroupSet(IReadOnlyCollection<string> groupIds) =>
        new(groupIds, StringComparer.Ordinal);

    /// <summary>The introspection order: the platform layer first, then by tree name and rule id.</summary>
    private static int CompareIntrospectionOrder(TenantRuleView a, TenantRuleView b)
    {
        var byLayer = a.Layer.CompareTo(b.Layer);
        if (byLayer != 0)
        {
            return byLayer;
        }

        var byTree = string.CompareOrdinal(a.TreeName, b.TreeName);
        return byTree != 0 ? byTree : string.CompareOrdinal(a.RuleId, b.RuleId);
    }
}
