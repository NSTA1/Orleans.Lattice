using Orleans.Lattice;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Api.TenantAdmin.Fakes;

/// <summary>
/// An in-memory <see cref="ILatticeTenantPolicyAdmin"/> for tests that bind to the
/// delegated tenant-access contract without a cluster (the Explorer's tenant Rules
/// and Explain pages, transport bindings). It keeps per-tenant tenant-tier rules
/// plus seeded platform rules, enforces the contract's guards and confinement,
/// the <c>MaxTenantRules</c> cap, and the layering: a matching platform rule is
/// final; otherwise a tenant-wide deny, then the most specific tree rule (key,
/// prefix, tree, deny-overrides), then a tenant-wide allow, then
/// <see cref="DefaultEffect"/>. Platform-wide and app role rules are reported by
/// id and effect only. Subjects match exactly; group expansion is not modelled.
/// </summary>
/// <param name="gate">The shared guard state, or <c>null</c> for a fresh one.</param>
internal sealed class FakeTenantPolicyAdmin(FakeTenantAccessGate? gate = null) : ILatticeTenantPolicyAdmin
{
    private const string AppOwnedTreePrefix = "a/";

    private readonly Dictionary<string, SortedDictionary<string, TenantRuleView>> _tenantRules = new(StringComparer.Ordinal);
    private readonly Dictionary<string, List<TenantRuleView>> _platformRules = new(StringComparer.Ordinal);

    /// <summary>The guard state: feature flag, scripted denial and failure, and the call log.</summary>
    public FakeTenantAccessGate Gate { get; } = gate ?? new FakeTenantAccessGate();

    /// <summary>The effect applied when no rule matches. Defaults to <see cref="LatticeEffect.Deny"/>.</summary>
    public LatticeEffect DefaultEffect { get; set; } = LatticeEffect.Deny;

    /// <summary>Reported as <see cref="TenantAccessPosture.CallerIsTenantAdmin"/>. Defaults to <see langword="true"/>.</summary>
    public bool CallerIsTenantAdmin { get; set; } = true;

    /// <summary>Reported as <see cref="TenantAccessPosture.CallerIsPlatformOperator"/>.</summary>
    public bool CallerIsPlatformOperator { get; set; }

    /// <summary>Reported as <see cref="TenantAccessPosture.Groups"/>.</summary>
    public TenantQuotaDimensionUsage Groups { get; set; } = new() { Usage = 0, Limit = 500, BurstLimit = 500 };

    /// <summary>Reported as <see cref="TenantAccessPosture.MembershipEdges"/>.</summary>
    public TenantQuotaDimensionUsage MembershipEdges { get; set; } = new() { Usage = 0, Limit = 10000, BurstLimit = 10000 };

    /// <summary>Reported as <see cref="TenantAccessPosture.MemberSubjects"/>.</summary>
    public TenantQuotaDimensionUsage MemberSubjects { get; set; } = new() { Usage = 0, Limit = 5000, BurstLimit = 5000 };

    /// <summary>The <c>MaxTenantRules</c> cap a new rule is admitted against. Defaults to 1000.</summary>
    public long MaxTenantRules { get; set; } = 1000;

    /// <summary>
    /// Seeds a platform rule that governs <paramref name="tenantId"/>, given in full.
    /// Its <see cref="TenantRuleView.Origin"/> decides how it is shown: a
    /// <see cref="TenantRuleOrigin.PlatformTree"/> rule is listed read-only, while a
    /// platform-wide or app role rule is never listed and is reported by id and
    /// effect only.
    /// </summary>
    /// <param name="tenantId">The tenant the rule governs.</param>
    /// <param name="rule">The rule, with every field populated. Must not be a tenant-tier rule.</param>
    public void SeedPlatformRule(string tenantId, TenantRuleView rule)
    {
        ArgumentNullException.ThrowIfNull(rule);
        if (rule.Origin == TenantRuleOrigin.Tenant)
        {
            throw new ArgumentException("A platform rule cannot have the tenant origin.", nameof(rule));
        }

        if (!_platformRules.TryGetValue(tenantId, out var rules))
        {
            rules = [];
            _platformRules[tenantId] = rules;
        }

        rules.Add(rule with { Layer = TenantRuleLayer.Platform, Editable = false });
    }

    /// <summary>
    /// Removes the tenant-tier rules of <paramref name="tenantId"/> whose subject is
    /// the tenant group <paramref name="groupName"/>, as a group removal cascades.
    /// </summary>
    /// <param name="tenantId">The tenant.</param>
    /// <param name="groupName">The removed group's local name.</param>
    /// <returns>The local ids of the removed rules, in ordinal order.</returns>
    public IReadOnlyList<string> RemoveRulesNaming(string tenantId, string groupName)
    {
        var removed = new List<string>();
        if (!_tenantRules.TryGetValue(tenantId, out var rules))
        {
            return removed;
        }

        foreach (var rule in rules.Values)
        {
            if (rule.SubjectKind == TenantSubjectKind.TenantGroup && rule.SubjectId == groupName)
            {
                removed.Add(rule.RuleId);
            }
        }

        foreach (var ruleId in removed)
        {
            rules.Remove(ruleId);
        }

        return removed;
    }

    /// <inheritdoc />
    public Task<TenantRuleView> PutRuleAsync(
        string tenantId, TenantRuleDraft rule, CancellationToken cancellationToken = default)
    {
        Gate.Check(tenantId, nameof(PutRuleAsync));
        ArgumentNullException.ThrowIfNull(rule);
        Validate(tenantId, rule);
        if (!_tenantRules.TryGetValue(tenantId, out var rules))
        {
            rules = new SortedDictionary<string, TenantRuleView>(StringComparer.Ordinal);
            _tenantRules[tenantId] = rules;
        }

        if (!rules.ContainsKey(rule.RuleId) && rules.Count >= MaxTenantRules)
        {
            throw new LatticeQuotaExceededException(
                $"Tenant '{tenantId}' is at its MaxTenantRules cap of {MaxTenantRules}.",
                string.Empty, "MaxTenantRules", rules.Count, MaxTenantRules, tenantId);
        }

        var view = new TenantRuleView
        {
            RuleId = rule.RuleId,
            Layer = TenantRuleLayer.Tenant,
            Origin = TenantRuleOrigin.Tenant,
            Editable = true,
            SubjectId = rule.SubjectId,
            SubjectKind = rule.SubjectKind,
            ScopeKind = rule.ScopeKind,
            TreeName = rule.TreeName,
            KeyOrPrefix = rule.KeyOrPrefix,
            Operations = rule.Operations,
            Effect = rule.Effect,
        };
        rules[rule.RuleId] = view;
        return Task.FromResult(view);
    }

    /// <inheritdoc />
    public Task<TenantRuleView?> GetRuleAsync(string tenantId, string ruleId, CancellationToken cancellationToken = default)
    {
        Gate.Check(tenantId, nameof(GetRuleAsync));
        ArgumentException.ThrowIfNullOrEmpty(ruleId);
        TenantRuleView? rule = _tenantRules.TryGetValue(tenantId, out var rules) && rules.TryGetValue(ruleId, out var found)
            ? found
            : null;
        return Task.FromResult(rule);
    }

    /// <inheritdoc />
    public Task<bool> RemoveRuleAsync(string tenantId, string ruleId, CancellationToken cancellationToken = default)
    {
        Gate.Check(tenantId, nameof(RemoveRuleAsync));
        ArgumentException.ThrowIfNullOrEmpty(ruleId);
        return Task.FromResult(_tenantRules.TryGetValue(tenantId, out var rules) && rules.Remove(ruleId));
    }

    /// <inheritdoc />
    public Task<TenantRulePage> ListRulesAsync(
        string tenantId, TenantAccessPageRequest page, CancellationToken cancellationToken = default)
    {
        Gate.Check(tenantId, nameof(ListRulesAsync));
        var visible = new List<TenantRuleView>();
        foreach (var rule in Platform(tenantId))
        {
            if (rule.Origin == TenantRuleOrigin.PlatformTree)
            {
                visible.Add(rule);
            }
        }

        if (_tenantRules.TryGetValue(tenantId, out var rules))
        {
            visible.AddRange(rules.Values);
        }

        visible.Sort(static (a, b) => string.CompareOrdinal(ListKey(a), ListKey(b)));
        var (entries, next) = FakeTenantAccessGate.Page(visible, ListKey, page);
        return Task.FromResult(new TenantRulePage { Entries = entries, NextPageToken = next });
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
        Gate.Check(tenantId, nameof(ExplainAsync));
        ArgumentException.ThrowIfNullOrEmpty(subjectId);
        ArgumentException.ThrowIfNullOrEmpty(treeName);

        var platform = new List<TenantRuleView>();
        foreach (var rule in Platform(tenantId))
        {
            if (Matches(rule, subjectId, subjectKind, treeName, key, operation))
            {
                platform.Add(rule);
            }
        }

        var tenant = new List<TenantRuleView>();
        if (_tenantRules.TryGetValue(tenantId, out var rules))
        {
            foreach (var rule in rules.Values)
            {
                if (Matches(rule, subjectId, subjectKind, treeName, key, operation))
                {
                    tenant.Add(rule);
                }
            }
        }

        var (layer, deciding) = platform.Count > 0
            ? (TenantRuleLayer.Platform, DenyOverrides(platform))
            : DecideTenantLayer(tenant);

        var matched = new List<TenantRuleView>();
        foreach (var rule in platform)
        {
            if (!rule.SubjectWithheld)
            {
                matched.Add(rule);
            }
        }

        matched.AddRange(tenant);
        var allowed = (deciding?.Effect ?? DefaultEffect) == LatticeEffect.Allow;
        return Task.FromResult(new TenantExplanation
        {
            TenantId = tenantId,
            SubjectId = subjectId,
            SubjectKind = subjectKind,
            TreeName = treeName,
            Key = key,
            Operation = operation,
            Allowed = allowed,
            Reason = deciding is null ? $"No rule matched; the default effect is {DefaultEffect}." : null,
            DecidingLayer = deciding is null ? null : layer,
            DecidingRule = deciding is null ? null : Project(deciding),
            DefaultEffect = DefaultEffect,
            MatchedRules = matched,
        });
    }

    /// <inheritdoc />
    public Task<TenantEffectivePermissions> EffectivePermissionsAsync(
        string tenantId,
        string subjectId,
        string? treeName = null,
        TenantSubjectKind subjectKind = TenantSubjectKind.User,
        CancellationToken cancellationToken = default)
    {
        Gate.Check(tenantId, nameof(EffectivePermissionsAsync));
        ArgumentException.ThrowIfNullOrEmpty(subjectId);
        var applying = new List<TenantRuleView>();
        foreach (var rule in Platform(tenantId))
        {
            if (Names(rule, subjectId, subjectKind) && Governs(rule, treeName))
            {
                applying.Add(Project(rule));
            }
        }

        if (_tenantRules.TryGetValue(tenantId, out var rules))
        {
            foreach (var rule in rules.Values)
            {
                if (Names(rule, subjectId, subjectKind) && Governs(rule, treeName))
                {
                    applying.Add(rule);
                }
            }
        }

        return Task.FromResult(new TenantEffectivePermissions
        {
            TenantId = tenantId,
            SubjectId = subjectId,
            SubjectKind = subjectKind,
            TreeName = treeName,
            Rules = applying,
        });
    }

    /// <inheritdoc />
    public Task<TenantAccessPosture> GetPostureAsync(string tenantId, CancellationToken cancellationToken = default)
    {
        Gate.Check(tenantId, nameof(GetPostureAsync), answersWhileDisabled: true);
        var ruleCount = _tenantRules.TryGetValue(tenantId, out var rules) ? rules.Count : 0;
        return Task.FromResult(new TenantAccessPosture
        {
            TenantId = tenantId,
            Enabled = Gate.Enabled,
            CallerIsTenantAdmin = CallerIsTenantAdmin,
            CallerIsPlatformOperator = CallerIsPlatformOperator,
            Groups = Groups,
            MembershipEdges = MembershipEdges,
            MemberSubjects = MemberSubjects,
            TenantRules = new TenantQuotaDimensionUsage
            {
                Usage = ruleCount,
                Limit = MaxTenantRules,
                BurstLimit = MaxTenantRules,
                Overage = Math.Max(0, ruleCount - MaxTenantRules),
            },
        });
    }

    private static void Validate(string tenantId, TenantRuleDraft rule)
    {
        ArgumentException.ThrowIfNullOrEmpty(rule.RuleId, nameof(rule));
        ArgumentException.ThrowIfNullOrEmpty(rule.SubjectId, nameof(rule));
        if (rule.RuleId.StartsWith("tenant:", StringComparison.Ordinal) || rule.RuleId.StartsWith("app:", StringComparison.Ordinal))
        {
            throw new TenantAccessConfinementException(
                tenantId, TenantAccessConfinementRule.ReservedRuleId, $"The rule id '{rule.RuleId}' carries a reserved prefix.", nameof(rule));
        }

        if (rule.Operations == LatticeOperation.None)
        {
            throw new ArgumentException("A rule must cover at least one operation.", nameof(rule));
        }

        if ((rule.Operations & ~LatticeAuthOperations.All) != LatticeOperation.None)
        {
            throw new TenantAccessConfinementException(
                tenantId, TenantAccessConfinementRule.RuleOperations, $"The operations {rule.Operations} leave the data-plane mask.", nameof(rule));
        }

        var shapeValid = rule.ScopeKind switch
        {
            TenantRuleScopeKind.Tree => !string.IsNullOrEmpty(rule.TreeName) && rule.KeyOrPrefix is null,
            TenantRuleScopeKind.Key or TenantRuleScopeKind.Prefix => !string.IsNullOrEmpty(rule.TreeName) && !string.IsNullOrEmpty(rule.KeyOrPrefix),
            TenantRuleScopeKind.TenantWide => rule.TreeName is null && rule.KeyOrPrefix is null,
            _ => false,
        };
        if (!shapeValid)
        {
            throw new ArgumentException($"The scope shape is not valid for a {rule.ScopeKind} rule.", nameof(rule));
        }

        if (rule.TreeName is not null && rule.TreeName.StartsWith(AppOwnedTreePrefix, StringComparison.Ordinal))
        {
            throw new TenantAccessConfinementException(
                tenantId, TenantAccessConfinementRule.RuleTree, $"'{rule.TreeName}' is an app-owned tree.", nameof(rule));
        }

        FakeTenantAccessGate.RejectForeignTenantGroup(tenantId, rule.SubjectId, rule.SubjectKind, nameof(rule));
    }

    private static (TenantRuleLayer Layer, TenantRuleView? Deciding) DecideTenantLayer(List<TenantRuleView> tenant)
    {
        var wide = tenant.FindAll(static r => r.ScopeKind == TenantRuleScopeKind.TenantWide);
        if (wide.Find(static r => r.Effect == LatticeEffect.Deny) is { } wideDeny)
        {
            return (TenantRuleLayer.Tenant, wideDeny);
        }

        foreach (var tier in (ReadOnlySpan<TenantRuleScopeKind>)[TenantRuleScopeKind.Key, TenantRuleScopeKind.Prefix, TenantRuleScopeKind.Tree])
        {
            var atTier = tenant.FindAll(r => r.ScopeKind == tier);
            if (atTier.Count > 0)
            {
                return (TenantRuleLayer.Tenant, DenyOverrides(atTier));
            }
        }

        return (TenantRuleLayer.Tenant, wide.Count > 0 ? wide[0] : null);
    }

    private static TenantRuleView DenyOverrides(List<TenantRuleView> matched) =>
        matched.Find(static r => r.Effect == LatticeEffect.Deny) ?? matched[0];

    private static bool Matches(
        TenantRuleView rule, string subjectId, TenantSubjectKind kind, string treeName, string? key, LatticeOperation operation)
    {
        if (!Names(rule, subjectId, kind) || operation == LatticeOperation.None || (rule.Operations & operation) != operation)
        {
            return false;
        }

        return rule.ScopeKind switch
        {
            TenantRuleScopeKind.TenantWide => !treeName.StartsWith(AppOwnedTreePrefix, StringComparison.Ordinal),
            TenantRuleScopeKind.Tree => rule.TreeName == treeName,
            TenantRuleScopeKind.Key => rule.TreeName == treeName && key is not null && key == rule.KeyOrPrefix,
            TenantRuleScopeKind.Prefix => rule.TreeName == treeName && key is not null
                && rule.KeyOrPrefix is not null && key.StartsWith(rule.KeyOrPrefix, StringComparison.Ordinal),
            _ => false,
        };
    }

    private static bool Names(TenantRuleView rule, string subjectId, TenantSubjectKind kind) =>
        rule.SubjectId == subjectId && rule.SubjectKind == kind;

    private static bool Governs(TenantRuleView rule, string? treeName) =>
        treeName is null || rule.ScopeKind == TenantRuleScopeKind.TenantWide || rule.TreeName == treeName;

    private static TenantRuleView Project(TenantRuleView rule) => rule.SubjectWithheld
        ? new TenantRuleView
        {
            RuleId = rule.RuleId,
            Layer = TenantRuleLayer.Platform,
            Origin = rule.Origin,
            Effect = rule.Effect,
        }
        : rule;

    private static string ListKey(TenantRuleView rule) => (rule.TreeName ?? string.Empty) + "\u0000" + rule.RuleId;

    private List<TenantRuleView> Platform(string tenantId) =>
        _platformRules.TryGetValue(tenantId, out var rules) ? rules : [];
}
