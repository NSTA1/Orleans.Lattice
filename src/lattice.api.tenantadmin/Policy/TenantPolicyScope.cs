using Orleans.Lattice.Auth;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// The tenant-relative view of the authorization policy the tenant policy facade
/// works in: it composes a tenant's tenant-local names into stored ids
/// (<c>tenant:{T}:{id}</c>, <c>t/{T}/{tree}</c>, <c>t/{T}/{group}</c>), maps stored
/// rules back to <see cref="TenantRuleView"/>s, classifies every stored rule by how
/// the tenant may see it (<see cref="TenantRuleVisibility"/>), and turns a
/// <see cref="TenantRuleDraft"/> into a confined stored rule.
/// </summary>
/// <remarks>
/// Draft confinement gives the typed <see cref="TenantAccessConfinementException"/>
/// first, using the auth add-on's single definition of a tenant-layer tree
/// (<see cref="TenantRuleConfinement"/>), and then re-runs that add-on's own D4, D7
/// and D9 checks so the facade can never admit a rule the policy store's guards
/// would refuse.
/// </remarks>
internal readonly struct TenantPolicyScope
{
    private const string RuleParameter = "rule";

    private TenantPolicyScope(TenantId tenant)
    {
        Tenant = tenant;
        RuleIdPrefix = string.Concat(LatticeTenantRuleIds.Prefix, tenant.Value, ":");
        TreePrefix = LatticeTenantTrees.ComposePrefix(tenant);
        TenantWideTreeId = LatticeScope.TenantWide(tenant).TreeId;
    }

    /// <summary>The tenant the scope is relative to. Never the reserved default tenant.</summary>
    public TenantId Tenant { get; }

    /// <summary>The stored-id prefix of the tenant's tenant-tier rules, <c>tenant:{T}:</c>.</summary>
    public string RuleIdPrefix { get; }

    /// <summary>The id prefix of the tenant's trees and groups, <c>t/{T}/</c>.</summary>
    public string TreePrefix { get; }

    /// <summary>The tenant-wide sentinel tree id, <c>t/{T}/*</c>, under which tenant-wide rules are stored.</summary>
    public string TenantWideTreeId { get; }

    /// <summary>Creates the scope for <paramref name="tenant"/>.</summary>
    /// <param name="tenant">The tenant. Must be a valid tenant other than the reserved default tenant.</param>
    /// <returns>The scope.</returns>
    public static TenantPolicyScope For(TenantId tenant) => new(tenant);

    /// <summary><see langword="true"/> when <paramref name="ruleId"/> is one of this tenant's tenant-tier rule ids.</summary>
    /// <param name="ruleId">A stored rule id.</param>
    /// <returns>Whether the id is owned by the tenant.</returns>
    public bool OwnsRuleId(string ruleId) =>
        ruleId.Length > RuleIdPrefix.Length && ruleId.StartsWith(RuleIdPrefix, StringComparison.Ordinal);

    /// <summary><see langword="true"/> when <paramref name="treeId"/> is one of this tenant's trees (including the tenant-wide sentinel).</summary>
    /// <param name="treeId">A stored tree id.</param>
    /// <returns>Whether the tree is owned by the tenant.</returns>
    public bool OwnsTree(string treeId) =>
        treeId.Length > TreePrefix.Length && treeId.StartsWith(TreePrefix, StringComparison.Ordinal);

    /// <summary>Composes the stored id of the tenant-tier rule with local id <paramref name="localId"/>.</summary>
    /// <param name="localId">The tenant-local rule id. Must not be <see langword="null"/> or empty.</param>
    /// <returns>The stored id, <c>tenant:{T}:{localId}</c>.</returns>
    public string ComposeRuleId(string localId) => LatticeTenantRuleIds.For(Tenant, localId);

    /// <summary>
    /// Composes and confines the tree id a tenant-local tree name addresses. The name
    /// must address a tree the tenant layer governs; when
    /// <paramref name="admitAppTrees"/> is set, one of the tenant's app-owned trees is
    /// admitted too (read-only introspection). Reserved and system names and the
    /// tenant-wide sentinel are never admitted.
    /// </summary>
    /// <param name="treeName">The tenant-local tree name.</param>
    /// <param name="admitAppTrees">Whether an app-owned tree (<c>a/...</c>) is admitted.</param>
    /// <param name="paramName">The parameter the name came from, for the exception.</param>
    /// <returns>The composed tree id, <c>t/{T}/{treeName}</c>.</returns>
    /// <exception cref="ArgumentException"><paramref name="treeName"/> is <see langword="null"/> or empty.</exception>
    /// <exception cref="TenantAccessConfinementException">The name does not address a tree the tenant may govern.</exception>
    public string ComposeTreeId(string? treeName, bool admitAppTrees, string paramName)
    {
        if (string.IsNullOrEmpty(treeName))
        {
            throw new ArgumentException("A tenant-local tree name must not be null or empty.", paramName);
        }

        var treeId = string.Concat(TreePrefix, treeName);
        if (TenantRuleConfinement.TryGetTenantLayerTree(treeId, out _)
            || (admitAppTrees && TenantRuleConfinement.TryGetAppOwnedTenantTree(treeId, out _)))
        {
            return treeId;
        }

        throw new TenantAccessConfinementException(
            Tenant.Value,
            TenantAccessConfinementRule.RuleTree,
            $"'{treeName}' does not name a tree tenant '{Tenant.Value}' may govern. A tenant rule may govern only the "
            + $"tenant's own trees, never its app-owned trees ('{LatticeConstants.AppTreePrefix}...'), a reserved or "
            + "system tree, or the tenant-wide sentinel; use the tenant-wide scope to govern every tree.",
            paramName);
    }

    /// <summary>Classifies a stored rule by how this tenant's administrators may see it.</summary>
    /// <param name="rule">The stored rule.</param>
    /// <returns>The rule's visibility to the tenant.</returns>
    public TenantRuleVisibility Classify(LatticeAuthorizationRule rule)
    {
        var ruleId = rule.RuleId;
        if (LatticeTenantRuleIds.IsTenantOwned(ruleId))
        {
            return OwnsRuleId(ruleId) ? TenantRuleVisibility.Tenant : TenantRuleVisibility.Hidden;
        }

        var treeId = rule.Scope.TreeId;
        if (LatticeAppRuleIds.IsAppOwned(ruleId))
        {
            return OwnsTree(treeId) ? TenantRuleVisibility.AppRole : TenantRuleVisibility.Hidden;
        }

        if (string.Equals(treeId, LatticeScope.ClusterWideTreeId, StringComparison.Ordinal))
        {
            return TenantRuleVisibility.PlatformWide;
        }

        return OwnsTree(treeId) ? TenantRuleVisibility.PlatformTree : TenantRuleVisibility.Hidden;
    }

    /// <summary>
    /// Projects a stored rule the tenant may see onto its tenant-relative view: in
    /// full for <see cref="TenantRuleVisibility.Tenant"/> and
    /// <see cref="TenantRuleVisibility.PlatformTree"/>, and by id and effect only for
    /// <see cref="TenantRuleVisibility.PlatformWide"/> and
    /// <see cref="TenantRuleVisibility.AppRole"/>.
    /// </summary>
    /// <param name="rule">The stored rule.</param>
    /// <param name="visibility">The rule's visibility, from <see cref="Classify"/>. Must not be <see cref="TenantRuleVisibility.Hidden"/>.</param>
    /// <returns>The view.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="visibility"/> is <see cref="TenantRuleVisibility.Hidden"/>.</exception>
    public TenantRuleView ToView(LatticeAuthorizationRule rule, TenantRuleVisibility visibility)
    {
        switch (visibility)
        {
            case TenantRuleVisibility.PlatformWide:
                return Withheld(rule.RuleId, TenantRuleOrigin.PlatformWide, rule.Effect);
            case TenantRuleVisibility.AppRole:
                return Withheld(rule.RuleId, TenantRuleOrigin.AppRole, rule.Effect);
            case TenantRuleVisibility.Tenant:
            case TenantRuleVisibility.PlatformTree:
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(visibility), visibility, "A hidden rule has no tenant view.");
        }

        var isTenant = visibility == TenantRuleVisibility.Tenant;
        var (subjectId, subjectKind) = ToSubject(rule.Subject);
        var tenantWide = rule.Scope.IsTenantWide();
        return new TenantRuleView
        {
            RuleId = isTenant ? rule.RuleId[RuleIdPrefix.Length..] : rule.RuleId,
            Layer = isTenant ? TenantRuleLayer.Tenant : TenantRuleLayer.Platform,
            Origin = isTenant ? TenantRuleOrigin.Tenant : TenantRuleOrigin.PlatformTree,
            Editable = isTenant,
            SubjectId = subjectId,
            SubjectKind = subjectKind,
            ScopeKind = tenantWide ? TenantRuleScopeKind.TenantWide : (TenantRuleScopeKind)(int)rule.Scope.Kind,
            TreeName = tenantWide ? null : LocalTreeName(rule.Scope.TreeId),
            KeyOrPrefix = rule.Scope.KeyOrPrefix,
            Operations = rule.Operations,
            Effect = rule.Effect,
        };
    }

    /// <summary>A view reported by id and effect only (a platform-wide or app role rule).</summary>
    /// <param name="ruleId">The stored rule id.</param>
    /// <param name="origin">The rule's origin: <see cref="TenantRuleOrigin.PlatformWide"/> or <see cref="TenantRuleOrigin.AppRole"/>.</param>
    /// <param name="effect">The rule's effect.</param>
    /// <returns>The withheld view.</returns>
    public static TenantRuleView Withheld(string ruleId, TenantRuleOrigin origin, LatticeEffect effect) =>
        new()
        {
            RuleId = ruleId,
            Layer = TenantRuleLayer.Platform,
            Origin = origin,
            Effect = effect,
        };

    /// <summary>
    /// Turns a draft into the confined stored tenant-tier rule: validates its shape,
    /// composes its tenant-local id, tree and group, and refuses anything outside the
    /// tenant layer with <see cref="TenantAccessConfinementException"/>.
    /// </summary>
    /// <param name="draft">The draft. Must not be <see langword="null"/>.</param>
    /// <returns>The stored rule.</returns>
    /// <exception cref="ArgumentException">The draft is malformed.</exception>
    /// <exception cref="TenantAccessConfinementException">The draft breaks tenant confinement.</exception>
    public LatticeAuthorizationRule ToStoredRule(TenantRuleDraft draft)
    {
        ArgumentNullException.ThrowIfNull(draft);
        if (string.IsNullOrEmpty(draft.RuleId))
        {
            throw new ArgumentException("A tenant rule needs a non-empty tenant-local id.", RuleParameter);
        }

        if (LatticeTenantRuleIds.IsTenantOwned(draft.RuleId) || LatticeAppRuleIds.IsAppOwned(draft.RuleId))
        {
            throw new TenantAccessConfinementException(
                Tenant.Value,
                TenantAccessConfinementRule.ReservedRuleId,
                $"Rule id '{draft.RuleId}' carries a reserved prefix ('{LatticeTenantRuleIds.Prefix}' or "
                + $"'{LatticeAppRuleIds.Prefix}'). Supply a tenant-local id; the facade composes the stored id.",
                RuleParameter);
        }

        if (string.IsNullOrEmpty(draft.SubjectId))
        {
            throw new ArgumentException("A tenant rule needs a non-empty subject id.", RuleParameter);
        }

        if (!Enum.IsDefined(draft.SubjectKind) || !Enum.IsDefined(draft.Effect))
        {
            throw new ArgumentException(
                $"A tenant rule's subject kind '{draft.SubjectKind}' or effect '{draft.Effect}' is not defined.",
                RuleParameter);
        }

        var scope = ToScope(draft);
        if (draft.Operations == LatticeOperation.None
            || (draft.Operations & ~LatticeAuthOperations.All) != LatticeOperation.None)
        {
            throw new TenantAccessConfinementException(
                Tenant.Value,
                TenantAccessConfinementRule.RuleOperations,
                $"Operations '{draft.Operations}' are not tenant-authorable. A tenant rule must carry a non-empty subset "
                + "of the data-plane operations (LatticeAuthOperations.All); Telemetry, Replication, TreeLifecycle and "
                + "AppInstall are platform-only.",
                RuleParameter);
        }

        var rule = new LatticeAuthorizationRule(
            ComposeRuleId(draft.RuleId), ToSelector(draft), scope, draft.Operations, draft.Effect);

        // Backstop: the policy store's own confinement checks, run here so a draft the
        // checks above admitted can never reach the store and fail there instead.
        var failure = TenantRuleConfinement.CheckTenantWideScope(rule)
            ?? TenantRuleConfinement.CheckTenantRule(rule, out _)
            ?? TenantRuleConfinement.CheckTenantGroupSubject(rule);
        if (failure is not null)
        {
            throw new TenantAccessConfinementException(
                Tenant.Value, TenantAccessConfinementRule.RuleTree, failure, RuleParameter);
        }

        return rule;
    }

    /// <summary>
    /// Composes the id of the principal a tenant-relative subject names: a user id as
    /// given, a tenant group's local name as <c>t/{T}/{name}</c>, and a cluster group
    /// as given. A cluster-group id in the reserved <c>t/</c> namespace is refused,
    /// so another tenant's group can never be named.
    /// </summary>
    /// <param name="subjectId">The subject id. Must not be <see langword="null"/> or empty.</param>
    /// <param name="kind">How to read <paramref name="subjectId"/>.</param>
    /// <param name="paramName">The parameter the id came from, for the exception.</param>
    /// <returns>The principal id.</returns>
    /// <exception cref="ArgumentException">A tenant group name is malformed, or the kind is undefined.</exception>
    /// <exception cref="TenantAccessConfinementException">A cluster-group id is in the reserved tenant group namespace.</exception>
    public string ComposeSubjectId(string subjectId, TenantSubjectKind kind, string paramName)
    {
        switch (kind)
        {
            case TenantSubjectKind.User:
                return subjectId;
            case TenantSubjectKind.TenantGroup:
                if (!LatticeTenantGroupId.IsValidName(subjectId))
                {
                    throw new ArgumentException(
                        $"'{subjectId}' is not a valid tenant group name. A tenant group name is 1 to "
                        + $"{LatticeTenantGroupId.MaxNameLength} characters of lower-case ASCII letters, digits, '-', "
                        + "'_' and '.'.",
                        paramName);
                }

                return LatticeTenantGroupId.Compose(Tenant, subjectId).Value;
            case TenantSubjectKind.ClusterGroup:
                if (subjectId.StartsWith(LatticeTenantTrees.SegmentPrefix, StringComparison.Ordinal))
                {
                    throw new TenantAccessConfinementException(
                        Tenant.Value,
                        TenantAccessConfinementRule.ForeignTenantGroup,
                        $"'{subjectId}' is in the reserved tenant group namespace '{LatticeTenantTrees.SegmentPrefix}', "
                        + "so it is not a cluster group. Name one of the tenant's own groups by its local name as a "
                        + "tenant group; another tenant's groups can never be named.",
                        paramName);
                }

                return subjectId;
            default:
                throw new ArgumentException($"Subject kind '{kind}' is not defined.", paramName);
        }
    }

    private LatticeScope ToScope(TenantRuleDraft draft)
    {
        switch (draft.ScopeKind)
        {
            case TenantRuleScopeKind.TenantWide:
                if (draft.TreeName is not null || draft.KeyOrPrefix is not null)
                {
                    throw new ArgumentException(
                        "A tenant-wide rule must carry neither a tree name nor a key or prefix.", RuleParameter);
                }

                return LatticeScope.TenantWide(Tenant);
            case TenantRuleScopeKind.Tree:
                if (draft.KeyOrPrefix is not null)
                {
                    throw new ArgumentException("A tree-scoped rule must not carry a key or prefix.", RuleParameter);
                }

                return LatticeScope.Tree(ComposeTreeId(draft.TreeName, admitAppTrees: false, RuleParameter));
            case TenantRuleScopeKind.Key:
            case TenantRuleScopeKind.Prefix:
                if (string.IsNullOrEmpty(draft.KeyOrPrefix))
                {
                    throw new ArgumentException(
                        $"A {draft.ScopeKind} rule needs a tree name and a non-empty key or prefix.", RuleParameter);
                }

                var treeId = ComposeTreeId(draft.TreeName, admitAppTrees: false, RuleParameter);
                return draft.ScopeKind == TenantRuleScopeKind.Key
                    ? LatticeScope.Key(treeId, draft.KeyOrPrefix)
                    : LatticeScope.Prefix(treeId, draft.KeyOrPrefix);
            default:
                throw new ArgumentException($"Scope kind '{draft.ScopeKind}' is not defined.", RuleParameter);
        }
    }

    private LatticeSubjectSelector ToSelector(TenantRuleDraft draft)
    {
        var id = ComposeSubjectId(draft.SubjectId, draft.SubjectKind, RuleParameter);
        return draft.SubjectKind == TenantSubjectKind.User
            ? LatticeSubjectSelector.User(id)
            : LatticeSubjectSelector.Group(id);
    }

    private (string SubjectId, TenantSubjectKind Kind) ToSubject(LatticeSubjectSelector selector)
    {
        if (selector.Kind != LatticeSubjectSelectorKind.Group)
        {
            return (selector.Id, TenantSubjectKind.User);
        }

        return LatticeTenantGroupId.TryParse(selector.Id, out var group) && group.Tenant.Equals(Tenant)
            ? (group.Name, TenantSubjectKind.TenantGroup)
            : (selector.Id, TenantSubjectKind.ClusterGroup);
    }

    private string LocalTreeName(string treeId) =>
        OwnsTree(treeId) ? treeId[TreePrefix.Length..] : treeId;
}
