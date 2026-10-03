using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Auth;

/// <summary>
/// The single definition of where tenant-tier rules and tenant groups may appear in
/// the authorization policy: the tree classification the tenant layer runs over,
/// and the confinement checks the policy store applies on every write. Shared by
/// the store (which throws), the snapshot compiler (which drops a non-conforming
/// tenant rule as defence in depth) and the evaluator (which classifies the target
/// tree), so the three can never disagree.
/// </summary>
/// <remarks>
/// <para>
/// A <b>tenant-layer tree</b> is a well-formed tenant tree <c>t/{T}/{name}</c> whose
/// tenant <c>T</c> is not <see cref="TenantId.Default"/> and whose tenant-local name
/// is not the tenant-wide sentinel <c>*</c>, not app-owned (<c>a/...</c>) and not a
/// system name (<c>sys-...</c> or <c>_lattice_...</c>). Only such a tree is ever
/// governed by the tenant layer, and only such a tree, or <c>TenantWide(T)</c>, can
/// carry a tenant-tier rule.
/// </para>
/// <para>
/// The store guards, all of which run before anything is read or written:
/// </para>
/// <list type="number">
/// <item><description>
/// <b>Tenant-wide is tenant-layer only (D9).</b> A rule whose tree id has the
/// tenant-wide sentinel shape <c>t/{x}/*</c> is refused unless its id is
/// <c>tenant:</c>-prefixed and its scope is exactly
/// <see cref="LatticeScope.TenantWide(TenantId)"/>.
/// </description></item>
/// <item><description>
/// <b>Tenant-tier rule shape (D7).</b> A <c>tenant:{T}:</c> rule must have a
/// well-formed id, be scoped to a tenant-layer tree of <c>T</c> or to
/// <c>TenantWide(T)</c>, carry a non-empty operation set inside
/// <see cref="LatticeAuthOperations.All"/>, and name a user, a group of <c>T</c> or
/// a cluster group (a group id outside the reserved <c>t/</c> namespace).
/// </description></item>
/// <item><description>
/// <b>Tenant group confinement (D4), for every caller.</b> A rule whose subject is
/// a group in the reserved <c>t/</c> namespace must name a well-formed tenant group
/// <c>t/{G}/{name}</c> and is admitted only when its scope is a tenant-layer tree of
/// <c>G</c>, or <c>TenantWide(G)</c> (on a tenant rule, per the first guard), or -
/// <b>only for an <c>app:</c>-prefixed rule id</b> - an app-owned tree of <c>G</c>
/// (<c>t/{G}/a/...</c>). Every other scope is refused: <c>Tree:*</c>, another
/// tenant's tree, a reserved, system or legacy tree, and an app-owned tree for a
/// non-<c>app:</c> rule.
/// </description></item>
/// </list>
/// </remarks>
internal static class TenantRuleConfinement
{
    /// <summary>The tenant-local name of the tenant-wide sentinel tree id.</summary>
    private const string TenantWideLocalName = "*";

    /// <summary>
    /// Classifies <paramref name="treeId"/> as a tenant-layer tree and returns its
    /// owning tenant slice. Allocation-free.
    /// </summary>
    /// <param name="treeId">The tree id to classify.</param>
    /// <param name="tenant">The owning tenant id slice when this returns <c>true</c>.</param>
    /// <returns><c>true</c> when the tenant layer may govern <paramref name="treeId"/>.</returns>
    internal static bool TryGetTenantLayerTree(ReadOnlySpan<char> treeId, out ReadOnlySpan<char> tenant)
    {
        if (!TrySplitTenantTree(treeId, out tenant, out var local)
            || local.SequenceEqual(TenantWideLocalName)
            || IsAppOwnedLocalName(local)
            || IsSystemLocalName(local))
        {
            tenant = default;
            return false;
        }

        return true;
    }

    /// <summary>
    /// Classifies <paramref name="treeId"/> as an app-owned tenant tree
    /// (<c>t/{T}/a/...</c>, <c>T</c> not the default tenant) and returns its owning
    /// tenant slice. Allocation-free.
    /// </summary>
    /// <param name="treeId">The tree id to classify.</param>
    /// <param name="tenant">The owning tenant id slice when this returns <c>true</c>.</param>
    /// <returns><c>true</c> when <paramref name="treeId"/> is an app-owned tree of a tenant.</returns>
    internal static bool TryGetAppOwnedTenantTree(ReadOnlySpan<char> treeId, out ReadOnlySpan<char> tenant)
    {
        if (!TrySplitTenantTree(treeId, out tenant, out var local) || !IsAppOwnedLocalName(local))
        {
            tenant = default;
            return false;
        }

        return true;
    }

    /// <summary>
    /// <c>true</c> when <paramref name="treeId"/> has the tenant-wide sentinel shape
    /// <c>t/{x}/*</c>, whatever <c>x</c> is. No real tree carries such an id, so a
    /// rule on it is meaningful only as a <see cref="LatticeScope.TenantWide(TenantId)"/>
    /// tenant-tier rule.
    /// </summary>
    /// <param name="treeId">The candidate tree id.</param>
    /// <returns><c>true</c> for a tenant-wide sentinel shape.</returns>
    internal static bool IsTenantWideSentinelShape(string treeId) =>
        treeId.StartsWith(LatticeTenantTrees.SegmentPrefix, StringComparison.Ordinal)
        && treeId.Length > LatticeTenantTrees.SegmentPrefix.Length + 2
        && treeId.EndsWith("/" + TenantWideLocalName, StringComparison.Ordinal);

    /// <summary>
    /// Returns the layer a stored rule belongs to, derived from its id alone:
    /// <see cref="PolicyDecisionLayer.Tenant"/> for a <c>tenant:</c>-prefixed id,
    /// otherwise <see cref="PolicyDecisionLayer.Operator"/>. Used by the
    /// effective-permissions surfaces to label each rule.
    /// </summary>
    /// <param name="ruleId">The rule id. Must not be <c>null</c>.</param>
    /// <returns>The rule's layer.</returns>
    internal static PolicyDecisionLayer LayerOf(string ruleId) =>
        LatticeTenantRuleIds.IsTenantOwned(ruleId) ? PolicyDecisionLayer.Tenant : PolicyDecisionLayer.Operator;

    /// <summary>
    /// Applies every confinement guard to <paramref name="rule"/> and throws on the
    /// first violation. Pure: reads nothing and writes nothing.
    /// </summary>
    /// <param name="rule">The candidate rule. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentException">The rule violates a confinement guard.</exception>
    internal static void EnsureConfined(LatticeAuthorizationRule rule)
    {
        ArgumentNullException.ThrowIfNull(rule);
        var failure = CheckTenantWideScope(rule)
            ?? (LatticeTenantRuleIds.IsTenantOwned(rule.RuleId) ? CheckTenantRule(rule, out _) : null)
            ?? CheckTenantGroupSubject(rule);
        if (failure is not null)
        {
            throw new ArgumentException(failure, nameof(rule));
        }
    }

    /// <summary>
    /// The D9 guard: a rule on a tenant-wide sentinel tree id must be a
    /// <c>tenant:</c> rule whose scope is exactly a tenant-wide scope.
    /// </summary>
    /// <param name="rule">The candidate rule.</param>
    /// <returns><c>null</c> when admitted; otherwise the refusal reason.</returns>
    internal static string? CheckTenantWideScope(LatticeAuthorizationRule rule)
    {
        if (!IsTenantWideSentinelShape(rule.Scope.TreeId))
        {
            return null;
        }

        if (!LatticeTenantRuleIds.IsTenantOwned(rule.RuleId))
        {
            return $"Rule '{rule.RuleId}' is scoped to the tenant-wide sentinel '{rule.Scope.TreeId}'. A tenant-wide "
                + $"scope is authorable only on a tenant-tier rule (an id starting with '{LatticeTenantRuleIds.Prefix}').";
        }

        if (!rule.Scope.IsTenantWide())
        {
            return $"Rule '{rule.RuleId}' targets the tenant-wide sentinel '{rule.Scope.TreeId}' with a scope that is "
                + "not a tenant-wide scope. Use LatticeScope.TenantWide(tenant) for a tenant other than 'default'.";
        }

        return null;
    }

    /// <summary>
    /// The D7 guard for a <c>tenant:</c>-prefixed rule: its id is well formed, its
    /// scope is a tenant-layer tree of its tenant or that tenant's tenant-wide scope,
    /// its operations are a non-empty subset of the data-plane mask, and its subject
    /// is a user, a group of its tenant or a cluster group.
    /// </summary>
    /// <param name="rule">The candidate tenant-tier rule.</param>
    /// <param name="tenant">The owning tenant when this returns <c>null</c>.</param>
    /// <returns><c>null</c> when admitted; otherwise the refusal reason.</returns>
    internal static string? CheckTenantRule(LatticeAuthorizationRule rule, out TenantId tenant)
    {
        if (!LatticeTenantRuleIds.TryGetTenant(rule.RuleId, out tenant))
        {
            return $"Rule id '{rule.RuleId}' is in the tenant-tier namespace but is not of the form "
                + $"'{LatticeTenantRuleIds.Prefix}{{tenant}}:{{localId}}' for a tenant other than '{TenantId.DefaultId}'.";
        }

        var owner = tenant.Value.AsSpan();
        if (rule.Scope.TryGetTenantWideTenant(out var wideTenant))
        {
            if (!string.Equals(wideTenant.Value, tenant.Value, StringComparison.Ordinal))
            {
                return $"Tenant-tier rule '{rule.RuleId}' belongs to tenant '{tenant.Value}' but is scoped tenant-wide "
                    + $"over tenant '{wideTenant.Value}'. A tenant rule may only govern its own tenant.";
            }
        }
        else if (!TryGetTenantLayerTree(rule.Scope.TreeId, out var treeTenant) || !treeTenant.SequenceEqual(owner))
        {
            return $"Tenant-tier rule '{rule.RuleId}' is scoped to '{rule.Scope.TreeId}', which is not a tree tenant "
                + $"'{tenant.Value}' owns. A tenant rule may govern only the tenant's own trees "
                + $"('{LatticeTenantTrees.SegmentPrefix}{tenant.Value}/...'), excluding app-owned and system trees, "
                + "or the tenant-wide scope.";
        }

        if (rule.Operations == LatticeOperation.None || (rule.Operations & ~LatticeAuthOperations.All) != LatticeOperation.None)
        {
            return $"Tenant-tier rule '{rule.RuleId}' carries operations '{rule.Operations}'. A tenant rule must carry "
                + "a non-empty subset of the data-plane operations (LatticeAuthOperations.All); Telemetry, "
                + "Replication, TreeLifecycle and AppInstall are never tenant-authorable.";
        }

        if (rule.Subject.Kind == LatticeSubjectSelectorKind.Group
            && rule.Subject.Id.StartsWith(LatticeTenantTrees.SegmentPrefix, StringComparison.Ordinal))
        {
            if (!LatticeTenantGroupId.TryParse(rule.Subject.Id, out var group)
                || !string.Equals(group.Tenant.Value, tenant.Value, StringComparison.Ordinal))
            {
                return $"Tenant-tier rule '{rule.RuleId}' names group '{rule.Subject.Id}', which is not a group of "
                    + $"tenant '{tenant.Value}'. A tenant rule may name a user, one of the tenant's own groups, or a "
                    + "cluster group.";
            }
        }

        return null;
    }

    /// <summary>
    /// The D4 guard: a rule whose subject is a group in the reserved <c>t/</c>
    /// namespace is confined to its tenant's trees (see the type remarks for the
    /// exact admitted set, including the <c>app:</c> allowance).
    /// </summary>
    /// <param name="rule">The candidate rule.</param>
    /// <returns><c>null</c> when admitted or not applicable; otherwise the refusal reason.</returns>
    internal static string? CheckTenantGroupSubject(LatticeAuthorizationRule rule)
    {
        if (rule.Subject.Kind != LatticeSubjectSelectorKind.Group
            || !rule.Subject.Id.StartsWith(LatticeTenantTrees.SegmentPrefix, StringComparison.Ordinal))
        {
            return null;
        }

        if (!LatticeTenantGroupId.TryParse(rule.Subject.Id, out var group))
        {
            return $"Rule '{rule.RuleId}' names group '{rule.Subject.Id}', which is in the reserved tenant group "
                + $"namespace '{LatticeTenantTrees.SegmentPrefix}' but is not a well-formed tenant group id.";
        }

        var owner = group.Tenant.Value.AsSpan();
        if (rule.Scope.TryGetTenantWideTenant(out var wideTenant))
        {
            return string.Equals(wideTenant.Value, group.Tenant.Value, StringComparison.Ordinal)
                ? null
                : Confinement(rule, group);
        }

        var treeId = rule.Scope.TreeId.AsSpan();
        if (TryGetTenantLayerTree(treeId, out var treeTenant))
        {
            return treeTenant.SequenceEqual(owner) ? null : Confinement(rule, group);
        }

        if (TryGetAppOwnedTenantTree(treeId, out var appTenant)
            && appTenant.SequenceEqual(owner)
            && LatticeAppRuleIds.IsAppOwned(rule.RuleId))
        {
            return null;
        }

        return Confinement(rule, group);
    }

    private static string Confinement(LatticeAuthorizationRule rule, LatticeTenantGroupId group) =>
        $"Rule '{rule.RuleId}' names tenant group '{group.Value}' on scope '{rule.Scope.TreeId}'. A tenant group may "
        + $"appear only in a rule scoped to a tree tenant '{group.Tenant.Value}' owns (excluding app-owned and system "
        + "trees, which only that tenant's app rules may name it on) or to that tenant's tenant-wide scope; never in "
        + "an all-trees rule, on another tenant's tree, or on a reserved, system or legacy tree.";

    private static bool TrySplitTenantTree(
        ReadOnlySpan<char> treeId,
        out ReadOnlySpan<char> tenant,
        out ReadOnlySpan<char> local)
    {
        tenant = default;
        local = default;
        if (!treeId.StartsWith(LatticeTenantTrees.SegmentPrefix, StringComparison.Ordinal))
        {
            return false;
        }

        var rest = treeId[LatticeTenantTrees.SegmentPrefix.Length..];
        var slash = rest.IndexOf('/');
        if (slash <= 0 || slash >= rest.Length - 1)
        {
            return false;
        }

        var tenantSlice = rest[..slash];
        if (!TenantId.IsValid(tenantSlice) || tenantSlice.Equals(TenantId.DefaultId, StringComparison.Ordinal))
        {
            return false;
        }

        tenant = tenantSlice;
        local = rest[(slash + 1)..];
        return true;
    }

    private static bool IsAppOwnedLocalName(ReadOnlySpan<char> local) =>
        local.StartsWith(LatticeConstants.AppTreePrefix, StringComparison.Ordinal);

    private static bool IsSystemLocalName(ReadOnlySpan<char> local) =>
        local.StartsWith(LatticeConstants.SystemDataTreePrefix, StringComparison.Ordinal)
        || local.StartsWith(LatticeConstants.SystemTreePrefix, StringComparison.Ordinal);
}
