namespace Orleans.Lattice.Auth;

/// <summary>
/// The pure, synchronous, allocation-light evaluation of a request against a
/// compiled policy snapshot. Shared by the decision engine and directly unit
/// testable without a maintainer, snapshot swap, or cluster. Does no I/O.
/// </summary>
internal static class PolicyEvaluator
{
    /// <summary>
    /// Evaluates a request against <paramref name="policy"/> and returns the
    /// access decision.
    /// </summary>
    /// <param name="policy">The compiled snapshot to evaluate against.</param>
    /// <param name="options">The tie-break and default-effect options.</param>
    /// <param name="subject">The requesting subject (its group closure is a flat set).</param>
    /// <param name="treeId">The target tree id.</param>
    /// <param name="operation">The requested operation.</param>
    /// <param name="key">
    /// The exact key for a point request, or <c>null</c> for a collection (range /
    /// whole-tree) request whose per-key admission is expressed as a
    /// <see cref="LatticeAccessDecision.KeyFilter"/>.
    /// </param>
    /// <param name="rangeStart">The inclusive range start of a collection request, or <c>null</c> for the start of the keyspace. Decides whether the range is uniformly governed (see <see cref="CompiledTree.TryResolveUniformRange"/>) and appears in the reason text.</param>
    /// <param name="rangeEnd">The exclusive range end of a collection request, or <c>null</c> for the end of the keyspace. Used as <paramref name="rangeStart"/> is.</param>
    /// <returns>The access decision.</returns>
    public static LatticeAccessDecision Evaluate(
        CompiledPolicy policy,
        LatticeAuthOptions options,
        in LatticeSubject subject,
        string treeId,
        LatticeOperation operation,
        string? key,
        string? rangeStart,
        string? rangeEnd) =>
        Evaluate(policy, options, subject, treeId, operation, key, rangeStart, rangeEnd, out _);

    /// <summary>
    /// Evaluates a request against <paramref name="policy"/> and returns the
    /// access decision, additionally surfacing the winning
    /// <paramref name="match"/> for observability / audit. For a point request
    /// <paramref name="match"/> is the resolved rule match (or a default
    /// unmatched value when the default effect applied); for a collection request
    /// - whose admission can vary key-by-key - it is always the default unmatched
    /// value, because no single rule decides the whole range.
    /// </summary>
    /// <param name="policy">The compiled snapshot to evaluate against.</param>
    /// <param name="options">The tie-break and default-effect options.</param>
    /// <param name="subject">The requesting subject (its group closure is a flat set).</param>
    /// <param name="treeId">The target tree id.</param>
    /// <param name="operation">The requested operation.</param>
    /// <param name="key">The exact key for a point request, or <c>null</c> for a collection request.</param>
    /// <param name="rangeStart">The inclusive range start of a collection request, or <c>null</c> for the start of the keyspace. Decides whether the range is uniformly governed (see <see cref="CompiledTree.TryResolveUniformRange"/>) and appears in the reason text.</param>
    /// <param name="rangeEnd">The exclusive range end of a collection request, or <c>null</c> for the end of the keyspace. Used as <paramref name="rangeStart"/> is.</param>
    /// <param name="match">The winning rule match, or a default (unmatched) value.</param>
    /// <returns>The access decision.</returns>
    public static LatticeAccessDecision Evaluate(
        CompiledPolicy policy,
        LatticeAuthOptions options,
        in LatticeSubject subject,
        string treeId,
        LatticeOperation operation,
        string? key,
        string? rangeStart,
        string? rangeEnd,
        out PolicyMatch match) =>
        Evaluate(policy, options, subject, treeId, operation, key, rangeStart, rangeEnd, tenantLayerActive: false, out match);

    /// <summary>
    /// Evaluates a request against <paramref name="policy"/> with the two layers of
    /// the decision algorithm, surfacing the winning <paramref name="match"/> (its
    /// <see cref="PolicyMatch.Layer"/> and <see cref="PolicyMatch.RuleId"/> are the
    /// explain trace).
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Layering.</b> The operator layer (every rule outside the
    /// <see cref="LatticeTenantRuleIds.Prefix"/> namespace, including the
    /// <c>Tree:*</c> tier and <c>app:</c> rules) is evaluated exactly as it is with
    /// the tenant layer off. A matched operator verdict - allow or deny - is final.
    /// Only when no operator rule matches, and only when
    /// <paramref name="tenantLayerActive"/> is set, the snapshot carries a tenant
    /// partition and the tree is a tenant-layer tree (see
    /// <see cref="TenantRuleConfinement.TryGetTenantLayerTree"/>), the tenant layer
    /// runs: a tenant-wide deny denies; otherwise the tree's own most-specific tenant
    /// verdict applies (key, then longest prefix, then tree, with deny-overrides and
    /// <see cref="LatticeAuthOptions.UserRuleBeatsGroupRuleAtEqualScope"/> honoured at
    /// an equal tier); otherwise a tenant-wide allow grants; otherwise
    /// <see cref="LatticeAuthOptions.DefaultEffect"/> applies. When the layer is off
    /// the only added work is reading <paramref name="tenantLayerActive"/>.
    /// </para>
    /// <para>
    /// <b>Range and scan filters.</b> A collection request's key filter is the
    /// pointwise composition of the two layers, so for every key <c>k</c> the filter
    /// admits <c>k</c> exactly when a point request for <c>k</c> is allowed. As sets:
    /// the admitted keys are the keys an operator rule allows, plus the keys a tenant
    /// rule (or the default effect) allows that no operator rule covers, minus the
    /// keys an operator rule denies. A key is "covered" by the operator layer when
    /// any operator rule - at key, prefix, tree or all-trees scope, for this subject
    /// and operation - matches it; coverage, not the effect, is what hands the key to
    /// the operator layer, so a tenant allow can never carve a hole in an operator
    /// deny and a tenant deny can never revoke an operator allow. The collection
    /// decision is returned uniform (no filter) only when neither layer can vary by
    /// key: the operator layer has no key or prefix rules (and so decides every key
    /// alike, ending the evaluation if it matched), and the tenant tree has none
    /// either.
    /// </para>
    /// </remarks>
    /// <param name="policy">The compiled snapshot to evaluate against.</param>
    /// <param name="options">The tie-break and default-effect options.</param>
    /// <param name="subject">The requesting subject (its group closure is a flat set).</param>
    /// <param name="treeId">The target tree id.</param>
    /// <param name="operation">The requested operation.</param>
    /// <param name="key">The exact key for a point request, or <c>null</c> for a collection request.</param>
    /// <param name="rangeStart">The inclusive range start, or <c>null</c>. Used only in the reason text.</param>
    /// <param name="rangeEnd">The exclusive range end, or <c>null</c>. Used only in the reason text.</param>
    /// <param name="tenantLayerActive">Whether the tenant layer is active (<see cref="ITenantRuleLayer.IsActive"/>).</param>
    /// <param name="match">The winning rule match, or a default (unmatched) value.</param>
    /// <returns>The access decision.</returns>
    public static LatticeAccessDecision Evaluate(
        CompiledPolicy policy,
        LatticeAuthOptions options,
        in LatticeSubject subject,
        string treeId,
        LatticeOperation operation,
        string? key,
        string? rangeStart,
        string? rangeEnd,
        bool tenantLayerActive,
        out PolicyMatch match)
    {
        // The tenant layer is entered only when it is active, the snapshot carries a
        // tenant partition, and that partition holds a bucket for this tree (a
        // tenant-layer tree with tenant rules of its own or of its tenant's
        // tenant-wide scope). Otherwise the operator-only algorithm below runs
        // byte-for-byte unchanged.
        if (tenantLayerActive
            && policy.Tenant is { } partition
            && partition.TryGetBuckets(treeId, out var tenantTree, out var tenantWide))
        {
            return EvaluateLayered(
                policy, options, subject, treeId, operation, key, rangeStart, rangeEnd, tenantTree, tenantWide, out match);
        }

        match = default;
        var hasTree = policy.TryGetTree(treeId, out var tree);
        var userBeatsGroup = options.UserRuleBeatsGroupRuleAtEqualScope;

        // All-trees tier participation. Cheap early-out: when the flag is off, no
        // "*" bucket exists, or the target is the reserved namespace / sentinel
        // itself, this is null and every path below takes the exact byte-for-byte
        // existing behaviour with no added lookup or allocation. Only when the flag
        // is on AND a "*" bucket exists AND the tree is a genuine application tree
        // is the extra whole-tree resolution done.
        var allTreesBucket = ShouldConsultAllTrees(policy, options, treeId) ? policy.AllTrees : null;

        // Point request: resolve the single key.
        if (key is not null)
        {
            var specific = hasTree ? tree!.ResolvePoint(subject, operation, key, userBeatsGroup) : default;
            if (allTreesBucket is null)
            {
                match = specific;
                return FromMatch(match, options.DefaultEffect, subject, treeId);
            }

            var allTrees = ResolveAllTrees(allTreesBucket, subject, operation, userBeatsGroup);
            match = ResolveTiered(specific, allTrees);
            return FromMatch(match, options.DefaultEffect, subject, treeId);
        }

        // Collection request (range read / whole-tree). When the tree carries no
        // per-key (exact/prefix) rules the decision is uniform, so return a plain
        // allow/deny. The all-trees verdict is itself whole-tree (uniform across
        // keys), so it folds cleanly into this uniform branch.
        //
        // A request targeting the sentinel takes this branch unconditionally. Such a
        // request is a scopeless cluster-wide capability check (see
        // ShouldConsultAllTrees), so it is whole-scope by construction and no key is
        // ever supplied: a per-key rule that happens to sit in the "*" bucket is
        // inert for the all-trees tier (ResolveAllTrees resolves tree-wide only) and
        // must not decide the capability either. Without this the bucket's
        // HasPerKeyRules would divert a scopeless request into the per-key Filtered
        // path below, whose winning match is by definition "unmatched" - so one
        // unrelated key- or prefix-scoped rule, possibly belonging to another
        // subject, would silently deny cluster telemetry for every caller under
        // control-plane isolation (issue #1795).
        if (!hasTree || !tree!.HasPerKeyRules || IsClusterWideCapabilityRequest(treeId))
        {
            var uniform = hasTree ? tree!.ResolvePoint(subject, operation, key: null, userBeatsGroup) : default;
            if (allTreesBucket is not null)
            {
                var allTrees = ResolveAllTrees(allTreesBucket, subject, operation, userBeatsGroup);
                uniform = ResolveTiered(uniform, allTrees);
            }

            match = uniform;
            return FromMatch(uniform, options.DefaultEffect, subject, treeId);
        }

        // The tree carries per-key rules, so the decision can vary key-by-key - but
        // not necessarily inside the requested range. When every key of the range
        // resolves to the same rule (for example a prefix range under the matching
        // prefix grant) and that shared verdict is an allow, return it as a plain
        // allow: an all-or-nothing operation over exactly a granted prefix is then
        // authorized, while an exact key or a narrower prefix inside the range keeps
        // the decision filtered (issue #4278). A uniform deny keeps the filtered
        // shape below, which rejects every key exactly as before.
        var allTreesMatch = allTreesBucket is null
            ? default
            : ResolveAllTrees(allTreesBucket, subject, operation, userBeatsGroup);
        if (tree!.TryResolveUniformRange(subject, operation, rangeStart, rangeEnd, userBeatsGroup, out var rangeMatch))
        {
            var uniformRange = allTreesBucket is null ? rangeMatch : ResolveTiered(rangeMatch, allTreesMatch);
            if (TieredEffect(uniformRange, default, options.DefaultEffect) == LatticeEffect.Allow)
            {
                match = uniformRange;
                return FromMatch(uniformRange, options.DefaultEffect, subject, treeId);
            }
        }

        // Return a Filtered decision whose predicate applies the identical tiered
        // algorithm per candidate key. The all-trees verdict is whole-tree, hence
        // uniform across keys, so it is resolved once outside the closure alongside
        // the existing captures and folded into each per-key decision.
        return OperatorFiltered(
            tree,
            subject,
            operation,
            userBeatsGroup,
            allTreesMatch,
            options.DefaultEffect,
            BuildRangeReason(treeId, rangeStart, rangeEnd));
    }

    /// <summary>
    /// Builds the operator-only per-key filtered decision. The closure lives in this
    /// helper, not in <see cref="Evaluate(CompiledPolicy, LatticeAuthOptions, in LatticeSubject, string, LatticeOperation, string?, string?, string?, bool, out PolicyMatch)"/>,
    /// because a lambda capturing a method parameter makes the compiler allocate its
    /// closure at method entry: inline, every decision - every point allow included -
    /// paid for a closure only the filtered path uses.
    /// </summary>
    private static LatticeAccessDecision OperatorFiltered(
        CompiledTree tree,
        LatticeSubject subject,
        LatticeOperation operation,
        bool userBeatsGroup,
        PolicyMatch allTreesMatch,
        LatticeEffect defaultEffect,
        string reason) =>
        LatticeAccessDecision.Filtered(
            candidateKey =>
            {
                var m = tree.ResolvePoint(subject, operation, candidateKey, userBeatsGroup);
                var effect = TieredEffect(m, allTreesMatch, defaultEffect);
                return effect == LatticeEffect.Allow;
            },
            reason);

    /// <summary>
    /// The two-layer evaluation for a tree the tenant layer governs. See the remarks
    /// on <see cref="Evaluate(CompiledPolicy, LatticeAuthOptions, in LatticeSubject, string, LatticeOperation, string?, string?, string?, bool, out PolicyMatch)"/>
    /// for the algorithm and the filter composition rule.
    /// </summary>
    private static LatticeAccessDecision EvaluateLayered(
        CompiledPolicy policy,
        LatticeAuthOptions options,
        in LatticeSubject subject,
        string treeId,
        LatticeOperation operation,
        string? key,
        string? rangeStart,
        string? rangeEnd,
        CompiledTree? tenantTree,
        CompiledTree? tenantWide,
        out PolicyMatch match)
    {
        var hasTree = policy.TryGetTree(treeId, out var tree);
        var userBeatsGroup = options.UserRuleBeatsGroupRuleAtEqualScope;
        var allTreesMatch = ShouldConsultAllTrees(policy, options, treeId)
            ? ResolveAllTrees(policy.AllTrees!, subject, operation, userBeatsGroup)
            : default;
        var tenantWideMatch = tenantWide is null
            ? default
            : tenantWide.ResolvePoint(subject, operation, key: null, userBeatsGroup).AsTenantLayer(tenantWide: true);

        // Point request: the operator verdict is final when it matched; otherwise the
        // tenant layer's tiered verdict (an unmatched value means the default effect).
        if (key is not null)
        {
            var specific = hasTree ? tree!.ResolvePoint(subject, operation, key, userBeatsGroup) : default;
            var operatorMatch = ResolveTiered(specific, allTreesMatch);
            match = operatorMatch.Matched
                ? operatorMatch
                : ResolveTenant(tenantTree, tenantWideMatch, subject, operation, key, userBeatsGroup);
            return FromMatch(match, options.DefaultEffect, subject, treeId);
        }

        // Collection request. When the operator layer has no per-key rules its verdict
        // is the same for every key: a match decides the whole collection, and only an
        // unmatched operator verdict lets the tenant layer speak.
        if (!hasTree || !tree!.HasPerKeyRules)
        {
            var operatorUniform = ResolveTiered(
                hasTree ? tree!.ResolvePoint(subject, operation, key: null, userBeatsGroup) : default,
                allTreesMatch);
            if (operatorUniform.Matched)
            {
                match = operatorUniform;
                return FromMatch(match, options.DefaultEffect, subject, treeId);
            }

            if (tenantTree is null || !tenantTree.HasPerKeyRules)
            {
                match = ResolveTenant(tenantTree, tenantWideMatch, subject, operation, key: null, userBeatsGroup);
                return FromMatch(match, options.DefaultEffect, subject, treeId);
            }
        }

        // A matched operator verdict is final (D8), so when the operator layer
        // resolves every key of the range to the same allowing rule (#4278), the
        // tenant layer cannot change any key's decision and the collection is a plain
        // allow, exactly as on an operator-only tree.
        if (hasTree
            && tree!.HasPerKeyRules
            && tree.TryResolveUniformRange(subject, operation, rangeStart, rangeEnd, userBeatsGroup, out var operatorRange))
        {
            var operatorUniformRange = ResolveTiered(operatorRange, allTreesMatch);
            if (operatorUniformRange.Matched && operatorUniformRange.Effect == LatticeEffect.Allow)
            {
                match = operatorUniformRange;
                return FromMatch(match, options.DefaultEffect, subject, treeId);
            }
        }

        // Some layer can vary key-by-key: the filter applies the point composition per
        // candidate key. Whole-tree verdicts (all-trees, tenant-wide) are uniform across
        // keys, so they are resolved once and captured.
        match = default;
        return LayeredFiltered(
            hasTree ? tree : null,
            tenantTree,
            allTreesMatch,
            tenantWideMatch,
            subject,
            operation,
            userBeatsGroup,
            options.DefaultEffect,
            BuildRangeReason(treeId, rangeStart, rangeEnd));
    }

    /// <summary>
    /// Builds the two-layer per-key filtered decision. Kept out of
    /// <see cref="EvaluateLayered"/> for the same reason as
    /// <see cref="OperatorFiltered"/>: the closure is then allocated only on the
    /// filtered path, never on a point decision.
    /// </summary>
    private static LatticeAccessDecision LayeredFiltered(
        CompiledTree? operatorTree,
        CompiledTree? tenantTree,
        PolicyMatch allTreesMatch,
        PolicyMatch tenantWideMatch,
        LatticeSubject subject,
        LatticeOperation operation,
        bool userBeatsGroup,
        LatticeEffect defaultEffect,
        string reason) =>
        LatticeAccessDecision.Filtered(
            candidateKey => LayeredEffect(
                operatorTree,
                tenantTree,
                allTreesMatch,
                tenantWideMatch,
                subject,
                operation,
                candidateKey,
                userBeatsGroup,
                defaultEffect) == LatticeEffect.Allow,
            reason);

    /// <summary>
    /// The two-layer effect for one key: the operator layer's tiered verdict when it
    /// matched (all-trees deny, then the tree's most-specific rule, then all-trees
    /// allow), otherwise the tenant layer's (tenant-wide deny, then the tenant tree's
    /// most-specific rule, then tenant-wide allow), otherwise the default effect.
    /// Allocation-free; the body of every layered key filter.
    /// </summary>
    internal static LatticeEffect LayeredEffect(
        CompiledTree? operatorTree,
        CompiledTree? tenantTree,
        in PolicyMatch allTreesMatch,
        in PolicyMatch tenantWideMatch,
        in LatticeSubject subject,
        LatticeOperation operation,
        string key,
        bool userBeatsGroup,
        LatticeEffect defaultEffect)
    {
        if (allTreesMatch.Matched && allTreesMatch.Effect == LatticeEffect.Deny)
        {
            return LatticeEffect.Deny;
        }

        if (operatorTree is not null)
        {
            var specific = operatorTree.ResolvePoint(subject, operation, key, userBeatsGroup);
            if (specific.Matched)
            {
                return specific.Effect;
            }
        }

        if (allTreesMatch.Matched)
        {
            return LatticeEffect.Allow;
        }

        if (tenantWideMatch.Matched && tenantWideMatch.Effect == LatticeEffect.Deny)
        {
            return LatticeEffect.Deny;
        }

        if (tenantTree is not null)
        {
            var tenantSpecific = tenantTree.ResolvePoint(subject, operation, key, userBeatsGroup);
            if (tenantSpecific.Matched)
            {
                return tenantSpecific.Effect;
            }
        }

        return tenantWideMatch.Matched ? LatticeEffect.Allow : defaultEffect;
    }

    /// <summary>
    /// The tenant layer's tiered verdict for one key (or, with a <c>null</c> key, for
    /// the tree as a whole), labelled as a tenant-layer match.
    /// </summary>
    private static PolicyMatch ResolveTenant(
        CompiledTree? tenantTree,
        in PolicyMatch tenantWideMatch,
        in LatticeSubject subject,
        LatticeOperation operation,
        string? key,
        bool userBeatsGroup)
    {
        var specific = tenantTree is null
            ? default
            : tenantTree.ResolvePoint(subject, operation, key, userBeatsGroup).AsTenantLayer(tenantWide: false);
        return ResolveTiered(specific, tenantWideMatch);
    }

    /// <summary>
    /// Whether the all-trees (<c>Tree:*</c>) tier participates in this evaluation:
    /// the opt-in flag is set, a compiled <c>"*"</c> bucket exists, and the target
    /// tree is a genuine application tree - not a control-plane namespace (the
    /// reserved authorization namespace <c>sys-auth-*</c>, the tenant-registry and
    /// app-registry system-data namespaces <c>sys-tenant-*</c> and <c>sys-app-*</c>,
    /// or the tenant-administration
    /// capability namespace <c>_lattice_tenant_admin_*</c>) and not the sentinel id
    /// <c>"*"</c> itself. The control-plane exclusion is the fail-closed guard that
    /// keeps a wildcard data grant from ever reaching the control plane -
    /// membership, policy, the cross-tenant registry, the app registry's ceilings
    /// and role bindings, or a delegated tenant-admin
    /// capability - so an all-trees read cannot exfiltrate tenant metadata and an
    /// all-trees <see cref="LatticeOperation.Admin"/> grant cannot be laundered into
    /// tenant administration over every tenant; the sentinel exclusion keeps a
    /// literal telemetry request on <c>"*"</c> resolving against its own bucket
    /// exactly as before, with no second all-trees fold.
    /// </summary>
    private static bool ShouldConsultAllTrees(CompiledPolicy policy, LatticeAuthOptions options, string treeId)
    {
        if (!options.AllTreesGrantsEnabled || policy.AllTrees is null)
        {
            return false;
        }

        if (IsClusterWideCapabilityRequest(treeId))
        {
            return false;
        }

        return !LatticeAuthReservedTrees.IsReserved(treeId)
            && !AuthConstants.IsControlPlaneRegistryTree(treeId)
            && !IsTenantAdminCapabilityNamespace(treeId);
    }

    /// <summary>
    /// Whether <paramref name="treeId"/> names a delegated per-tenant-administration
    /// capability scope (<see cref="LatticeTenantAdminScope.TenantScopePrefix"/>).
    /// Such an id is a control-plane capability, not an application tree, so the
    /// all-trees tier must never fold into it: the id starts with neither
    /// <c>sys-auth-</c> nor <c>sys-tenant-</c>, so without this test it was the one
    /// control-plane namespace a <c>Tree:*</c> wildcard could still reach.
    /// Mirrors <c>PolicyAccessGate.IsTenantAdminCapabilityNamespace</c>, which routes
    /// the same ids to the fail-closed control-plane branch.
    /// </summary>
    private static bool IsTenantAdminCapabilityNamespace(string treeId) =>
        treeId.StartsWith(LatticeTenantAdminScope.TenantScopePrefix, StringComparison.Ordinal);

    /// <summary>
    /// Whether the request targets the all-trees sentinel itself, which means a
    /// <b>scopeless cluster-wide capability</b> check (notably
    /// <see cref="LatticeOperation.Telemetry"/>) rather than a data-plane request:
    /// an ordinary read or write always names a real tree. Such a request resolves
    /// against the <c>"*"</c> bucket's tree-wide tier directly, with no second
    /// all-trees fold and no per-key narrowing.
    /// </summary>
    private static bool IsClusterWideCapabilityRequest(string treeId) =>
        string.Equals(treeId, LatticeScope.ClusterWideTreeId, StringComparison.Ordinal);

    /// <summary>
    /// Resolves the all-trees verdict: the whole-tree resolution of the <c>"*"</c>
    /// bucket for the subject and operation, marked as originating from the
    /// all-trees tier so a decision reason can render "all trees".
    /// </summary>
    private static PolicyMatch ResolveAllTrees(
        CompiledTree allTreesBucket,
        in LatticeSubject subject,
        LatticeOperation operation,
        bool userBeatsGroup)
    {
        var m = allTreesBucket.ResolvePoint(subject, operation, key: null, userBeatsGroup);
        return m.Matched
            ? new PolicyMatch(m.Effect, m.RuleId!, m.ScopeKind, m.ScopeValue, allTrees: true)
            : default;
    }

    /// <summary>
    /// Applies the four-tier precedence to a specific-tree match and an all-trees
    /// match and returns the winning <see cref="PolicyMatch"/> (a default, unmatched
    /// value means the caller applies its default effect). See
    /// <see cref="LatticeAuthOptions.AllTreesGrantsEnabled"/> for the tier rules.
    /// </summary>
    private static PolicyMatch ResolveTiered(in PolicyMatch specific, in PolicyMatch allTrees)
    {
        // Tier 1: an all-trees deny wins outright.
        if (allTrees.Matched && allTrees.Effect == LatticeEffect.Deny)
        {
            return allTrees;
        }

        // Tier 2: the specific tree's own most-specific-wins verdict.
        if (specific.Matched)
        {
            return specific;
        }

        // Tier 3: an all-trees allow (the only remaining matched all-trees effect).
        // Tier 4 (default effect) is signalled by the default, unmatched value.
        return allTrees;
    }

    /// <summary>
    /// The effect form of <see cref="ResolveTiered"/> for the per-key range
    /// predicate: returns the winning effect, folding in the caller's default
    /// effect for Tier 4 so the predicate never allocates a <see cref="PolicyMatch"/>.
    /// </summary>
    private static LatticeEffect TieredEffect(in PolicyMatch specific, in PolicyMatch allTrees, LatticeEffect defaultEffect)
    {
        if (allTrees.Matched && allTrees.Effect == LatticeEffect.Deny)
        {
            return LatticeEffect.Deny;
        }

        if (specific.Matched)
        {
            return specific.Effect;
        }

        return allTrees.Matched ? LatticeEffect.Allow : defaultEffect;
    }

    /// <summary>
    /// <c>true</c> when <paramref name="subject"/> can read at least one key of
    /// <paramref name="treeId"/> under <paramref name="operation"/> - the
    /// structural "any grant" signal that existence-hiding needs. The probe mirrors
    /// the enforcement tiers so it never out-reaches them: an all-trees deny (tier 1)
    /// or a whole-tree deny with no allow carve-out (tier 2) removes the entire
    /// keyspace, which enforcement resolves as a deny for every key, so the probe
    /// hides the tree. Otherwise, under default allow a subject reads every tree it
    /// is not explicitly denied on; under default deny the subject needs at least one
    /// allow rule whose effective decision at its own scope resolves to allow (see
    /// <see cref="CompiledTree.HasAnyResolvedAllow"/>) or an all-trees allow. This
    /// distinguishes a partial (prefix) grant - which must keep the tree visible -
    /// from no grant at all, which a plain collection decision cannot do (that is
    /// allow-with-filter for every subject once the tree carries per-key rules).
    /// </summary>
    public static bool HasAnyGrant(
        CompiledPolicy policy,
        LatticeAuthOptions options,
        in LatticeSubject subject,
        string treeId,
        LatticeOperation operation) =>
        HasAnyGrant(policy, options, subject, treeId, operation, tenantLayerActive: false);

    /// <summary>
    /// The tenant-layer-aware form of
    /// <see cref="HasAnyGrant(CompiledPolicy, LatticeAuthOptions, in LatticeSubject, string, LatticeOperation)"/>.
    /// When the tenant layer does not govern the tree this is exactly the
    /// operator-only probe. When it does, the probe asks whether the two-layer
    /// composition (<see cref="LayeredEffect"/>) allows at least one key: it
    /// evaluates the composition at every position a rule of either layer can change
    /// the decision at - a key no rule names, each exact key, and each prefix (below
    /// its exact tier) - so, like the operator-only probe, it never reports a grant
    /// that enforcement would deny for every key.
    /// </summary>
    /// <param name="policy">The compiled snapshot to probe.</param>
    /// <param name="options">The tie-break and default-effect options.</param>
    /// <param name="subject">The requesting subject.</param>
    /// <param name="treeId">The target tree id.</param>
    /// <param name="operation">The operation whose grant is probed.</param>
    /// <param name="tenantLayerActive">Whether the tenant layer is active.</param>
    /// <returns><see langword="true"/> when the subject can read at least one key.</returns>
    public static bool HasAnyGrant(
        CompiledPolicy policy,
        LatticeAuthOptions options,
        in LatticeSubject subject,
        string treeId,
        LatticeOperation operation,
        bool tenantLayerActive)
    {
        if (tenantLayerActive
            && policy.Tenant is { } partition
            && partition.TryGetBuckets(treeId, out var tenantTree, out var tenantWide))
        {
            return HasAnyLayeredGrant(policy, options, subject, treeId, operation, tenantTree, tenantWide);
        }

        return HasAnyOperatorGrant(policy, options, subject, treeId, operation);
    }

    private static bool HasAnyLayeredGrant(
        CompiledPolicy policy,
        LatticeAuthOptions options,
        in LatticeSubject subject,
        string treeId,
        LatticeOperation operation,
        CompiledTree? tenantTree,
        CompiledTree? tenantWide)
    {
        var userBeatsGroup = options.UserRuleBeatsGroupRuleAtEqualScope;
        var operatorTree = policy.TryGetTree(treeId, out var tree) ? tree : null;
        var allTreesMatch = ShouldConsultAllTrees(policy, options, treeId)
            ? ResolveAllTrees(policy.AllTrees!, subject, operation, userBeatsGroup)
            : default;
        var tenantWideMatch = tenantWide is null
            ? default
            : tenantWide.ResolvePoint(subject, operation, key: null, userBeatsGroup).AsTenantLayer(tenantWide: true);

        // A key no rule names: only whole-tree rules of either layer apply.
        if (AllowsAt(operatorTree, tenantTree, allTreesMatch, tenantWideMatch, subject, operation, null, exact: false, userBeatsGroup, options.DefaultEffect))
        {
            return true;
        }

        foreach (var source in new[] { operatorTree, tenantTree })
        {
            if (source is null)
            {
                continue;
            }

            foreach (var exactKey in source.ExactKeys)
            {
                if (AllowsAt(operatorTree, tenantTree, allTreesMatch, tenantWideMatch, subject, operation, exactKey, exact: true, userBeatsGroup, options.DefaultEffect))
                {
                    return true;
                }
            }

            foreach (var prefix in source.Prefixes)
            {
                if (AllowsAt(operatorTree, tenantTree, allTreesMatch, tenantWideMatch, subject, operation, prefix, exact: false, userBeatsGroup, options.DefaultEffect))
                {
                    return true;
                }
            }
        }

        return false;
    }

    /// <summary>
    /// The two-layer composition at one probe position: an exact key (full
    /// resolution), or a prefix / unnamed key (resolution below the exact tier, so
    /// the keys the position stands for are the ones no exact rule names).
    /// </summary>
    private static bool AllowsAt(
        CompiledTree? operatorTree,
        CompiledTree? tenantTree,
        in PolicyMatch allTreesMatch,
        in PolicyMatch tenantWideMatch,
        in LatticeSubject subject,
        LatticeOperation operation,
        string? position,
        bool exact,
        bool userBeatsGroup,
        LatticeEffect defaultEffect)
    {
        var operatorSpecific = Resolve(operatorTree, subject, operation, position, exact, userBeatsGroup);
        var operatorMatch = ResolveTiered(operatorSpecific, allTreesMatch);
        if (operatorMatch.Matched)
        {
            return operatorMatch.Effect == LatticeEffect.Allow;
        }

        var tenantSpecific = Resolve(tenantTree, subject, operation, position, exact, userBeatsGroup);
        var tenantMatch = ResolveTiered(tenantSpecific, tenantWideMatch);
        return tenantMatch.Matched ? tenantMatch.Effect == LatticeEffect.Allow : defaultEffect == LatticeEffect.Allow;

        static PolicyMatch Resolve(
            CompiledTree? source,
            in LatticeSubject subject,
            LatticeOperation operation,
            string? position,
            bool exact,
            bool userBeatsGroup) =>
            source is null
                ? default
                : exact
                    ? source.ResolvePoint(subject, operation, position, userBeatsGroup)
                    : source.ResolveBelowExactTier(subject, operation, position, userBeatsGroup);
    }

    private static bool HasAnyOperatorGrant(
        CompiledPolicy policy,
        LatticeAuthOptions options,
        in LatticeSubject subject,
        string treeId,
        LatticeOperation operation)
    {
        // Tier 1: an all-trees deny wins outright over every specific-tree rule and
        // over the default effect, and the all-trees verdict is resolved tree-wide
        // (hence uniform across keys), so it removes the entire keyspace. Enforcement
        // resolves deny for every key, so the probe hides the tree rather than
        // out-reaching that decision.
        if (HasAllTreesDeny(policy, options, subject, treeId, operation))
        {
            return false;
        }

        var hasTree = policy.TryGetTree(treeId, out var tree) && tree is not null;

        if (options.DefaultEffect == LatticeEffect.Allow)
        {
            // Default-allow: a subject reads every tree it is not explicitly denied
            // on. But a whole-tree deny with no allow carve-out removes the entire
            // keyspace - enforcement resolves that tree-wide deny for every key, so
            // the subject can read nothing. An existence probe must never out-reach
            // that enforcement decision (see PolicyAccessGate), so hide such a tree
            // rather than reporting a grant the subject does not have.
            return !hasTree || !DeniesEveryKey(tree!, options, subject, operation);
        }

        if (!hasTree)
        {
            // No specific-tree rules; the all-trees tier may still grant.
            return HasAllTreesAllow(policy, options, subject, treeId, operation);
        }

        if (tree!.HasAnyResolvedAllow(subject, operation, options.UserRuleBeatsGroupRuleAtEqualScope))
        {
            return true;
        }

        // Tier 2: the specific tree's own verdict beats an all-trees allow, so a
        // whole-tree deny with no allow carve-out denies every key and the all-trees
        // allow below is never reached by enforcement.
        if (DeniesEveryKey(tree!, options, subject, operation))
        {
            return false;
        }

        return HasAllTreesAllow(policy, options, subject, treeId, operation);
    }

    /// <summary>
    /// <c>true</c> when <paramref name="tree"/>'s own rules deny every one of its
    /// keys for <paramref name="subject"/> and <paramref name="operation"/>: the
    /// whole-tree scope resolves deny and no rule at any scope resolves allow, so
    /// every key falls through to that tree-wide deny. Enforcement then resolves
    /// deny for every key (tier 2 of <see cref="ResolveTiered"/>, which beats an
    /// all-trees allow), so an existence probe must hide such a tree.
    /// </summary>
    private static bool DeniesEveryKey(
        CompiledTree tree,
        LatticeAuthOptions options,
        in LatticeSubject subject,
        LatticeOperation operation)
    {
        var treeWide = tree.ResolvePoint(
            subject, operation, key: null, options.UserRuleBeatsGroupRuleAtEqualScope);
        return treeWide.Matched
            && treeWide.Effect == LatticeEffect.Deny
            && !tree.HasAnyResolvedAllow(subject, operation, options.UserRuleBeatsGroupRuleAtEqualScope);
    }

    /// <summary>
    /// <c>true</c> when the all-trees (<c>Tree:*</c>) tier resolves a whole-tree
    /// <b>deny</b> for <paramref name="subject"/> and <paramref name="operation"/> on
    /// <paramref name="treeId"/>. Tier 1 of <see cref="ResolveTiered"/> gives that
    /// deny precedence over every specific-tree rule and over the default effect,
    /// and the all-trees verdict is resolved tree-wide - hence uniform across keys -
    /// so it removes the entire keyspace. Gated exactly as enforcement through
    /// <see cref="ShouldConsultAllTrees"/>.
    /// </summary>
    private static bool HasAllTreesDeny(
        CompiledPolicy policy,
        LatticeAuthOptions options,
        in LatticeSubject subject,
        string treeId,
        LatticeOperation operation)
    {
        if (!ShouldConsultAllTrees(policy, options, treeId))
        {
            return false;
        }

        var all = policy.AllTrees!.ResolvePoint(
            subject, operation, key: null, options.UserRuleBeatsGroupRuleAtEqualScope);
        return all.Matched && all.Effect == LatticeEffect.Deny;
    }

    /// <summary>
    /// <c>true</c> when the all-trees (<c>Tree:*</c>) tier grants
    /// <paramref name="operation"/> to <paramref name="subject"/> on
    /// <paramref name="treeId"/> - a whole-tree allow on the <c>"*"</c> bucket -
    /// so a tree reachable only through a wildcard grant is not hidden from
    /// listings while being readable. Gated exactly as enforcement: skipped when
    /// the flag is off, no <c>"*"</c> bucket exists, or the tree is a control-plane
    /// namespace (reserved authorization or tenant registry) / the sentinel. A
    /// wildcard <b>deny</b> is handled ahead of this by
    /// <see cref="HasAllTreesDeny"/>, which hides the tree outright because tier 1
    /// gives that deny precedence over every other rule.
    /// </summary>
    private static bool HasAllTreesAllow(
        CompiledPolicy policy,
        LatticeAuthOptions options,
        in LatticeSubject subject,
        string treeId,
        LatticeOperation operation)
    {
        if (!ShouldConsultAllTrees(policy, options, treeId))
        {
            return false;
        }

        var all = policy.AllTrees!.ResolvePoint(subject, operation, key: null, options.UserRuleBeatsGroupRuleAtEqualScope);
        return all.Matched && all.Effect == LatticeEffect.Allow;
    }

    private static LatticeAccessDecision FromMatch(
        in PolicyMatch match,
        LatticeEffect defaultEffect,
        in LatticeSubject subject,
        string treeId)
    {
        if (!match.Matched)
        {
            return defaultEffect == LatticeEffect.Allow
                ? LatticeAccessDecision.Allow()
                : LatticeAccessDecision.Deny(
                    $"No matching rule for subject '{subject.SubjectId}' on tree '{treeId}'; applied default effect Deny.");
        }

        return match.Effect == LatticeEffect.Allow
            ? LatticeAccessDecision.Allow()
            : LatticeAccessDecision.Deny(BuildDenyReason(match, subject, treeId));
    }

    private static string BuildDenyReason(in PolicyMatch match, in LatticeSubject subject, string treeId)
    {
        var scope = match.AllTrees
            ? "all trees"
            : match.TenantWide
            ? "tenant-wide"
            : match.ScopeKind switch
            {
                LatticeScopeKind.Key => $"key '{match.ScopeValue}'",
                LatticeScopeKind.Prefix => $"prefix '{match.ScopeValue}'",
                _ => "tree",
            };

        return match.Layer == PolicyDecisionLayer.Tenant
            ? $"Denied by tenant rule '{match.RuleId}' ({scope} scope) for subject '{subject.SubjectId}' on tree '{treeId}'."
            : $"Denied by rule '{match.RuleId}' ({scope} scope) for subject '{subject.SubjectId}' on tree '{treeId}'.";
    }

    private static string BuildRangeReason(string treeId, string? rangeStart, string? rangeEnd)
    {
        var start = rangeStart ?? "(start)";
        var end = rangeEnd ?? "(end)";
        return $"Range read over tree '{treeId}' [{start}, {end}) filtered per-key by policy.";
    }
}
