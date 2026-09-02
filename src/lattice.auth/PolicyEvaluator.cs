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
    /// <param name="rangeStart">The inclusive range start, or <c>null</c>. Used only in the reason text.</param>
    /// <param name="rangeEnd">The exclusive range end, or <c>null</c>. Used only in the reason text.</param>
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
    /// <param name="rangeStart">The inclusive range start, or <c>null</c>. Used only in the reason text.</param>
    /// <param name="rangeEnd">The exclusive range end, or <c>null</c>. Used only in the reason text.</param>
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
        out PolicyMatch match)
    {
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

        // The tree carries per-key rules, so the decision can vary key-by-key.
        // Return a Filtered decision whose predicate applies the identical tiered
        // algorithm per candidate key. The all-trees verdict is whole-tree, hence
        // uniform across keys, so it is resolved once outside the closure alongside
        // the existing captures and folded into each per-key decision.
        var defaultEffect = options.DefaultEffect;
        var reason = BuildRangeReason(treeId, rangeStart, rangeEnd);
        var capturedSubject = subject;
        var capturedTree = tree!;
        var allTreesMatch = allTreesBucket is null
            ? default
            : ResolveAllTrees(allTreesBucket, subject, operation, userBeatsGroup);
        return LatticeAccessDecision.Filtered(
            candidateKey =>
            {
                var m = capturedTree.ResolvePoint(capturedSubject, operation, candidateKey, userBeatsGroup);
                var effect = TieredEffect(m, allTreesMatch, defaultEffect);
                return effect == LatticeEffect.Allow;
            },
            reason);
    }

    /// <summary>
    /// Whether the all-trees (<c>Tree:*</c>) tier participates in this evaluation:
    /// the opt-in flag is set, a compiled <c>"*"</c> bucket exists, and the target
    /// tree is a genuine application tree - not a control-plane namespace (the
    /// reserved authorization namespace <c>sys-auth-*</c>, the tenant-registry
    /// system-data namespace <c>sys-tenant-*</c>, or the tenant-administration
    /// capability namespace <c>_lattice_tenant_admin_*</c>) and not the sentinel id
    /// <c>"*"</c> itself. The control-plane exclusion is the fail-closed guard that
    /// keeps a wildcard data grant from ever reaching the control plane -
    /// membership, policy, the cross-tenant registry, or a delegated tenant-admin
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
            && !AuthConstants.IsTenantRegistryTree(treeId)
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
    /// structural "any grant" signal that existence-hiding needs. A non-anonymous
    /// subject reads by default when the tree's default effect is allow; otherwise
    /// it needs at least one allow rule whose effective decision at its own scope
    /// resolves to allow (see <see cref="CompiledTree.HasAnyResolvedAllow"/>). This
    /// distinguishes a partial (prefix) grant - which must keep the tree visible -
    /// from no grant at all, which a plain collection decision cannot do (that is
    /// allow-with-filter for every subject once the tree carries per-key rules).
    /// </summary>
    public static bool HasAnyGrant(
        CompiledPolicy policy,
        LatticeAuthOptions options,
        in LatticeSubject subject,
        string treeId,
        LatticeOperation operation)
    {
        if (options.DefaultEffect == LatticeEffect.Allow)
        {
            // Default-allow: a non-anonymous subject can read every tree it is not
            // explicitly denied on, so it can always read at least one key.
            return true;
        }

        if (!policy.TryGetTree(treeId, out var tree) || tree is null)
        {
            // No specific-tree rules; the all-trees tier may still grant.
            return HasAllTreesAllow(policy, options, subject, treeId, operation);
        }

        if (tree.HasAnyResolvedAllow(subject, operation, options.UserRuleBeatsGroupRuleAtEqualScope))
        {
            return true;
        }

        return HasAllTreesAllow(policy, options, subject, treeId, operation);
    }

    /// <summary>
    /// <c>true</c> when the all-trees (<c>Tree:*</c>) tier grants
    /// <paramref name="operation"/> to <paramref name="subject"/> on
    /// <paramref name="treeId"/> - a whole-tree allow on the <c>"*"</c> bucket -
    /// so a tree reachable only through a wildcard grant is not hidden from
    /// listings while being readable. Gated exactly as enforcement: skipped when
    /// the flag is off, no <c>"*"</c> bucket exists, or the tree is a control-plane
    /// namespace (reserved authorization or tenant registry) / the sentinel. A
    /// wildcard <b>deny</b> is deliberately not consulted here: existence-hiding is
    /// a pure "any resolved allow" signal, so a wildcard deny never hides a tree the
    /// subject can otherwise read.
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
            : match.ScopeKind switch
            {
                LatticeScopeKind.Key => $"key '{match.ScopeValue}'",
                LatticeScopeKind.Prefix => $"prefix '{match.ScopeValue}'",
                _ => "tree",
            };

        return $"Denied by rule '{match.RuleId}' ({scope} scope) for subject '{subject.SubjectId}' on tree '{treeId}'.";
    }

    private static string BuildRangeReason(string treeId, string? rangeStart, string? rangeEnd)
    {
        var start = rangeStart ?? "(start)";
        var end = rangeEnd ?? "(end)";
        return $"Range read over tree '{treeId}' [{start}, {end}) filtered per-key by policy.";
    }
}
