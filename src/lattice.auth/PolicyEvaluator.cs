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
        string? rangeEnd)
    {
        var hasTree = policy.TryGetTree(treeId, out var tree);
        var userBeatsGroup = options.UserRuleBeatsGroupRuleAtEqualScope;

        // Point request: resolve the single key.
        if (key is not null)
        {
            var match = hasTree ? tree!.ResolvePoint(subject, operation, key, userBeatsGroup) : default;
            return FromMatch(match, options.DefaultEffect, subject, treeId);
        }

        // Collection request (range read / whole-tree). When the tree carries no
        // per-key (exact/prefix) rules the decision is uniform, so return a plain
        // allow/deny. Otherwise the decision can vary key-by-key, so return a
        // Filtered decision whose predicate admits a key iff its point decision
        // is an allow.
        if (!hasTree || !tree!.HasPerKeyRules)
        {
            var uniform = hasTree ? tree!.ResolvePoint(subject, operation, key: null, userBeatsGroup) : default;
            return FromMatch(uniform, options.DefaultEffect, subject, treeId);
        }

        var defaultEffect = options.DefaultEffect;
        var reason = BuildRangeReason(treeId, rangeStart, rangeEnd);
        var capturedSubject = subject;
        var capturedTree = tree!;
        return LatticeAccessDecision.Filtered(
            candidateKey =>
            {
                var m = capturedTree.ResolvePoint(capturedSubject, operation, candidateKey, userBeatsGroup);
                var effect = m.Matched ? m.Effect : defaultEffect;
                return effect == LatticeEffect.Allow;
            },
            reason);
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
        var scope = match.ScopeKind switch
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
