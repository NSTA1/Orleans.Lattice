using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Explorer.Access;

/// <summary>
/// A presentation-only ordering of authorization rules that surfaces the two
/// precedence principles the decision engine applies - <b>deny-override</b> and
/// <b>most-specific-wins</b> - so an operator can read a set of matched or
/// effective rules and understand which one would decide a request. This is a UX
/// aid over data the facade already returned; it never re-implements the verdict
/// (the authoritative decision is always the facade's <c>Allowed</c> flag on an
/// <see cref="Orleans.Lattice.Api.Auth.AuthExplanation"/>).
/// </summary>
public static class RulePrecedence
{
    /// <summary>
    /// Ranks <paramref name="rules"/> from highest precedence to lowest, mirroring
    /// the decision engine's verdict ordering: an all-trees (<c>Tree:*</c>) deny
    /// sorts to the very top (a cluster-wide deny is never overridden), then the
    /// specific-tree rules ordered by scope - more specific scopes first (a key
    /// beats a prefix, a longer prefix beats a shorter one, any of which beats a
    /// whole-tree scope), deny ahead of allow at equal specificity (deny-override) -
    /// then an all-trees <b>allow</b> sorts to the bottom (a specific-tree rule
    /// outranks a cluster-wide allow). Ties break on rule id for a stable order.
    /// </summary>
    /// <param name="rules">The rules to rank. Must not be <see langword="null"/>.</param>
    /// <returns>The ranked rules, highest precedence first.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="rules"/> is <see langword="null"/>.</exception>
    public static IReadOnlyList<RankedRule> Rank(IEnumerable<LatticeAuthorizationRule> rules)
    {
        ArgumentNullException.ThrowIfNull(rules);

        var ranked = new List<RankedRule>();
        foreach (var rule in rules)
        {
            ranked.Add(new RankedRule
            {
                Rule = rule,
                Specificity = SpecificityOf(rule.Scope),
                DenyOverrides = rule.Effect == LatticeEffect.Deny,
            });
        }

        // Highest tier first (all-trees deny > specific > all-trees allow), then
        // highest specificity, then deny ahead of allow at equal specificity, then
        // rule id for a deterministic tie-break. A plain in-place sort keeps this
        // allocation-light for the small matched-rule sets it ranks.
        ranked.Sort(static (a, b) =>
        {
            var byTier = PrecedenceTier(b.Rule).CompareTo(PrecedenceTier(a.Rule));
            if (byTier != 0)
            {
                return byTier;
            }

            var bySpecificity = b.Specificity.CompareTo(a.Specificity);
            if (bySpecificity != 0)
            {
                return bySpecificity;
            }

            var byEffect = b.DenyOverrides.CompareTo(a.DenyOverrides);
            return byEffect != 0
                ? byEffect
                : string.CompareOrdinal(a.Rule.RuleId, b.Rule.RuleId);
        });

        return ranked;
    }

    /// <summary>
    /// The coarse precedence tier a rule occupies, reproducing the engine's
    /// four-tier all-trees algorithm: an all-trees deny is Tier <c>2</c> (top,
    /// wins outright), an ordinary specific-tree rule is Tier <c>1</c> (middle),
    /// and an all-trees allow is Tier <c>0</c> (bottom, a specific-tree rule
    /// outranks it). Rules within the same tier are ordered by scope specificity
    /// and deny-override.
    /// </summary>
    private static int PrecedenceTier(LatticeAuthorizationRule rule)
    {
        if (!IsAllTrees(rule.Scope))
        {
            return 1;
        }

        return rule.Effect == LatticeEffect.Deny ? 2 : 0;
    }

    /// <summary>
    /// <see langword="true"/> when <paramref name="scope"/> is the all-trees
    /// (<c>Tree:*</c>) whole-tree scope over
    /// <see cref="LatticeScope.ClusterWideTreeId"/>.
    /// </summary>
    private static bool IsAllTrees(LatticeScope scope) =>
        scope.Kind == LatticeScopeKind.Tree
        && string.Equals(scope.TreeId, LatticeScope.ClusterWideTreeId, StringComparison.Ordinal);

    /// <summary>
    /// Scores a scope's specificity: a key scope is the most specific, a prefix
    /// scope is more specific the longer its prefix, a specific whole-tree scope is
    /// the least specific of the tree-local scopes (<c>0</c>), and the all-trees
    /// (<c>Tree:*</c>) scope scores below every specific scope (<c>-1</c>) so a
    /// cluster-wide allow ranks beneath a specific whole-tree rule. Higher scores
    /// rank ahead. The all-trees deny's top placement is handled by the coarse
    /// precedence tier in <see cref="Rank"/>, not by this score.
    /// </summary>
    /// <param name="scope">The scope to score. Must not be <see langword="null"/>.</param>
    /// <returns>The specificity score.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="scope"/> is <see langword="null"/>.</exception>
    public static int SpecificityOf(LatticeScope scope)
    {
        ArgumentNullException.ThrowIfNull(scope);
        if (IsAllTrees(scope))
        {
            return -1;
        }

        return scope.Kind switch
        {
            LatticeScopeKind.Key => 2_000_000,
            LatticeScopeKind.Prefix => 1_000_000 + (scope.KeyOrPrefix?.Length ?? 0),
            _ => 0,
        };
    }
}
