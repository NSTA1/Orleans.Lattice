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
    /// Ranks <paramref name="rules"/> from highest precedence to lowest: more
    /// specific scopes first (a key beats a prefix, a longer prefix beats a
    /// shorter one, any of which beats a whole-tree scope), and within an equal
    /// specificity a <see cref="LatticeEffect.Deny"/> ranks ahead of an
    /// <see cref="LatticeEffect.Allow"/> (deny-override). Ties break on rule id
    /// for a stable order.
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

        // Highest specificity first; deny ahead of allow at equal specificity;
        // then rule id for a deterministic tie-break. A plain in-place sort keeps
        // this allocation-light for the small matched-rule sets it ranks.
        ranked.Sort(static (a, b) =>
        {
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
    /// Scores a scope's specificity: a key scope is the most specific, a prefix
    /// scope is more specific the longer its prefix, and a whole-tree scope is the
    /// least specific. Higher scores rank ahead.
    /// </summary>
    /// <param name="scope">The scope to score. Must not be <see langword="null"/>.</param>
    /// <returns>The specificity score.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="scope"/> is <see langword="null"/>.</exception>
    public static int SpecificityOf(LatticeScope scope)
    {
        ArgumentNullException.ThrowIfNull(scope);
        return scope.Kind switch
        {
            LatticeScopeKind.Key => 2_000_000,
            LatticeScopeKind.Prefix => 1_000_000 + (scope.KeyOrPrefix?.Length ?? 0),
            _ => 0,
        };
    }
}
