using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Regression coverage for <see cref="LatticeAuthorizationPolicyStore.PrefixUpperBound"/>,
/// the helper that <c>ListRulesForTreeAsync</c> uses to derive the exclusive
/// upper bound of the authorization-rule range scan for a tree. The bound must
/// be strictly greater than every key sharing the prefix; a trailing
/// <c>U+FFFF</c> code unit must roll over rather than wrap to <c>U+0000</c>
/// (which would invert the half-open range and silently yield an empty rule
/// set from an access-control read).
/// </summary>
public class LatticeAuthorizationPolicyStorePrefixUpperBoundTests
{
    [Test]
    public void PrefixUpperBound_increments_last_unit_for_an_ordinary_prefix()
    {
        Assert.That(LatticeAuthorizationPolicyStore.PrefixUpperBound("abc"), Is.EqualTo("abd"));
    }

    [Test]
    public void PrefixUpperBound_is_strictly_greater_than_the_prefix()
    {
        const string prefix = "tree-42\u001f";
        var bound = LatticeAuthorizationPolicyStore.PrefixUpperBound(prefix);

        Assert.That(bound, Is.Not.Null);
        Assert.That(string.CompareOrdinal(bound, prefix), Is.GreaterThan(0));
    }

    [Test]
    public void PrefixUpperBound_rolls_over_a_trailing_max_code_unit()
    {
        // Regression: a prefix ending in U+FFFF must drop the trailing max unit
        // and increment the previous one, yielding a bound that still sorts
        // strictly above the prefix. The pre-fix unconditional increment wrapped
        // U+FFFF to U+0000, producing "a\u0000" - which sorts BELOW "a\uFFFF"
        // and inverts the [prefix, bound) range so the rule scan captured
        // nothing.
        var prefix = "a" + '\uFFFF';
        var bound = LatticeAuthorizationPolicyStore.PrefixUpperBound(prefix);

        Assert.That(bound, Is.EqualTo("b"));
        Assert.That(string.CompareOrdinal(bound, prefix), Is.GreaterThan(0));
    }

    [Test]
    public void PrefixUpperBound_returns_null_when_no_finite_upper_bound_exists()
    {
        // A prefix consisting solely of U+FFFF units has no finite exclusive
        // upper bound: the range is unbounded above, so the scan end must be
        // null rather than a wrapped-around string that sorts below the prefix.
        var prefix = new string('\uFFFF', 3);

        Assert.That(LatticeAuthorizationPolicyStore.PrefixUpperBound(prefix), Is.Null);
    }
}
