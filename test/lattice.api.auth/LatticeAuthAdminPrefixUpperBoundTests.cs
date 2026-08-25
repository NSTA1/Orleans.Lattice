using Orleans.Lattice.Api.Auth;

namespace Orleans.Lattice.Api.Auth.Tests;

/// <summary>
/// Regression coverage for <see cref="LatticeAuthAdmin.PrefixUpperBound"/>, the
/// helper that <c>TranslateScope</c> uses to turn a <c>Prefix</c>-scoped
/// introspection request into the exclusive upper bound handed to the access
/// gate as <c>LatticeAccessRequest.RangeEnd</c>. The bound must be strictly
/// greater than every key sharing the prefix; a trailing <c>U+FFFF</c> code unit
/// must roll over rather than wrap to <c>U+0000</c> (which would invert the
/// half-open range and silently drop matching rules from the explained verdict).
/// </summary>
public class LatticeAuthAdminPrefixUpperBoundTests
{
    [Test]
    public void PrefixUpperBound_increments_last_unit_for_an_ordinary_prefix()
    {
        Assert.That(LatticeAuthAdmin.PrefixUpperBound("abc"), Is.EqualTo("abd"));
    }

    [Test]
    public void PrefixUpperBound_is_strictly_greater_than_the_prefix()
    {
        const string prefix = "tenant-42:";
        var bound = LatticeAuthAdmin.PrefixUpperBound(prefix);

        Assert.That(bound, Is.Not.Null);
        Assert.That(string.CompareOrdinal(bound, prefix), Is.GreaterThan(0));
    }

    [Test]
    public void PrefixUpperBound_rolls_over_a_trailing_max_code_unit()
    {
        // Regression: a prefix ending in U+FFFF must drop the trailing max unit
        // and increment the previous one, yielding a bound that still sorts
        // strictly above the prefix. The pre-fix unconditional increment wrapped
        // U+FFFF to U+0000, producing "a\u0000" - which sorts BELOW "a\uFFFF" and
        // inverts the [prefix, bound) range.
        var prefix = "a" + '\uFFFF';
        var bound = LatticeAuthAdmin.PrefixUpperBound(prefix);

        Assert.That(bound, Is.EqualTo("b"));
        Assert.That(string.CompareOrdinal(bound, prefix), Is.GreaterThan(0));
    }

    [Test]
    public void PrefixUpperBound_returns_null_when_no_finite_upper_bound_exists()
    {
        // A prefix consisting solely of U+FFFF units has no finite exclusive upper
        // bound: the range is unbounded above and RangeEnd must be null, not a
        // wrapped-around string that sorts below the prefix.
        var prefix = new string('\uFFFF', 3);

        Assert.That(LatticeAuthAdmin.PrefixUpperBound(prefix), Is.Null);
    }
}
