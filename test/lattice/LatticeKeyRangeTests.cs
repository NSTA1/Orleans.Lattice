namespace Orleans.Lattice.Tests;

/// <summary>
/// Thorough coverage for <see cref="LatticeKeyRange.PrefixUpperBound(string)"/>,
/// the single shared definition of a prefix range's exclusive upper bound. The
/// bound must sort strictly above the prefix and above every key that starts
/// with it, must roll a trailing <c>U+FFFF</c> back rather than wrap it to
/// <c>U+0000</c> (which would invert the half-open <c>[prefix, bound)</c> range
/// and silently capture nothing), and must report an unbounded range as
/// <see langword="null"/>.
/// </summary>
[TestFixture]
public sealed class LatticeKeyRangeTests
{
    [Test]
    public void PrefixUpperBound_increments_the_last_code_unit()
    {
        Assert.That(LatticeKeyRange.PrefixUpperBound("abc"), Is.EqualTo("abd"));
    }

    [Test]
    public void PrefixUpperBound_of_a_single_unit_prefix_increments_it()
    {
        Assert.That(LatticeKeyRange.PrefixUpperBound("a"), Is.EqualTo("b"));
    }

    [Test]
    public void PrefixUpperBound_increments_only_the_last_unit_and_preserves_the_head()
    {
        // '/' (0x2F) is one below '0' (0x30): a common separator-terminated case.
        Assert.That(LatticeKeyRange.PrefixUpperBound("e/"), Is.EqualTo("e0"));
        Assert.That(LatticeKeyRange.PrefixUpperBound("repo/acme/"), Is.EqualTo("repo/acme0"));
    }

    [Test]
    public void PrefixUpperBound_rolls_over_a_single_trailing_max_unit()
    {
        // 'a' + U+FFFF: the trailing max unit is dropped and the preceding 'a'
        // is incremented to 'b'. The naive `chars[^1]++` produced "a\u0000".
        Assert.That(LatticeKeyRange.PrefixUpperBound("a\uFFFF"), Is.EqualTo("b"));
    }

    [Test]
    public void PrefixUpperBound_rolls_over_multiple_trailing_max_units()
    {
        // Every trailing U+FFFF is dropped, then the first unit below max is
        // incremented: "z\uFFFF\uFFFF" -> "{" ('z' is 0x7A, '{' is 0x7B).
        Assert.That(LatticeKeyRange.PrefixUpperBound("z\uFFFF\uFFFF"), Is.EqualTo("{"));
    }

    [Test]
    public void PrefixUpperBound_keeps_an_interior_max_unit_and_advances_a_later_one()
    {
        // Only the trailing run of max units rolls over; an interior U+FFFF is
        // preserved because a lower unit after it can still be incremented.
        Assert.That(LatticeKeyRange.PrefixUpperBound("\uFFFFa"), Is.EqualTo("\uFFFFb"));
    }

    [Test]
    public void PrefixUpperBound_of_all_max_code_units_is_null()
    {
        Assert.That(LatticeKeyRange.PrefixUpperBound("\uFFFF"), Is.Null);
        Assert.That(LatticeKeyRange.PrefixUpperBound("\uFFFF\uFFFF"), Is.Null);
    }

    [Test]
    public void PrefixUpperBound_of_empty_prefix_is_null()
    {
        Assert.That(LatticeKeyRange.PrefixUpperBound(string.Empty), Is.Null);
    }

    [Test]
    public void PrefixUpperBound_throws_on_null_prefix()
    {
        Assert.That(
            () => LatticeKeyRange.PrefixUpperBound(null!),
            Throws.TypeOf<ArgumentNullException>().With.Property("ParamName").EqualTo("prefix"));
    }

    [Test]
    public void PrefixUpperBound_operates_at_the_code_unit_level_for_surrogates()
    {
        // A key ending in a surrogate code unit is bounded at the UTF-16
        // code-unit level, which is exactly the granularity of ordinal
        // comparison. High surrogate U+D800 -> U+D801; U+DFFF is not char.MaxValue
        // so it increments to U+E000 rather than rolling over.
        Assert.That(LatticeKeyRange.PrefixUpperBound("\uD800"), Is.EqualTo("\uD801"));
        Assert.That(LatticeKeyRange.PrefixUpperBound("k\uDFFF"), Is.EqualTo("k\uE000"));
    }

    [Test]
    public void PrefixUpperBound_never_sorts_at_or_below_the_prefix()
    {
        foreach (var prefix in FinitelyBoundedPrefixes)
        {
            var bound = LatticeKeyRange.PrefixUpperBound(prefix);
            Assert.That(bound, Is.Not.Null, $"expected a finite bound for '{Escape(prefix)}'");
            Assert.That(
                string.CompareOrdinal(prefix, bound),
                Is.LessThan(0),
                $"bound '{Escape(bound!)}' must sort strictly above prefix '{Escape(prefix)}'");
        }
    }

    [Test]
    public void PrefixUpperBound_sorts_above_every_key_that_starts_with_the_prefix()
    {
        foreach (var prefix in FinitelyBoundedPrefixes)
        {
            var bound = LatticeKeyRange.PrefixUpperBound(prefix)!;

            // A representative spread of in-range keys, including the prefix
            // itself and the ordinally-largest suffixes, must all sort below
            // the exclusive bound.
            foreach (var suffix in new[] { "", "\0", "0", "abc", "\uFFFF", "\uFFFF\uFFFF" })
            {
                var inRange = prefix + suffix;
                Assert.That(
                    string.CompareOrdinal(inRange, bound),
                    Is.LessThan(0),
                    $"in-range key '{Escape(inRange)}' must sort below bound '{Escape(bound)}'");
            }
        }
    }

    [Test]
    public void PrefixUpperBound_is_the_least_upper_bound()
    {
        // Tightness: the next-smaller candidate bound - the returned bound with
        // its final unit decremented and a maximal U+FFFF tail appended - must
        // fail to be a strict upper bound, because the in-range key
        // `prefix + U+FFFF` sorts at or above it. That proves no smaller string
        // excludes the whole prefix subtree, so the returned bound is least.
        foreach (var prefix in FinitelyBoundedPrefixes)
        {
            var bound = LatticeKeyRange.PrefixUpperBound(prefix)!;

            // The bound's final unit is always >= U+0001 (it is some source unit
            // + 1), so it can always be decremented to form the candidate.
            var smallerCandidate = DecrementLastUnit(bound) + '\uFFFF';

            Assert.That(
                string.CompareOrdinal(prefix + '\uFFFF', smallerCandidate),
                Is.GreaterThanOrEqualTo(0),
                $"bound '{Escape(bound)}' is not the least upper bound of prefix '{Escape(prefix)}'");
        }
    }

    private static readonly string[] FinitelyBoundedPrefixes =
    [
        "a", "abc", "e/", "repo/acme/", "m\u001f", "z\uFFFF", "z\uFFFF\uFFFF",
        "\uFFFFa", "\uFFFF\uFFFFm", "key\0", "\uD800", "k\uDFFF",
    ];

    private static string DecrementLastUnit(string value)
    {
        var chars = value.ToCharArray();
        chars[^1]--;
        return new string(chars);
    }

    private static string Escape(string value) =>
        string.Concat(value.Select(c => c < ' ' || c > '~' ? $"\\u{(int)c:X4}" : c.ToString()));
}
