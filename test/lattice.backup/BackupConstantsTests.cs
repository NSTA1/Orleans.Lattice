namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Unit coverage for <see cref="BackupConstants.PrefixUpperBound(string)"/>: the
/// exclusive upper bound of a prefix range must always sort strictly above the
/// prefix so the half-open <c>[prefix, bound)</c> scan is non-empty. Advancing
/// the final code unit unconditionally wrapped a trailing <c>U+FFFF</c> to
/// <c>U+0000</c>, producing a bound that sorts below the prefix and inverting
/// the range so a prefix-scoped backup / restore silently captured nothing.
/// </summary>
public sealed class BackupConstantsTests
{
    [Test]
    public void PrefixUpperBound_increments_the_last_code_unit()
    {
        Assert.That(BackupConstants.PrefixUpperBound("abc"), Is.EqualTo("abd"));
    }

    [Test]
    public void PrefixUpperBound_rolls_over_a_trailing_max_code_unit()
    {
        // 'a' followed by U+FFFF: the trailing max unit is dropped and the
        // preceding 'a' is incremented to 'b'. The buggy implementation
        // produced "a\u0000" instead.
        Assert.That(BackupConstants.PrefixUpperBound("a\uFFFF"), Is.EqualTo("b"));
    }

    [Test]
    public void PrefixUpperBound_of_all_max_code_units_is_null()
    {
        Assert.That(BackupConstants.PrefixUpperBound("\uFFFF\uFFFF"), Is.Null);
    }

    [Test]
    public void PrefixUpperBound_of_empty_prefix_is_null()
    {
        Assert.That(BackupConstants.PrefixUpperBound(string.Empty), Is.Null);
    }

    [Test]
    public void AllTrees_contains_the_three_reserved_tree_names()
    {
        // Line 83: the AllTrees property getter.
        Assert.That(
            BackupConstants.AllTrees,
            Is.EquivalentTo(new[]
            {
                BackupConstants.StoreTree,
                BackupConstants.CatalogTree,
                BackupConstants.HealthTree,
            }));
    }

    [Test]
    public void PrefixUpperBound_never_sorts_at_or_below_the_prefix()
    {
        // The range-validity invariant that the bug violated: whenever a finite
        // bound exists it must sort strictly above the prefix, so [prefix, bound)
        // is a non-empty forward range.
        foreach (var prefix in new[] { "abc", "a\uFFFF", "m\u001f", "\uFFFFa", "z\uFFFF\uFFFF" })
        {
            var bound = BackupConstants.PrefixUpperBound(prefix);
            Assert.That(bound, Is.Not.Null, $"expected a finite bound for '{prefix}'");
            Assert.That(
                string.CompareOrdinal(prefix, bound),
                Is.LessThan(0),
                $"upper bound '{bound}' must sort above prefix '{prefix}'");
        }
    }
}
