using System.Linq;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Fast, dependency-free unit tests for <see cref="CanonicalStringSet"/> - the
/// shared canonicalisation the cross-tree and view coordination barriers use to
/// freeze their wait/participant sets. These pin the invariant that the
/// allocation-lean HashSet-plus-in-place-sort form produces exactly the same
/// ordinal-sorted, de-duplicated sequence as the LINQ
/// <c>Distinct(Ordinal).OrderBy(Ordinal)</c> form it replaced, so the exact-match
/// barrier comparison stays deterministic regardless of input order.
/// </summary>
[TestFixture]
public sealed class CanonicalStringSetTests
{
    private static List<string> LinqList(IEnumerable<string> source) =>
        source.Distinct(StringComparer.Ordinal).OrderBy(v => v, StringComparer.Ordinal).ToList();

    private static string[] LinqArray(IEnumerable<string> source) =>
        source.Distinct(StringComparer.Ordinal).OrderBy(v => v, StringComparer.Ordinal).ToArray();

    private static readonly string[][] Cases =
    [
        [],
        ["only"],
        ["b", "a", "c"],
        ["a", "a", "a"],
        ["a", "b", "c"],
        ["c", "b", "a"],
        ["tree-2", "tree-10", "tree-1", "tree-2"],
        ["Z", "a", "B", "y", "A", "z"],
    ];

    [TestCaseSource(nameof(Cases))]
    public void SortedDistinct_matches_linq_ordinal_form(string[] input)
    {
        Assert.That(CanonicalStringSet.SortedDistinct(input), Is.EqualTo(LinqList(input)));
    }

    [TestCaseSource(nameof(Cases))]
    public void SortedDistinctArray_matches_linq_ordinal_form(string[] input)
    {
        Assert.That(CanonicalStringSet.SortedDistinctArray(input), Is.EqualTo(LinqArray(input)));
    }

    [Test]
    public void SortedDistinct_deduplicates_and_sorts_ordinal()
    {
        Assert.That(
            CanonicalStringSet.SortedDistinct(["delta", "alpha", "delta", "charlie"]),
            Is.EqualTo(new[] { "alpha", "charlie", "delta" }));
    }

    [Test]
    public void SortedDistinct_is_order_insensitive()
    {
        var forward = CanonicalStringSet.SortedDistinct(["a", "b", "c", "a"]);
        var reverse = CanonicalStringSet.SortedDistinct(["c", "a", "b", "c"]);
        Assert.That(forward, Is.EqualTo(reverse));
    }

    [Test]
    public void SortedDistinct_uses_ordinal_not_culture_ordering()
    {
        // Ordinal orders uppercase ('A'=65) before lowercase ('a'=97); a
        // culture-sensitive sort would interleave them. Pin the ordinal contract.
        Assert.That(
            CanonicalStringSet.SortedDistinct(["a", "B", "A", "b"]),
            Is.EqualTo(new[] { "A", "B", "a", "b" }));
    }

    [Test]
    public void SortedDistinctArray_deduplicates_and_sorts_ordinal()
    {
        Assert.That(
            CanonicalStringSet.SortedDistinctArray(["delta", "alpha", "delta", "charlie"]),
            Is.EqualTo(new[] { "alpha", "charlie", "delta" }));
    }

    [Test]
    public void SortedDistinct_null_source_throws()
    {
        Assert.That(() => CanonicalStringSet.SortedDistinct(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void SortedDistinctArray_null_source_throws()
    {
        Assert.That(() => CanonicalStringSet.SortedDistinctArray(null!), Throws.ArgumentNullException);
    }
}
