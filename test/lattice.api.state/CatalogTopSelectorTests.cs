namespace Orleans.Lattice.Api.State.Tests;

/// <summary>
/// Pins <see cref="CatalogTopSelector"/>, the bounded selection the catalog
/// endpoints use in place of buffering and fully sorting every surviving id.
/// </summary>
/// <remarks>
/// The load-bearing property is <b>equivalence</b>: for any candidate stream and
/// any limit, the selector must emit exactly the prefix a full ordinal sort of
/// the whole stream would have yielded. Every test here is written as that
/// comparison rather than against a hand-written expectation, so a heap bug
/// cannot be masked by an expectation written to match it.
/// </remarks>
[TestFixture]
public sealed class CatalogTopSelectorTests
{
    /// <summary>
    /// The reference answer: sort every candidate ordinally and take the limit.
    /// </summary>
    private static string[] FullSortPrefix(IEnumerable<string> candidates, int limit)
    {
        var all = candidates.ToList();
        all.Sort(StringComparer.Ordinal);
        return [.. all.Take(limit)];
    }

    private static string[] Select(IEnumerable<string> candidates, int limit)
    {
        var selector = new CatalogTopSelector(limit);
        foreach (var candidate in candidates)
        {
            selector.Offer(candidate);
        }

        return selector.ToOrderedArray();
    }

    [Test]
    public void ToOrderedArray_returns_the_full_sort_prefix_for_an_ascending_stream()
    {
        var candidates = Enumerable.Range(0, 500).Select(i => $"tree-{i:D4}").ToArray();

        Assert.That(Select(candidates, 101), Is.EqualTo(FullSortPrefix(candidates, 101)));
    }

    [Test]
    public void ToOrderedArray_returns_the_full_sort_prefix_for_a_descending_stream()
    {
        var candidates = Enumerable.Range(0, 500).Reverse().Select(i => $"tree-{i:D4}").ToArray();

        Assert.That(Select(candidates, 101), Is.EqualTo(FullSortPrefix(candidates, 101)));
    }

    [Test]
    public void ToOrderedArray_returns_the_full_sort_prefix_for_shuffled_streams()
    {
        // Deterministic seeds, so a failure is reproducible rather than flaky.
        for (var seed = 0; seed < 32; seed++)
        {
            var random = new Random(seed);
            var candidates = Enumerable.Range(0, 300)
                .Select(i => $"tree-{i:D4}")
                .OrderBy(_ => random.Next())
                .ToArray();

            Assert.That(
                Select(candidates, 37),
                Is.EqualTo(FullSortPrefix(candidates, 37)),
                $"seed {seed}");
        }
    }

    [Test]
    public void ToOrderedArray_returns_every_candidate_when_the_limit_exceeds_the_stream()
    {
        var candidates = new[] { "delta", "alpha", "charlie", "bravo" };

        Assert.That(Select(candidates, 100), Is.EqualTo(new[] { "alpha", "bravo", "charlie", "delta" }));
    }

    [Test]
    public void ToOrderedArray_retains_duplicate_ids_rather_than_de_duplicating_them()
    {
        // The selector is a bounded ordering, not a set: a full sort would keep
        // both copies, so the selection must too.
        var candidates = new[] { "bravo", "alpha", "bravo", "alpha" };

        Assert.That(Select(candidates, 3), Is.EqualTo(new[] { "alpha", "alpha", "bravo" }));
    }

    [Test]
    public void ToOrderedArray_is_empty_for_an_empty_stream()
    {
        Assert.That(Select([], 10), Is.Empty);
    }

    [Test]
    public void A_zero_limit_selector_retains_nothing()
    {
        var selector = new CatalogTopSelector(0);
        selector.Offer("alpha");

        Assert.Multiple(() =>
        {
            Assert.That(selector.Count, Is.Zero);
            Assert.That(selector.ToOrderedArray(), Is.Empty);
        });
    }

    [Test]
    public void A_negative_limit_selector_retains_nothing()
    {
        var selector = new CatalogTopSelector(-5);
        selector.Offer("alpha");

        Assert.That(selector.ToOrderedArray(), Is.Empty);
    }

    [Test]
    public void Count_never_exceeds_the_limit()
    {
        var selector = new CatalogTopSelector(4);
        for (var i = 0; i < 50; i++)
        {
            selector.Offer($"tree-{i:D3}");
        }

        Assert.That(selector.Count, Is.EqualTo(4));
    }

    [Test]
    public void Ordering_is_ordinal_not_culture_sensitive()
    {
        // Ordinal puts every upper-case letter before every lower-case one; a
        // culture-sensitive comparison interleaves them. The catalog page
        // contract is ordinal, so the selector must be too.
        var candidates = new[] { "apple", "Banana", "Apple", "banana" };

        Assert.That(Select(candidates, 4), Is.EqualTo(FullSortPrefix(candidates, 4)));
    }

    [Test]
    public void A_limit_of_one_selects_the_ordinal_minimum()
    {
        var candidates = new[] { "mike", "alpha", "zulu", "bravo" };

        Assert.That(Select(candidates, 1), Is.EqualTo(new[] { "alpha" }));
    }
}
