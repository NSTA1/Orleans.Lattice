namespace Orleans.Lattice.Vector.Tests;

/// <summary>
/// Unit tests for the ranked-hit value type.
/// </summary>
[TestFixture]
public sealed class VectorSearchResultTests
{
    [Test]
    public void It_carries_the_key_and_the_score()
    {
        var result = new VectorSearchResult(12L, 0.75f);

        Assert.That(result.Key, Is.EqualTo(12L));
        Assert.That(result.Score, Is.EqualTo(0.75f));
    }

    [Test]
    public void Two_results_with_the_same_key_and_score_are_equal()
    {
        Assert.That(new VectorSearchResult(1, 0.5f), Is.EqualTo(new VectorSearchResult(1, 0.5f)));
        Assert.That(new VectorSearchResult(1, 0.5f), Is.Not.EqualTo(new VectorSearchResult(2, 0.5f)));
        Assert.That(new VectorSearchResult(1, 0.5f), Is.Not.EqualTo(new VectorSearchResult(1, 0.6f)));
    }

    [Test]
    public void A_default_result_is_the_zero_key_at_a_zero_score()
    {
        Assert.That(default(VectorSearchResult), Is.EqualTo(new VectorSearchResult(0, 0f)));
    }
}
