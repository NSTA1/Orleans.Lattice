namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Tests for <see cref="RepoContextKnnRanker"/>: ranking correctness on a
/// normalized space (dot-product path) and an un-normalized space (cosine path),
/// the top-k bound, the descending order, the space-mismatch skip, and the
/// length-mismatch skip.
/// </summary>
[TestFixture]
public sealed class RepoContextKnnRankerTests
{
    private static EmbeddingSpaceTag NormalizedSpace(int dimension = 3)
        => new("rank-model", dimension, VectorNormalization.UnitL2);

    private static RepoContextVectorCandidate Candidate(string id, float[] vector, EmbeddingSpaceTag space)
        => new(id, "repo/r/file/" + id, vector, space);

    [Test]
    public void Rank_orders_candidates_by_descending_similarity()
    {
        var space = NormalizedSpace();
        var query = new[] { 1f, 0f, 0f };
        var candidates = new[]
        {
            Candidate("far", new[] { 0f, 1f, 0f }, space),
            Candidate("near", new[] { 0.9f, 0.1f, 0f }, space),
            Candidate("mid", new[] { 0.5f, 0.5f, 0f }, space),
        };

        var ranked = RepoContextKnnRanker.Rank(query, space, candidates, k: 3);

        Assert.That(ranked.Select(m => m.VectorId), Is.EqualTo(new[] { "near", "mid", "far" }));
    }

    [Test]
    public void Rank_bounds_the_result_to_k()
    {
        var space = NormalizedSpace();
        var query = new[] { 1f, 0f, 0f };
        var candidates = Enumerable.Range(0, 10)
            .Select(i => Candidate($"c{i}", new[] { 1f - (i * 0.05f), i * 0.05f, 0f }, space))
            .ToArray();

        var ranked = RepoContextKnnRanker.Rank(query, space, candidates, k: 3);

        Assert.Multiple(() =>
        {
            Assert.That(ranked, Has.Count.EqualTo(3));
            Assert.That(ranked[0].VectorId, Is.EqualTo("c0"));
            Assert.That(ranked[2].Score, Is.LessThanOrEqualTo(ranked[1].Score));
        });
    }

    [Test]
    public void Rank_uses_cosine_when_the_space_is_not_normalized()
    {
        var space = new EmbeddingSpaceTag("raw-model", 2, VectorNormalization.None);
        var query = new[] { 1f, 0f };
        var candidates = new[]
        {
            Candidate("scaled", new[] { 5f, 0f }, space),
            Candidate("off", new[] { 1f, 1f }, space),
        };

        var ranked = RepoContextKnnRanker.Rank(query, space, candidates, k: 2);

        Assert.Multiple(() =>
        {
            Assert.That(ranked[0].VectorId, Is.EqualTo("scaled"));
            Assert.That(ranked[0].Score, Is.EqualTo(1d).Within(1e-6));
        });
    }

    [Test]
    public void Rank_skips_a_candidate_whose_space_does_not_match()
    {
        var querySpace = NormalizedSpace();
        var otherSpace = new EmbeddingSpaceTag("other-model", 3, VectorNormalization.UnitL2);
        var query = new[] { 1f, 0f, 0f };
        var candidates = new[]
        {
            Candidate("wrong-space", new[] { 1f, 0f, 0f }, otherSpace),
            Candidate("right-space", new[] { 0.8f, 0.2f, 0f }, querySpace),
        };

        var ranked = RepoContextKnnRanker.Rank(query, querySpace, candidates, k: 5);

        Assert.That(ranked.Select(m => m.VectorId), Is.EqualTo(new[] { "right-space" }));
    }

    [Test]
    public void Rank_skips_a_candidate_of_the_wrong_length()
    {
        var space = NormalizedSpace();
        var query = new[] { 1f, 0f, 0f };
        var candidates = new[]
        {
            Candidate("short", new[] { 1f, 0f }, space),
            Candidate("ok", new[] { 1f, 0f, 0f }, space),
        };

        var ranked = RepoContextKnnRanker.Rank(query, space, candidates, k: 5);

        Assert.That(ranked.Select(m => m.VectorId), Is.EqualTo(new[] { "ok" }));
    }

    [Test]
    public void Rank_rejects_a_non_positive_k()
        => Assert.Throws<ArgumentOutOfRangeException>(
            () => RepoContextKnnRanker.Rank(new[] { 1f }, NormalizedSpace(1), Array.Empty<RepoContextVectorCandidate>(), 0));

    [Test]
    public void Rank_rejects_a_null_candidate_set()
        => Assert.Throws<ArgumentNullException>(
            () => RepoContextKnnRanker.Rank(new[] { 1f }, NormalizedSpace(1), null!, 1));
}
