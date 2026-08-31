namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Unit tests for the <see cref="RepoContextRetrievalPath"/> vocabulary. The wire names
/// are a contract other packages code against, so they are asserted literally: a rename
/// is a breaking change and must fail here first.
/// </summary>
[TestFixture]
public sealed class RepoContextRetrievalPathTests
{
    [Test]
    public void Vocabulary_wire_names_are_stable()
        => Assert.Multiple(() =>
        {
            Assert.That(RepoContextRetrievalPath.SemanticExact, Is.EqualTo("semantic.exact"));
            Assert.That(RepoContextRetrievalPath.SemanticApproximate, Is.EqualTo("semantic.approximate"));
            Assert.That(RepoContextRetrievalPath.KeywordNoEmbedder, Is.EqualTo("keyword.no_embedder"));
            Assert.That(
                RepoContextRetrievalPath.KeywordVectorPlaneUnavailable,
                Is.EqualTo("keyword.vector_plane_unavailable"));
            Assert.That(RepoContextRetrievalPath.KeywordIndexDegraded, Is.EqualTo("keyword.index_degraded"));
        });

    [Test]
    public void Vocabulary_values_are_all_distinct()
    {
        string[] all =
        [
            RepoContextRetrievalPath.SemanticExact,
            RepoContextRetrievalPath.SemanticApproximate,
            RepoContextRetrievalPath.KeywordNoEmbedder,
            RepoContextRetrievalPath.KeywordVectorPlaneUnavailable,
            RepoContextRetrievalPath.KeywordIndexDegraded,
        ];

        Assert.That(all, Is.Unique);
    }

    [Test]
    public void NormalizeSemantic_honours_an_exact_declaration()
        => Assert.That(
            RepoContextRetrievalPath.NormalizeSemantic(RepoContextRetrievalPath.SemanticExact),
            Is.EqualTo(RepoContextRetrievalPath.SemanticExact));

    [Test]
    public void NormalizeSemantic_honours_an_approximate_declaration()
        => Assert.That(
            RepoContextRetrievalPath.NormalizeSemantic(RepoContextRetrievalPath.SemanticApproximate),
            Is.EqualTo(RepoContextRetrievalPath.SemanticApproximate));

    [TestCase(null)]
    [TestCase("")]
    [TestCase("   ")]
    [TestCase("Semantic.Exact")]
    [TestCase("exact")]
    [TestCase("keyword.no_embedder")]
    public void NormalizeSemantic_fails_closed_to_the_weaker_recall_claim(string? declared)
        => Assert.That(
            RepoContextRetrievalPath.NormalizeSemantic(declared),
            Is.EqualTo(RepoContextRetrievalPath.SemanticApproximate),
            "An unrecognised declaration must never be promoted to the complete-recall claim.");

    [Test]
    public void IsSemantic_is_true_for_both_semantic_values()
        => Assert.Multiple(() =>
        {
            Assert.That(RepoContextRetrievalPath.IsSemantic(RepoContextRetrievalPath.SemanticExact), Is.True);
            Assert.That(RepoContextRetrievalPath.IsSemantic(RepoContextRetrievalPath.SemanticApproximate), Is.True);
        });

    [TestCase(null)]
    [TestCase("")]
    [TestCase("keyword.no_embedder")]
    [TestCase("keyword.vector_plane_unavailable")]
    [TestCase("keyword.index_degraded")]
    [TestCase("semantic")]
    public void IsSemantic_is_false_for_every_other_value(string? path)
        => Assert.That(RepoContextRetrievalPath.IsSemantic(path), Is.False);
}
