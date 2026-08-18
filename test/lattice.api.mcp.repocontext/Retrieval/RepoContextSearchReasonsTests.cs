namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Tests for <see cref="RepoContextSearchReasons.ForSemantic(string?)"/>: the
/// semantic-path reason vocabulary derived server-side from the matched vector's
/// canonical source key. A symbol vector explains itself as a symbol chunk with
/// its fully-qualified name; a file-chunk vector as a file chunk; anything else
/// degrades to the bare <c>semantic</c> reason.
/// </summary>
[TestFixture]
public sealed class RepoContextSearchReasonsTests
{
    [Test]
    public void ForSemantic_explains_a_symbol_vector_with_its_fully_qualified_name()
        => Assert.That(
            RepoContextSearchReasons.ForSemantic("repo/acme/symbol/Acme.Orders.OrderService"),
            Is.EqualTo(new[] { "semantic", "chunk:symbol", "symbol:Acme.Orders.OrderService" }));

    [Test]
    public void ForSemantic_explains_a_file_chunk_vector()
        => Assert.That(
            RepoContextSearchReasons.ForSemantic("repo/acme/file/src/OrderService.cs"),
            Is.EqualTo(new[] { "semantic", "chunk:file" }));

    [Test]
    public void ForSemantic_treats_a_content_source_as_a_file_chunk()
        => Assert.That(
            RepoContextSearchReasons.ForSemantic("repo/acme/content/src/OrderService.cs"),
            Is.EqualTo(new[] { "semantic", "chunk:file" }));

    [Test]
    public void ForSemantic_falls_back_to_semantic_only_for_a_non_structural_source()
        => Assert.That(
            RepoContextSearchReasons.ForSemantic("repo/acme/mem/decisions/x"),
            Is.EqualTo(new[] { "semantic" }));

    [Test]
    public void ForSemantic_falls_back_to_semantic_only_for_an_unparseable_key()
        => Assert.That(
            RepoContextSearchReasons.ForSemantic("not-a-key"),
            Is.EqualTo(new[] { "semantic" }));

    [Test]
    public void ForSemantic_falls_back_to_semantic_only_for_a_null_or_empty_key()
        => Assert.Multiple(() =>
        {
            Assert.That(RepoContextSearchReasons.ForSemantic(null), Is.EqualTo(new[] { "semantic" }));
            Assert.That(RepoContextSearchReasons.ForSemantic(string.Empty), Is.EqualTo(new[] { "semantic" }));
        });

    [Test]
    public void ForSemantic_never_exceeds_the_reason_cap()
        => Assert.That(
            RepoContextSearchReasons.ForSemantic("repo/acme/symbol/Acme.Orders.OrderService").Count,
            Is.LessThanOrEqualTo(RepoContextSearchReasons.MaxReasons));
}
