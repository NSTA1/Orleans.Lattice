namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Tests for the search response payloads <see cref="RepoContextSearchResult"/>
/// and <see cref="RepoContextSearchHit"/>: their members round-trip the values
/// they are constructed with, so the JSON the tool projects is faithful.
/// </summary>
[TestFixture]
public sealed class RepoContextSearchResultTests
{
    private static RepoContextEntryView Entry(string key)
        => new()
        {
            Key = key,
            Exists = true,
            Kind = "File",
            RepoId = "acme",
            Fields = new Dictionary<string, string>(),
            Tags = Array.Empty<string>(),
            Links = new Dictionary<string, IReadOnlyList<string>>(),
            Expires = false,
            ExpiresAtUtc = null,
            HasExpired = false,
        };

    [Test]
    public void Hit_exposes_its_score_entry_and_optional_vector_id()
    {
        var entry = Entry("repo/acme/file/a.cs");
        var hit = new RepoContextSearchHit { Score = 0.87, Entry = entry, VectorId = "sid.addr" };

        Assert.Multiple(() =>
        {
            Assert.That(hit.Score, Is.EqualTo(0.87));
            Assert.That(hit.Entry, Is.SameAs(entry));
            Assert.That(hit.VectorId, Is.EqualTo("sid.addr"));
        });
    }

    [Test]
    public void Hit_vector_id_defaults_to_null_for_a_keyword_hit()
        => Assert.That(new RepoContextSearchHit { Score = 1, Entry = Entry("k") }.VectorId, Is.Null);

    [Test]
    public void Hit_reasons_default_to_an_empty_non_null_list()
    {
        // Back-compatible additive surface: a hit constructed without reasons (as
        // every pre-existing consumer does) exposes an empty list, never null.
        var hit = new RepoContextSearchHit { Score = 1, Entry = Entry("k") };

        Assert.Multiple(() =>
        {
            Assert.That(hit.Reasons, Is.Not.Null);
            Assert.That(hit.Reasons, Is.Empty);
        });
    }

    [Test]
    public void Hit_exposes_the_reasons_it_is_constructed_with()
    {
        var reasons = new[] { "semantic", "chunk:file" };
        var hit = new RepoContextSearchHit { Score = 1, Entry = Entry("k"), Reasons = reasons };

        Assert.That(hit.Reasons, Is.SameAs(reasons));
    }

    [Test]
    public void Result_exposes_repo_query_mode_and_hits()
    {
        var hits = new[] { new RepoContextSearchHit { Score = 1, Entry = Entry("k") } };
        var result = new RepoContextSearchResult
        {
            RepoId = "acme",
            Query = "order service",
            Mode = "semantic",
            RetrievalPath = RepoContextRetrievalPath.SemanticExact,
            Hits = hits,
        };

        Assert.Multiple(() =>
        {
            Assert.That(result.RepoId, Is.EqualTo("acme"));
            Assert.That(result.Query, Is.EqualTo("order service"));
            Assert.That(result.Mode, Is.EqualTo("semantic"));
            Assert.That(result.RetrievalPath, Is.EqualTo("semantic.exact"));
            Assert.That(result.Hits, Is.SameAs(hits));
        });
    }
}
