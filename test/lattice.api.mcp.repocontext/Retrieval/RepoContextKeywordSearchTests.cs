namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Tests for <see cref="RepoContextKeywordSearch"/>: identifier-aware tokenization
/// and the corpus-relative Okapi BM25 ranking used as the structural/keyword
/// search fallback when no semantic index is available.
/// </summary>
[TestFixture]
public sealed class RepoContextKeywordSearchTests
{
    private static RepoContextEntryView Entry(
        string key,
        string? path = null,
        string? fqn = null,
        string? topic = null,
        string? id = null,
        IReadOnlyList<string>? tags = null,
        IReadOnlyDictionary<string, string>? fields = null)
        => new()
        {
            Key = key,
            Exists = true,
            Kind = "File",
            RepoId = "r",
            Path = path,
            FullyQualifiedName = fqn,
            Topic = topic,
            Id = id,
            Fields = fields ?? new Dictionary<string, string>(),
            Tags = tags ?? Array.Empty<string>(),
            Links = new Dictionary<string, IReadOnlyList<string>>(),
            Expires = false,
            ExpiresAtUtc = null,
            RemainingSeconds = null,
            HasExpired = false,
        };

    [Test]
    public void Tokenize_lowercases_splits_and_deduplicates()
        => Assert.That(
            RepoContextKeywordSearch.Tokenize("Order-Service order.Service"),
            Is.EqualTo(new[] { "order", "service" }));

    [Test]
    public void Tokenize_returns_empty_for_a_blank_query()
        => Assert.That(RepoContextKeywordSearch.Tokenize("   "), Is.Empty);

    [Test]
    public void Tokenize_splits_camel_case_humps()
        => Assert.That(
            RepoContextKeywordSearch.Tokenize("OrderService"),
            Is.EqualTo(new[] { "order", "service" }));

    [Test]
    public void Tokenize_splits_letter_digit_boundaries()
        => Assert.That(
            RepoContextKeywordSearch.Tokenize("utf8Writer"),
            Is.EqualTo(new[] { "utf", "8", "writer" }));

    [Test]
    public void Tokenize_preserves_first_seen_order_of_distinct_terms()
        => Assert.That(
            RepoContextKeywordSearch.Tokenize("beta ALPHA beta gamma"),
            Is.EqualTo(new[] { "beta", "alpha", "gamma" }));

    [Test]
    public void Rank_matches_a_query_term_against_a_compound_identifier_sub_token()
    {
        // BM25 over identifier-aware tokens keeps the recall the old substring scorer
        // had: "order" still matches inside "OrderService" because tokenization splits
        // the camelCase hump.
        var entries = new[]
        {
            Entry("repo/r/file/src/OrderService.cs", path: "src/OrderService.cs"),
            Entry("repo/r/file/src/Unrelated.cs", path: "src/Unrelated.cs"),
        };

        var ranked = RepoContextKeywordSearch.Rank(
            entries, RepoContextKeywordSearch.Tokenize("order"), k: 5);

        Assert.Multiple(() =>
        {
            Assert.That(ranked, Has.Count.EqualTo(1), "Only the matching entry is returned.");
            Assert.That(ranked[0].Entry.Key, Is.EqualTo("repo/r/file/src/OrderService.cs"));
            Assert.That(ranked[0].VectorId, Is.Null, "Keyword hits carry no vector id.");
            Assert.That(ranked[0].Score, Is.GreaterThan(0d));
        });
    }

    [Test]
    public void Rank_matches_a_token_present_only_in_the_content_field()
    {
        // The content projection folds the file body into a weighted field; a token
        // present only in the body must still rank, which is the point of the content
        // projection on the no-embedder path.
        var entries = new[]
        {
            Entry(
                "repo/r/content/src/A.cs",
                path: "src/A.cs",
                fields: new Dictionary<string, string> { ["text"] = "the quick brown widget jumps" }),
            Entry("repo/r/content/src/B.cs", path: "src/B.cs"),
        };

        var ranked = RepoContextKeywordSearch.Rank(
            entries, RepoContextKeywordSearch.Tokenize("widget"), k: 5);

        Assert.That(ranked, Has.Count.EqualTo(1));
        Assert.That(ranked[0].Entry.Key, Is.EqualTo("repo/r/content/src/A.cs"));
    }

    [Test]
    public void Rank_ignores_pure_noise_fields()
    {
        // A digest / size / timestamp field carries no searchable signal and must not
        // produce a hit even when the query token appears inside it verbatim.
        var entries = new[]
        {
            Entry(
                "repo/r/file/src/A.cs",
                path: "src/A.cs",
                fields: new Dictionary<string, string> { ["digest"] = "deadbeef", ["sizeBytes"] = "123" }),
        };

        var ranked = RepoContextKeywordSearch.Rank(
            entries, RepoContextKeywordSearch.Tokenize("deadbeef"), k: 5);

        Assert.That(ranked, Is.Empty, "The content digest is not a searchable field.");
    }

    [Test]
    public void Rank_weights_a_title_match_above_a_body_match()
    {
        // Two otherwise-similar docs: the one matching in the high-weight title field
        // must outrank the one matching only in the low-weight body.
        var titled = Entry(
            "repo/r/mem/topic/a",
            fields: new Dictionary<string, string> { ["title"] = "widget", ["body"] = "filler filler filler" });
        var bodied = Entry(
            "repo/r/mem/topic/b",
            fields: new Dictionary<string, string> { ["title"] = "filler", ["body"] = "filler filler widget" });

        var ranked = RepoContextKeywordSearch.Rank(
            new[] { bodied, titled }, RepoContextKeywordSearch.Tokenize("widget"), k: 5);

        Assert.That(ranked, Has.Count.EqualTo(2));
        Assert.That(ranked[0].Entry.Key, Is.EqualTo("repo/r/mem/topic/a"), "Title match ranks first.");
    }

    [Test]
    public void Rank_lets_inverse_document_frequency_favour_the_rarer_term()
    {
        // "common" appears in every doc (idf ~ 0); "rare" appears in one. A doc that
        // matches only the rare term must outrank a doc that matches only the ubiquitous
        // one, which a flat overlap count could never express.
        var entries = new[]
        {
            Entry("repo/r/file/a.cs", fields: new Dictionary<string, string> { ["body"] = "common rare" }),
            Entry("repo/r/file/b.cs", fields: new Dictionary<string, string> { ["body"] = "common filler" }),
            Entry("repo/r/file/c.cs", fields: new Dictionary<string, string> { ["body"] = "common filler" }),
            Entry("repo/r/file/d.cs", fields: new Dictionary<string, string> { ["body"] = "common filler" }),
        };

        var ranked = RepoContextKeywordSearch.Rank(
            entries, RepoContextKeywordSearch.Tokenize("common rare"), k: 5);

        Assert.That(ranked[0].Entry.Key, Is.EqualTo("repo/r/file/a.cs"),
            "The doc carrying the rare term ranks first on IDF.");
    }

    [Test]
    public void Rank_saturates_term_frequency_so_one_flooded_field_cannot_dominate()
    {
        // BM25 term-frequency saturation: a doc repeating the term many times scores
        // more than a single occurrence, but far less than linearly - so a flooded
        // field cannot run away with the ranking.
        var flooded = Entry(
            "repo/r/file/flooded.cs",
            fields: new Dictionary<string, string>
            {
                ["body"] = string.Join(' ', Enumerable.Repeat("widget", 100)),
            });
        var single = Entry(
            "repo/r/file/single.cs",
            fields: new Dictionary<string, string> { ["body"] = "widget" });

        var ranked = RepoContextKeywordSearch.Rank(
            new[] { flooded, single }, RepoContextKeywordSearch.Tokenize("widget"), k: 5);

        var floodedScore = ranked.Single(h => h.Entry.Key == "repo/r/file/flooded.cs").Score;
        var singleScore = ranked.Single(h => h.Entry.Key == "repo/r/file/single.cs").Score;
        Assert.Multiple(() =>
        {
            Assert.That(floodedScore, Is.GreaterThan(singleScore), "More occurrences still rank higher.");
            Assert.That(floodedScore, Is.LessThan(singleScore * 100d), "But nowhere near linearly (saturation).");
        });
    }

    [Test]
    public void Rank_is_deterministic_and_breaks_ties_by_ordinal_key()
    {
        // Two entries with identical searchable content and length tie on score; the
        // ranker orders them by ordinal key so the result is stable.
        var entries = new[]
        {
            Entry("repo/r/file/b.cs", fields: new Dictionary<string, string> { ["body"] = "widget" }),
            Entry("repo/r/file/a.cs", fields: new Dictionary<string, string> { ["body"] = "widget" }),
        };

        var ranked = RepoContextKeywordSearch.Rank(
            entries, RepoContextKeywordSearch.Tokenize("widget"), k: 5);

        Assert.Multiple(() =>
        {
            Assert.That(ranked[0].Entry.Key, Is.EqualTo("repo/r/file/a.cs"));
            Assert.That(ranked[1].Entry.Key, Is.EqualTo("repo/r/file/b.cs"));
            Assert.That(ranked[0].Score, Is.EqualTo(ranked[1].Score));
        });
    }

    [Test]
    public void Rank_drops_non_matching_entries()
    {
        var entries = new[]
        {
            Entry("repo/r/file/src/OrderService.cs", path: "src/OrderService.cs"),
            Entry("repo/r/file/src/Unrelated.cs", path: "src/Unrelated.cs"),
            Entry("repo/r/sym/Order", fqn: "Acme.Order", tags: new[] { "order" }),
        };

        var ranked = RepoContextKeywordSearch.Rank(
            entries, RepoContextKeywordSearch.Tokenize("order"), k: 5);

        Assert.That(ranked, Has.Count.EqualTo(2), "The unrelated file is dropped.");
    }

    [Test]
    public void Rank_bounds_the_result_to_k()
    {
        var entries = Enumerable.Range(0, 5)
            .Select(i => Entry($"repo/r/file/order{i}.cs", path: $"order{i}.cs"))
            .ToArray();

        var ranked = RepoContextKeywordSearch.Rank(entries, RepoContextKeywordSearch.Tokenize("order"), k: 2);

        Assert.That(ranked, Has.Count.EqualTo(2));
    }

    [Test]
    public void Rank_returns_empty_for_an_empty_corpus()
        => Assert.That(
            RepoContextKeywordSearch.Rank(
                Array.Empty<RepoContextEntryView>(), RepoContextKeywordSearch.Tokenize("order"), k: 5),
            Is.Empty);

    [Test]
    public void Rank_returns_empty_when_there_are_no_query_tokens()
        => Assert.That(
            RepoContextKeywordSearch.Rank(
                new[] { Entry("repo/r/file/a.cs", path: "a.cs") }, Array.Empty<string>(), k: 5),
            Is.Empty);

    [Test]
    public void Rank_rejects_a_non_positive_k()
        => Assert.Throws<ArgumentOutOfRangeException>(
            () => RepoContextKeywordSearch.Rank(Array.Empty<RepoContextEntryView>(), Array.Empty<string>(), 0));

    [Test]
    public void Rank_rejects_null_arguments()
        => Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentNullException>(
                () => RepoContextKeywordSearch.Rank(null!, Array.Empty<string>(), 1));
            Assert.Throws<ArgumentNullException>(
                () => RepoContextKeywordSearch.Rank(Array.Empty<RepoContextEntryView>(), null!, 1));
        });
}
