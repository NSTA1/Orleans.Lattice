namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Tests for <see cref="RepoContextKeywordSearch"/>: tokenization, per-entry
/// scoring (substring plus whole-token bonus), and the descending, deterministic
/// top-k ranking used as the structural/keyword search fallback.
/// </summary>
[TestFixture]
public sealed class RepoContextKeywordSearchTests
{
    private static RepoContextEntryView Entry(
        string key,
        string? path = null,
        string? fqn = null,
        string? topic = null,
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
            Id = null,
            Fields = fields ?? new Dictionary<string, string>(),
            Tags = tags ?? Array.Empty<string>(),
            Links = new Dictionary<string, IReadOnlyList<string>>(),
            Expires = false,
            ExpiresAtTicks = 0,
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
    public void Score_counts_distinct_matched_tokens_with_a_whole_token_bonus()
    {
        var entry = Entry("repo/r/file/src/OrderService.cs", path: "src/OrderService.cs");
        var tokens = RepoContextKeywordSearch.Tokenize("order missing");

        // "order" appears (substring +1) as part of "orderservice" but not as a whole
        // token, so no bonus; "missing" does not appear.
        Assert.That(RepoContextKeywordSearch.Score(entry, tokens), Is.EqualTo(1d));
    }

    [Test]
    public void Score_awards_the_bonus_for_a_whole_token_match()
    {
        var entry = Entry("repo/r/mem/topic/x", topic: "billing", tags: new[] { "invoice" });
        var tokens = RepoContextKeywordSearch.Tokenize("invoice");

        // whole-token match: +1 substring +1 bonus.
        Assert.That(RepoContextKeywordSearch.Score(entry, tokens), Is.EqualTo(2d));
    }

    [Test]
    public void Score_is_zero_when_nothing_matches()
        => Assert.That(
            RepoContextKeywordSearch.Score(Entry("repo/r/file/a.cs"), RepoContextKeywordSearch.Tokenize("zzz")),
            Is.EqualTo(0d));

    [Test]
    public void Rank_returns_best_matches_descending_and_drops_non_matches()
    {
        var entries = new[]
        {
            Entry("repo/r/file/src/OrderService.cs", path: "src/OrderService.cs"),
            Entry("repo/r/file/src/Unrelated.cs", path: "src/Unrelated.cs"),
            Entry("repo/r/sym/Order", fqn: "Acme.Order", tags: new[] { "order" }),
        };
        var tokens = RepoContextKeywordSearch.Tokenize("order");

        var ranked = RepoContextKeywordSearch.Rank(entries, tokens, k: 5);

        Assert.Multiple(() =>
        {
            // The symbol scores higher: whole-token match in the tag "order".
            Assert.That(ranked[0].Entry.Key, Is.EqualTo("repo/r/sym/Order"));
            Assert.That(ranked, Has.Count.EqualTo(2), "The unrelated file is dropped.");
            Assert.That(ranked.All(h => h.VectorId is null), Is.True, "Keyword hits carry no vector id.");
        });
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
    public void Rank_rejects_a_non_positive_k()
        => Assert.Throws<ArgumentOutOfRangeException>(
            () => RepoContextKeywordSearch.Rank(Array.Empty<RepoContextEntryView>(), Array.Empty<string>(), 0));
}
