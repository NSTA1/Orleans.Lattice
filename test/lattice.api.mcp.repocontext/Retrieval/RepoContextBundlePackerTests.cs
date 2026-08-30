namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Unit tests for <see cref="RepoContextBundlePacker"/>, the pure deterministic
/// packing core behind <c>repocontext_context</c>. They prove the load-bearing
/// invariants with the real exact-BPE counter and no grains: the hard ceiling is
/// never exceeded, packing is deterministic and greedy in rank order, a non-fitting
/// pack hands back a guaranteed-to-fit retry budget, and per-entry provenance
/// (reasons, full-read cost) is carried through unchanged.
/// </summary>
[TestFixture]
public sealed class RepoContextBundlePackerTests
{
    private static readonly IRepoContextTokenCounter Counter =
        new TiktokenRepoContextTokenCounter(new RepoContextIndexingOptions());

    private static RepoContextBundlePacker.Candidate Candidate(
        string path, string content, double score = 1.0, int? fullRead = null, params string[] reasons)
        => new(path, score, reasons, content, fullRead);

    private static string Words(int count) => string.Join(' ', Enumerable.Repeat("lattice", count));

    /// <summary>
    /// The bundle-level reserve the packer charges before any entry: the top-level and
    /// <c>CallToolResult</c> scaffolding, after the dual-emission factor.
    /// </summary>
    private static int Reserve()
        => RepoContextResponseCost.WithDualEmission(RepoContextResponseCost.BundleScaffoldTokens);

    /// <summary>
    /// The true response cost the packer charges for one candidate - its content plus its
    /// JSON envelope, times the SDK dual-emission factor. Budget arithmetic in these tests
    /// is expressed in these units, because that is what the ceiling now bounds.
    /// </summary>
    private static int Cost(RepoContextBundlePacker.Candidate candidate)
        => RepoContextResponseCost.WithDualEmission(
            RepoContextResponseCost.EntryEnvelopeTokens(
                Counter.CountTokens(candidate.Content),
                candidate.Path,
                candidate.Reasons,
                candidate.ContentHash,
                candidate.Units ?? Array.Empty<RepoContextRenderedUnit>(),
                Counter));

    [Test]
    public void Pack_with_no_candidates_returns_an_empty_outcome()
    {
        var outcome = RepoContextBundlePacker.Pack([], 1000, Counter);

        Assert.Multiple(() =>
        {
            Assert.That(outcome.Entries, Is.Empty);
            Assert.That(outcome.TotalTokens, Is.Zero);
            Assert.That(outcome.Truncated, Is.False);
            Assert.That(outcome.MinCandidateTokens, Is.Zero);
        });
    }

    [TestCase(1)]
    [TestCase(8)]
    [TestCase(32)]
    [TestCase(200)]
    [TestCase(10_000)]
    public void Pack_total_never_exceeds_the_budget_and_equals_the_exact_bpe_sum(int budget)
    {
        var candidates = new List<RepoContextBundlePacker.Candidate>
        {
            Candidate("a", Words(3)),
            Candidate("b", Words(20)),
            Candidate("c", Words(1)),
            Candidate("d", Words(120)),
            Candidate("e", Words(7)),
        };

        var outcome = RepoContextBundlePacker.Pack(candidates, budget, Counter);

        var exactSum = outcome.Entries.Sum(e => Counter.CountTokens(e.Content));
        Assert.Multiple(() =>
        {
            Assert.That(outcome.ResponseTokens, Is.LessThanOrEqualTo(budget),
                "The hard ceiling bounds the response the caller receives, not merely the content inside it.");
            Assert.That(outcome.TotalTokens, Is.LessThanOrEqualTo(budget),
                "The hard ceiling must never be exceeded.");
            Assert.That(outcome.TotalTokens, Is.EqualTo(exactSum),
                "The reported total must be the exact BPE sum of the packed content.");
            foreach (var entry in outcome.Entries)
            {
                Assert.That(entry.TokenCount, Is.EqualTo(Counter.CountTokens(entry.Content)),
                    $"Entry '{entry.Path}' must carry its own exact BPE count.");
            }
        });
    }

    [Test]
    public void Pack_admits_candidates_greedily_in_rank_order()
    {
        var candidates = new List<RepoContextBundlePacker.Candidate>
        {
            Candidate("first", Words(5)),
            Candidate("second", Words(5)),
            Candidate("third", Words(5)),
        };
        var twoFit = Reserve() + Cost(candidates[0]) + Cost(candidates[1]);

        var outcome = RepoContextBundlePacker.Pack(candidates, twoFit, Counter);

        Assert.That(
            outcome.Entries.Select(e => e.Path),
            Is.EqualTo(new[] { "first", "second" }),
            "Packing admits the highest-ranked candidates first, in order.");
    }

    [Test]
    public void Pack_is_deterministic_across_repeated_runs()
    {
        var candidates = new List<RepoContextBundlePacker.Candidate>
        {
            Candidate("a", Words(4)),
            Candidate("b", Words(40)),
            Candidate("c", Words(9)),
            Candidate("d", Words(2)),
        };
        const int budget = 30;

        var first = RepoContextBundlePacker.Pack(candidates, budget, Counter);
        var second = RepoContextBundlePacker.Pack(candidates, budget, Counter);

        Assert.Multiple(() =>
        {
            Assert.That(first.Entries.Select(e => e.Path), Is.EqualTo(second.Entries.Select(e => e.Path)));
            Assert.That(first.TotalTokens, Is.EqualTo(second.TotalTokens));
            Assert.That(first.Truncated, Is.EqualTo(second.Truncated));
            Assert.That(first.MinCandidateTokens, Is.EqualTo(second.MinCandidateTokens));
        });
    }

    [Test]
    public void Pack_skips_an_oversized_candidate_and_still_admits_later_ones()
    {
        var candidates = new List<RepoContextBundlePacker.Candidate>
        {
            Candidate("small-1", Words(3)),
            Candidate("huge", Words(500)),
            Candidate("small-2", Words(3)),
        };
        var budget = Reserve() + Cost(candidates[0]) + Cost(candidates[2]);

        var outcome = RepoContextBundlePacker.Pack(candidates, budget, Counter);

        Assert.Multiple(() =>
        {
            Assert.That(outcome.Entries.Select(e => e.Path), Is.EqualTo(new[] { "small-1", "small-2" }),
                "A candidate that does not fit is skipped, not a stop condition.");
            Assert.That(outcome.Truncated, Is.True, "Dropping a candidate marks the bundle truncated.");
            Assert.That(outcome.ResponseTokens, Is.LessThanOrEqualTo(budget));
        });
    }

    [Test]
    public void Pack_is_not_truncated_when_every_candidate_fits()
    {
        var candidates = new List<RepoContextBundlePacker.Candidate>
        {
            Candidate("a", Words(2)),
            Candidate("b", Words(3)),
        };

        var outcome = RepoContextBundlePacker.Pack(candidates, 10_000, Counter);

        Assert.Multiple(() =>
        {
            Assert.That(outcome.Entries, Has.Count.EqualTo(2));
            Assert.That(outcome.Truncated, Is.False);
        });
    }

    [Test]
    public void Pack_reports_the_cheapest_candidate_cost_even_when_it_is_dropped()
    {
        var candidates = new List<RepoContextBundlePacker.Candidate>
        {
            Candidate("big", Words(60)),
            Candidate("cheapest", Words(1)),
            Candidate("mid", Words(20)),
        };
        // The cheapest candidate's retry budget includes the bundle reserve it ships inside.
        var cheapest = Reserve() + Cost(candidates[1]);

        // A budget below every candidate admits nothing.
        var outcome = RepoContextBundlePacker.Pack(candidates, 1, Counter);

        Assert.Multiple(() =>
        {
            Assert.That(outcome.Entries, Is.Empty);
            Assert.That(outcome.MinCandidateTokens, Is.EqualTo(cheapest),
                "The minimum candidate cost spans every candidate, including dropped ones.");
        });
    }

    [Test]
    public void Pack_retry_budget_is_guaranteed_to_admit_at_least_one_entry()
    {
        var candidates = new List<RepoContextBundlePacker.Candidate>
        {
            Candidate("a", Words(30)),
            Candidate("b", Words(12)),
            Candidate("c", Words(45)),
        };

        // A budget of 1 admits nothing (every candidate is larger).
        var failed = RepoContextBundlePacker.Pack(candidates, 1, Counter);
        Assert.That(failed.Entries, Is.Empty, "Precondition: nothing fits the tiny budget.");

        // Retrying with the reported retry budget must admit at least the cheapest one.
        var retried = RepoContextBundlePacker.Pack(candidates, failed.MinCandidateTokens, Counter);

        Assert.Multiple(() =>
        {
            Assert.That(failed.MinCandidateTokens, Is.GreaterThan(1));
            Assert.That(retried.Entries, Is.Not.Empty,
                "The retry budget the packer reports must fit at least one entry.");
            Assert.That(retried.ResponseTokens, Is.LessThanOrEqualTo(failed.MinCandidateTokens),
                "The retry must actually fit inside the budget it was told to use.");
        });
    }

    [Test]
    public void Pack_carries_score_reasons_and_full_read_count_through_unchanged()
    {
        var candidates = new List<RepoContextBundlePacker.Candidate>
        {
            Candidate("src/Widget.cs", Words(2), score: 0.87, fullRead: 4096, "semantic", "chunk:file"),
        };

        var outcome = RepoContextBundlePacker.Pack(candidates, 10_000, Counter);

        var entry = outcome.Entries.Single();
        Assert.Multiple(() =>
        {
            Assert.That(entry.Path, Is.EqualTo("src/Widget.cs"));
            Assert.That(entry.Score, Is.EqualTo(0.87));
            Assert.That(entry.Reasons, Is.EqualTo(new[] { "semantic", "chunk:file" }));
            Assert.That(entry.FullReadTokenCount, Is.EqualTo(4096));
            Assert.That(entry.Content, Is.EqualTo(Words(2)));
        });
    }

    [Test]
    public void Pack_admits_nothing_for_a_non_positive_budget()
    {
        var candidates = new List<RepoContextBundlePacker.Candidate> { Candidate("a", Words(1)) };

        var outcome = RepoContextBundlePacker.Pack(candidates, 0, Counter);

        Assert.Multiple(() =>
        {
            Assert.That(outcome.Entries, Is.Empty);
            Assert.That(outcome.Truncated, Is.True);
        });
    }

    [Test]
    public void Pack_null_arguments_throw()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => RepoContextBundlePacker.Pack(null!, 10, Counter), Throws.ArgumentNullException);
            Assert.That(
                () => RepoContextBundlePacker.Pack([], 10, null!), Throws.ArgumentNullException);
        });
    }
}
