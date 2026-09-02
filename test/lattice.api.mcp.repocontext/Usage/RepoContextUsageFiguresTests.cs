namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Usage;

/// <summary>
/// Unit tests for <see cref="RepoContextUsageFigures"/>, the conservative crediting rule. They prove
/// that read-replacement credit is given only for delivered whole-file-equivalent content (slices
/// detail), and never for discovery, partial detail, reused/suppressed content, or missing figures.
/// </summary>
[TestFixture]
public sealed class RepoContextUsageFiguresTests
{
    private static RepoContextContextEntry Entry(int tokenCount, int? fullReadTokenCount)
        => new()
        {
            Path = "src/Widget.cs",
            Score = 1.0,
            TokenCount = tokenCount,
            FullReadTokenCount = fullReadTokenCount,
            Content = "body",
        };

    private static RepoContextContextResult Result(
        string detail, int totalTokens, params RepoContextContextEntry[] entries)
        => Result(detail, totalTokens, totalTokens, entries);

    private static RepoContextContextResult Result(
        string detail, int totalTokens, int responseTokens, params RepoContextContextEntry[] entries)
        => new()
        {
            RepoId = "acme",
            Task = "task",
            Mode = "keyword",
            RetrievalPath = RepoContextRetrievalPath.KeywordNoEmbedder,
            Detail = detail,
            BudgetTokens = 10_000,
            TotalTokens = totalTokens,
            ResponseTokens = responseTokens,
            Truncated = false,
            RetryBudgetTokens = null,
            Entries = entries,
        };

    [Test]
    public void ForContextBundle_null_result_throws()
        => Assert.That(() => RepoContextUsageFigures.ForContextBundle(null!), Throws.ArgumentNullException);

    [Test]
    public void ForContextBundle_attributes_the_context_command()
    {
        var usage = RepoContextUsageFigures.ForContextBundle(Result("slices", 0));
        Assert.That(usage.Command, Is.EqualTo("repocontext_context"));
    }

    [Test]
    public void ForContextBundle_records_the_exact_total_as_response_tokens()
    {
        var usage = RepoContextUsageFigures.ForContextBundle(Result("slices", 123, Entry(123, 400)));
        Assert.That(usage.ResponseTokens, Is.EqualTo(123));
    }

    [Test]
    public void ForContextBundle_charges_the_wire_cost_not_the_content_total()
    {
        // The accounting must charge what the caller actually received - envelope and the
        // SDK's dual emission included - otherwise it under-reports the cost of every
        // bundle, the same blind spot that let the response outgrow its budget (#1811).
        var result = Result("slices", 100, 260, Entry(100, 400));
        var usage = RepoContextUsageFigures.ForContextBundle(result);
        Assert.Multiple(() =>
        {
            Assert.That(usage.ResponseTokens, Is.EqualTo(260));
            Assert.That(usage.NetSavedTokens, Is.EqualTo(400 - 260));
        });
    }

    [Test]
    public void ForContextBundle_slices_credits_the_sum_of_full_read_costs()
    {
        var result = Result("slices", 30, Entry(10, 400), Entry(20, 600));
        var usage = RepoContextUsageFigures.ForContextBundle(result);
        Assert.Multiple(() =>
        {
            Assert.That(usage.ReplacedReadTokens, Is.EqualTo(1000),
                "Slices delivers whole-file-equivalent content, so each entry's full-read cost is credited.");
            Assert.That(usage.NetSavedTokens, Is.EqualTo(1000 - 30));
        });
    }

    [Test]
    public void ForContextBundle_outline_earns_zero_read_replacement_credit()
    {
        // Outline is partial (declared-symbol skeleton), never a whole-file replacement.
        var usage = RepoContextUsageFigures.ForContextBundle(Result("outline", 50, Entry(50, 900)));
        Assert.That(usage.ReplacedReadTokens, Is.Zero);
    }

    [Test]
    public void ForContextBundle_paths_earns_zero_read_replacement_credit()
    {
        // Paths is a pointer only, never a whole-file replacement.
        var usage = RepoContextUsageFigures.ForContextBundle(Result("paths", 5, Entry(5, 900)));
        Assert.That(usage.ReplacedReadTokens, Is.Zero);
    }

    [Test]
    public void ForContextBundle_slices_treats_a_null_full_read_as_zero_credit()
    {
        // A file that was never content-processed carries no full-read basis, so it is not credited.
        var usage = RepoContextUsageFigures.ForContextBundle(Result("slices", 10, Entry(10, null)));
        Assert.That(usage.ReplacedReadTokens, Is.Zero);
    }

    [Test]
    public void ForContextBundle_slices_ignores_a_non_positive_full_read()
    {
        var usage = RepoContextUsageFigures.ForContextBundle(Result("slices", 10, Entry(10, 0), Entry(10, -5)));
        Assert.That(usage.ReplacedReadTokens, Is.Zero);
    }

    [Test]
    public void ForContextBundle_empty_bundle_records_a_zero_cost_zero_credit_call()
    {
        var usage = RepoContextUsageFigures.ForContextBundle(Result("paths", 0));
        Assert.Multiple(() =>
        {
            Assert.That(usage.ResponseTokens, Is.Zero);
            Assert.That(usage.ReplacedReadTokens, Is.Zero);
            Assert.That(usage.NetSavedTokens, Is.Zero);
        });
    }

    [Test]
    public void ForContextBundle_does_not_credit_reused_content()
    {
        // Reused/suppressed content lives in result.Reused, never in Entries, so it is structurally
        // excluded from the credit. A slices bundle whose only surviving entry is a fresh delivery
        // credits only that entry - the reuse ledger is never consulted for credit.
        var result = new RepoContextContextResult
        {
            RepoId = "acme",
            Task = "task",
            Mode = "keyword",
            RetrievalPath = RepoContextRetrievalPath.KeywordNoEmbedder,
            Detail = "slices",
            BudgetTokens = 10_000,
            TotalTokens = 10,
            ResponseTokens = 10,
            Truncated = false,
            RetryBudgetTokens = null,
            Entries = [Entry(10, 400)],
            Reused =
            [
                new RepoContextReuseAck { Path = "src/Other.cs", Kind = "span", Receipt = "r1" },
            ],
        };

        var usage = RepoContextUsageFigures.ForContextBundle(result);
        Assert.That(usage.ReplacedReadTokens, Is.EqualTo(400),
            "Only the delivered entry is credited; suppressed content in Reused is never credited.");
    }
}
