using Microsoft.Extensions.Logging;
using NSubstitute;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Lattice.Vector.Persistence;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Tests that <see cref="AnnRepoContextSemanticIndex"/> reports what its two
/// retrieval-ladder guards did at a level a deployed container running at
/// information actually emits.
/// <para>
/// These are behavioural tests about the log, which is unusual and deliberate.
/// The guards shipped in pull request #2206 are correct and unobservable, and an
/// unobservable guard cannot be verified in the field: issue #2253 exists because
/// the budget's zero-skips (a measured absence) and the breaker's zero-skips
/// (merely unlogged, because that branch sat at debug) read identically from
/// outside the process. The lines asserted here are the fix, so a test that lets
/// them regress to debug or to silence would let the defect back in.
/// </para>
/// </summary>
[TestFixture]
public sealed class AnnRepoContextSemanticIndexGuardObservabilityTests
{
    private const string RepoId = "acme";

    private static readonly EmbeddingSpaceTag Space =
        new("test-model", 3, VectorNormalization.UnitL2);

    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    [Test]
    public async Task A_guard_that_declines_every_query_is_reported_rather_than_silent()
    {
        using var logs = new CapturingLoggerProvider();
        // The field state issue #2253 describes: the plane is still building, so
        // the budget is reached on every query, but no corpus count exists yet so
        // it fails open every time and never skips anything.
        var index = Create(logs, BootstrappingPlaneWithCorpus(0), ExactReturning("repo/acme/file/src/A.cs"));

        await index.SearchAsync(RepoId, new float[] { 1f, 0f, 0f }, Space, 5, Ct);

        var announced = Information(logs);

        Assert.Multiple(() =>
        {
            Assert.That(
                announced.Any(m => m.Contains("CorpusUnknown", StringComparison.Ordinal)),
                Is.True,
                "Before this line, a budget that was reached on every query and declined produced exactly the "
                + "same output as one that was never reached at all: none. Naming the decision is what makes "
                + "the fail-open branch a reading rather than an inference.");
            Assert.That(
                announced.Any(m => m.Contains("no corpus count exists yet", StringComparison.Ordinal)),
                Is.True,
                "An operator reading the container needs why, not only what: an uncounted corpus is a "
                + "bootstrap state that resolves itself, and is not the same finding as a corpus that was "
                + "counted and judged too large.");
        });
    }

    [Test]
    public async Task The_periodic_summary_distinguishes_never_reached_from_reached_and_failed_open()
    {
        using var neverReachedLogs = new CapturingLoggerProvider();
        using var failedOpenLogs = new CapturingLoggerProvider();

        var serving = Create(
            neverReachedLogs,
            PlaneReturning(Answer(RepoContextAnnServingState.Approximate, "repo/acme/file/src/A.cs")),
            ExactReturning("repo/acme/file/src/A.cs"));
        var failingOpen = Create(
            failedOpenLogs, BootstrappingPlaneWithCorpus(0), ExactReturning("repo/acme/file/src/A.cs"));

        await serving.SearchAsync(RepoId, new float[] { 1f, 0f, 0f }, Space, 5, Ct);
        await failingOpen.SearchAsync(RepoId, new float[] { 1f, 0f, 0f }, Space, 5, Ct);

        var neverReached = Summary(neverReachedLogs);
        var failedOpen = Summary(failedOpenLogs);

        Assert.Multiple(() =>
        {
            Assert.That(neverReached, Does.Contain("0 evaluation(s)"),
                "The approximate plane answered, so the ladder below it never ran. Reporting zero evaluations "
                + "says that positively instead of leaving it to be inferred from an absent line.");
            Assert.That(failedOpen, Does.Contain("1 evaluation(s)"));
            Assert.That(
                failedOpen,
                Does.Contain("1 that read an uncounted corpus and so failed open and let the gather run"),
                "Both deployments skipped nothing, and only the evaluation count separates a guard that was "
                + "never asked from one that was asked - the distinction issue #2253 says could not be made "
                + "from a deployed container. The verb matters as much as the count: this line said "
                + "'declined' until issue #2362, and CorpusUnknown does not decline anything. It fails open "
                + "and the gather runs, which is why the breaker rather than the budget is what holds the "
                + "bootstrap window shut - and reading 'declined' here sends an operator to tune a budget "
                + "that is not the thing suppressing their retrieval.");
        });
    }

    [Test]
    public async Task The_summary_reports_the_breakers_live_state_and_not_only_its_transitions()
    {
        using var logs = new CapturingLoggerProvider();
        var breaker = new RepoContextExactScanBreaker();
        var index = Create(logs, BootstrappingPlaneWithCorpus(0), ExactThatStalls(), breaker);

        await index.SearchAsync(RepoId, new float[] { 1f, 0f, 0f }, Space, 5, Ct);
        await index.SearchAsync(RepoId, new float[] { 1f, 0f, 0f }, Space, 5, Ct);

        var summaries = Information(logs)
            .Where(m => m.Contains("retrieval-ladder guards", StringComparison.Ordinal))
            .ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(summaries, Is.Not.Empty);
            Assert.That(summaries[^1], Does.Contain("currently open"),
                "A breaker that tripped once and is still holding is the state an operator most needs, and it "
                + "is precisely the state a transition-only log cannot report: the transition has already "
                + "scrolled away, and the steady state emits nothing.");
            Assert.That(summaries[^1], Does.Contain("1 trip(s)"));
            Assert.That(summaries[^1], Does.Contain("1 gather(s) suppressed while open"));
        });
    }

    [Test]
    public async Task The_first_suppressed_gather_is_visible_at_information_and_the_rest_are_not()
    {
        using var logs = new CapturingLoggerProvider();
        var index = Create(logs, BootstrappingPlaneWithCorpus(0), ExactThatStalls());

        await index.SearchAsync(RepoId, new float[] { 1f, 0f, 0f }, Space, 5, Ct);
        await index.SearchAsync(RepoId, new float[] { 1f, 0f, 0f }, Space, 5, Ct);
        await index.SearchAsync(RepoId, new float[] { 1f, 0f, 0f }, Space, 5, Ct);

        var suppressionAnnouncements = Information(logs)
            .Count(m => m.Contains("is open and suppressed its first gather", StringComparison.Ordinal));

        Assert.That(suppressionAnnouncements, Is.EqualTo(1),
            "That this branch executes at all had to be visible without a debug-level restart, but it runs on "
            + "every query once the breaker holds. Announcing the first and counting the rest into the "
            + "summary keeps the proof without the flood.");
    }

    [Test]
    public async Task Closing_the_breaker_is_reported_every_time_it_happens()
    {
        using var logs = new CapturingLoggerProvider();
        var plane = Substitute.For<IRepoContextAnnIndex>();
        var outcomes = new Queue<RepoContextAnnSearchOutcome>(new[]
        {
            RepoContextAnnSearchOutcome.Bootstrapping,
            Answer(RepoContextAnnServingState.Approximate, "repo/acme/file/src/A.cs"),
        });
        plane.SearchAsync(
                Arg.Any<string>(),
                Arg.Any<ReadOnlyMemory<float>>(),
                Arg.Any<EmbeddingSpaceTag>(),
                Arg.Any<int>(),
                Arg.Any<CancellationToken>())
            .Returns(_ => new ValueTask<RepoContextAnnSearchOutcome>(outcomes.Dequeue()));
        var index = Create(logs, plane, ExactThatStalls());

        await index.SearchAsync(RepoId, new float[] { 1f, 0f, 0f }, Space, 5, Ct);
        await index.SearchAsync(RepoId, new float[] { 1f, 0f, 0f }, Space, 5, Ct);

        Assert.That(
            Information(logs).Any(m => m.Contains("breaker for acme closed", StringComparison.Ordinal)),
            Is.True,
            "The reset path has never executed in production, so its first execution is exactly the event "
            + "worth an operator-visible line. It is rare by construction - once per trip - so logging it "
            + "every time costs nothing and proves recovery happened rather than leaving it to be assumed.");
    }

    [Test]
    public async Task An_unbounded_budget_is_reported_as_inert_rather_than_as_working()
    {
        using var logs = new CapturingLoggerProvider();
        var index = Create(
            logs,
            BootstrappingPlaneWithCorpus(90_000),
            ExactReturning("repo/acme/file/src/A.cs"),
            budget: RepoContextExactScanBudgets.Unbounded());

        await index.SearchAsync(RepoId, new float[] { 1f, 0f, 0f }, Space, 5, Ct);

        Assert.Multiple(() =>
        {
            Assert.That(
                Information(logs).Any(m => m.Contains("Unbounded", StringComparison.Ordinal)),
                Is.True,
                "A deployment whose scan-page configuration disables the ceiling has the guard present and "
                + "inert. That reads as a working guard from every other signal, so it has to be stated.");
            Assert.That(Summary(logs), Does.Contain("an affordable unbounded"),
                "Rendering the sentinel as int.MaxValue would read as a very large but real threshold, which "
                + "is a different and reassuring claim from the true one: there is no threshold.");
        });
    }

    [Test]
    public async Task A_skipped_gather_reports_the_corpus_it_judged_against()
    {
        using var logs = new CapturingLoggerProvider();
        var index = Create(
            logs,
            BootstrappingPlaneWithCorpus(90_000),
            ExactReturning("repo/acme/file/src/A.cs"),
            budget: RepoContextExactScanBudgets.Default());

        await index.SearchAsync(RepoId, new float[] { 1f, 0f, 0f }, Space, 5, Ct);

        var announced = Information(logs);

        Assert.Multiple(() =>
        {
            Assert.That(
                announced.Any(m => m.Contains("Exceeded", StringComparison.Ordinal)
                    && m.Contains("90000", StringComparison.Ordinal)),
                Is.True,
                "A skip is only actionable with the two numbers that produced it: without the corpus and the "
                + "affordable count, an operator cannot tell an over-tight budget from a genuinely large "
                + "repository.");
            Assert.That(Summary(logs), Does.Contain("1 skipped as unaffordable"));
        });
    }

    [Test]
    public async Task Guard_counters_are_kept_per_repository()
    {
        using var logs = new CapturingLoggerProvider();
        // Only this repository's gather stalls. The other is small enough to answer,
        // which is exactly why the breaker is keyed per repository in the first
        // place - and therefore what the summary has to report per repository too.
        var index = Create(logs, BootstrappingPlaneWithCorpus(0), ExactThatStallsFor(RepoId));

        await index.SearchAsync(RepoId, new float[] { 1f, 0f, 0f }, Space, 5, Ct);
        await index.SearchAsync(RepoId, new float[] { 1f, 0f, 0f }, Space, 5, Ct);
        await index.SearchAsync("other", new float[] { 1f, 0f, 0f }, Space, 5, Ct);

        var acme = Information(logs).Last(m => m.Contains("guards for acme", StringComparison.Ordinal));
        var other = Information(logs).Last(m => m.Contains("guards for other", StringComparison.Ordinal));

        Assert.Multiple(() =>
        {
            Assert.That(acme, Does.Contain("currently open"));
            Assert.That(acme, Does.Contain("1 gather(s) suppressed while open"));
            Assert.That(other, Does.Contain("currently closed"),
                "The breaker acts per repository, so a summary that reported another repository's open "
                + "breaker would send an operator to investigate a fallback that is in fact armed and "
                + "working here.");
            Assert.That(other, Does.Contain("0 trip(s)"),
                "Cumulative counters shared across repositories would attribute one repository's contention "
                + "to every other, which is worse than no counter: it is a confident wrong answer.");
        });
    }

    private static AnnRepoContextSemanticIndex Create(
        CapturingLoggerProvider logs,
        IRepoContextAnnIndex plane,
        IRepoContextSemanticIndex exact,
        RepoContextExactScanBreaker? breaker = null,
        RepoContextExactScanBudget? budget = null)
        => new(
            plane,
            exact,
            budget ?? RepoContextExactScanBudgets.Default(),
            breaker ?? new RepoContextExactScanBreaker(),
            // Zero spacing so the steady-state line is readable per search. The
            // pacing itself is covered by RepoContextRetrievalGuardReporterTests;
            // asserting it again here would only make these fixtures a test of the
            // clock.
            new RepoContextRetrievalGuardReporter(summaryInterval: TimeSpan.Zero),
            new LoggerFactory([logs]).CreateLogger<AnnRepoContextSemanticIndex>());

    private static string[] Information(CapturingLoggerProvider logs)
        => logs.Entries
            .Where(e => e.Level == LogLevel.Information)
            .Select(e => e.Message)
            .ToArray();

    private static string Summary(CapturingLoggerProvider logs)
        => Information(logs).Last(m => m.Contains("retrieval-ladder guards", StringComparison.Ordinal));

    private static IRepoContextAnnIndex PlaneReturning(RepoContextAnnSearchOutcome outcome)
    {
        var plane = Substitute.For<IRepoContextAnnIndex>();
        plane.SearchAsync(
                Arg.Any<string>(),
                Arg.Any<ReadOnlyMemory<float>>(),
                Arg.Any<EmbeddingSpaceTag>(),
                Arg.Any<int>(),
                Arg.Any<CancellationToken>())
            .Returns(new ValueTask<RepoContextAnnSearchOutcome>(outcome));
        return plane;
    }

    private static IRepoContextAnnIndex BootstrappingPlaneWithCorpus(int corpus)
    {
        var plane = PlaneReturning(RepoContextAnnSearchOutcome.Bootstrapping);
        plane.KnownVectorCount(Arg.Any<string>()).Returns(corpus);
        return plane;
    }

    private static IRepoContextSemanticIndex ExactReturning(params string[] keys)
    {
        var exact = Substitute.For<IRepoContextSemanticIndex>();
        exact.RetrievalPath.Returns(RepoContextRetrievalPath.SemanticExact);
        exact.SearchAsync(
                Arg.Any<string>(),
                Arg.Any<ReadOnlyMemory<float>>(),
                Arg.Any<EmbeddingSpaceTag>(),
                Arg.Any<int>(),
                Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<RepoContextVectorMatch>>(
                keys.Select((k, i) => new RepoContextVectorMatch($"ann-{i}", k, 1d)).ToArray()));
        return exact;
    }

    private static IRepoContextSemanticIndex ExactThatStalls() => ExactThatStallsFor(repoId: null);

    private static IRepoContextSemanticIndex ExactThatStallsFor(string? repoId)
    {
        var exact = Substitute.For<IRepoContextSemanticIndex>();
        exact.RetrievalPath.Returns(RepoContextRetrievalPath.SemanticExact);
        exact.SearchAsync(
                Arg.Any<string>(),
                Arg.Any<ReadOnlyMemory<float>>(),
                Arg.Any<EmbeddingSpaceTag>(),
                Arg.Any<int>(),
                Arg.Any<CancellationToken>())
            .Returns<Task<IReadOnlyList<RepoContextVectorMatch>>>(call =>
            {
                if (repoId is null || string.Equals((string)call[0], repoId, StringComparison.Ordinal))
                {
                    throw new ScanPageStalledException("a page fill did not return inside its ceiling");
                }

                return Task.FromResult<IReadOnlyList<RepoContextVectorMatch>>(
                    [new RepoContextVectorMatch("ann-0", "repo/other/file/src/A.cs", 1d)]);
            });
        return exact;
    }

    private static RepoContextAnnSearchOutcome Answer(
        RepoContextAnnServingState state, params string[] keys)
        => new(state, keys.Select((k, i) => new RepoContextVectorMatch($"ann-{i}", k, 1d)).ToArray());
}
