using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Usage;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Unit tests for <see cref="RepoContextRetrievalGuardReporter"/>, the counter
/// that makes the retrieval ladder's two guards readable from a container running
/// at information level.
/// <para>
/// The invariant every test here defends is one distinction: a guard that was
/// never reached and a guard that was reached and declined must not read the same.
/// Before issue #2253 they both produced silence, and an operator could not tell
/// an inert budget from an active one.
/// </para>
/// </summary>
[TestFixture]
public sealed class RepoContextRetrievalGuardReporterTests
{
    private const string RepoId = "acme";

    [Test]
    public void A_repository_nothing_was_recorded_for_reads_as_all_zeros()
    {
        var reporter = new RepoContextRetrievalGuardReporter();

        var snapshot = reporter.Snapshot(RepoId);

        Assert.Multiple(() =>
        {
            Assert.That(snapshot.IsEmpty, Is.True);
            Assert.That(snapshot.BudgetEvaluations, Is.Zero,
                "An unseen repository must read as zeros rather than as an absence, because the caller "
                + "renders it into a line whose whole job is to say positively that nothing happened.");
        });
    }

    [Test]
    public void Zero_evaluations_and_zero_skips_are_different_states()
    {
        var neverReached = new RepoContextRetrievalGuardReporter();
        var reachedAndDeclined = new RepoContextRetrievalGuardReporter();

        neverReached.RecordSearch(RepoId);
        neverReached.RecordPlaneServed(RepoId);

        reachedAndDeclined.RecordSearch(RepoId);
        reachedAndDeclined.RecordBudgetDecision(
            RepoId, RepoContextExactScanBudgetDecision.CorpusUnknown, corpus: 0, affordable: 1_280);

        var never = neverReached.Snapshot(RepoId);
        var declined = reachedAndDeclined.Snapshot(RepoId);

        Assert.Multiple(() =>
        {
            Assert.That(never.BudgetEvaluations, Is.Zero);
            Assert.That(declined.BudgetEvaluations, Is.EqualTo(1));
            Assert.That(never.BudgetExceeded, Is.Zero);
            Assert.That(declined.BudgetExceeded, Is.Zero,
                "Both report zero skips, which is exactly why the skip count alone cannot distinguish them. "
                + "The evaluation count is what separates absence of evidence from evidence of absence, and "
                + "collapsing the two is the defect issue #2253 was filed over.");
            Assert.That(never.Bootstrapping, Is.Zero,
                "A search the plane answered never reached the ladder, so it must not be counted as one that "
                + "did and found nothing to do.");
            Assert.That(declined.Bootstrapping, Is.EqualTo(1));
        });
    }

    [Test]
    public void Each_distinct_budget_decision_is_announced_exactly_once()
    {
        var reporter = new RepoContextRetrievalGuardReporter();

        var firstUnknown = reporter.RecordBudgetDecision(
            RepoId, RepoContextExactScanBudgetDecision.CorpusUnknown, 0, 1_280);
        var secondUnknown = reporter.RecordBudgetDecision(
            RepoId, RepoContextExactScanBudgetDecision.CorpusUnknown, 0, 1_280);
        var firstExceeded = reporter.RecordBudgetDecision(
            RepoId, RepoContextExactScanBudgetDecision.Exceeded, 90_000, 1_280);
        var secondExceeded = reporter.RecordBudgetDecision(
            RepoId, RepoContextExactScanBudgetDecision.Exceeded, 90_000, 1_280);

        Assert.Multiple(() =>
        {
            Assert.That(firstUnknown, Is.True);
            Assert.That(secondUnknown, Is.False,
                "The ladder reaches the same decision on every query once it settles, so announcing each "
                + "repetition would trade an unreadable state for a flood.");
            Assert.That(firstExceeded, Is.True,
                "A decision the repository has not reported before is a new fact about the deployment even "
                + "when another decision has already been announced.");
            Assert.That(secondExceeded, Is.False);
        });
    }

    [Test]
    public void Announcements_are_tracked_per_repository()
    {
        var reporter = new RepoContextRetrievalGuardReporter();

        reporter.RecordBudgetDecision(RepoId, RepoContextExactScanBudgetDecision.Exceeded, 90_000, 1_280);
        var otherRepoFirst = reporter.RecordBudgetDecision(
            "other", RepoContextExactScanBudgetDecision.Exceeded, 90_000, 1_280);

        Assert.That(otherRepoFirst, Is.True,
            "The guards act per repository - one repository's corpus says nothing about another's - so an "
            + "announcement that has fired for one must not suppress the proof for the next.");
    }

    [Test]
    public void The_first_suppressed_gather_is_announced_and_the_rest_are_counted()
    {
        var reporter = new RepoContextRetrievalGuardReporter();

        var first = reporter.RecordBreakerRepeatSkip(RepoId);
        var second = reporter.RecordBreakerRepeatSkip(RepoId);
        var third = reporter.RecordBreakerRepeatSkip(RepoId);

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.True,
                "That the path executed at all is the fact issue #2253 says could not be established from a "
                + "deployed container, because this branch only ever logged at debug.");
            Assert.That(second, Is.False);
            Assert.That(third, Is.False);
            Assert.That(reporter.Snapshot(RepoId).BreakerRepeatSkips, Is.EqualTo(3),
                "The steady state is carried by the count rather than by a line per query.");
        });
    }

    [Test]
    public void The_summary_is_paced_by_the_configured_interval()
    {
        var clock = new SettableTimeProvider();
        var reporter = new RepoContextRetrievalGuardReporter(clock, TimeSpan.FromMinutes(1));
        reporter.RecordSearch(RepoId);

        var immediate = reporter.TryTakeSummary(RepoId, out _);
        var tooSoon = reporter.TryTakeSummary(RepoId, out _);
        clock.Advance(TimeSpan.FromSeconds(59));
        var stillTooSoon = reporter.TryTakeSummary(RepoId, out _);
        clock.Advance(TimeSpan.FromSeconds(2));
        var due = reporter.TryTakeSummary(RepoId, out var snapshot);

        Assert.Multiple(() =>
        {
            Assert.That(immediate, Is.True,
                "A repository seen for the first time reports at once. Waiting out an interval it was never "
                + "in would leave the first minute of a container's life - the minute an operator is most "
                + "likely watching - unreadable.");
            Assert.That(tooSoon, Is.False);
            Assert.That(stillTooSoon, Is.False);
            Assert.That(due, Is.True);
            Assert.That(snapshot.Searches, Is.EqualTo(1));
        });
    }

    [Test]
    public void A_repository_with_no_activity_reports_nothing()
    {
        var reporter = new RepoContextRetrievalGuardReporter(summaryInterval: TimeSpan.Zero);

        Assert.That(reporter.TryTakeSummary(RepoId, out _), Is.False,
            "A repository this process has never served must not emit a line of zeros every interval. The "
            + "summary reports on activity; the honest answer for no activity is no line, because the "
            + "repository may simply not be searched here.");
    }

    [Test]
    public void A_negative_interval_is_clamped_rather_than_rejected()
    {
        var reporter = new RepoContextRetrievalGuardReporter(summaryInterval: TimeSpan.FromMinutes(-5));

        Assert.That(reporter.SummaryInterval, Is.EqualTo(TimeSpan.Zero),
            "A cadence is a diagnostic knob, so a nonsensical value degrades to the most frequent reading "
            + "rather than faulting a search path that is otherwise working.");
    }

    [Test]
    public void Counters_survive_concurrent_searches()
    {
        var reporter = new RepoContextRetrievalGuardReporter();

        Parallel.For(0, 1_000, _ =>
        {
            reporter.RecordSearch(RepoId);
            reporter.RecordBudgetDecision(
                RepoId, RepoContextExactScanBudgetDecision.CorpusUnknown, 0, 1_280);
        });

        var snapshot = reporter.Snapshot(RepoId);

        Assert.Multiple(() =>
        {
            Assert.That(snapshot.Searches, Is.EqualTo(1_000));
            Assert.That(snapshot.BudgetCorpusUnknown, Is.EqualTo(1_000),
                "Searches for one repository run concurrently, so a lost increment would understate a guard "
                + "that is in fact acting on every query - the precise misreading this counter exists to "
                + "prevent.");
        });
    }

    [Test]
    public void A_null_repository_is_rejected_on_every_entry_point()
    {
        var reporter = new RepoContextRetrievalGuardReporter();

        Assert.Multiple(() =>
        {
            Assert.That(() => reporter.RecordSearch(null!), Throws.ArgumentNullException);
            Assert.That(() => reporter.RecordPlaneServed(null!), Throws.ArgumentNullException);
            Assert.That(
                () => reporter.RecordBudgetDecision(
                    null!, RepoContextExactScanBudgetDecision.Unbounded, 0, 0),
                Throws.ArgumentNullException);
            Assert.That(() => reporter.RecordBreakerTrip(null!), Throws.ArgumentNullException);
            Assert.That(() => reporter.RecordBreakerRepeatSkip(null!), Throws.ArgumentNullException);
            Assert.That(() => reporter.RecordBreakerReset(null!), Throws.ArgumentNullException);
            Assert.That(() => reporter.Snapshot(null!), Throws.ArgumentNullException);
            Assert.That(() => reporter.TryTakeSummary(null!, out _), Throws.ArgumentNullException);
        });
    }
}
