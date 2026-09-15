using Microsoft.Extensions.Logging;
using NSubstitute;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Tests the fault-RATE discriminator issue #2948 was filed for, which is a
/// different question from the fault-CLASS classification the rest of this fixture
/// covers.
/// <para>
/// <b>What the classification alone could not see.</b> Run 13 of the acceptance
/// deployment answered zero of 189 searches from the approximate plane over six
/// hours. The exact fallback that should have covered that window was behind a
/// breaker that had been open for 6h11m across 25 gather faults, 24 half-open
/// probes of which none closed it - and all 25 faults were recorded as
/// <b>absorbed as capacity</b>, none as a degraded index. Every one of them was
/// individually a textbook timeout, so
/// <see cref="RepoContextExactGatherFault.Classify(System.Exception)"/> was right
/// about each in isolation and the aggregate was still wrong: the summary's own
/// reading rule turned "25 absorbed, 0 propagated" into "load", the container
/// reported healthy throughout, and nothing escalated.
/// </para>
/// <para>
/// <b>The discriminator is the rate, and these two tests are the same fault count
/// under different rates.</b> Both drive exactly three gather faults from exactly
/// three gathers. In
/// <see cref="A_capacity_fault_that_never_once_succeeds_is_reported_as_a_degraded_index"/>
/// they are consecutive; in
/// <see cref="Control_an_intermittent_capacity_fault_stays_absorbed_as_capacity"/>
/// a success is interleaved between each pair. Load produces the second shape and
/// cannot produce the first. Holding the fault count equal across the two is what
/// makes this a test of the rate rather than of the volume - a fix that propagated
/// on the third fault however it arrived would pass the first test and redden the
/// second.
/// </para>
/// <para>
/// <b>Every count here is asserted against an independent denominator.</b> The
/// number of faults recorded is checked against
/// <c>FaultingGather.Searches</c>, the gathers the harness actually ran, because a
/// tally of arms is an enumerated sample and not a population until something
/// outside the tally agrees on its size. Run 13's own
/// <c>last read corpus 0</c> is what an unasserted denominator looks like in the
/// field.
/// </para>
/// </summary>
public sealed partial class AnnRepoContextSemanticIndexGatherFaultTests
{
    /// <summary>
    /// Consecutive faults past the threshold, which the deployment had and the
    /// ladder could not say. The third fault must be reported rather than absorbed:
    /// counted on the <c>deterministic</c> arm, summarised as propagated, and
    /// rethrown so the search service answers
    /// <see cref="RepoContextRetrievalPath.KeywordIndexDegraded"/> instead of
    /// claiming a transient suppression that will clear.
    /// </summary>
    [Test]
    public async Task A_capacity_fault_that_never_once_succeeds_is_reported_as_a_degraded_index()
    {
        using var faults = new GatherFaultMeasurements();
        using var logs = new CapturingLoggerProvider();
        var exact = new FaultingGather
        {
            Fault = () => new TimeoutException("Response did not arrive on time in 00:00:30."),
        };
        var clock = new FaultClock();
        var breaker = Breaker(clock);
        var index = Create(UncountedBootstrappingPlane(), exact, breaker, logs);

        // Fault one: the breaker is closed, so the gather runs and is absorbed.
        Assert.That(await index.SearchAsync(RepoId, Query, Space, 5, Ct), Is.Empty);

        // A query inside the window is suppressed, so it runs no gather and cannot
        // contribute a fault. Included so the consecutive count below is known to
        // come from gathers rather than from queries.
        Assert.That(await index.SearchAsync(RepoId, Query, Space, 5, Ct), Is.Empty);

        // Fault two, on the first half-open probe. Still absorbed: two faults is
        // consistent with contention that has not cleared yet.
        clock.Advance(ProbeDelay + TimeSpan.FromSeconds(1));
        Assert.That(await index.SearchAsync(RepoId, Query, Space, 5, Ct), Is.Empty);

        // Fault three, on the second probe. The rate is now one hundred percent
        // across the whole episode and two elapsed probe windows, which load does
        // not produce.
        clock.Advance(ProbeDelay + ProbeDelay + TimeSpan.FromSeconds(1));
        Assert.That(
            async () => await index.SearchAsync(RepoId, Query, Space, 5, Ct),
            Throws.InstanceOf<TimeoutException>(),
            "Absorbing a fault asserts that waiting will fix it. After three faults with no success between "
            + "them that assertion is false, and the caller must be told the index is degraded rather than "
            + "told to keep waiting. RepoContextSearchService catches this and still answers keyword recall, "
            + "so the served answer does not change - only the claim made about why.");

        var recorded = faults.Count(RepoContextExactGatherFault.TimedOutTag)
            + faults.Count(RepoContextExactGatherFault.DeterministicTag)
            + faults.Count(RepoContextExactGatherFault.PropagatedTag)
            + faults.Count(RepoContextExactGatherFault.StalledTag)
            + faults.Count(RepoContextExactGatherFault.ExhaustedTag)
            + faults.Count(RepoContextExactGatherFault.AbandonedTag);

        Assert.Multiple(() =>
        {
            Assert.That(exact.Searches, Is.EqualTo(3),
                "Three gathers, not four: the suppressed query must not have reached the exact index. If it "
                + "did, the fault count below is denominated by something other than the gathers that ran.");
            Assert.That(recorded, Is.EqualTo(exact.Searches),
                "Every gather that ran faulted, and every fault was recorded exactly once. Without this the "
                + "arm counts are an enumerated sample rather than a population, which is the measurement "
                + "error this epic keeps paying for.");
            Assert.That(faults.Count(RepoContextExactGatherFault.DeterministicTag), Is.EqualTo(1),
                "Only the fault at the threshold escalates. Escalating earlier would report ordinary "
                + "contention as a broken index.");
            Assert.That(faults.Count(RepoContextExactGatherFault.TimedOutTag), Is.EqualTo(2),
                "The two faults below the threshold keep their own cause arm, so the episode reads as the "
                + "capacity fault it started as followed by the verdict that it stopped being one. "
                + "Overwriting them would trade one lost distinction for another.");
            Assert.That(faults.Count(RepoContextExactGatherFault.PropagatedTag), Is.Zero,
                "A deterministic capacity fault is not an integrity fault. Collapsing the two arms would "
                + "make 'the tree will not answer' and 'the stored bytes are wrong' the same observation.");
            Assert.That(breaker.ConsecutiveStalls(RepoId), Is.EqualTo(3),
                "Reporting the fault must not cost the backoff: the breaker still trips, so it keeps "
                + "probing and can still close on its own if the dependency recovers.");
        });

        var summary = LastGuardSummary(logs);
        Assert.Multiple(() =>
        {
            Assert.That(summary, Does.Contain("2 absorbed as capacity"));
            Assert.That(summary, Does.Contain("1 propagated as a degraded index"),
                "The summary is the line an operator reads. Run 13's said '25 absorbed, 0 propagated', and "
                + "its own reading rule turned that into 'load'.");
        });
    }

    /// <summary>
    /// The discriminator's negative arm, and the reason the positive one means
    /// anything. Three faults again, from three gathers again - but a gather
    /// succeeds between each pair, which is what load looks like. Nothing may
    /// escalate here, or the fix would have replaced a classifier that never
    /// reports with one that always does, and a loaded deployment would page on
    /// contention that is clearing on its own.
    /// </summary>
    [Test]
    public async Task Control_an_intermittent_capacity_fault_stays_absorbed_as_capacity()
    {
        using var faults = new GatherFaultMeasurements();
        using var logs = new CapturingLoggerProvider();
        var exact = new FaultingGather();
        var clock = new FaultClock();
        var breaker = Breaker(clock);
        var index = Create(UncountedBootstrappingPlane(), exact, breaker, logs);

        for (var round = 0; round < 3; round++)
        {
            exact.Fault = () => new TimeoutException("Response did not arrive on time in 00:00:30.");
            Assert.That(await index.SearchAsync(RepoId, Query, Space, 5, Ct), Is.Empty,
                $"Round {round}: the fault is absorbed, so the caller sees the no-matches answer.");
            Assert.That(breaker.ConsecutiveStalls(RepoId), Is.EqualTo(1),
                $"Round {round}: the intervening success must have reset the episode, so each fault is the "
                + "first of its own episode rather than the next of one long one.");

            // The half-open probe finds the contention gone, which is exactly the
            // evidence a deterministic fault cannot produce.
            exact.Fault = null;
            clock.Advance(ProbeDelay + TimeSpan.FromSeconds(1));
            Assert.That(await index.SearchAsync(RepoId, Query, Space, 5, Ct), Is.Not.Empty,
                $"Round {round}: the probe's gather completes, so the breaker closes on its own evidence.");
        }

        Assert.Multiple(() =>
        {
            Assert.That(exact.Searches, Is.EqualTo(6),
                "Three faulting gathers and three succeeding ones. The fault count below is denominated by "
                + "half of this, and the other half is what makes the rate less than one.");
            Assert.That(faults.Count(RepoContextExactGatherFault.TimedOutTag), Is.EqualTo(3),
                "The same three faults the deterministic test drives. Only their spacing differs.");
            Assert.That(faults.Count(RepoContextExactGatherFault.DeterministicTag), Is.Zero,
                "A measured zero on a primed arm. This is the assertion that fails if the escalation is "
                + "keyed on a cumulative fault count rather than on consecutive faults - which would report "
                + "every sufficiently long-lived loaded deployment as a broken index.");
            Assert.That(faults.Count(RepoContextExactGatherFault.PropagatedTag), Is.Zero);
        });

        Assert.That(LastGuardSummary(logs), Does.Contain("3 absorbed as capacity"));
    }

    /// <summary>
    /// The reading defect the issue calls out beside the loop: the summary rendered
    /// an <i>unknown</i> corpus as a <i>measured zero</i>. All 174 samples of run 13
    /// said "last read corpus 0" on a rig that demonstrably held vectors, and the
    /// only thing disambiguating it was a different clause several lines earlier
    /// about a different quantity.
    /// </summary>
    [Test]
    public async Task An_uncounted_corpus_is_reported_as_uncounted_rather_than_as_zero()
    {
        using var logs = new CapturingLoggerProvider();
        var index = Create(
            UncountedBootstrappingPlane(), new FaultingGather { Fault = null }, Breaker(new FaultClock()), logs);

        await index.SearchAsync(RepoId, Query, Space, 5, Ct);

        var summary = LastGuardSummary(logs);
        Assert.Multiple(() =>
        {
            Assert.That(summary, Does.Contain("last read corpus uncounted"));
            Assert.That(summary, Does.Not.Contain("last read corpus 0"),
                "An absence presented as a measurement. A reader concludes the repository is empty, and in "
                + "issue #2948 that conclusion was false for six hours.");
            Assert.That(summary, Does.Contain("1 evaluation(s)"),
                "The corpus is uncounted because the budget was asked and could not count it, not because "
                + "the budget was never asked. The two are different states and the line must separate "
                + "them, so this pins which one produced the wording above.");
        });
    }

    /// <summary>
    /// The other uncounted state: the budget was never reached at all, because the
    /// plane answered for itself. Reporting that as "uncounted" would claim a
    /// reading was attempted and failed when none was attempted.
    /// </summary>
    [Test]
    public async Task A_corpus_the_budget_never_read_is_reported_as_never_read()
    {
        using var logs = new CapturingLoggerProvider();
        var index = Create(ServingPlane(), new FaultingGather { Fault = null }, Breaker(new FaultClock()), logs);

        await index.SearchAsync(RepoId, Query, Space, 5, Ct);

        var summary = LastGuardSummary(logs);
        Assert.Multiple(() =>
        {
            Assert.That(summary, Does.Contain("0 evaluation(s)"));
            Assert.That(summary, Does.Contain("last read corpus not read (the budget was never reached)"));
        });
    }

    /// <summary>
    /// A plane that answers for itself, so neither guard is consulted.
    /// </summary>
    private static IRepoContextAnnIndex ServingPlane()
    {
        var plane = Substitute.For<IRepoContextAnnIndex>();
        plane.SearchAsync(
                Arg.Any<string>(),
                Arg.Any<ReadOnlyMemory<float>>(),
                Arg.Any<EmbeddingSpaceTag>(),
                Arg.Any<int>(),
                Arg.Any<CancellationToken>())
            .Returns(new ValueTask<RepoContextAnnSearchOutcome>(new RepoContextAnnSearchOutcome(
                RepoContextAnnServingState.Exhaustive,
                [new RepoContextVectorMatch("plane-0", "repo/acme/file/src/A.cs", 1d)])));
        plane.KnownVectorCount(Arg.Any<string>()).Returns(0);
        return plane;
    }

    /// <summary>
    /// The most recent periodic guard summary, which is the line an operator reads.
    /// </summary>
    private static string LastGuardSummary(CapturingLoggerProvider logs)
        => logs.Entries
            .Where(e => e.Level == LogLevel.Information)
            .Select(e => e.Message)
            .Last(m => m.Contains("retrieval-ladder guards", StringComparison.Ordinal));
}
