using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;
using Orleans.Lattice.Vector.Persistence;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Tests that the state the approximate plane answered from is observable from
/// outside the process, per query.
/// <para>
/// Issue #2252 was filed because semantic retrieval had never been <i>observed</i>
/// serving from the approximate plane, and the reason it could not be observed is
/// that <see cref="RepoContextAnnServingState"/> was computed per query in
/// <see cref="RepoContextAnnIndexHandle"/> and then discarded one frame up: the
/// caller tested only <c>state != Bootstrapping</c>. That collapsed the two states
/// that matter - a plane answering by exhaustive scan while it warms, and a plane
/// answering from a trained partitioning - into a single "served" count, so the
/// trained path was <b>unobservable</b>, and an unobservable path is
/// indistinguishable from a dead one.
/// </para>
/// <para>
/// The instrument these tests defend deliberately counts <i>every</i> outcome
/// rather than only the serving ones. A counter that rose only when the trained
/// path served would read a structural zero at the highest rate of the very hazard
/// it exists to catch, which manufactures reassurance instead of removing it.
/// Counting the whole partition makes the zero denominated, and therefore evidence.
/// </para>
/// </summary>
[TestFixture]
public sealed class RepoContextAnnServingStateObservabilityTests
{
    private const string RepoId = "acme";

    private static readonly EmbeddingSpaceTag Space =
        new("test-model", 3, VectorNormalization.UnitL2);

    private CancellationToken Ct => TestContext.CurrentContext.CancellationToken;

    [Test]
    public async Task A_plane_that_never_serves_leaves_a_denominated_zero_rather_than_a_silence()
    {
        using var logs = new CapturingLoggerProvider();
        using var measurements = new AnnSearchMeasurements();
        // The live field state issue #2252 describes: every query reaches the
        // fallback because the plane holds no usable index.
        var index = Create(logs, BootstrappingPlaneWithCorpus(0), ExactReturning("repo/acme/file/src/A.cs"));

        for (var i = 0; i < 5; i++)
        {
            await index.SearchAsync(RepoId, new float[] { 1f, 0f, 0f }, Space, 5, Ct);
        }

        Assert.Multiple(() =>
        {
            Assert.That(
                measurements.Count(RepoContextRetrievalGuardReporter.StateBootstrappingTag),
                Is.EqualTo(5),
                "The total has to rise with traffic, because it is the denominator that gives the other two "
                + "states' zeros their meaning.");
            Assert.That(
                measurements.Count(RepoContextRetrievalGuardReporter.StateApproximateTag),
                Is.Zero);
            Assert.That(
                measurements.Count(RepoContextRetrievalGuardReporter.StateExhaustiveTag),
                Is.Zero,
                "Five queries were served and none of them by the plane. That is a positive statement about "
                + "the deployment, and it is exactly the statement issue #2252 could not get from the "
                + "container: 'approximate is zero' alone would be indistinguishable from 'nobody asked'.");
        });
    }

    [Test]
    public async Task The_first_answer_from_a_trained_partitioning_is_announced_at_information()
    {
        using var logs = new CapturingLoggerProvider();
        var index = Create(
            logs,
            PlaneReturning(Answer(RepoContextAnnServingState.Approximate, "repo/acme/file/src/A.cs")),
            ExactReturning("repo/acme/file/src/A.cs"));

        await index.SearchAsync(RepoId, new float[] { 1f, 0f, 0f }, Space, 5, Ct);

        var announced = Information(logs);

        Assert.Multiple(() =>
        {
            Assert.That(
                announced.Any(m => m.Contains("from state Approximate for the first time", StringComparison.Ordinal)),
                Is.True,
                "This transition has never been observed in any deployment, which is the whole of issue "
                + "#2252. Its first execution is therefore the single most informative line the container "
                + "can emit, and it must not depend on someone reading a search response's retrievalPath.");
            Assert.That(
                announced.Any(m => m.Contains("trained partitioning", StringComparison.Ordinal)),
                Is.True,
                "The enumeration name alone does not tell an operator that this is the state the plane "
                + "exists to reach, so the meaning travels with the line.");
        });
    }

    [Test]
    public async Task Answering_at_all_and_answering_from_a_trained_partitioning_are_announced_apart()
    {
        using var logs = new CapturingLoggerProvider();
        var plane = Substitute.For<IRepoContextAnnIndex>();
        var outcomes = new Queue<RepoContextAnnSearchOutcome>(
        [
            Answer(RepoContextAnnServingState.Exhaustive, "repo/acme/file/src/A.cs"),
            Answer(RepoContextAnnServingState.Exhaustive, "repo/acme/file/src/A.cs"),
            Answer(RepoContextAnnServingState.Approximate, "repo/acme/file/src/A.cs"),
        ]);
        plane.SearchAsync(
                Arg.Any<string>(),
                Arg.Any<ReadOnlyMemory<float>>(),
                Arg.Any<EmbeddingSpaceTag>(),
                Arg.Any<int>(),
                Arg.Any<CancellationToken>())
            .Returns(_ => new ValueTask<RepoContextAnnSearchOutcome>(outcomes.Dequeue()));
        var index = Create(logs, plane, ExactReturning("repo/acme/file/src/A.cs"));

        await index.SearchAsync(RepoId, new float[] { 1f, 0f, 0f }, Space, 5, Ct);
        await index.SearchAsync(RepoId, new float[] { 1f, 0f, 0f }, Space, 5, Ct);
        await index.SearchAsync(RepoId, new float[] { 1f, 0f, 0f }, Space, 5, Ct);

        var announced = Information(logs);

        Assert.Multiple(() =>
        {
            Assert.That(
                announced.Count(m => m.Contains("from state Exhaustive for the first time", StringComparison.Ordinal)),
                Is.EqualTo(1),
                "Announced once and counted thereafter: the state holds on every query while the plane "
                + "warms, so a line per query would trade an unreadable state for a flood.");
            Assert.That(
                announced.Count(m => m.Contains("from state Approximate for the first time", StringComparison.Ordinal)),
                Is.EqualTo(1),
                "Reaching a trained partitioning is a new fact about the deployment even though the plane "
                + "had already answered. Before this, both states set the same flag and the second "
                + "transition - the one that matters - was silent.");
        });
    }

    [Test]
    public async Task The_summary_reports_the_trained_and_exhaustive_answers_apart()
    {
        using var logs = new CapturingLoggerProvider();
        var index = Create(
            logs,
            PlaneReturning(Answer(RepoContextAnnServingState.Exhaustive, "repo/acme/file/src/A.cs")),
            ExactReturning("repo/acme/file/src/A.cs"));

        await index.SearchAsync(RepoId, new float[] { 1f, 0f, 0f }, Space, 5, Ct);
        await index.SearchAsync(RepoId, new float[] { 1f, 0f, 0f }, Space, 5, Ct);

        var summary = Summary(logs);

        Assert.Multiple(() =>
        {
            Assert.That(summary, Does.Contain("0 came from a trained partitioning"),
                "A plane warming up and a plane in its steady state both report as 'served' in the "
                + "aggregate, so the aggregate cannot answer the question issue #2252 asks.");
            Assert.That(summary, Does.Contain("2 from an exhaustive scan"));
        });
    }

    [Test]
    public void An_unrecognised_serving_state_cannot_reach_the_meter_as_an_unbounded_tag()
    {
        using var measurements = new AnnSearchMeasurements();
        using var reporter = new RepoContextRetrievalGuardReporter(summaryInterval: TimeSpan.Zero);

        reporter.RecordPlaneOutcome(RepoId, (RepoContextAnnServingState)97);

        Assert.That(
            measurements.Count(RepoContextRetrievalGuardReporter.StateBootstrappingTag),
            Is.EqualTo(1),
            "An out-of-range state resolves to the conservative value rather than putting arbitrary text on "
            + "the meter, because an unbounded tag would cost far more than the misattribution.");
    }

    [Test]
    public void Each_serving_state_is_announced_exactly_once_and_per_repository()
    {
        using var reporter = new RepoContextRetrievalGuardReporter(summaryInterval: TimeSpan.Zero);

        var firstExhaustive = reporter.RecordPlaneOutcome(RepoId, RepoContextAnnServingState.Exhaustive);
        var secondExhaustive = reporter.RecordPlaneOutcome(RepoId, RepoContextAnnServingState.Exhaustive);
        var firstApproximate = reporter.RecordPlaneOutcome(RepoId, RepoContextAnnServingState.Approximate);
        var otherRepoFirst = reporter.RecordPlaneOutcome("other", RepoContextAnnServingState.Approximate);

        Assert.Multiple(() =>
        {
            Assert.That(firstExhaustive, Is.True);
            Assert.That(secondExhaustive, Is.False);
            Assert.That(firstApproximate, Is.True,
                "The states are announced independently, so a plane that warmed before it trained still "
                + "reports the transition that matters.");
            Assert.That(otherRepoFirst, Is.True,
                "The plane is built per repository and embedding space, so one repository reaching a "
                + "trained partitioning says nothing about another's.");
        });
    }

    [Test]
    public void A_bootstrapping_outcome_is_metered_but_not_counted_as_served()
    {
        using var reporter = new RepoContextRetrievalGuardReporter(summaryInterval: TimeSpan.Zero);

        reporter.RecordSearch(RepoId);
        reporter.RecordPlaneOutcome(RepoId, RepoContextAnnServingState.Bootstrapping);
        reporter.RecordSearch(RepoId);
        reporter.RecordPlaneOutcome(RepoId, RepoContextAnnServingState.Approximate);

        var snapshot = reporter.Snapshot(RepoId);

        Assert.Multiple(() =>
        {
            Assert.That(snapshot.PlaneServed, Is.EqualTo(1));
            Assert.That(snapshot.PlaneApproximate, Is.EqualTo(1));
            Assert.That(snapshot.PlaneExhaustive, Is.Zero);
            Assert.That(snapshot.Bootstrapping, Is.EqualTo(1),
                "A query the plane could not answer reached the guards below it, and must keep counting as "
                + "one: folding it into PlaneServed would overstate the plane and hide the ladder.");
        });
    }

    [Test]
    public void Serving_state_counters_survive_concurrent_searches()
    {
        using var reporter = new RepoContextRetrievalGuardReporter(summaryInterval: TimeSpan.Zero);

        Parallel.For(0, 1_000, i =>
            reporter.RecordPlaneOutcome(
                RepoId,
                i % 2 == 0
                    ? RepoContextAnnServingState.Approximate
                    : RepoContextAnnServingState.Exhaustive));

        var snapshot = reporter.Snapshot(RepoId);

        Assert.Multiple(() =>
        {
            Assert.That(snapshot.PlaneApproximate, Is.EqualTo(500));
            Assert.That(snapshot.PlaneExhaustive, Is.EqualTo(500));
            Assert.That(snapshot.PlaneServed, Is.EqualTo(1_000),
                "A lost increment here would understate the trained path, which is the one reading anybody "
                + "is going to act on.");
        });
    }

    private static AnnRepoContextSemanticIndex Create(
        CapturingLoggerProvider logs,
        IRepoContextAnnIndex plane,
        IRepoContextSemanticIndex exact)
        => new(
            plane,
            exact,
            RepoContextExactScanBudgets.Default(),
            new RepoContextExactScanBreaker(),
            // Zero spacing so the steady-state line is readable per search; the
            // pacing itself is covered by RepoContextRetrievalGuardReporterTests.
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

    private static RepoContextAnnSearchOutcome Answer(
        RepoContextAnnServingState state, params string[] keys)
        => new(state, keys.Select((k, i) => new RepoContextVectorMatch($"ann-{i}", k, 1d)).ToArray());

    /// <summary>
    /// Collects measurements on the approximate-plane search instrument, keyed by
    /// the serving-state tag, for the lifetime of one test.
    /// </summary>
    private sealed class AnnSearchMeasurements : IDisposable
    {
        private readonly Dictionary<string, long> _byState = new(StringComparer.Ordinal);
        private readonly MeterListener _listener = new();

        public AnnSearchMeasurements()
        {
            _listener.InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == RepoContextUsageRecorder.MeterName
                    && instrument.Name == RepoContextRetrievalGuardReporter.AnnSearchInstrumentName)
                {
                    l.EnableMeasurementEvents(instrument);
                }
            };
            _listener.SetMeasurementEventCallback<long>((_, measurement, tags, _) =>
            {
                foreach (var tag in tags)
                {
                    if (tag.Key != RepoContextRetrievalGuardReporter.StateTagKey
                        || tag.Value is not string state)
                    {
                        continue;
                    }

                    lock (_byState)
                    {
                        _byState[state] = _byState.GetValueOrDefault(state) + measurement;
                    }
                }
            });
            _listener.Start();
        }

        public long Count(string state)
        {
            lock (_byState)
            {
                return _byState.GetValueOrDefault(state);
            }
        }

        public void Dispose() => _listener.Dispose();
    }
}
