using Orleans.Lattice.Vector.Persistence;
using Orleans.Lattice.Vector.Tests.Fakes;

namespace Orleans.Lattice.Vector.Tests.Persistence;

/// <summary>
/// The wall-clock half of the build-slice bound.
/// <para>
/// A slice used to be bounded only by a vector count, which bounds it in units of
/// source items when every caller of the host driving it cares about units of
/// time. Those coincide only while the per-item cost is small and predictable,
/// which a source streaming over a remote store of record does not promise:
/// issue #2483 measured a 4,096-vector slice running for twenty minutes thirteen
/// seconds and holding a grain turn for the whole of it. These fixtures pin the
/// time bound, and - more importantly - pin the two ways adding one could go
/// silently wrong: a slice that reports the corpus exhausted when it merely ran
/// out of budget, and a budget so small the slice consumes nothing and never
/// advances.
/// </para>
/// </summary>
[TestFixture]
public sealed class DurableVectorIndexSliceBudgetTests
{
    private const int Corpus = 300;

    private static DurableVectorIndexOptions Options(
        TimeSpan budget, TimeProvider clock, int ingestBatchSize = 4_096)
    {
        var options = DurableIndexHarness.Options(ingestBatchSize: ingestBatchSize);
        options.IngestSliceBudget = budget;
        options.TimeProvider = clock;
        return options;
    }

    [Test]
    public async Task A_slice_yields_on_the_time_budget_long_before_the_work_budget()
    {
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(Corpus);

        // One second per vector against a five second budget, and a work budget
        // far larger than the corpus, so only the clock can end the slice.
        var options = Options(TimeSpan.FromSeconds(5), new SteppingTimeProvider(TimeSpan.FromSeconds(1)));
        var index = await DurableIndexHarness.OpenAsync(store, source, options);

        await index.BuildStepAsync();
        var afterStart = index.Progress;
        await index.BuildStepAsync();
        var afterFirstIngest = index.Progress;

        Assert.Multiple(() =>
        {
            Assert.That(afterStart.Phase, Is.EqualTo(VectorIndexBuildPhase.Ingesting));
            Assert.That(afterFirstIngest.VectorsIndexed, Is.EqualTo(5),
                "The clock is charged once per vector, so a five second budget at one second a vector "
                + "is five vectors.");
            Assert.That(afterFirstIngest.VectorsIndexed, Is.LessThan(Corpus),
                "A slice bounded by the clock must stop short of the corpus, or the fixture proves nothing.");
        });
    }

    [Test]
    public async Task A_slice_that_runs_out_of_budget_does_not_report_the_corpus_exhausted()
    {
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(Corpus);
        var options = Options(TimeSpan.FromSeconds(5), new SteppingTimeProvider(TimeSpan.FromSeconds(1)));

        var index = await DurableIndexHarness.OpenAsync(store, source, options);
        await index.BuildStepAsync();
        var progress = await index.BuildStepAsync();

        // This is the failure the ingest loop's own comment warns about, and it is
        // silent: a build that mistakes "out of budget" for "out of corpus" trains
        // on a fraction of it, persists that, and reports Ready with no error
        // anywhere. Staying in Ingesting is the whole assertion.
        Assert.That(progress.Phase, Is.EqualTo(VectorIndexBuildPhase.Ingesting),
            "Running out of budget is not evidence the source ran out, so the build must not advance "
            + "to Training.");
    }

    [Test]
    public async Task A_build_sliced_by_the_clock_still_indexes_the_whole_corpus()
    {
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(Corpus);
        var options = Options(TimeSpan.FromSeconds(5), new SteppingTimeProvider(TimeSpan.FromSeconds(1)));

        var index = await DurableIndexHarness.OpenAsync(store, source, options);

        var steps = 0;
        while (index.Progress.Phase != VectorIndexBuildPhase.Ready)
        {
            await index.BuildStepAsync();
            Assert.That(++steps, Is.LessThan(500), "The build must converge, not spin.");
        }

        Assert.Multiple(() =>
        {
            Assert.That(index.Progress.VectorsIndexed, Is.EqualTo(Corpus),
                "Resuming across many small slices must lose nothing and duplicate nothing.");
            Assert.That(steps, Is.GreaterThan(Corpus / 5),
                "The corpus can only be consumed a budget's worth at a time, so it takes many slices.");
        });

        var hits = DurableIndexHarness.SearchIds(index, source[DurableIndexHarness.Id(7)], 1);
        Assert.That(hits, Does.Contain(DurableIndexHarness.Id(7)),
            "An index built in clock-bounded slices answers exactly as one built in a single slice.");
    }

    [Test]
    public async Task A_budget_smaller_than_one_vector_still_makes_progress()
    {
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(Corpus);

        // Every vector overruns the budget on its own. The budget is checked only
        // after an item is consumed precisely so this degrades to one vector per
        // slice rather than to a slice that consumes nothing and spins forever.
        var options = Options(TimeSpan.FromTicks(1), new SteppingTimeProvider(TimeSpan.FromSeconds(1)));
        var index = await DurableIndexHarness.OpenAsync(store, source, options);

        // The first step is the NotStarted transition and consumes nothing; the
        // ingest slices are the ones being measured.
        await index.BuildStepAsync();
        await index.BuildStepAsync();
        var first = index.Progress.VectorsIndexed;
        await index.BuildStepAsync();
        var second = index.Progress.VectorsIndexed;

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.EqualTo(1), "A slice always consumes at least one vector.");
            Assert.That(second, Is.EqualTo(2), "Successive slices keep advancing rather than stalling.");
        });
    }

    [Test]
    public async Task A_non_positive_budget_leaves_the_work_count_as_the_only_bound()
    {
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(Corpus);
        var clock = new SteppingTimeProvider(TimeSpan.FromHours(1));
        var options = Options(TimeSpan.Zero, clock, ingestBatchSize: 32);

        var index = await DurableIndexHarness.OpenAsync(store, source, options);
        await index.BuildStepAsync();
        await index.BuildStepAsync();

        Assert.Multiple(() =>
        {
            Assert.That(index.Progress.VectorsIndexed, Is.EqualTo(32),
                "With the time bound off, the slice ends on the work count exactly as it always did.");
            Assert.That(clock.Readings, Is.Zero,
                "A disabled budget must not read the clock at all, so it costs nothing to leave off.");
        });
    }

    [Test]
    public async Task The_time_bound_yields_to_whichever_bound_is_reached_first()
    {
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(Corpus);

        // A generous budget and a small work count: the count wins, which is the
        // pre-existing behaviour the time bound must not disturb.
        var options = Options(
            TimeSpan.FromHours(1), new SteppingTimeProvider(TimeSpan.FromSeconds(1)), ingestBatchSize: 16);
        var index = await DurableIndexHarness.OpenAsync(store, source, options);

        await index.BuildStepAsync();
        await index.BuildStepAsync();

        Assert.That(index.Progress.VectorsIndexed, Is.EqualTo(16));
    }
}
