using Orleans.Lattice.Vector.Persistence;
using Orleans.Lattice.Vector.Tests.Fakes;

namespace Orleans.Lattice.Vector.Tests.Persistence;

/// <summary>
/// Progress reporting, which a readiness probe and a retrieval-path attribution
/// are built from, so it has to be true rather than merely encouraging.
/// </summary>
[TestFixture]
public sealed class DurableVectorIndexProgressTests
{
    private const int Corpus = 500;

    [Test]
    public async Task A_fresh_index_reports_that_nothing_has_been_built()
    {
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(Corpus);

        var index = await DurableIndexHarness.OpenAsync(store, source, DurableIndexHarness.Options());
        var progress = index.Progress;

        Assert.Multiple(() =>
        {
            Assert.That(progress.Phase, Is.EqualTo(VectorIndexBuildPhase.NotStarted));
            Assert.That(progress.IsReady, Is.False);
            Assert.That(progress.VectorsIndexed, Is.Zero);
            Assert.That(progress.VectorsExpected, Is.Zero);
            Assert.That(progress.PartitionsTotal, Is.Zero);
            Assert.That(progress.RestoredFromDurableState, Is.False);
        });
    }

    [Test]
    public async Task Progress_advances_monotonically_through_every_phase()
    {
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(Corpus);
        var options = DurableIndexHarness.Options(ingestBatchSize: 64);

        var index = await DurableIndexHarness.OpenAsync(store, source, options);
        var phases = new List<VectorIndexBuildPhase>();
        var indexed = new List<int>();

        while (index.Progress.Phase != VectorIndexBuildPhase.Ready)
        {
            var progress = await index.BuildStepAsync();
            phases.Add(progress.Phase);
            indexed.Add(progress.VectorsIndexed);
        }

        Assert.Multiple(() =>
        {
            Assert.That(phases, Does.Contain(VectorIndexBuildPhase.Ingesting));
            Assert.That(phases, Does.Contain(VectorIndexBuildPhase.Training));
            Assert.That(phases, Does.Contain(VectorIndexBuildPhase.Persisting));
            Assert.That(phases[^1], Is.EqualTo(VectorIndexBuildPhase.Ready));
            Assert.That(phases.Select(phase => (int)phase), Is.Ordered.Ascending,
                "A build never moves backwards, so a consumer can trust the phase as a watermark.");
            Assert.That(indexed, Is.Ordered.Ascending);
            Assert.That(indexed[^1], Is.EqualTo(Corpus));
        });
    }

    [Test]
    public async Task The_expected_count_is_reported_as_soon_as_the_build_starts()
    {
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(Corpus);

        var index = await DurableIndexHarness.OpenAsync(store, source, DurableIndexHarness.Options());
        var progress = await index.BuildStepAsync();

        Assert.Multiple(() =>
        {
            Assert.That(progress.VectorsExpected, Is.EqualTo(Corpus));
            Assert.That(progress.Phase, Is.EqualTo(VectorIndexBuildPhase.Ingesting));
        });
    }

    [Test]
    public async Task The_ingested_fraction_climbs_from_zero_to_one()
    {
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(Corpus);
        var options = DurableIndexHarness.Options(ingestBatchSize: 64);

        var index = await DurableIndexHarness.OpenAsync(store, source, options);
        await index.BuildStepAsync();

        var fractions = new List<double> { index.Progress.IngestedFraction };
        while (index.Progress.Phase != VectorIndexBuildPhase.Ready)
        {
            await index.BuildStepAsync();
            fractions.Add(index.Progress.IngestedFraction);
        }

        Assert.Multiple(() =>
        {
            Assert.That(fractions[0], Is.LessThan(1d));
            Assert.That(fractions, Is.Ordered.Ascending);
            Assert.That(fractions[^1], Is.EqualTo(1d));
        });
    }

    [Test]
    public async Task A_build_in_progress_still_answers_exactly_and_says_so()
    {
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(Corpus);
        var options = DurableIndexHarness.Options(ingestBatchSize: 64);

        var index = await DurableIndexHarness.OpenAsync(store, source, options);
        await index.BuildStepAsync();
        await index.BuildStepAsync();

        Assert.That(index.Progress.Phase, Is.EqualTo(VectorIndexBuildPhase.Ingesting));
        Assert.That(index.Count, Is.GreaterThan(0).And.LessThan(Corpus));

        // A vector that is in already must be found, and the answer over what is
        // resident must be exact rather than approximate. This is the difference
        // between "warming up" and "degraded", and a consumer has to be able to
        // tell them apart.
        var id = DurableIndexHarness.Id(0);
        var results = new VectorSearchResult[1];
        var found = index.Search(source[id], results, out var mode);

        Assert.Multiple(() =>
        {
            Assert.That(found, Is.EqualTo(1));
            Assert.That(mode, Is.EqualTo(VectorSearchMode.Exhaustive));
            Assert.That(index.TryGetId(results[0].Key, out var top) ? top : null, Is.EqualTo(id));
            Assert.That(index.Status.State, Is.EqualTo(VectorIndexState.Building));
            Assert.That(index.Progress.IsReady, Is.False);
        });
    }

    [Test]
    public async Task A_completed_build_reports_ready_on_both_signals()
    {
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(Corpus);

        var index = await DurableIndexHarness.BuiltAsync(store, source, DurableIndexHarness.Options());
        var progress = index.Progress;

        Assert.Multiple(() =>
        {
            Assert.That(progress.IsReady, Is.True);
            Assert.That(progress.IngestedFraction, Is.EqualTo(1d));
            Assert.That(progress.VectorsIndexed, Is.EqualTo(Corpus));
            Assert.That(progress.PartitionsTotal, Is.EqualTo(index.Status.PartitionCount));
            Assert.That(progress.PartitionsPersisted, Is.EqualTo(progress.PartitionsTotal),
                "Every partition is durable once the build commits.");
            Assert.That(index.Status.State, Is.EqualTo(VectorIndexState.Ready));
            Assert.That(index.Search(source[DurableIndexHarness.Id(0)], new VectorSearchResult[1], out var mode),
                Is.EqualTo(1));
            Assert.That(mode, Is.EqualTo(VectorSearchMode.Approximate));
        });
    }

    [Test]
    public async Task A_resumed_build_reports_that_it_was_restored()
    {
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(Corpus);
        var options = DurableIndexHarness.Options(ingestBatchSize: 64);

        var index = await DurableIndexHarness.OpenAsync(store, source, options);
        await index.BuildStepAsync();
        await index.BuildStepAsync();
        await index.BuildStepAsync();

        var resumed = await DurableIndexHarness.OpenAsync(store, source, options);

        Assert.Multiple(() =>
        {
            Assert.That(resumed.Progress.RestoredFromDurableState, Is.True);
            Assert.That(resumed.Progress.Phase, Is.EqualTo(VectorIndexBuildPhase.Ingesting));
            Assert.That(resumed.Progress.VectorsExpected, Is.EqualTo(Corpus),
                "The expected count is part of the checkpoint, so a resumed build reports progress honestly.");
        });
    }

    [Test]
    public async Task Stepping_a_completed_build_is_a_no_op()
    {
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(200);

        var index = await DurableIndexHarness.BuiltAsync(store, source, DurableIndexHarness.Options());
        var writes = store.Writes;

        var progress = await index.BuildStepAsync();

        Assert.Multiple(() =>
        {
            Assert.That(progress.Phase, Is.EqualTo(VectorIndexBuildPhase.Ready));
            Assert.That(store.Writes, Is.EqualTo(writes));
        });
    }

    [Test]
    public async Task Running_a_build_honours_cancellation_between_steps()
    {
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(Corpus);
        using var cancellation = new CancellationTokenSource();

        var index = await DurableIndexHarness.OpenAsync(store, source, DurableIndexHarness.Options());
        await cancellation.CancelAsync();

        Assert.That(async () => await index.RunBuildAsync(cancellation.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task The_generation_reported_by_progress_matches_the_index()
    {
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(300);

        var index = await DurableIndexHarness.BuiltAsync(store, source, DurableIndexHarness.Options());

        Assert.That(index.Progress.Generation, Is.EqualTo(index.Generation));
    }
}
