using Orleans.Lattice.Vector.Persistence;
using Orleans.Lattice.Vector.Tests.Fakes;

namespace Orleans.Lattice.Vector.Tests.Persistence;

/// <summary>
/// Covers the build's stage-timing observer, its batched key-map writes, and the
/// retry that recovers an expected count the source refused the first time.
/// </summary>
/// <remarks>
/// <para>
/// These three are one change seen from three sides. The build issued a durable
/// single-key write per vector, which on a corpus-sized build is one write-ahead
/// log append per vector; nothing in the telemetry said so, because no instrument
/// split a slice's time by stage; and the expected count - taken once, and 0 when
/// it failed - left both the progress fraction and the host's exact-scan budget
/// without a corpus size for the life of the index.
/// </para>
/// </remarks>
[TestFixture]
public sealed class DurableVectorIndexBuildObserverTests
{
    private const int Corpus = 200;

    private static DurableVectorIndexOptions Options(IVectorIndexBuildObserver? observer = null)
    {
        var options = DurableIndexHarness.Options(ingestBatchSize: 64);
        options.BuildObserver = observer;
        return options;
    }

    [Test]
    public async Task OnSliceCompleted_is_reported_once_per_slice_and_accounts_for_every_item()
    {
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(Corpus);
        var observer = new RecordingBuildObserver();

        var index = await DurableIndexHarness.OpenAsync(store, source, Options(observer));
        await index.RunBuildAsync();

        Assert.Multiple(() =>
        {
            Assert.That(observer.Slices, Is.Not.Empty, "the seam is not driven at all");
            Assert.That(observer.TotalConsumed, Is.EqualTo(Corpus),
                "every item the build consumed must appear in exactly one slice report");
            Assert.That(observer.Slices.Count, Is.GreaterThan(1),
                "the corpus is deliberately larger than one ingest batch, so several slices must run");
        });
    }

    [Test]
    public async Task OnSliceCompleted_reports_non_negative_durations_for_every_stage()
    {
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(Corpus);
        var observer = new RecordingBuildObserver();

        var index = await DurableIndexHarness.OpenAsync(store, source, Options(observer));
        await index.RunBuildAsync();

        // Guarded before the loop. Iterating an empty collection passes every
        // assertion inside it vacuously, which is how this fixture read green
        // while the observer was never called at all - the Clone() that dropped
        // BuildObserver was caught by the sibling fixture, not by this one.
        Assert.That(observer.Slices, Is.Not.Empty);

        // Deliberately not asserting a positive duration. These run against the
        // system clock, so a stage fast enough to fall inside its resolution
        // legitimately reports zero, and demanding more would be a flake rather
        // than a property.
        Assert.Multiple(() =>
        {
            foreach (var slice in observer.Slices)
            {
                Assert.That(slice.SourceWait, Is.GreaterThanOrEqualTo(TimeSpan.Zero));
                Assert.That(slice.KeyAssign, Is.GreaterThanOrEqualTo(TimeSpan.Zero));
                Assert.That(slice.IndexUpsert, Is.GreaterThanOrEqualTo(TimeSpan.Zero));
                Assert.That(slice.KeyFlush, Is.GreaterThanOrEqualTo(TimeSpan.Zero));
            }
        });
    }

    [Test]
    public async Task A_build_without_an_observer_is_unaffected()
    {
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(Corpus);

        var index = await DurableIndexHarness.OpenAsync(store, source, Options());
        await index.RunBuildAsync();

        Assert.Multiple(() =>
        {
            Assert.That(index.Progress.Phase, Is.EqualTo(VectorIndexBuildPhase.Ready));
            Assert.That(index.Count, Is.EqualTo(Corpus));
        });
    }

    [Test]
    public async Task A_build_batches_its_key_map_writes_instead_of_writing_one_per_vector()
    {
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(Corpus);

        var index = await DurableIndexHarness.BuiltAsync(store, source, Options());

        // The property, stated so it cannot pass vacuously: a build that wrote one
        // key-map record per vector could not possibly issue fewer writes than it
        // ingested vectors, whatever else it also wrote. Every partition, manifest
        // and reservation write is counted here too, so this bound is generous.
        Assert.Multiple(() =>
        {
            Assert.That(index.Count, Is.EqualTo(Corpus));
            Assert.That(store.Writes, Is.LessThan(Corpus),
                "a per-vector key-map write would make this at least the corpus size");
            Assert.That(store.LargestBatchEntries, Is.GreaterThan(1),
                "at least one write must carry a real batch");
        });
    }

    [Test]
    public async Task An_expected_count_that_failed_once_is_retried_and_recovers()
    {
        var store = new InMemoryVectorIndexStore();
        var inner = DurableIndexHarness.Source(Corpus);

        // Fails the count StartBuildAsync takes, then answers. Before the retry
        // the field stayed 0 for the life of the index - and because it is
        // persisted in the build state, for the life of every later process too.
        var source = new RecoveringCountVectorSource(inner, failures: 1);

        var index = await DurableVectorIndex.OpenAsync(store, source, Options(), VectorIndexLoadMode.Full);
        await index.RunBuildAsync();

        Assert.Multiple(() =>
        {
            Assert.That(source.CountAttempts, Is.GreaterThan(1), "the count was never retried");
            Assert.That(index.Progress.VectorsExpected, Is.EqualTo(Corpus));
            Assert.That(index.Count, Is.EqualTo(Corpus));
        });
    }

    [Test]
    public async Task An_expected_count_that_never_succeeds_still_completes_the_build()
    {
        var store = new InMemoryVectorIndexStore();
        var inner = DurableIndexHarness.Source(Corpus);
        var source = new CountFailingVectorSource(inner, () => new InvalidOperationException("no count"));

        var index = await DurableVectorIndex.OpenAsync(store, source, Options(), VectorIndexLoadMode.Full);
        await index.RunBuildAsync();

        // The retry must not convert a degradation into a failure: the count is a
        // hint, and a build that cannot obtain it still has everything it needs.
        Assert.Multiple(() =>
        {
            Assert.That(index.Progress.Phase, Is.EqualTo(VectorIndexBuildPhase.Ready));
            Assert.That(index.Count, Is.EqualTo(Corpus));
        });
    }
}
