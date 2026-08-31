using Orleans.Lattice.Vector.Persistence;
using Orleans.Lattice.Vector.Tests.Fakes;

namespace Orleans.Lattice.Vector.Tests.Persistence;

/// <summary>
/// A background build must survive being cut off, at any point, without
/// duplicating or losing a vector.
/// <para>
/// The interruption is caused rather than simulated: the store is told to fail on
/// its <c>n</c>th write, the index instance is discarded exactly as a process
/// would be, and a fresh one is opened over the same store. No clock, no delay,
/// and no background thread takes part, so the test asserts on the persistence
/// contract rather than on timing.
/// </para>
/// </summary>
[TestFixture]
public sealed class DurableVectorIndexResumabilityTests
{
    private const int Corpus = 700;

    private static async Task<InMemoryVectorIndexStore> InterruptedBuildAsync(
        ListVectorSource source, DurableVectorIndexOptions options, int failAfterWrites)
    {
        var store = new InMemoryVectorIndexStore();
        var index = await DurableIndexHarness.OpenAsync(store, source, options);
        store.FailAfterWrites = failAfterWrites;

        try
        {
            await index.RunBuildAsync();
        }
        catch (SimulatedStoreFailureException)
        {
            // The point of the test: the process died mid-build.
        }

        store.FailAfterWrites = -1;
        return store;
    }

    [TestCase(0)]
    [TestCase(1)]
    [TestCase(2)]
    [TestCase(3)]
    [TestCase(5)]
    [TestCase(8)]
    [TestCase(13)]
    [TestCase(21)]
    [TestCase(34)]
    [TestCase(55)]
    public async Task An_interrupted_build_completes_correctly_on_the_next_start(int failAfterWrites)
    {
        var source = DurableIndexHarness.Source(Corpus);
        var options = DurableIndexHarness.Options();

        var store = await InterruptedBuildAsync(source, options, failAfterWrites);

        var resumed = await DurableIndexHarness.OpenAsync(store, source, options);
        await resumed.RunBuildAsync();

        Assert.Multiple(() =>
        {
            Assert.That(resumed.Count, Is.EqualTo(Corpus),
                "Every vector must be present exactly once: no duplicates, none lost.");
            Assert.That(resumed.Progress.Phase, Is.EqualTo(VectorIndexBuildPhase.Ready));
        });

        foreach (var id in source.Ids)
        {
            Assert.That(resumed.TryGetKey(id, out _), Is.True, $"'{id}' is missing after the resume.");
        }
    }

    [TestCase(3)]
    [TestCase(8)]
    [TestCase(21)]
    public async Task A_resumed_build_answers_identically_to_an_uninterrupted_one(int failAfterWrites)
    {
        var source = DurableIndexHarness.Source(Corpus);
        var options = DurableIndexHarness.Options();
        var query = source[DurableIndexHarness.Id(11)];

        var clean = await DurableIndexHarness.BuiltAsync(
            new InMemoryVectorIndexStore(), source, DurableIndexHarness.Options());
        var expected = DurableIndexHarness.SearchIds(clean, query, 10);

        var store = await InterruptedBuildAsync(source, options, failAfterWrites);
        var resumed = await DurableIndexHarness.OpenAsync(store, source, options);
        await resumed.RunBuildAsync();

        Assert.That(DurableIndexHarness.SearchIds(resumed, query, 10), Is.EqualTo(expected),
            "A build that was interrupted must converge on the same index, not merely a working one.");
    }

    [Test]
    public async Task Every_interruption_point_across_a_whole_build_resumes_correctly()
    {
        // Sweeping every write index rather than a sample: the interesting
        // failures cluster at the commit boundaries, and which write those are
        // is an implementation detail that must not have to be guessed at.
        var source = DurableIndexHarness.Source(300);
        var options = DurableIndexHarness.Options(ingestBatchSize: 64, maxItemsPerChunk: 32);

        var probe = new InMemoryVectorIndexStore();
        var complete = await DurableIndexHarness.OpenAsync(probe, source, options);
        await complete.RunBuildAsync();
        var writes = probe.Writes;

        for (var failAfter = 0; failAfter <= writes; failAfter++)
        {
            var store = await InterruptedBuildAsync(source, options, failAfter);
            var resumed = await DurableIndexHarness.OpenAsync(store, source, options);
            await resumed.RunBuildAsync();

            Assert.That(resumed.Count, Is.EqualTo(300),
                $"A build interrupted after {failAfter} of {writes} writes did not converge.");
        }
    }

    [Test]
    public async Task An_interrupted_build_never_serves_more_vectors_than_it_durably_holds()
    {
        var source = DurableIndexHarness.Source(Corpus);
        var options = DurableIndexHarness.Options();

        var store = await InterruptedBuildAsync(source, options, failAfterWrites: 6);
        var resumed = await DurableIndexHarness.OpenAsync(store, source, options);

        Assert.That(resumed.Count, Is.LessThanOrEqualTo(Corpus));
        foreach (var id in source.Ids)
        {
            if (resumed.TryGetKey(id, out var key))
            {
                var results = new VectorSearchResult[Corpus];
                var found = resumed.Search(source[id], results, out _);
                Assert.That(results.AsSpan(0, found).ToArray().Select(result => result.Key), Does.Contain(key),
                    $"'{id}' is mapped and loaded, so it must be findable by its own vector.");
            }
        }
    }

    [Test]
    public async Task A_build_interrupted_after_training_resumes_without_re_reading_the_source()
    {
        var source = DurableIndexHarness.Source(Corpus);
        var options = DurableIndexHarness.Options();

        var store = new InMemoryVectorIndexStore();
        var index = await DurableIndexHarness.OpenAsync(store, source, options);
        while (index.Progress.Phase != VectorIndexBuildPhase.Training)
        {
            await index.BuildStepAsync();
        }

        // Cut the process off during the write of the trained generation.
        store.FailAfterWrites = store.Writes + 1;
        Assert.That(async () => await index.RunBuildAsync(), Throws.TypeOf<SimulatedStoreFailureException>());
        store.FailAfterWrites = -1;

        var emptied = new ListVectorSource(DurableIndexHarness.Dimensions);
        var resumed = await DurableIndexHarness.OpenAsync(store, emptied, options);

        Assert.That(resumed.Count, Is.EqualTo(Corpus),
            "The untrained generation was still committed, so the corpus is reloaded rather than re-read.");
        Assert.That(resumed.Progress.Phase, Is.EqualTo(VectorIndexBuildPhase.Training),
            "A persist that never landed resumes at training, which is deterministic and needs no source.");

        await resumed.RunBuildAsync();
        Assert.That(resumed.Count, Is.EqualTo(Corpus));
        Assert.That(resumed.Status.State, Is.EqualTo(VectorIndexState.Ready));
    }

    [Test]
    public async Task The_persisted_cursor_never_runs_ahead_of_the_persisted_vectors()
    {
        var source = DurableIndexHarness.Source(Corpus);
        var options = DurableIndexHarness.Options(ingestBatchSize: 100, maxItemsPerChunk: 64);

        var store = new InMemoryVectorIndexStore();
        var index = await DurableIndexHarness.OpenAsync(store, source, options);

        while (index.Progress.Phase is VectorIndexBuildPhase.NotStarted or VectorIndexBuildPhase.Ingesting)
        {
            await index.BuildStepAsync();

            // Reopening mid-build is exactly what a restart does. Whatever the
            // cursor says, the vectors it claims must already be loadable.
            var reopened = await DurableIndexHarness.OpenAsync(store, source, options);
            var loaded = reopened.Count;
            var record = await store.ReadAsync(VectorIndexStorageKeys.BuildState(options.KeyPrefix));

            if (record is null || !VectorIndexBuildState.TryReadRecord(record, out var state) ||
                state.Cursor is null)
            {
                continue;
            }

            var consumed = source.Ids.Count(id => string.CompareOrdinal(id, state.Cursor) <= 0);
            Assert.That(consumed, Is.LessThanOrEqualTo(loaded),
                "The cursor claims more of the source has been consumed than the index durably holds, "
                + "so a resume would skip vectors.");
        }
    }

    [Test]
    public async Task Resuming_is_idempotent_when_nothing_was_lost()
    {
        var source = DurableIndexHarness.Source(200);
        var options = DurableIndexHarness.Options();
        var store = new InMemoryVectorIndexStore();

        var first = await DurableIndexHarness.OpenAsync(store, source, options);
        await first.RunBuildAsync();
        var writes = store.Writes;

        var second = await DurableIndexHarness.OpenAsync(store, source, options);
        await second.RunBuildAsync();

        Assert.Multiple(() =>
        {
            Assert.That(second.Count, Is.EqualTo(200));
            Assert.That(store.Writes, Is.EqualTo(writes),
                "A completed build has nothing left to do, so reopening it must write nothing.");
        });
    }
}
