using Orleans.Lattice.Vector.Persistence;
using Orleans.Lattice.Vector.Tests.Fakes;

namespace Orleans.Lattice.Vector.Tests.Persistence;

/// <summary>
/// The property the whole exercise exists for: an index that was built once
/// answers identically after a restart, without touching the store of record
/// again.
/// </summary>
[TestFixture]
public sealed class DurableVectorIndexRoundTripTests
{
    [Test]
    public async Task A_reloaded_index_returns_the_identical_result_set()
    {
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(600);
        var options = DurableIndexHarness.Options();

        var built = await DurableIndexHarness.BuiltAsync(store, source, options);
        var query = source[DurableIndexHarness.Id(7)];
        var before = DurableIndexHarness.SearchResults(built, query, 10);

        var reloaded = await DurableIndexHarness.OpenAsync(store, source, options);
        var after = DurableIndexHarness.SearchResults(reloaded, query, 10);

        Assert.Multiple(() =>
        {
            Assert.That(after, Is.EqualTo(before), "A restart must not change a single ranked hit.");
            Assert.That(reloaded.Count, Is.EqualTo(built.Count));
            Assert.That(reloaded.Status.PartitionCount, Is.EqualTo(built.Status.PartitionCount));
            Assert.That(reloaded.Status.State, Is.EqualTo(VectorIndexState.Ready));
        });
    }

    [Test]
    public async Task A_reloaded_index_resolves_the_same_identifiers()
    {
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(400);
        var options = DurableIndexHarness.Options();

        var built = await DurableIndexHarness.BuiltAsync(store, source, options);
        var query = source[DurableIndexHarness.Id(21)];
        var before = DurableIndexHarness.SearchIds(built, query, 10);

        var reloaded = await DurableIndexHarness.OpenAsync(store, source, options);
        var after = DurableIndexHarness.SearchIds(reloaded, query, 10);

        Assert.Multiple(() =>
        {
            Assert.That(after, Is.EqualTo(before));
            Assert.That(after, Does.Not.Contain(null).And.All.StartWith("doc-"),
                "Every key must resolve, so the mapping survived the restart intact.");
        });
    }

    [Test]
    public async Task A_reloaded_index_reports_that_it_was_restored_rather_than_recomputed()
    {
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(300);
        var options = DurableIndexHarness.Options();

        var built = await DurableIndexHarness.BuiltAsync(store, source, options);
        var reloaded = await DurableIndexHarness.OpenAsync(store, source, options);

        Assert.Multiple(() =>
        {
            Assert.That(built.Progress.RestoredFromDurableState, Is.False);
            Assert.That(reloaded.Progress.RestoredFromDurableState, Is.True);
            Assert.That(reloaded.Progress.Phase, Is.EqualTo(VectorIndexBuildPhase.Ready));
        });
    }

    [Test]
    public async Task Reloading_never_reads_the_store_of_record()
    {
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(300);
        var options = DurableIndexHarness.Options();

        await DurableIndexHarness.BuiltAsync(store, source, options);

        var emptied = new ListVectorSource(DurableIndexHarness.Dimensions);
        var reloaded = await DurableIndexHarness.OpenAsync(store, emptied, options);

        Assert.That(reloaded.Count, Is.EqualTo(300),
            "A cold start must be served from the durable index, not by re-reading the corpus.");
    }

    [Test]
    public async Task A_reloaded_index_survives_a_second_round_trip_unchanged()
    {
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(300);
        var options = DurableIndexHarness.Options();

        await DurableIndexHarness.BuiltAsync(store, source, options);
        var once = await DurableIndexHarness.OpenAsync(store, source, options);
        var query = source[DurableIndexHarness.Id(3)];
        var first = DurableIndexHarness.SearchResults(once, query, 10);

        var twice = await DurableIndexHarness.OpenAsync(store, source, options);
        var second = DurableIndexHarness.SearchResults(twice, query, 10);

        Assert.That(second, Is.EqualTo(first));
    }

    [Test]
    public async Task A_corpus_below_the_training_threshold_round_trips_without_a_partitioning()
    {
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(10);
        var options = DurableIndexHarness.Options();

        var built = await DurableIndexHarness.BuiltAsync(store, source, options);
        var query = source[DurableIndexHarness.Id(2)];
        var before = DurableIndexHarness.SearchResults(built, query, 5);

        var reloaded = await DurableIndexHarness.OpenAsync(store, source, options);
        var results = new VectorSearchResult[5];
        var found = reloaded.Search(query, results, out var mode);

        Assert.Multiple(() =>
        {
            Assert.That(results.AsSpan(0, found).ToArray(), Is.EqualTo(before));
            Assert.That(mode, Is.EqualTo(VectorSearchMode.Exhaustive),
                "Below the training threshold an exhaustive scan is both exact and cheaper, and must say so.");
            Assert.That(reloaded.Progress.Phase, Is.EqualTo(VectorIndexBuildPhase.Ready),
                "The build is complete even though there is no partitioning to be ready with.");
        });
    }

    [Test]
    public async Task An_empty_source_produces_an_index_that_is_ready_and_empty()
    {
        var store = new InMemoryVectorIndexStore();
        var source = new ListVectorSource(DurableIndexHarness.Dimensions);
        var options = DurableIndexHarness.Options();

        var built = await DurableIndexHarness.BuiltAsync(store, source, options);
        var reloaded = await DurableIndexHarness.OpenAsync(store, source, options);

        Assert.Multiple(() =>
        {
            Assert.That(built.Count, Is.Zero);
            Assert.That(reloaded.Count, Is.Zero);
            Assert.That(reloaded.Progress.Phase, Is.EqualTo(VectorIndexBuildPhase.Ready));
            Assert.That(reloaded.Search([.. new float[DurableIndexHarness.Dimensions]],
                new VectorSearchResult[3], out _), Is.Zero);
        });
    }

    [Test]
    public async Task No_persisted_record_grows_without_bound()
    {
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(800);
        var options = DurableIndexHarness.Options(maxItemsPerChunk: 32);

        await DurableIndexHarness.BuiltAsync(store, source, options);

        var vectorRecordCeiling = VectorIndexRecord.Measure(
            VectorIndexFormat.ChunkHeaderSize
            + (32 * ((DurableIndexHarness.Dimensions * sizeof(float)) + sizeof(long))));

        foreach (var key in store.Keys)
        {
            Assert.That(store.Read(key).Length, Is.LessThanOrEqualTo(vectorRecordCeiling),
                $"Record '{key}' exceeds the bound implied by MaxItemsPerChunk.");
        }
    }

    [Test]
    public async Task Two_indexes_on_one_store_do_not_see_each_other()
    {
        var store = new InMemoryVectorIndexStore();
        var first = DurableIndexHarness.Source(200, seed: 3);
        var second = DurableIndexHarness.Source(120, seed: 4);

        var one = await DurableIndexHarness.BuiltAsync(store, first, DurableIndexHarness.Options(prefix: "a/"));
        var two = await DurableIndexHarness.BuiltAsync(store, second, DurableIndexHarness.Options(prefix: "b/"));

        var reloadedOne = await DurableIndexHarness.OpenAsync(store, first, DurableIndexHarness.Options(prefix: "a/"));

        Assert.Multiple(() =>
        {
            Assert.That(one.Count, Is.EqualTo(200));
            Assert.That(two.Count, Is.EqualTo(120));
            Assert.That(reloadedOne.Count, Is.EqualTo(200));
        });
    }

    [Test]
    public async Task Opening_a_store_that_has_never_held_an_index_writes_nothing()
    {
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(50);

        var index = await DurableIndexHarness.OpenAsync(store, source, DurableIndexHarness.Options());

        Assert.Multiple(() =>
        {
            Assert.That(store.Writes, Is.Zero, "A first open must not write to establish that there is nothing there.");
            Assert.That(index.Progress.Phase, Is.EqualTo(VectorIndexBuildPhase.NotStarted));
            Assert.That(index.Count, Is.Zero);
        });
    }

    [Test]
    public void Opening_refuses_a_source_whose_dimensionality_contradicts_the_options()
    {
        var store = new InMemoryVectorIndexStore();
        var source = new ListVectorSource(DurableIndexHarness.Dimensions + 1);

        Assert.That(
            async () => await DurableIndexHarness.OpenAsync(store, source, DurableIndexHarness.Options()),
            Throws.ArgumentException);
    }

    [Test]
    public void Opening_refuses_null_arguments()
    {
        var store = new InMemoryVectorIndexStore();
        var source = new ListVectorSource(DurableIndexHarness.Dimensions);
        var options = DurableIndexHarness.Options();

        Assert.Multiple(() =>
        {
            Assert.That(async () => await DurableVectorIndex.OpenAsync(null!, source, options),
                Throws.ArgumentNullException);
            Assert.That(async () => await DurableVectorIndex.OpenAsync(store, null!, options),
                Throws.ArgumentNullException);
            Assert.That(async () => await DurableVectorIndex.OpenAsync(store, source, null!),
                Throws.ArgumentNullException);
        });
    }

    [Test]
    public async Task The_key_prefix_and_generation_are_reported()
    {
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(200);
        var options = DurableIndexHarness.Options(prefix: "custom/");

        var index = await DurableIndexHarness.BuiltAsync(store, source, options);

        Assert.Multiple(() =>
        {
            Assert.That(index.KeyPrefix, Is.EqualTo("custom/"));
            Assert.That(index.LoadMode, Is.EqualTo(VectorIndexLoadMode.Full));
            Assert.That(index.Generation, Is.GreaterThan(0),
                "Training writes a fresh generation rather than editing the untrained one in place.");
            Assert.That(store.Keys, Has.Some.StartWith("custom/"));
        });
    }

    [Test]
    public async Task The_superseded_generation_is_reclaimed_once_training_commits()
    {
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(400);
        var options = DurableIndexHarness.Options();

        var index = await DurableIndexHarness.BuiltAsync(store, source, options);

        var superseded = VectorIndexStorageKeys.GenerationPrefix(options.KeyPrefix, index.Generation - 1);
        Assert.That(store.KeysWithPrefix(superseded), Is.Empty,
            "The untrained generation is unreachable once the manifest names the trained one, so it is swept.");
    }
}
