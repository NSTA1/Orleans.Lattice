using Orleans.Lattice.Vector.Persistence;
using Orleans.Lattice.Vector.Tests.Fakes;

namespace Orleans.Lattice.Vector.Tests.Persistence;

/// <summary>
/// The property that makes chunked persistence worth the trouble: a box can
/// start answering from the centroids alone and fetch only the cells a query
/// actually probes, and the answer is identical to the fully resident index
/// rather than merely close to it.
/// </summary>
[TestFixture]
public sealed class DurableVectorIndexLazyLoadTests
{
    private const int Corpus = 1_200;
    private const int K = 10;

    private static DurableVectorIndexOptions Options() => new()
    {
        KeyPrefix = "lazy/",
        MaxItemsPerChunk = 64,
        IngestBatchSize = 512,
        Index = new VectorIndexOptions
        {
            Dimensions = DurableIndexHarness.Dimensions,
            PartitionCount = 32,
            Probes = 4,
            MinimumTrainingCount = 16,
            TrainingSampleSize = 2_048,
        },
    };

    [Test]
    public async Task A_lazily_loaded_index_answers_identically_to_the_resident_one()
    {
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(Corpus);
        var options = Options();

        var resident = await DurableIndexHarness.BuiltAsync(store, source, options);
        var lazy = await DurableIndexHarness.OpenAsync(store, source, options, VectorIndexLoadMode.Lazy);

        var results = new VectorSearchResult[K];
        for (var i = 0; i < Corpus; i += 97)
        {
            var query = source[DurableIndexHarness.Id(i)];
            var expected = DurableIndexHarness.SearchResults(resident, query, K);

            var outcome = await lazy.SearchAsync(query, results);

            Assert.That(results.AsSpan(0, outcome.Count).ToArray(), Is.EqualTo(expected),
                "A query scores exactly the cells it selects, so fetching only those must change nothing.");
            Assert.That(outcome.Mode, Is.EqualTo(VectorSearchMode.Approximate));
        }
    }

    [Test]
    public async Task A_lazily_loaded_index_holds_only_a_fraction_of_the_corpus_after_one_query()
    {
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(Corpus);
        var options = Options();

        await DurableIndexHarness.BuiltAsync(store, source, options);
        var lazy = await DurableIndexHarness.OpenAsync(store, source, options, VectorIndexLoadMode.Lazy);

        Assert.That(lazy.Count, Is.Zero, "Only the centroids are loaded up front.");

        await lazy.SearchAsync(source[DurableIndexHarness.Id(3)], new VectorSearchResult[K]);

        TestContext.Out.WriteLine(
            $"one query made {lazy.Count} of {Corpus} vectors resident "
            + $"({(double)lazy.Count / Corpus:P1}) across {options.Index.Probes} of "
            + $"{options.Index.PartitionCount} cells");

        Assert.Multiple(() =>
        {
            Assert.That(lazy.Count, Is.GreaterThan(0));
            Assert.That(lazy.Count, Is.LessThan(Corpus / 2),
                "Fetching the probed cells must cost a fraction of the corpus, not most of it.");
        });
    }

    [Test]
    public async Task A_lazily_loaded_index_warms_rather_than_refetching()
    {
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(Corpus);
        var options = Options();

        await DurableIndexHarness.BuiltAsync(store, source, options);
        var lazy = await DurableIndexHarness.OpenAsync(store, source, options, VectorIndexLoadMode.Lazy);

        var query = source[DurableIndexHarness.Id(11)];
        var results = new VectorSearchResult[K];

        await lazy.SearchAsync(query, results);
        var afterFirst = store.Reads;
        var resident = lazy.Count;

        await lazy.SearchAsync(query, results);

        Assert.Multiple(() =>
        {
            Assert.That(store.Reads, Is.EqualTo(afterFirst),
                "A cell that is already resident must not be fetched again: the index warms as it serves.");
            Assert.That(lazy.Count, Is.EqualTo(resident));
        });
    }

    [Test]
    public async Task Repeated_queries_converge_on_the_fully_resident_index()
    {
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(Corpus);
        var options = Options();

        var resident = await DurableIndexHarness.BuiltAsync(store, source, options);
        var lazy = await DurableIndexHarness.OpenAsync(store, source, options, VectorIndexLoadMode.Lazy);

        var results = new VectorSearchResult[K];
        foreach (var id in source.Ids)
        {
            await lazy.SearchAsync(source[id], results);
        }

        Assert.That(lazy.Count, Is.EqualTo(resident.Count),
            "Once every cell has been probed the lazy index holds exactly what the resident one does.");
    }

    [Test]
    public async Task A_lazy_search_on_an_untrained_index_answers_exhaustively_from_what_is_resident()
    {
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(10);
        var options = Options();

        await DurableIndexHarness.BuiltAsync(store, source, options);
        var lazy = await DurableIndexHarness.OpenAsync(store, source, options, VectorIndexLoadMode.Lazy);

        var outcome = await lazy.SearchAsync(source[DurableIndexHarness.Id(1)], new VectorSearchResult[K]);

        Assert.That(outcome.Mode, Is.EqualTo(VectorSearchMode.Exhaustive),
            "With no partitioning there is nothing to page in, and the answer is exact.");
    }

    [Test]
    public async Task Searching_a_fully_resident_index_asynchronously_matches_the_synchronous_path()
    {
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(Corpus);
        var options = Options();

        var index = await DurableIndexHarness.BuiltAsync(store, source, options);
        var query = source[DurableIndexHarness.Id(5)];
        var expected = DurableIndexHarness.SearchResults(index, query, K);

        var results = new VectorSearchResult[K];
        var outcome = await index.SearchAsync(query, results);

        Assert.Multiple(() =>
        {
            Assert.That(results.AsSpan(0, outcome.Count).ToArray(), Is.EqualTo(expected));
            Assert.That(outcome.Mode, Is.EqualTo(VectorSearchMode.Approximate));
        });
    }

    [Test]
    public async Task A_lazily_loaded_index_resolves_identifiers_without_reading_the_store()
    {
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(Corpus);
        var options = Options();

        await DurableIndexHarness.BuiltAsync(store, source, options);
        var lazy = await DurableIndexHarness.OpenAsync(store, source, options, VectorIndexLoadMode.Lazy);

        var results = new VectorSearchResult[K];
        var outcome = await lazy.SearchAsync(source[DurableIndexHarness.Id(21)], results);
        var reads = store.Reads;

        for (var i = 0; i < outcome.Count; i++)
        {
            Assert.That(lazy.TryGetId(results[i].Key, out var id), Is.True);
            Assert.That(id, Does.StartWith("doc-"));
        }

        Assert.That(store.Reads, Is.EqualTo(reads),
            "Resolving a result must not cost a round trip: the mapping is already in memory.");
    }

    [Test]
    public async Task A_lazily_loaded_index_reports_the_load_mode_it_was_opened_with()
    {
        var store = new InMemoryVectorIndexStore();
        var source = DurableIndexHarness.Source(200);
        var options = Options();

        await DurableIndexHarness.BuiltAsync(store, source, options);
        var lazy = await DurableIndexHarness.OpenAsync(store, source, options, VectorIndexLoadMode.Lazy);

        Assert.Multiple(() =>
        {
            Assert.That(lazy.LoadMode, Is.EqualTo(VectorIndexLoadMode.Lazy));
            Assert.That(lazy.Progress.RestoredFromDurableState, Is.True);
        });
    }
}
