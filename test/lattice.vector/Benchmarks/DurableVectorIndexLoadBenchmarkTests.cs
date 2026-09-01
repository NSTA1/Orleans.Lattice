using System.Diagnostics;
using Orleans.Lattice.Vector.Persistence;
using Orleans.Lattice.Vector.Tests.Fakes;

namespace Orleans.Lattice.Vector.Tests.Benchmarks;

/// <summary>
/// What a restart actually costs: how long a persisted index takes to build, how
/// much it occupies on the store, how long it takes to load back, and how little
/// of it a lazy open has to touch before it can answer.
/// <para>
/// The sweep runs well past the seventy-odd thousand vectors of the largest live
/// corpus, because the property that matters is how these figures <i>scale</i>,
/// not what they are at one size. It is gated on the same
/// <c>LATTICE_VECTOR_BENCH</c> variable as the core scale sweep, so an ordinary
/// lane reports every case ignored in microseconds, and it lives here rather than
/// under <c>benchmark/</c> because it needs no host, no cluster, and no Orleans
/// runtime. Run it with:
/// </para>
/// <code>
/// $env:LATTICE_VECTOR_BENCH = "1"
/// dotnet test test/lattice.vector/Orleans.Lattice.Vector.Tests.csproj -c Release --filter "TestCategory=Benchmark"
/// </code>
/// <para>
/// The measured output is committed in <c>test/lattice.vector/MEASUREMENTS.md</c>.
/// </para>
/// </summary>
[TestFixture]
[Category("Benchmark")]
public sealed class DurableVectorIndexLoadBenchmarkTests
{
    private const string Gate = "LATTICE_VECTOR_BENCH";
    private const int Dimensions = 384;
    private const int K = 10;

    private static void RequireGate()
    {
        if (string.IsNullOrEmpty(Environment.GetEnvironmentVariable(Gate)))
        {
            Assert.Ignore($"Set {Gate}=1 to run the durable vector index load sweep.");
        }
    }

    private static DurableVectorIndexOptions Options() => new()
    {
        KeyPrefix = "bench/",
        MaxItemsPerChunk = 1_024,
        IngestBatchSize = 16_384,
        Index = new VectorIndexOptions { Dimensions = Dimensions },
    };

    [TestCase(10_000)]
    [TestCase(50_000)]
    [TestCase(100_000)]
    [TestCase(250_000)]
    public async Task LoadSweep(int count)
    {
        RequireGate();

        var corpus = VectorCorpus.Clustered(count, Dimensions, clusters: 256, seed: 555);
        var source = new ListVectorSource(Dimensions);
        for (var i = 0; i < count; i++)
        {
            source.Set($"doc-{i:D8}", corpus[i]);
        }

        var store = new InMemoryVectorIndexStore();
        var options = Options();

        var build = Stopwatch.StartNew();
        var built = await DurableVectorIndex.OpenAsync(store, source, options);
        await built.RunBuildAsync();
        build.Stop();

        var status = built.Status;
        var persistedBytes = store.TotalBytes;
        var records = store.RecordCount;

        // A cold start: nothing in memory, everything read back from the store.
        var load = Stopwatch.StartNew();
        var reloaded = await DurableVectorIndex.OpenAsync(store, source, options);
        load.Stop();

        Assert.That(reloaded.Count, Is.EqualTo(count));

        var lazyOpen = Stopwatch.StartNew();
        var lazy = await DurableVectorIndex.OpenAsync(store, source, options, VectorIndexLoadMode.Lazy);
        lazyOpen.Stop();

        var results = new VectorSearchResult[K];
        var firstQuery = Stopwatch.StartNew();
        await lazy.SearchAsync(corpus[0], results);
        firstQuery.Stop();

        var residentAfterFirstQuery = lazy.Count;
        var memory = VectorIndexMemory.Bytes(status.Capacity, status.Dimensions, status.PartitionCount);

        TestContext.Out.WriteLine(
            $"| {count} | {status.PartitionCount} | {build.Elapsed.TotalSeconds:F2} "
            + $"| {load.Elapsed.TotalSeconds:F2} | {lazyOpen.Elapsed.TotalSeconds:F3} "
            + $"| {firstQuery.Elapsed.TotalMilliseconds:F1} "
            + $"| {(double)residentAfterFirstQuery / count:P1} "
            + $"| {memory / (1024d * 1024d):F0} | {persistedBytes / (1024d * 1024d):F0} "
            + $"| {records} | {persistedBytes / count} |");
    }

    [TestCase(50_000)]
    [TestCase(250_000)]
    public async Task IncrementalFlushSweep(int count)
    {
        RequireGate();

        var corpus = VectorCorpus.Clustered(count, Dimensions, clusters: 256, seed: 556);
        var source = new ListVectorSource(Dimensions);
        for (var i = 0; i < count; i++)
        {
            source.Set($"doc-{i:D8}", corpus[i]);
        }

        var store = new InMemoryVectorIndexStore();
        var index = await DurableVectorIndex.OpenAsync(store, source, Options());
        await index.RunBuildAsync();

        var full = Stopwatch.StartNew();
        await index.RetrainAsync();
        full.Stop();

        for (var i = 0; i < 100; i++)
        {
            await index.UpsertAsync($"doc-{i:D8}", corpus[(i + 1) % count]);
        }

        var writesBefore = store.Writes;
        var incremental = Stopwatch.StartNew();
        await index.FlushAsync();
        incremental.Stop();

        TestContext.Out.WriteLine(
            $"| {count} | {index.Status.PartitionCount} | {full.Elapsed.TotalSeconds:F2} "
            + $"| {incremental.Elapsed.TotalMilliseconds:F1} | {store.Writes - writesBefore} |");
    }
}
