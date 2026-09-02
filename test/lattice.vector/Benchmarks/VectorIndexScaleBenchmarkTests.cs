using System.Diagnostics;

namespace Orleans.Lattice.Vector.Tests.Benchmarks;

/// <summary>
/// The scale sweep: build time, query latency, and bytes per vector at corpus
/// sizes from ten thousand to a million - an order of magnitude beyond the
/// largest live corpus this index is built for.
/// <para>
/// It lives here rather than under <c>benchmark/</c> because it needs no host, no
/// cluster, and no Orleans runtime: it measures a self-contained algorithmic
/// core, so the test project is where it can be run and read most cheaply. It is
/// gated on the <c>LATTICE_VECTOR_BENCH</c> environment variable rather than on a
/// category alone, so it can never slow an ordinary lane down - without the
/// variable every case reports itself ignored in microseconds. Run it with:
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
public sealed class VectorIndexScaleBenchmarkTests
{
    private const string Gate = "LATTICE_VECTOR_BENCH";
    private const int Dimensions = 384;
    private const int QueryCount = 200;
    private const int ExhaustiveQueryCount = 20;
    private const int K = 10;

    private static void RequireGate()
    {
        if (string.IsNullOrEmpty(Environment.GetEnvironmentVariable(Gate)))
        {
            Assert.Ignore($"Set {Gate}=1 to run the vector index scale sweep.");
        }
    }

    [TestCase(10_000)]
    [TestCase(50_000)]
    [TestCase(100_000)]
    [TestCase(250_000)]
    [TestCase(1_000_000)]
    public void Sweep(int count)
    {
        RequireGate();

        var corpus = VectorCorpus.Clustered(count, Dimensions, clusters: 256, seed: 777);
        var queries = VectorCorpus.Clustered(QueryCount, Dimensions, clusters: 256, seed: 778);

        var index = new VectorIndex(new VectorIndexOptions { Dimensions = Dimensions });

        var insert = Stopwatch.StartNew();
        index.EnsureCapacity(count);
        for (var i = 0; i < count; i++)
        {
            index.Add(i, corpus[i]);
        }

        insert.Stop();

        // Before training the index is in Building state, so a search is an
        // exhaustive scan of the same contiguous block. Measuring the baseline
        // here rather than from a second index halves the benchmark's footprint
        // and compares like with like.
        var results = new VectorSearchResult[K];
        Assert.That(index.State, Is.EqualTo(VectorIndexState.Building));
        index.Search(queries[0], results, out var baselineMode);
        Assert.That(baselineMode, Is.EqualTo(VectorSearchMode.Exhaustive));

        var brute = Time(index, queries, results, ExhaustiveQueryCount);

        var train = Stopwatch.StartNew();
        index.Train();
        train.Stop();

        Assert.That(index.State, Is.EqualTo(VectorIndexState.Ready));

        var approximate = Time(index, queries, results, QueryCount);

        var status = index.Status;
        var bytes = VectorIndexMemory.Bytes(status.Capacity, status.Dimensions, status.PartitionCount);

        TestContext.Out.WriteLine(
            $"| {count} | {Dimensions} | {status.PartitionCount} | {status.Probes} "
            + $"| {insert.Elapsed.TotalSeconds:F2} | {train.Elapsed.TotalSeconds:F2} "
            + $"| {approximate:F3} | {brute:F3} | {brute / approximate:F1}x "
            + $"| {bytes / count} | {bytes / (1024d * 1024d):F0} |");
    }

    [Test]
    public void RecallSweep()
    {
        RequireGate();

        const int Count = 100_000;
        var corpus = VectorCorpus.Clustered(Count, Dimensions, clusters: 256, seed: 777);
        var keys = new long[Count];
        for (var i = 0; i < Count; i++)
        {
            keys[i] = i;
        }

        var queries = VectorCorpus.Clustered(50, Dimensions, clusters: 256, seed: 778);
        var exact = new long[queries.Length][];
        for (var q = 0; q < queries.Length; q++)
        {
            exact[q] = VectorCorpus.ExactTopK(corpus, keys, queries[q], K, VectorDistanceMetric.Cosine);
        }

        var partitions = VectorIndexOptions.AutoPartitionCount(Count);
        var auto = VectorIndexOptions.AutoProbes(partitions);

        TestContext.Out.WriteLine($"| corpus | partitions | probes | scanned | recall@{K} | mean query ms |");
        TestContext.Out.WriteLine("|---|---|---|---|---|---|");

        foreach (var probes in new[] { 1, 4, 8, 16, auto, 64, 158, partitions })
        {
            var index = new VectorIndex(new VectorIndexOptions
            {
                Dimensions = Dimensions,
                PartitionCount = partitions,
                Probes = probes,
            });

            index.EnsureCapacity(Count);
            for (var i = 0; i < Count; i++)
            {
                index.Add(i, corpus[i]);
            }

            index.Train();

            var results = new VectorSearchResult[K];
            for (var q = 0; q < 10; q++)
            {
                index.Search(queries[q % queries.Length], results);
            }

            var recall = 0d;
            var stopwatch = Stopwatch.StartNew();
            for (var q = 0; q < queries.Length; q++)
            {
                var found = index.Search(queries[q], results);
                recall += VectorCorpus.Recall(results.AsSpan(0, found), exact[q]);
            }

            stopwatch.Stop();

            TestContext.Out.WriteLine(
                $"| {Count} | {partitions} | {probes}{(probes == auto ? " (default)" : string.Empty)} "
                + $"| {(double)probes / partitions:P1} | {recall / queries.Length:F4} "
                + $"| {stopwatch.Elapsed.TotalMilliseconds / queries.Length:F3} |");
        }
    }

    private static double Time(
        VectorIndex index, float[][] queries, VectorSearchResult[] results, int iterations)
    {
        for (var q = 0; q < Math.Min(10, iterations); q++)
        {
            index.Search(queries[q % queries.Length], results);
        }

        var stopwatch = Stopwatch.StartNew();
        for (var q = 0; q < iterations; q++)
        {
            index.Search(queries[q % queries.Length], results);
        }

        stopwatch.Stop();
        return stopwatch.Elapsed.TotalMilliseconds / iterations;
    }
}
