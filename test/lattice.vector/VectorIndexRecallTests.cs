namespace Orleans.Lattice.Vector.Tests;

/// <summary>
/// The committed recall harness. It computes the exact top-k by brute force over
/// fixed, reproducible corpora and measures what the index actually returns, so
/// the package's recall contract is a measurement rather than an assertion.
/// <para>
/// Two corpora are measured. The clustered one mirrors real embedding geometry,
/// which is what the index is deployed against. The unclustered one is the
/// adversarial case - independent Gaussian vectors have no cluster structure for
/// a partitioning to exploit - and is reported so the published figure is not
/// flattered by a friendly corpus.
/// </para>
/// <para>
/// This fixture deliberately measures recall only. Latency is measured by the
/// warmed-up scale sweep under <c>Benchmarks/</c>; timings taken here, where
/// every case builds and trains a fresh index, would be dominated by cold-start
/// effects and would be misleading if published.
/// </para>
/// </summary>
[TestFixture]
public sealed class VectorIndexRecallTests
{
    private const int Count = 20_000;
    private const int Dimensions = 64;
    private const int Queries = 50;
    private const int K = 10;

    /// <summary>
    /// The published recall floor at the default probe setting over a clustered
    /// corpus - the geometry every real embedding space has. Consumers that make
    /// approximate retrieval the default path quote this figure.
    /// </summary>
    private const double ClusteredRecallTarget = 0.95d;

    /// <summary>
    /// The published recall floor at the default probe setting over an
    /// unclustered corpus. No partitioning can do well here: with no cluster
    /// structure the true neighbours spread evenly over every cell, so recall
    /// tracks the fraction of the corpus scanned almost exactly. A caller whose
    /// space really is unclustered raises <see cref="VectorIndexOptions.Probes"/>.
    /// </summary>
    private const double UnclusteredRecallTarget = 0.55d;

    [Test]
    public void Recall_at_the_default_configuration_meets_the_published_clustered_target()
    {
        var corpus = VectorCorpus.Clustered(Count, Dimensions, clusters: 128, seed: 101);
        var recall = Measure(corpus, partitionCount: 0, probes: 0, seed: 0x9E3779B97F4A7C15UL);

        TestContext.Out.WriteLine($"clustered default: recall@{K} = {recall:F4} over {Count} vectors");

        Assert.That(recall, Is.GreaterThanOrEqualTo(ClusteredRecallTarget),
            $"Recall at the default configuration fell to {recall:F4}, below the published target of {ClusteredRecallTarget:F2}.");
    }

    [Test]
    public void Recall_at_the_default_configuration_meets_the_published_unclustered_target()
    {
        var corpus = VectorCorpus.Uniform(Count, Dimensions, seed: 202);
        var recall = Measure(corpus, partitionCount: 0, probes: 0, seed: 0x9E3779B97F4A7C15UL);

        TestContext.Out.WriteLine($"unclustered default: recall@{K} = {recall:F4} over {Count} vectors");

        Assert.That(recall, Is.GreaterThanOrEqualTo(UnclusteredRecallTarget),
            $"Recall on an unclustered corpus fell to {recall:F4}, below the published target of {UnclusteredRecallTarget:F2}.");
    }

    [Test]
    public void Recall_rises_monotonically_with_the_probe_count()
    {
        var corpus = VectorCorpus.Clustered(Count, Dimensions, clusters: 128, seed: 101);
        const int Partitions = 141;

        TestContext.Out.WriteLine($"| corpus | partitions | probes | scanned | recall@{K} |");
        TestContext.Out.WriteLine("|---|---|---|---|---|");

        var previous = 0d;
        foreach (var probes in new[] { 1, 2, 4, 8, 16, 24, 32, 64, Partitions })
        {
            var recall = Measure(corpus, Partitions, probes, 0x9E3779B97F4A7C15UL);
            TestContext.Out.WriteLine(
                $"| clustered {Count} | {Partitions} | {probes} | {(double)probes / Partitions:P1} | {recall:F4} |");

            Assert.That(recall, Is.GreaterThanOrEqualTo(previous - 1e-9d),
                $"Recall fell from {previous:F4} to {recall:F4} when the probe count rose to {probes}.");
            previous = recall;
        }

        Assert.That(previous, Is.EqualTo(1d).Within(1e-9d),
            "Probing every partition must reproduce the exact result set.");
    }

    [Test]
    public void Recall_on_an_unclustered_corpus_rises_with_the_probe_count_too()
    {
        var corpus = VectorCorpus.Uniform(Count, Dimensions, seed: 202);
        const int Partitions = 141;

        TestContext.Out.WriteLine($"| corpus | partitions | probes | scanned | recall@{K} |");
        TestContext.Out.WriteLine("|---|---|---|---|---|");

        var previous = 0d;
        foreach (var probes in new[] { 1, 4, 24, 36, 71, Partitions })
        {
            var recall = Measure(corpus, Partitions, probes, 0x9E3779B97F4A7C15UL);
            TestContext.Out.WriteLine(
                $"| unclustered {Count} | {Partitions} | {probes} | {(double)probes / Partitions:P1} | {recall:F4} |");

            Assert.That(recall, Is.GreaterThanOrEqualTo(previous - 1e-9d));
            previous = recall;
        }

        Assert.That(previous, Is.EqualTo(1d).Within(1e-9d));
    }

    [Test]
    public void Recall_at_the_default_holds_as_the_corpus_grows()
    {
        // The default probe rule grows with the square root of the partition
        // count, so the fraction of the corpus it scans shrinks as the corpus
        // grows. This measures that recall nevertheless holds, which is the claim
        // that makes the default safe to ship as the primary retrieval path.
        TestContext.Out.WriteLine($"| corpus | partitions | probes | scanned | recall@{K} |");
        TestContext.Out.WriteLine("|---|---|---|---|---|");

        foreach (var count in new[] { 5_000, 20_000, 60_000 })
        {
            var corpus = VectorCorpus.Clustered(count, Dimensions, clusters: 128, seed: 404);
            var recall = Measure(corpus, partitionCount: 0, probes: 0, seed: 0x9E3779B97F4A7C15UL, out var status);

            TestContext.Out.WriteLine(
                $"| clustered {count} | {status.PartitionCount} | {status.Probes} "
                + $"| {(double)status.Probes / status.PartitionCount:P1} | {recall:F4} |");

            Assert.That(recall, Is.GreaterThanOrEqualTo(ClusteredRecallTarget),
                $"Recall at the default configuration fell to {recall:F4} at {count} vectors.");
        }
    }

    [Test]
    public void Recall_is_stable_across_seeds()
    {
        var corpus = VectorCorpus.Clustered(Count, Dimensions, clusters: 128, seed: 101);

        foreach (var seed in new ulong[] { 1, 2, 3, 0x9E3779B97F4A7C15UL })
        {
            var recall = Measure(corpus, partitionCount: 141, probes: 24, seed: seed);
            TestContext.Out.WriteLine($"seed {seed}: recall@{K} = {recall:F4}");

            Assert.That(recall, Is.GreaterThanOrEqualTo(ClusteredRecallTarget),
                $"Recall with seed {seed} fell to {recall:F4}, so the published target depends on a lucky seed.");
        }
    }

    [Test]
    public void The_measurement_is_repeatable()
    {
        var corpus = VectorCorpus.Clustered(5_000, Dimensions, clusters: 64, seed: 303);

        var first = Measure(corpus, partitionCount: 71, probes: 9, seed: 5);
        var second = Measure(corpus, partitionCount: 71, probes: 9, seed: 5);

        Assert.That(second, Is.EqualTo(first));
    }

    private static double Measure(float[][] corpus, int partitionCount, int probes, ulong seed) =>
        Measure(corpus, partitionCount, probes, seed, out _);

    private static double Measure(
        float[][] corpus, int partitionCount, int probes, ulong seed, out VectorIndexStatus status)
    {
        var keys = new long[corpus.Length];
        var index = new VectorIndex(new VectorIndexOptions
        {
            Dimensions = Dimensions,
            PartitionCount = partitionCount,
            Probes = probes,
            MinimumTrainingCount = 16,
            TrainingSampleSize = 8_192,
            Seed = seed,
        });

        index.EnsureCapacity(corpus.Length);
        for (var i = 0; i < corpus.Length; i++)
        {
            keys[i] = i;
            index.Add(i, corpus[i]);
        }

        index.Train();
        status = index.Status;
        Assert.That(status.State, Is.EqualTo(VectorIndexState.Ready));

        // Queries are drawn from the same distribution but are not corpus members,
        // which is the honest case: a query that is itself in the index trivially
        // ranks its own cell first.
        var queries = VectorCorpus.Clustered(Queries, Dimensions, clusters: 128, seed: 909);

        var results = new VectorSearchResult[K];
        var total = 0d;
        for (var q = 0; q < Queries; q++)
        {
            var found = index.Search(queries[q], results, out var mode);
            Assert.That(mode, Is.EqualTo(VectorSearchMode.Approximate));
            total += VectorCorpus.Recall(
                results.AsSpan(0, found),
                VectorCorpus.ExactTopK(corpus, keys, queries[q], K, index.Metric));
        }

        return total / Queries;
    }
}
