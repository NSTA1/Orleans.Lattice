namespace Orleans.Lattice.Vector.Tests;

/// <summary>
/// Unit tests for the query path: exhaustive and approximate search, the probe
/// selection seam, and the edge cases a caller can reach with an unusual k or an
/// unusual corpus.
/// </summary>
[TestFixture]
public sealed class VectorIndexSearchTests
{
    private const int Dimensions = 6;

    private static VectorIndex CreateIndex(VectorDistanceMetric metric = VectorDistanceMetric.Cosine) =>
        new(new VectorIndexOptions { Dimensions = Dimensions, Metric = metric });

    private static float[] Unit(int axis)
    {
        var vector = new float[Dimensions];
        vector[axis] = 1f;
        return vector;
    }

    [Test]
    public void Searching_an_empty_index_returns_nothing_exhaustively()
    {
        var index = CreateIndex();
        Span<VectorSearchResult> results = stackalloc VectorSearchResult[3];

        var found = index.Search(Unit(0), results, out var mode);

        Assert.That(found, Is.EqualTo(0));
        Assert.That(mode, Is.EqualTo(VectorSearchMode.Exhaustive));
    }

    [Test]
    public void Searching_with_an_empty_destination_returns_nothing()
    {
        var index = CreateIndex();
        index.Add(1, Unit(0));

        Assert.That(index.Search(Unit(0), Span<VectorSearchResult>.Empty), Is.EqualTo(0));
    }

    [Test]
    public void Searching_rejects_a_query_of_the_wrong_dimensionality()
    {
        var index = CreateIndex();
        index.Add(1, Unit(0));

        Assert.Throws<ArgumentException>(() =>
        {
            Span<VectorSearchResult> results = stackalloc VectorSearchResult[1];
            index.Search(new float[Dimensions + 2], results);
        });
    }

    [Test]
    public void A_single_vector_index_returns_that_vector()
    {
        var index = CreateIndex();
        index.Add(42, Unit(3));

        Span<VectorSearchResult> results = stackalloc VectorSearchResult[5];
        var found = index.Search(Unit(3), results);

        Assert.That(found, Is.EqualTo(1));
        Assert.That(results[0].Key, Is.EqualTo(42));
        Assert.That(results[0].Score, Is.EqualTo(1f).Within(1e-5f));
    }

    [Test]
    public void Asking_for_more_results_than_the_corpus_holds_returns_the_whole_corpus()
    {
        var index = CreateIndex();
        index.Add(1, Unit(0));
        index.Add(2, Unit(1));

        Span<VectorSearchResult> results = stackalloc VectorSearchResult[50];
        var found = index.Search(Unit(0), results);

        Assert.That(found, Is.EqualTo(2));
    }

    [Test]
    public void Results_arrive_in_descending_score_order()
    {
        var index = CreateIndex();
        index.Add(1, [1f, 0f, 0f, 0f, 0f, 0f]);
        index.Add(2, [0.9f, 0.1f, 0f, 0f, 0f, 0f]);
        index.Add(3, [0f, 1f, 0f, 0f, 0f, 0f]);

        Span<VectorSearchResult> results = stackalloc VectorSearchResult[3];
        var found = index.Search(Unit(0), results);

        Assert.That(found, Is.EqualTo(3));
        Assert.That(results[0].Key, Is.EqualTo(1));
        Assert.That(results[1].Key, Is.EqualTo(2));
        Assert.That(results[2].Key, Is.EqualTo(3));
        Assert.That(results[0].Score, Is.GreaterThan(results[1].Score));
        Assert.That(results[1].Score, Is.GreaterThan(results[2].Score));
    }

    [Test]
    public void Identical_vectors_tie_and_are_ordered_by_ascending_key()
    {
        var index = CreateIndex();
        index.Add(30, Unit(0));
        index.Add(10, Unit(0));
        index.Add(20, Unit(0));

        Span<VectorSearchResult> results = stackalloc VectorSearchResult[3];
        var found = index.Search(Unit(0), results);

        Assert.That(found, Is.EqualTo(3));
        Assert.That(results[0].Key, Is.EqualTo(10));
        Assert.That(results[1].Key, Is.EqualTo(20));
        Assert.That(results[2].Key, Is.EqualTo(30));
    }

    [Test]
    public void A_tie_at_the_cutoff_keeps_the_lower_key()
    {
        var index = CreateIndex();
        index.Add(5, Unit(0));
        index.Add(6, Unit(0));

        Span<VectorSearchResult> results = stackalloc VectorSearchResult[1];
        var found = index.Search(Unit(0), results);

        Assert.That(found, Is.EqualTo(1));
        Assert.That(results[0].Key, Is.EqualTo(5));
    }

    [Test]
    public void A_zero_magnitude_stored_vector_scores_zero_under_cosine()
    {
        var index = CreateIndex();
        index.Add(1, new float[Dimensions]);

        Span<VectorSearchResult> results = stackalloc VectorSearchResult[1];
        index.Search(Unit(0), results);

        Assert.That(results[0].Score, Is.EqualTo(0f));
    }

    [Test]
    public void A_zero_magnitude_query_scores_zero_under_cosine()
    {
        var index = CreateIndex();
        index.Add(1, Unit(0));

        Span<VectorSearchResult> results = stackalloc VectorSearchResult[1];
        index.Search(new float[Dimensions], results);

        Assert.That(results[0].Score, Is.EqualTo(0f));
    }

    [Test]
    public void The_dot_product_metric_ranks_by_magnitude_as_well_as_direction()
    {
        var index = CreateIndex(VectorDistanceMetric.DotProduct);
        index.Add(1, [1f, 0f, 0f, 0f, 0f, 0f]);
        index.Add(2, [5f, 0f, 0f, 0f, 0f, 0f]);

        Span<VectorSearchResult> results = stackalloc VectorSearchResult[2];
        index.Search(Unit(0), results);

        Assert.That(results[0].Key, Is.EqualTo(2));
        Assert.That(results[0].Score, Is.EqualTo(5f).Within(1e-5f));
        Assert.That(results[1].Score, Is.EqualTo(1f).Within(1e-5f));
    }

    [Test]
    public void An_untrained_index_answers_exhaustively_and_reports_that_it_did()
    {
        var index = CreateIndex();
        index.Add(1, Unit(0));

        Span<VectorSearchResult> results = stackalloc VectorSearchResult[1];
        index.Search(Unit(0), results, out var mode);

        Assert.That(mode, Is.EqualTo(VectorSearchMode.Exhaustive));
        Assert.That(index.State, Is.EqualTo(VectorIndexState.Building));
    }

    [Test]
    public void A_trained_index_answers_approximately_and_reports_that_it_did()
    {
        var index = Trained(out _, out _);

        Span<VectorSearchResult> results = stackalloc VectorSearchResult[10];
        index.Search(new float[16], results, out var mode);

        Assert.That(mode, Is.EqualTo(VectorSearchMode.Approximate));
        Assert.That(index.State, Is.EqualTo(VectorIndexState.Ready));
        Assert.That(index.IsReady, Is.True);
    }

    [Test]
    public void SelectPartitions_returns_nothing_for_an_untrained_index()
    {
        var index = CreateIndex();
        index.Add(1, Unit(0));

        Span<int> partitions = stackalloc int[4];

        Assert.That(index.SelectPartitions(Unit(0), partitions), Is.EqualTo(0));
    }

    [Test]
    public void SelectPartitions_returns_nothing_for_an_empty_destination()
    {
        var index = Trained(out _, out _);

        Assert.That(index.SelectPartitions(new float[16], Span<int>.Empty), Is.EqualTo(0));
    }

    [Test]
    public void SelectPartitions_fills_the_destination_with_distinct_partitions()
    {
        var index = Trained(out _, out _);
        Span<int> partitions = stackalloc int[6];

        var found = index.SelectPartitions(new float[16], partitions);

        Assert.That(found, Is.EqualTo(6));
        Assert.That(partitions.ToArray().Distinct().Count(), Is.EqualTo(6));
        Assert.That(partitions.ToArray(), Is.All.InRange(0, index.PartitionCount - 1));
    }

    [Test]
    public void SelectPartitions_never_returns_more_than_the_partition_count()
    {
        var index = Trained(out _, out _);
        var destination = new int[index.PartitionCount + 10];

        Assert.That(index.SelectPartitions(new float[16], destination), Is.EqualTo(index.PartitionCount));
    }

    [Test]
    public void SelectPartitions_is_stable_across_calls()
    {
        var index = Trained(out var corpus, out _);
        var first = new int[5];
        var second = new int[5];

        index.SelectPartitions(corpus[3], first);
        index.SelectPartitions(corpus[3], second);

        Assert.That(second, Is.EqualTo(first));
    }

    [Test]
    public void Probing_a_single_partition_still_finds_the_query_vector_itself()
    {
        var corpus = VectorCorpus.Clustered(2_000, 16, clusters: 32, seed: 11);
        var index = new VectorIndex(new VectorIndexOptions
        {
            Dimensions = 16,
            PartitionCount = 32,
            Probes = 1,
            MinimumTrainingCount = 16,
            TrainingSampleSize = 2_048,
        });

        index.EnsureCapacity(corpus.Length);
        for (var i = 0; i < corpus.Length; i++)
        {
            index.Add(i, corpus[i]);
        }

        index.Train();

        Span<VectorSearchResult> results = stackalloc VectorSearchResult[1];
        for (var q = 0; q < 25; q++)
        {
            var found = index.Search(corpus[q], results);

            Assert.That(found, Is.EqualTo(1));
            Assert.That(results[0].Key, Is.EqualTo(q),
                $"A vector must fall in the cell its own centroid ranks first, but key {q} did not.");
        }
    }

    [Test]
    public void SelectPartitions_rejects_a_query_of_the_wrong_dimensionality()
    {
        var index = Trained(out _, out _);

        Assert.Throws<ArgumentException>(() => index.SelectPartitions(new float[3], new int[2]));
    }

    [Test]
    public void Probing_every_partition_reproduces_the_exact_result_set()
    {
        var corpus = VectorCorpus.Clustered(1_500, 16, clusters: 24, seed: 13);

        var everything = new VectorIndex(new VectorIndexOptions
        {
            Dimensions = 16,
            PartitionCount = 24,
            Probes = 24,
            MinimumTrainingCount = 16,
            TrainingSampleSize = 1_500,
        });

        var exhaustive = new VectorIndex(new VectorIndexOptions { Dimensions = 16 });

        for (var i = 0; i < corpus.Length; i++)
        {
            everything.Add(i, corpus[i]);
            exhaustive.Add(i, corpus[i]);
        }

        everything.Train();
        Assert.That(everything.Probes, Is.EqualTo(24));

        var approximate = new VectorSearchResult[10];
        var exact = new VectorSearchResult[10];
        var queries = VectorCorpus.Clustered(20, 16, clusters: 24, seed: 14);

        foreach (var query in queries)
        {
            var foundApproximate = everything.Search(query, approximate, out var approximateMode);
            var foundExact = exhaustive.Search(query, exact, out var exactMode);

            Assert.That(approximateMode, Is.EqualTo(VectorSearchMode.Approximate));
            Assert.That(exactMode, Is.EqualTo(VectorSearchMode.Exhaustive));
            Assert.That(foundApproximate, Is.EqualTo(foundExact));
            Assert.That(approximate[..foundApproximate], Is.EqualTo(exact[..foundExact]));
        }
    }

    private static VectorIndex Trained(out float[][] corpus, out long[] keys)
    {
        corpus = VectorCorpus.Clustered(2_000, 16, clusters: 32, seed: 11);
        keys = new long[corpus.Length];
        var index = new VectorIndex(new VectorIndexOptions
        {
            Dimensions = 16,
            PartitionCount = 32,
            Probes = 8,
            MinimumTrainingCount = 16,
            TrainingSampleSize = 2_048,
        });

        index.EnsureCapacity(corpus.Length);
        for (var i = 0; i < corpus.Length; i++)
        {
            keys[i] = i;
            index.Add(i, corpus[i]);
        }

        index.Train();
        return index;
    }
}
