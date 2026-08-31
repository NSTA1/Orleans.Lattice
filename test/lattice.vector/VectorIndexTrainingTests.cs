namespace Orleans.Lattice.Vector.Tests;

/// <summary>
/// Unit tests for the training pass: when it partitions, when it declines to,
/// how the sizing knobs resolve, and how retraining behaves.
/// </summary>
[TestFixture]
public sealed class VectorIndexTrainingTests
{
    private const int Dimensions = 12;

    private static VectorIndex Fill(VectorIndexOptions options, int count, ulong corpusSeed = 3)
    {
        var corpus = VectorCorpus.Clustered(count, options.Dimensions, clusters: 16, seed: corpusSeed);
        var index = new VectorIndex(options);
        index.EnsureCapacity(count);
        for (var i = 0; i < count; i++)
        {
            index.Add(i, corpus[i]);
        }

        return index;
    }

    [Test]
    public void Training_an_empty_index_declines_and_leaves_it_empty()
    {
        var index = new VectorIndex(new VectorIndexOptions { Dimensions = Dimensions });

        Assert.That(index.Train(), Is.False);
        Assert.That(index.State, Is.EqualTo(VectorIndexState.Empty));
        Assert.That(index.PartitionCount, Is.EqualTo(0));
    }

    [Test]
    public void Training_below_the_minimum_corpus_declines_and_stays_exhaustive()
    {
        var index = Fill(new VectorIndexOptions { Dimensions = Dimensions, MinimumTrainingCount = 500 }, 100);

        Assert.That(index.Train(), Is.False);
        Assert.That(index.State, Is.EqualTo(VectorIndexState.Building));

        Span<VectorSearchResult> results = stackalloc VectorSearchResult[5];
        index.Search(new float[Dimensions], results, out var mode);
        Assert.That(mode, Is.EqualTo(VectorSearchMode.Exhaustive));
    }

    [Test]
    public void Training_a_corpus_that_resolves_to_one_partition_declines()
    {
        var index = Fill(
            new VectorIndexOptions
            {
                Dimensions = Dimensions,
                MinimumTrainingCount = 4,
                PartitionCount = 1,
                TrainingSampleSize = 64,
            },
            50);

        Assert.That(index.Train(), Is.False);
        Assert.That(index.PartitionCount, Is.EqualTo(0));
    }

    [Test]
    public void Training_partitions_the_corpus_and_reports_ready()
    {
        var index = Fill(
            new VectorIndexOptions
            {
                Dimensions = Dimensions,
                MinimumTrainingCount = 16,
                PartitionCount = 8,
                TrainingSampleSize = 512,
            },
            1_000);

        Assert.That(index.Train(), Is.True);
        Assert.That(index.State, Is.EqualTo(VectorIndexState.Ready));
        Assert.That(index.IsReady, Is.True);
        Assert.That(index.PartitionCount, Is.EqualTo(8));
    }

    [Test]
    public void Every_live_vector_lands_in_exactly_one_partition()
    {
        var index = Fill(
            new VectorIndexOptions
            {
                Dimensions = Dimensions,
                MinimumTrainingCount = 16,
                PartitionCount = 12,
                TrainingSampleSize = 512,
            },
            1_000);

        index.Train();

        var total = 0;
        for (var p = 0; p < index.PartitionCount; p++)
        {
            total += index.PartitionSize(p);
        }

        Assert.That(total, Is.EqualTo(index.Count));
    }

    [Test]
    public void An_unset_partition_count_is_derived_from_the_corpus()
    {
        var index = Fill(
            new VectorIndexOptions { Dimensions = Dimensions, MinimumTrainingCount = 16, TrainingSampleSize = 512 },
            900);

        index.Train();

        Assert.That(index.PartitionCount, Is.EqualTo(VectorIndexOptions.AutoPartitionCount(900)));
        Assert.That(index.Probes, Is.EqualTo(VectorIndexOptions.AutoProbes(index.PartitionCount)));
    }

    [Test]
    public void A_requested_partition_count_is_capped_at_the_corpus_size()
    {
        var index = Fill(
            new VectorIndexOptions
            {
                Dimensions = Dimensions,
                MinimumTrainingCount = 4,
                PartitionCount = 500,
                TrainingSampleSize = 512,
            },
            40);

        index.Train();

        Assert.That(index.PartitionCount, Is.EqualTo(40));
    }

    [Test]
    public void A_requested_probe_count_is_capped_at_the_partition_count()
    {
        var index = Fill(
            new VectorIndexOptions
            {
                Dimensions = Dimensions,
                MinimumTrainingCount = 16,
                PartitionCount = 6,
                Probes = 100,
                TrainingSampleSize = 512,
            },
            600);

        index.Train();

        Assert.That(index.Probes, Is.EqualTo(6));
    }

    [Test]
    public void Retraining_a_shrunken_corpus_drops_the_partitioning()
    {
        var index = Fill(
            new VectorIndexOptions
            {
                Dimensions = Dimensions,
                MinimumTrainingCount = 500,
                PartitionCount = 8,
                TrainingSampleSize = 512,
            },
            1_000);

        Assert.That(index.Train(), Is.True);

        for (var i = 0; i < 900; i++)
        {
            index.Remove(i);
        }

        Assert.That(index.Train(), Is.False);
        Assert.That(index.PartitionCount, Is.EqualTo(0));
        Assert.That(index.State, Is.EqualTo(VectorIndexState.Building));

        Span<VectorSearchResult> results = stackalloc VectorSearchResult[5];
        var found = index.Search(new float[Dimensions], results, out var mode);
        Assert.That(mode, Is.EqualTo(VectorSearchMode.Exhaustive));
        Assert.That(found, Is.EqualTo(5));
    }

    [Test]
    public void Retraining_after_growth_repartitions_and_keeps_every_vector()
    {
        var options = new VectorIndexOptions
        {
            Dimensions = Dimensions,
            MinimumTrainingCount = 16,
            PartitionCount = 8,
            TrainingSampleSize = 512,
        };

        var index = Fill(options, 500);
        index.Train();

        var more = VectorCorpus.Clustered(500, Dimensions, clusters: 16, seed: 8);
        for (var i = 0; i < more.Length; i++)
        {
            index.Add(1_000 + i, more[i]);
        }

        Assert.That(index.Train(), Is.True);
        Assert.That(index.Count, Is.EqualTo(1_000));

        var total = 0;
        for (var p = 0; p < index.PartitionCount; p++)
        {
            total += index.PartitionSize(p);
        }

        Assert.That(total, Is.EqualTo(1_000));
    }

    [Test]
    public void Training_a_corpus_of_identical_vectors_still_partitions_every_vector()
    {
        var index = new VectorIndex(new VectorIndexOptions
        {
            Dimensions = Dimensions,
            MinimumTrainingCount = 4,
            PartitionCount = 8,
            TrainingSampleSize = 256,
        });

        var identical = new float[Dimensions];
        identical[0] = 1f;
        for (var i = 0; i < 200; i++)
        {
            index.Add(i, identical);
        }

        Assert.That(index.Train(), Is.True);

        var total = 0;
        for (var p = 0; p < index.PartitionCount; p++)
        {
            total += index.PartitionSize(p);
        }

        Assert.That(total, Is.EqualTo(200));

        Span<VectorSearchResult> results = stackalloc VectorSearchResult[10];
        var found = index.Search(identical, results);
        Assert.That(found, Is.GreaterThan(0));
        Assert.That(results[0].Score, Is.EqualTo(1f).Within(1e-5f));
    }

    [Test]
    public void Training_with_the_dot_product_metric_partitions_the_corpus()
    {
        var index = Fill(
            new VectorIndexOptions
            {
                Dimensions = Dimensions,
                Metric = VectorDistanceMetric.DotProduct,
                MinimumTrainingCount = 16,
                PartitionCount = 8,
                TrainingSampleSize = 512,
            },
            800);

        Assert.That(index.Train(), Is.True);

        var total = 0;
        for (var p = 0; p < index.PartitionCount; p++)
        {
            total += index.PartitionSize(p);
        }

        Assert.That(total, Is.EqualTo(800));
    }

    [Test]
    public void Training_samples_only_up_to_the_configured_sample_size()
    {
        var index = Fill(
            new VectorIndexOptions
            {
                Dimensions = Dimensions,
                MinimumTrainingCount = 16,
                PartitionCount = 8,
                TrainingSampleSize = 64,
            },
            2_000);

        Assert.That(index.Train(), Is.True);
        Assert.That(index.Count, Is.EqualTo(2_000));

        var total = 0;
        for (var p = 0; p < index.PartitionCount; p++)
        {
            total += index.PartitionSize(p);
        }

        Assert.That(total, Is.EqualTo(2_000));
    }

    [Test]
    public void A_vector_added_after_training_joins_a_partition_immediately()
    {
        var index = Fill(
            new VectorIndexOptions
            {
                Dimensions = Dimensions,
                MinimumTrainingCount = 16,
                PartitionCount = 8,
                Probes = 8,
                TrainingSampleSize = 512,
            },
            600);

        index.Train();

        var newcomer = new float[Dimensions];
        newcomer[0] = 100f;
        index.Add(9_999, newcomer);

        var total = 0;
        for (var p = 0; p < index.PartitionCount; p++)
        {
            total += index.PartitionSize(p);
        }

        Assert.That(total, Is.EqualTo(601));

        Span<VectorSearchResult> results = stackalloc VectorSearchResult[1];
        var found = index.Search(newcomer, results);
        Assert.That(found, Is.EqualTo(1));
        Assert.That(results[0].Key, Is.EqualTo(9_999));
    }
}
