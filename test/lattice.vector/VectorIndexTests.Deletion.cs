namespace Orleans.Lattice.Vector.Tests;

public sealed partial class VectorIndexTests
{
    [Test]
    public void Remove_deletes_a_vector_and_reports_that_it_did()
    {
        var index = CreateIndex();
        index.Add(1, Vector(Dimensions, 1f));

        Assert.That(index.Remove(1), Is.True);
        Assert.That(index.Count, Is.EqualTo(0));
        Assert.That(index.Contains(1), Is.False);
    }

    [Test]
    public void Remove_of_an_absent_key_is_a_no_op()
    {
        var index = CreateIndex();

        Assert.That(index.Remove(99), Is.False);
        Assert.That(index.Count, Is.EqualTo(0));
        Assert.That(index.Version, Is.EqualTo(0));
    }

    [Test]
    public void Remove_is_idempotent()
    {
        var index = CreateIndex();
        index.Add(1, Vector(Dimensions, 1f));

        Assert.That(index.Remove(1), Is.True);
        Assert.That(index.Remove(1), Is.False);
        Assert.That(index.Remove(1), Is.False);
        Assert.That(index.Count, Is.EqualTo(0));
    }

    [Test]
    public void A_removed_vector_never_appears_in_an_exhaustive_result()
    {
        var index = CreateIndex();
        index.Add(1, Vector(Dimensions, 1f));
        index.Add(2, Vector(Dimensions, 0.99f, 0.01f));
        index.Add(3, Vector(Dimensions, 0.98f, 0.02f));

        index.Remove(2);

        Span<VectorSearchResult> results = stackalloc VectorSearchResult[10];
        var found = index.Search(Vector(Dimensions, 1f), results);

        Assert.That(found, Is.EqualTo(2));
        Assert.That(results[..found].ToArray().Select(r => r.Key), Does.Not.Contain(2L));
    }

    [Test]
    public void A_removed_vector_never_appears_in_an_approximate_result()
    {
        var index = BuildTrainedIndex(count: 2_000, dimensions: Dimensions, partitionCount: 8, probes: 8);
        var query = new float[Dimensions];
        Assert.That(index.TryGetVector(123, query), Is.True);

        index.Remove(123);

        Span<VectorSearchResult> results = stackalloc VectorSearchResult[50];
        var found = index.Search(query, results);

        Assert.That(found, Is.GreaterThan(0));
        Assert.That(results[..found].ToArray().Select(r => r.Key), Does.Not.Contain(123L));
    }

    [Test]
    public void Removing_from_a_partition_shrinks_exactly_that_partition_and_bumps_its_version()
    {
        var index = BuildTrainedIndex(count: 2_000, dimensions: Dimensions, partitionCount: 8, probes: 8);

        var sizesBefore = new int[index.PartitionCount];
        var versionsBefore = new long[index.PartitionCount];
        for (var p = 0; p < index.PartitionCount; p++)
        {
            sizesBefore[p] = index.PartitionSize(p);
            versionsBefore[p] = index.PartitionVersion(p);
        }

        Assert.That(index.Remove(500), Is.True);

        var shrunk = new List<int>();
        for (var p = 0; p < index.PartitionCount; p++)
        {
            if (index.PartitionSize(p) != sizesBefore[p])
            {
                shrunk.Add(p);
            }
        }

        Assert.That(shrunk, Has.Count.EqualTo(1));
        Assert.That(index.PartitionSize(shrunk[0]), Is.EqualTo(sizesBefore[shrunk[0]] - 1));
        Assert.That(index.PartitionVersion(shrunk[0]), Is.GreaterThan(versionsBefore[shrunk[0]]));
    }

    [Test]
    public void Posting_lists_always_sum_to_the_live_count_across_a_churn_cycle()
    {
        var index = BuildTrainedIndex(count: 2_000, dimensions: Dimensions, partitionCount: 16, probes: 16);
        var corpus = VectorCorpus.Clustered(500, Dimensions, clusters: 16, seed: 99);

        for (var i = 0; i < 500; i++)
        {
            index.Remove(i * 3);
            index.Add(100_000 + i, corpus[i]);
        }

        var total = 0;
        for (var p = 0; p < index.PartitionCount; p++)
        {
            total += index.PartitionSize(p);
        }

        Assert.That(total, Is.EqualTo(index.Count));
    }

    [Test]
    public void A_deleted_slot_is_reused_by_the_next_insert()
    {
        var index = CreateIndex();
        index.EnsureCapacity(4);
        for (var i = 0; i < 4; i++)
        {
            index.Add(i, Vector(Dimensions, i));
        }

        index.Remove(2);
        index.Add(99, Vector(Dimensions, 42f));

        Assert.That(index.Count, Is.EqualTo(4));
        Assert.That(index.Capacity, Is.EqualTo(4));
    }

    [Test]
    public void Searching_an_index_with_every_vector_deleted_returns_nothing()
    {
        var index = CreateIndex();
        index.Add(1, Vector(Dimensions, 1f));
        index.Add(2, Vector(Dimensions, 2f));
        index.Remove(1);
        index.Remove(2);

        Span<VectorSearchResult> results = stackalloc VectorSearchResult[5];

        Assert.That(index.Search(Vector(Dimensions, 1f), results), Is.EqualTo(0));
    }

    [Test]
    public void Recall_does_not_degrade_materially_across_a_realistic_churn_cycle()
    {
        const int Initial = 4_000;
        const int Replaced = 1_000;
        const int QueryCount = 50;
        const int Dims = 32;
        const int Partitions = 64;
        const int Probes = 16;

        // One draw, split three ways, so the replacements and the queries live in
        // the same embedding space as the corpus the index was trained on. That is
        // the realistic case: a file is re-embedded by the same model into the same
        // space. A genuine distribution shift is a different scenario and is
        // answered by retraining, not by the posting lists.
        var all = VectorCorpus.Clustered(Initial + Replaced + QueryCount, Dims, clusters: Partitions, seed: 5);

        var churned = NewIndex(Dims, Partitions, Probes);
        churned.EnsureCapacity(Initial + Replaced);
        for (var i = 0; i < Initial; i++)
        {
            churned.Add(i, all[i]);
        }

        churned.Train();

        // Retire a quarter of the corpus and replace it with fresh vectors from
        // the same space, without retraining - exactly what a live maintenance
        // loop does between rebuilds.
        var live = new List<float[]>();
        var liveKeys = new List<long>();
        for (var i = 0; i < Initial; i++)
        {
            if (i % 4 == 0)
            {
                churned.Remove(i);
                continue;
            }

            live.Add(all[i]);
            liveKeys.Add(i);
        }

        for (var i = 0; i < Replaced; i++)
        {
            var vector = all[Initial + i];
            churned.Add(1_000_000 + i, vector);
            live.Add(vector);
            liveKeys.Add(1_000_000 + i);
        }

        Assert.That(churned.Count, Is.EqualTo(live.Count));

        // The baseline is an index trained from scratch over the same final
        // contents. The churned index is allowed to trail it slightly, because its
        // cells were fitted to a corpus a quarter of which has since been replaced
        // - but not materially.
        var rebuilt = NewIndex(Dims, Partitions, Probes);
        rebuilt.EnsureCapacity(live.Count);
        for (var i = 0; i < live.Count; i++)
        {
            rebuilt.Add(liveKeys[i], live[i]);
        }

        rebuilt.Train();

        var queries = all[(Initial + Replaced)..];
        var churnedRecall = MeasureRecall(churned, live, liveKeys, queries, k: 10);
        var rebuiltRecall = MeasureRecall(rebuilt, live, liveKeys, queries, k: 10);

        TestContext.Out.WriteLine(
            $"churn cycle: recall@10 without retraining = {churnedRecall:F4}, after a rebuild = {rebuiltRecall:F4}");

        Assert.That(churnedRecall, Is.GreaterThanOrEqualTo(rebuiltRecall - 0.05d),
            $"Recall after a 25 percent churn cycle without retraining ({churnedRecall:F4}) fell materially below the "
            + $"recall of an index rebuilt over the same contents ({rebuiltRecall:F4}).");
    }

    private static VectorIndex NewIndex(int dimensions, int partitionCount, int probes) =>
        new(new VectorIndexOptions
        {
            Dimensions = dimensions,
            PartitionCount = partitionCount,
            Probes = probes,
            MinimumTrainingCount = 16,
            TrainingSampleSize = 4_096,
        });

    private static double MeasureRecall(
        VectorIndex index,
        IReadOnlyList<float[]> corpus,
        IReadOnlyList<long> keys,
        IReadOnlyList<float[]> queries,
        int k)
    {
        var results = new VectorSearchResult[k];
        var total = 0d;
        foreach (var query in queries)
        {
            var found = index.Search(query, results);
            var exact = VectorCorpus.ExactTopK(corpus, keys, query, k, index.Metric);
            total += VectorCorpus.Recall(results.AsSpan(0, found), exact);
        }

        return total / queries.Count;
    }
}
