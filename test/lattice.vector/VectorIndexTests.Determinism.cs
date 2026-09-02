namespace Orleans.Lattice.Vector.Tests;

public sealed partial class VectorIndexTests
{
    private const int DeterminismDimensions = 16;
    private const int DeterminismCount = 1_500;

    [Test]
    public void Two_indexes_over_the_same_corpus_and_options_return_identical_results()
    {
        var corpus = VectorCorpus.Clustered(DeterminismCount, DeterminismDimensions, clusters: 24, seed: 21);

        var left = BuildDeterministic(corpus, ascending: true);
        var right = BuildDeterministic(corpus, ascending: true);

        AssertIdenticalResults(left, right, corpus);
    }

    [Test]
    public void Insertion_order_does_not_change_the_result_set()
    {
        var corpus = VectorCorpus.Clustered(DeterminismCount, DeterminismDimensions, clusters: 24, seed: 22);

        var ascending = BuildDeterministic(corpus, ascending: true);
        var descending = BuildDeterministic(corpus, ascending: false);

        Assert.That(descending.Count, Is.EqualTo(ascending.Count));
        AssertIdenticalResults(ascending, descending, corpus);
    }

    [Test]
    public void Deleting_and_reinserting_a_vector_does_not_change_the_result_set()
    {
        var corpus = VectorCorpus.Clustered(DeterminismCount, DeterminismDimensions, clusters: 24, seed: 23);

        var pristine = BuildDeterministic(corpus, ascending: true);
        var churned = BuildDeterministic(corpus, ascending: true);

        for (var i = 0; i < 200; i++)
        {
            Assert.That(churned.Remove(i * 7), Is.True);
            churned.Add(i * 7, corpus[i * 7]);
        }

        churned.Train();
        AssertIdenticalResults(pristine, churned, corpus);
    }

    [Test]
    public void Repeated_searches_against_one_index_return_identical_results()
    {
        var corpus = VectorCorpus.Clustered(DeterminismCount, DeterminismDimensions, clusters: 24, seed: 24);
        var index = BuildDeterministic(corpus, ascending: true);

        var first = new VectorSearchResult[10];
        var second = new VectorSearchResult[10];

        var foundFirst = index.Search(corpus[42], first);
        var foundSecond = index.Search(corpus[42], second);

        Assert.That(foundSecond, Is.EqualTo(foundFirst));
        Assert.That(second, Is.EqualTo(first));
    }

    [Test]
    public void A_different_seed_still_produces_a_usable_index()
    {
        var corpus = VectorCorpus.Clustered(DeterminismCount, DeterminismDimensions, clusters: 24, seed: 25);

        var index = BuildDeterministic(corpus, ascending: true, seed: 0xDEADBEEFUL);

        Assert.That(index.State, Is.EqualTo(VectorIndexState.Ready));

        Span<VectorSearchResult> results = stackalloc VectorSearchResult[10];
        var found = index.Search(corpus[3], results);

        Assert.That(found, Is.EqualTo(10));
        Assert.That(results[0].Key, Is.EqualTo(3));
    }

    [Test]
    public void The_exhaustive_path_is_insertion_order_independent_too()
    {
        var corpus = VectorCorpus.Clustered(200, DeterminismDimensions, clusters: 8, seed: 26);

        var ascending = new VectorIndex(new VectorIndexOptions { Dimensions = DeterminismDimensions });
        var descending = new VectorIndex(new VectorIndexOptions { Dimensions = DeterminismDimensions });

        for (var i = 0; i < corpus.Length; i++)
        {
            ascending.Add(i, corpus[i]);
        }

        for (var i = corpus.Length - 1; i >= 0; i--)
        {
            descending.Add(i, corpus[i]);
        }

        var left = new VectorSearchResult[10];
        var right = new VectorSearchResult[10];
        for (var q = 0; q < 20; q++)
        {
            var foundLeft = ascending.Search(corpus[q], left);
            var foundRight = descending.Search(corpus[q], right);

            Assert.That(foundRight, Is.EqualTo(foundLeft));
            Assert.That(right, Is.EqualTo(left));
        }
    }

    private static VectorIndex BuildDeterministic(
        float[][] corpus, bool ascending, ulong seed = 0x9E3779B97F4A7C15UL)
    {
        var index = new VectorIndex(new VectorIndexOptions
        {
            Dimensions = DeterminismDimensions,
            PartitionCount = 24,
            Probes = 6,
            MinimumTrainingCount = 16,
            TrainingSampleSize = 1_024,
            Seed = seed,
        });

        index.EnsureCapacity(corpus.Length);
        if (ascending)
        {
            for (var i = 0; i < corpus.Length; i++)
            {
                index.Add(i, corpus[i]);
            }
        }
        else
        {
            for (var i = corpus.Length - 1; i >= 0; i--)
            {
                index.Add(i, corpus[i]);
            }
        }

        index.Train();
        return index;
    }

    private static void AssertIdenticalResults(VectorIndex left, VectorIndex right, float[][] queries)
    {
        var leftResults = new VectorSearchResult[10];
        var rightResults = new VectorSearchResult[10];

        for (var q = 0; q < 40; q++)
        {
            var foundLeft = left.Search(queries[q], leftResults, out var leftMode);
            var foundRight = right.Search(queries[q], rightResults, out var rightMode);

            Assert.That(rightMode, Is.EqualTo(leftMode));
            Assert.That(foundRight, Is.EqualTo(foundLeft), $"Result count diverged on query {q}.");
            Assert.That(rightResults, Is.EqualTo(leftResults), $"Result set diverged on query {q}.");
        }
    }
}
