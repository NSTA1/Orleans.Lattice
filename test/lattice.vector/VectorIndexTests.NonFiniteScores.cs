namespace Orleans.Lattice.Vector.Tests;

public sealed partial class VectorIndexTests
{
    // A vector with a non-finite component scores NaN against every query: a NaN
    // norm under Cosine, and Infinity * 0 under DotProduct. NaN compares false
    // against every number, so before the ranking ordered it explicitly its rank
    // depended on the order the vectors were scanned in - offered first it pinned
    // rank 0 above every real match, offered last it evicted the k-th one - which
    // broke the documented total order and insertion-order independence.

    [TestCase(VectorDistanceMetric.Cosine)]
    [TestCase(VectorDistanceMetric.DotProduct)]
    public void A_vector_that_scores_NaN_never_outranks_a_real_match(VectorDistanceMetric metric)
    {
        var index = CreateIndex(metric: metric);
        index.Add(1, Vector(Dimensions, float.NaN, 1f));
        index.Add(2, Vector(Dimensions, 1f));
        index.Add(3, Vector(Dimensions, 0.5f, 0.5f));

        var results = new VectorSearchResult[3];
        var found = index.Search(Vector(Dimensions, 1f), results);

        Assert.Multiple(() =>
        {
            Assert.That(found, Is.EqualTo(3));
            Assert.That(results[0].Key, Is.EqualTo(2), "the exact match must rank first");
            Assert.That(results[1].Key, Is.EqualTo(3));
            Assert.That(results[2].Key, Is.EqualTo(1), "the unrankable vector must rank last");
            Assert.That(float.IsNaN(results[2].Score), Is.True, "the NaN score is reported, not replaced");
        });
    }

    [Test]
    public void A_vector_that_scores_NaN_does_not_evict_a_real_match_from_a_full_result_set()
    {
        var index = CreateIndex();
        index.Add(1, Vector(Dimensions, 1f));
        index.Add(2, Vector(Dimensions, float.PositiveInfinity, 1f));

        var results = new VectorSearchResult[1];
        var found = index.Search(Vector(Dimensions, 1f), results);

        Assert.Multiple(() =>
        {
            Assert.That(found, Is.EqualTo(1));
            Assert.That(results[0].Key, Is.EqualTo(1));
            Assert.That(results[0].Score, Is.EqualTo(1f).Within(1e-6f));
        });
    }

    [Test]
    public void Insertion_order_does_not_change_the_result_set_when_a_vector_scores_NaN()
    {
        var pairs = new (long Key, float[] Vector)[]
        {
            (10, Vector(Dimensions, float.NaN)),
            (11, Vector(Dimensions, 1f, 0.25f)),
            (12, Vector(Dimensions, float.NaN, 2f)),
            (13, Vector(Dimensions, 0.25f, 1f)),
            (14, Vector(Dimensions, 1f)),
        };

        var ascending = CreateIndex();
        foreach (var (key, vector) in pairs)
        {
            ascending.Add(key, vector);
        }

        var descending = CreateIndex();
        for (var i = pairs.Length - 1; i >= 0; i--)
        {
            descending.Add(pairs[i].Key, pairs[i].Vector);
        }

        foreach (var k in new[] { 1, 2, 4, 5 })
        {
            var left = new VectorSearchResult[k];
            var right = new VectorSearchResult[k];
            var foundLeft = ascending.Search(Vector(Dimensions, 1f), left);
            var foundRight = descending.Search(Vector(Dimensions, 1f), right);

            Assert.That(foundRight, Is.EqualTo(foundLeft), $"k={k}");
            Assert.That(right, Is.EqualTo(left), $"k={k}");
            Assert.That(left[0].Key, Is.EqualTo(14), $"k={k}: the exact match must rank first");
        }
    }

    [Test]
    public void A_NaN_query_ranks_every_vector_by_ascending_key()
    {
        var index = CreateIndex();
        index.Add(7, Vector(Dimensions, 1f));
        index.Add(3, Vector(Dimensions, 0f, 1f));
        index.Add(5, Vector(Dimensions, 1f, 1f));

        var results = new VectorSearchResult[3];
        var found = index.Search(Vector(Dimensions, float.NaN), results);

        Assert.Multiple(() =>
        {
            Assert.That(found, Is.EqualTo(3));
            Assert.That(results[0].Key, Is.EqualTo(3));
            Assert.That(results[1].Key, Is.EqualTo(5));
            Assert.That(results[2].Key, Is.EqualTo(7));
        });
    }
}
