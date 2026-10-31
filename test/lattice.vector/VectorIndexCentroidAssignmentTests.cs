namespace Orleans.Lattice.Vector.Tests;

[TestFixture]
public sealed class VectorIndexCentroidAssignmentTests
{
    [TestCase(VectorDistanceMetric.Cosine, 0f)]
    [TestCase(VectorDistanceMetric.Cosine, 2f)]
    [TestCase(VectorDistanceMetric.DotProduct, 0f)]
    [TestCase(VectorDistanceMetric.DotProduct, 2f)]
    public void Train_and_Add_equal_centroid_affinities_choose_the_first_partition(
        VectorDistanceMetric metric, float component)
    {
        var index = new VectorIndex(new VectorIndexOptions
        {
            Dimensions = 2,
            Metric = metric,
            MinimumTrainingCount = 4,
            PartitionCount = 4,
            Probes = 4,
            TrainingSampleSize = 8,
        });
        float[] vector = [component, 0f];
        for (var key = 0; key < 8; key++)
        {
            index.Add(key, vector);
        }

        Assert.That(index.Train(), Is.True);
        Assert.That(index.PartitionSize(0), Is.EqualTo(8));
        for (var partition = 1; partition < index.PartitionCount; partition++)
        {
            Assert.That(index.PartitionSize(partition), Is.Zero);
        }

        var selected = new int[4];
        Assert.That(index.SelectPartitions(vector, selected), Is.EqualTo(4));
        Assert.That(selected, Is.EqualTo(new[] { 0, 1, 2, 3 }));

        index.Add(8, vector);
        Assert.That(index.PartitionSize(0), Is.EqualTo(9));

        var results = new VectorSearchResult[9];
        Assert.That(index.Search(vector, results, out var mode), Is.EqualTo(9));
        Assert.That(mode, Is.EqualTo(VectorSearchMode.Approximate));
        Assert.That(results.Select(result => result.Key), Is.EqualTo(Enumerable.Range(0, 9).Select(key => (long)key)));
    }
}
