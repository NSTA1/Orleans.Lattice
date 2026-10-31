using System.Buffers.Binary;

namespace Orleans.Lattice.Vector.Tests;

[TestFixture]
public sealed class VectorIndexCentroidAssignmentTests
{
    [TestCase(VectorDistanceMetric.Cosine, 0f, 0f)]
    [TestCase(VectorDistanceMetric.Cosine, 2f, 1f)]
    [TestCase(VectorDistanceMetric.Cosine, -2f, 1f)]
    [TestCase(VectorDistanceMetric.DotProduct, 0f, 0f)]
    [TestCase(VectorDistanceMetric.DotProduct, 2f, 1f)]
    [TestCase(VectorDistanceMetric.DotProduct, -2f, 1f)]
    public void SelectPartitions_and_Add_rank_centroids_by_squared_distance(
        VectorDistanceMetric metric, float x, float y)
    {
        float[][] centroids = [[-0.5f, 0f], [0f, 0.5f], [1f, 0f]];
        var options = new VectorIndexOptions { Dimensions = 2, Metric = metric, Probes = 1 };
        var header = new VectorIndexHeader(
            VectorIndexFormat.Version, 2, metric, centroids.Length, 1, options.Seed, 0, 1, 1, 0);
        var index = VectorIndex.Restore(header, options);
        var chunk = new byte[VectorIndexFormat.ChunkHeaderSize + centroids.Length * 2 * sizeof(float)];
        BinaryPrimitives.WriteUInt32LittleEndian(chunk, VectorIndexFormat.ChunkMagic);
        BinaryPrimitives.WriteInt32LittleEndian(chunk.AsSpan(4), VectorIndexFormat.Version);
        BinaryPrimitives.WriteInt32LittleEndian(chunk.AsSpan(8), (int)VectorIndexChunkKind.Centroids);
        BinaryPrimitives.WriteInt32LittleEndian(chunk.AsSpan(20), centroids.Length);
        for (var partition = 0; partition < centroids.Length; partition++)
        {
            for (var dimension = 0; dimension < 2; dimension++)
            {
                BinaryPrimitives.WriteSingleLittleEndian(
                    chunk.AsSpan(VectorIndexFormat.ChunkHeaderSize + (partition * 2 + dimension) * sizeof(float)),
                    centroids[partition][dimension]);
            }
        }

        index.ApplyChunk(chunk);

        // A distance oracle, not a copy of the production dot-product affinity.
        var norm = Math.Sqrt((double)x * x + (double)y * y);
        var scale = metric == VectorDistanceMetric.Cosine && norm != 0 ? 1 / norm : 1;
        var expected = Enumerable.Range(0, centroids.Length)
            .OrderBy(partition =>
                Math.Pow(x * scale - centroids[partition][0], 2)
                + Math.Pow(y * scale - centroids[partition][1], 2))
            .ThenBy(partition => partition)
            .ToArray();
        float[] query = [x, y];
        var selected = new int[centroids.Length];

        Assert.That(index.SelectPartitions(query, selected), Is.EqualTo(centroids.Length));
        Assert.That(selected, Is.EqualTo(expected));

        index.Add(42, query);
        Assert.That(index.PartitionSize(expected[0]), Is.EqualTo(1));
        var results = new VectorSearchResult[1];
        Assert.That(index.Search(query, results), Is.EqualTo(1));
        Assert.That(results[0].Key, Is.EqualTo(42));
    }

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
