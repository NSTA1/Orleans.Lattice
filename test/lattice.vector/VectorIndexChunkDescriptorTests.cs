namespace Orleans.Lattice.Vector.Tests;

/// <summary>
/// Unit tests for the chunk descriptor value type.
/// </summary>
[TestFixture]
public sealed class VectorIndexChunkDescriptorTests
{
    [Test]
    public void It_carries_every_field_it_was_constructed_with()
    {
        var descriptor = new VectorIndexChunkDescriptor(VectorIndexChunkKind.Vectors, 3, 1, 64, 4_120);

        Assert.That(descriptor.Kind, Is.EqualTo(VectorIndexChunkKind.Vectors));
        Assert.That(descriptor.PartitionId, Is.EqualTo(3));
        Assert.That(descriptor.Sequence, Is.EqualTo(1));
        Assert.That(descriptor.ItemCount, Is.EqualTo(64));
        Assert.That(descriptor.ByteCount, Is.EqualTo(4_120));
    }

    [Test]
    public void Two_descriptors_with_the_same_fields_are_equal()
    {
        var left = new VectorIndexChunkDescriptor(VectorIndexChunkKind.Centroids, 0, 0, 8, 100);
        var right = new VectorIndexChunkDescriptor(VectorIndexChunkKind.Centroids, 0, 0, 8, 100);

        Assert.That(right, Is.EqualTo(left));
        Assert.That(left with { Sequence = 1 }, Is.Not.EqualTo(right));
    }
}
