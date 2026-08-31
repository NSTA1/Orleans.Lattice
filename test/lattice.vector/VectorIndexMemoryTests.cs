namespace Orleans.Lattice.Vector.Tests;

/// <summary>
/// Unit tests for the index's memory accounting.
/// </summary>
[TestFixture]
public sealed class VectorIndexMemoryTests
{
    [Test]
    public void The_per_slot_side_cost_is_the_sum_of_its_parts()
    {
        Assert.That(VectorIndexMemory.SideBytesPerSlot, Is.EqualTo(sizeof(float) + sizeof(long)));
    }

    [Test]
    public void An_empty_shape_costs_nothing()
    {
        Assert.That(VectorIndexMemory.Bytes(0, 0, 0), Is.EqualTo(0L));
    }

    [Test]
    public void The_total_is_the_vector_blocks_plus_the_side_arrays_plus_the_centroids()
    {
        var bytes = VectorIndexMemory.Bytes(capacity: 100, dimensions: 8, partitionCount: 10);

        var expected = (100L * 8 * sizeof(float))
            + (100L * VectorIndexMemory.SideBytesPerSlot)
            + (10L * 8 * sizeof(float));

        Assert.That(bytes, Is.EqualTo(expected));
    }

    [Test]
    public void A_large_shape_does_not_overflow_a_signed_thirty_two_bit_total()
    {
        var bytes = VectorIndexMemory.Bytes(capacity: 1_000_000, dimensions: 768, partitionCount: 1_000);

        Assert.That(bytes, Is.GreaterThan(int.MaxValue));
    }

    [Test]
    public void It_rejects_a_negative_shape()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => VectorIndexMemory.Bytes(-1, 8, 4));
        Assert.Throws<ArgumentOutOfRangeException>(() => VectorIndexMemory.Bytes(8, -1, 4));
        Assert.Throws<ArgumentOutOfRangeException>(() => VectorIndexMemory.Bytes(8, 4, -1));
    }
}
