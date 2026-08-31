namespace Orleans.Lattice.Vector.Tests;

/// <summary>
/// Unit tests for the readiness snapshot value type and its derived members.
/// </summary>
[TestFixture]
public sealed class VectorIndexStatusTests
{
    private static VectorIndexStatus Sample(
        VectorIndexState state = VectorIndexState.Ready,
        int count = 1_000,
        int capacity = 1_024,
        int dimensions = 8,
        int partitionCount = 32) =>
        new(state, count, capacity, dimensions, VectorDistanceMetric.Cosine, partitionCount, 8, 42L);

    [Test]
    public void It_carries_every_field_it_was_constructed_with()
    {
        var status = Sample();

        Assert.That(status.State, Is.EqualTo(VectorIndexState.Ready));
        Assert.That(status.Count, Is.EqualTo(1_000));
        Assert.That(status.Capacity, Is.EqualTo(1_024));
        Assert.That(status.Dimensions, Is.EqualTo(8));
        Assert.That(status.Metric, Is.EqualTo(VectorDistanceMetric.Cosine));
        Assert.That(status.PartitionCount, Is.EqualTo(32));
        Assert.That(status.Probes, Is.EqualTo(8));
        Assert.That(status.Version, Is.EqualTo(42L));
    }

    [Test]
    public void IsReady_is_true_only_in_the_ready_state()
    {
        Assert.That(Sample(VectorIndexState.Ready).IsReady, Is.True);
        Assert.That(Sample(VectorIndexState.Building).IsReady, Is.False);
        Assert.That(Sample(VectorIndexState.Empty).IsReady, Is.False);
    }

    [Test]
    public void BytesPerVector_is_zero_for_an_empty_index()
    {
        Assert.That(Sample(VectorIndexState.Empty, count: 0, capacity: 0, partitionCount: 0).BytesPerVector,
            Is.EqualTo(0));
    }

    [Test]
    public void BytesPerVector_divides_the_backing_arrays_by_the_live_count()
    {
        var status = Sample(count: 1_024, capacity: 1_024, dimensions: 8, partitionCount: 32);

        var expected = VectorIndexMemory.Bytes(1_024, 8, 32) / 1_024;

        Assert.That(status.BytesPerVector, Is.EqualTo((int)expected));
    }

    [Test]
    public void Two_statuses_with_the_same_fields_are_equal()
    {
        Assert.That(Sample(), Is.EqualTo(Sample()));
        Assert.That(Sample(), Is.Not.EqualTo(Sample(count: 999)));
    }
}
