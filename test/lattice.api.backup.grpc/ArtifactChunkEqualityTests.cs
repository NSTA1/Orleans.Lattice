using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Backup.Grpc.Tests;

/// <summary>
/// Value-equality regression tests for <see cref="ArtifactChunk"/>. Its
/// <see cref="ArtifactChunk.Data"/> byte array was compared by reference under
/// the compiler-generated record equality, so two chunks built from
/// independently allocated but byte-identical payloads - including a chunk and
/// its post-serialization self - never compared equal.
/// </summary>
[TestFixture]
public sealed class ArtifactChunkEqualityTests
{
    private static ArtifactChunk Sample(byte[]? data = null) => new() { Data = data ?? [1, 2, 3] };

    [Test]
    public void Equal_when_data_bytes_match_across_distinct_arrays()
    {
        var a = Sample([7, 8, 9]);
        var b = Sample([7, 8, 9]);

        Assert.Multiple(() =>
        {
            Assert.That(ReferenceEquals(a.Data, b.Data), Is.False);
            Assert.That(a.Equals(b), Is.True);
            Assert.That(a == b, Is.True);
            Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
        });
    }

    [Test]
    public void Not_equal_when_data_bytes_differ()
    {
        var a = Sample([1, 2, 3]);
        var b = Sample([1, 2, 4]);

        Assert.That(a.Equals(b), Is.False);
    }

    [Test]
    public void Equal_when_data_is_empty_on_both_sides()
    {
        var a = Sample([]);
        var b = Sample([]);

        Assert.Multiple(() =>
        {
            Assert.That(a.Equals(b), Is.True);
            Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
        });
    }

    [Test]
    public void Not_equal_to_null()
    {
        var a = Sample();

        Assert.That(a.Equals(null), Is.False);
    }

    [Test]
    public void Serialization_round_trip_preserves_value_equality()
    {
        var value = Sample();

        using var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var serializer = services.GetRequiredService<Serializer<ArtifactChunk>>();
        var decoded = serializer.Deserialize(serializer.SerializeToArray(value));

        Assert.Multiple(() =>
        {
            Assert.That(ReferenceEquals(decoded.Data, value.Data), Is.False);
            Assert.That(decoded.Equals(value), Is.True);
            Assert.That(decoded.GetHashCode(), Is.EqualTo(value.GetHashCode()));
        });
    }
}
