using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests;

/// <summary>
/// Value-equality regression tests for <see cref="RepoContextSnapshotRecord"/>,
/// the portable snapshot tuple. Its <see cref="RepoContextSnapshotRecord.Value"/>
/// and <see cref="RepoContextSnapshotRecord.Vector"/> byte arrays were compared
/// by reference under the compiler-generated record equality, so two structurally
/// identical records - and, in particular, a record and its post-serialization
/// self - never compared equal, defeating any dedup or round-trip verification
/// framed as record equality.
/// </summary>
[TestFixture]
public sealed class RepoContextSnapshotRecordEqualityTests
{
    private static RepoContextSnapshotRecord Sample(byte[]? value = null, byte[]? vector = null, string? space = "space-a") => new()
    {
        Key = "repo/x/file/a.cs",
        Value = value ?? [1, 2, 3],
        Vector = vector ?? [9, 8, 7],
        EmbeddingSpace = space,
    };

    [Test]
    public void Equal_across_distinct_arrays()
    {
        var a = Sample([1, 2, 3], [9, 8, 7]);
        var b = Sample([1, 2, 3], [9, 8, 7]);

        Assert.Multiple(() =>
        {
            Assert.That(ReferenceEquals(a.Value, b.Value), Is.False);
            Assert.That(ReferenceEquals(a.Vector, b.Vector), Is.False);
            Assert.That(a.Equals(b), Is.True);
            Assert.That(a == b, Is.True);
            Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
        });
    }

    [Test]
    public void Not_equal_when_value_bytes_differ()
    {
        var a = Sample();
        var b = a with { Value = [4, 5] };

        Assert.That(a.Equals(b), Is.False);
    }

    [Test]
    public void Not_equal_when_vector_bytes_differ()
    {
        var a = Sample();
        var b = a with { Vector = [1] };

        Assert.That(a.Equals(b), Is.False);
    }

    [Test]
    public void Not_equal_when_a_scalar_field_differs()
    {
        var a = Sample();

        Assert.Multiple(() =>
        {
            Assert.That(a.Equals(a with { Key = "repo/x/file/b.cs" }), Is.False);
            Assert.That(a.Equals(a with { EmbeddingSpace = "space-b" }), Is.False);
            Assert.That(a.Equals(a with { EmbeddingSpace = null }), Is.False);
        });
    }

    [Test]
    public void Equal_when_vector_null_on_both_sides()
    {
        var a = Sample() with { Vector = null };
        var b = Sample() with { Vector = null };

        Assert.Multiple(() =>
        {
            Assert.That(a.Equals(b), Is.True);
            Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
        });
    }

    [Test]
    public void Serialization_round_trip_preserves_value_equality()
    {
        var record = Sample();

        using var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var serializer = services.GetRequiredService<Serializer<RepoContextSnapshotRecord>>();
        var decoded = serializer.Deserialize(serializer.SerializeToArray(record));

        Assert.Multiple(() =>
        {
            Assert.That(ReferenceEquals(decoded.Value, record.Value), Is.False);
            Assert.That(decoded.Equals(record), Is.True);
            Assert.That(decoded.GetHashCode(), Is.EqualTo(record.GetHashCode()));
        });
    }
}
