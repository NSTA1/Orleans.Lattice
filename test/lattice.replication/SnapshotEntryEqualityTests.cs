using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Primitives;
using Orleans.Serialization;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Value-equality regression tests for <see cref="SnapshotEntry"/>. Its
/// <see cref="SnapshotEntry.Value"/> and <see cref="SnapshotEntry.Delta"/> byte
/// arrays were compared by reference under the compiler-generated record-struct
/// equality, so two entries built from independently allocated but
/// byte-identical payloads - including an entry and its post-serialization self -
/// never compared equal.
/// </summary>
[TestFixture]
public sealed class SnapshotEntryEqualityTests
{
    private static readonly HybridLogicalClock SampleClock = HybridLogicalClock.Tick(HybridLogicalClock.Zero);

    private static SnapshotEntry Sample(byte[]? value = null, byte[]? delta = null) => new()
    {
        Key = "tenant/key",
        Value = value ?? [1, 2, 3],
        Timestamp = SampleClock,
        IsPrepared = true,
        IsTombstone = false,
        TransactionId = new Guid("11111111-1111-1111-1111-111111111111"),
        SourceShardIndex = 2,
        AtomicBatchSize = 4,
        AtomicBatchIndex = 1,
        ExpiresAtTicks = 1234L,
        Delta = delta,
        Mode = LatticeMergeMode.LwwRegister,
    };

    [Test]
    public void Equal_when_all_fields_and_payload_bytes_match_across_distinct_arrays()
    {
        var a = Sample([7, 8, 9], [4, 5]);
        var b = Sample([7, 8, 9], [4, 5]);

        Assert.Multiple(() =>
        {
            Assert.That(ReferenceEquals(a.Value, b.Value), Is.False);
            Assert.That(ReferenceEquals(a.Delta, b.Delta), Is.False);
            Assert.That(a.Equals(b), Is.True);
            Assert.That(a == b, Is.True);
            Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
        });
    }

    [Test]
    public void Not_equal_when_value_bytes_differ()
    {
        var a = Sample([1, 2, 3]);
        var b = Sample([1, 2, 4]);

        Assert.That(a.Equals(b), Is.False);
    }

    [Test]
    public void Not_equal_when_delta_bytes_differ()
    {
        var a = Sample(delta: [1, 2, 3]);
        var b = Sample(delta: [1, 2, 4]);

        Assert.That(a.Equals(b), Is.False);
    }

    [Test]
    public void Not_equal_when_a_scalar_field_differs()
    {
        var a = Sample();

        Assert.Multiple(() =>
        {
            Assert.That(a.Equals(a with { Key = "tenant/other" }), Is.False);
            Assert.That(a.Equals(a with { IsPrepared = false }), Is.False);
            Assert.That(a.Equals(a with { IsTombstone = true }), Is.False);
            Assert.That(a.Equals(a with { TransactionId = Guid.Empty }), Is.False);
            Assert.That(a.Equals(a with { SourceShardIndex = 9 }), Is.False);
            Assert.That(a.Equals(a with { AtomicBatchSize = 9 }), Is.False);
            Assert.That(a.Equals(a with { AtomicBatchIndex = 9 }), Is.False);
            Assert.That(a.Equals(a with { ExpiresAtTicks = 5678L }), Is.False);
            Assert.That(a.Equals(a with { Mode = LatticeMergeMode.OrSet }), Is.False);
            Assert.That(a.Equals(a with { Timestamp = HybridLogicalClock.Tick(SampleClock) }), Is.False);
        });
    }

    [Test]
    public void Equal_when_value_is_empty_on_both_sides()
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
    public void Not_equal_when_only_one_delta_is_null()
    {
        var a = Sample(delta: [1, 2, 3]);
        var b = a with { Delta = null };

        Assert.That(a.Equals(b), Is.False);
    }

    [Test]
    public void Serialization_round_trip_preserves_value_equality()
    {
        var value = Sample(delta: [9, 9]);

        using var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var serializer = services.GetRequiredService<Serializer<SnapshotEntry>>();
        var decoded = serializer.Deserialize(serializer.SerializeToArray(value));

        Assert.Multiple(() =>
        {
            Assert.That(ReferenceEquals(decoded.Value, value.Value), Is.False);
            Assert.That(decoded.Equals(value), Is.True);
            Assert.That(decoded.GetHashCode(), Is.EqualTo(value.GetHashCode()));
        });
    }
}
