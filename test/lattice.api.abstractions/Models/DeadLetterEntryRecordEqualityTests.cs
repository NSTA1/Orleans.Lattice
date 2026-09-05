using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.State;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Abstractions.Tests;

/// <summary>
/// Value-equality regression tests for <see cref="DeadLetterEntryRecord"/>, the
/// strict-mode dead-letter entry surfaced by
/// <c>ILatticeStateQuery.ListDeadLettersAsync</c>. Its
/// <see cref="DeadLetterEntryRecord.ValuePreview"/> byte array was compared by
/// reference under the compiler-generated record equality, so two structurally
/// identical records - including a record and its post-serialization self - never
/// compared equal.
/// </summary>
[TestFixture]
public sealed class DeadLetterEntryRecordEqualityTests
{
    private static DeadLetterEntryRecord Sample(byte[]? valuePreview = null) => new()
    {
        Key = "k",
        ValuePreview = valuePreview ?? [1, 2, 3],
        ValueByteLength = 9,
        Reason = "schema mismatch",
        Source = DeadLetterSourceKind.LocalRejected,
        TimestampUtc = new DateTimeOffset(2026, 1, 2, 3, 4, 5, TimeSpan.Zero),
        PreviewTruncated = true,
    };

    [Test]
    public void Equal_across_distinct_arrays()
    {
        var a = Sample([1, 2, 3]);
        var b = Sample([1, 2, 3]);

        Assert.Multiple(() =>
        {
            Assert.That(ReferenceEquals(a.ValuePreview, b.ValuePreview), Is.False);
            Assert.That(a.Equals(b), Is.True);
            Assert.That(a == b, Is.True);
            Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
        });
    }

    [Test]
    public void Not_equal_when_value_preview_bytes_differ()
    {
        var a = Sample();
        var b = a with { ValuePreview = [9, 9] };

        Assert.That(a.Equals(b), Is.False);
    }

    [Test]
    public void Not_equal_when_a_scalar_field_differs()
    {
        var a = Sample();

        Assert.Multiple(() =>
        {
            Assert.That(a.Equals(a with { Key = "other" }), Is.False);
            Assert.That(a.Equals(a with { ValueByteLength = 10 }), Is.False);
            Assert.That(a.Equals(a with { Reason = "other" }), Is.False);
            Assert.That(a.Equals(a with { Source = DeadLetterSourceKind.Replication }), Is.False);
            Assert.That(a.Equals(a with { PreviewTruncated = false }), Is.False);
            Assert.That(a.Equals(a with { TimestampUtc = a.TimestampUtc.AddSeconds(1) }), Is.False);
        });
    }

    [Test]
    public void Equal_when_previews_empty_on_both_sides()
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
    public void Serialization_round_trip_preserves_value_equality()
    {
        var record = Sample();

        using var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var serializer = services.GetRequiredService<Serializer<DeadLetterEntryRecord>>();
        var decoded = serializer.Deserialize(serializer.SerializeToArray(record));

        Assert.Multiple(() =>
        {
            Assert.That(ReferenceEquals(decoded.ValuePreview, record.ValuePreview), Is.False);
            Assert.That(decoded.Equals(record), Is.True);
            Assert.That(decoded.GetHashCode(), Is.EqualTo(record.GetHashCode()));
        });
    }
}
