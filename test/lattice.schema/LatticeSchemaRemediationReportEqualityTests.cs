using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Value-equality regression tests for <see cref="LatticeSchemaRemediationReport"/>.
/// Its <see cref="LatticeSchemaRemediationReport.OffendingValuePreview"/> byte array
/// was compared by reference under the compiler-generated record-struct equality, so
/// two structurally identical aborted reports - including a report and its
/// post-serialization self - never compared equal.
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class LatticeSchemaRemediationReportEqualityTests
{
    private static LatticeSchemaRemediationReport Aborted(byte[] preview) =>
        LatticeSchemaRemediationReport.Aborted(
            scannedCount: 3, offendingKey: "k", reason: "bad", offendingValuePreview: preview, operationId: "op");

    [Test]
    public void Equal_when_preview_bytes_match_across_distinct_arrays()
    {
        var a = Aborted([1, 2, 3]);
        var b = Aborted([1, 2, 3]);

        Assert.Multiple(() =>
        {
            Assert.That(ReferenceEquals(a.OffendingValuePreview, b.OffendingValuePreview), Is.False);
            Assert.That(a.Equals(b), Is.True);
            Assert.That(a == b, Is.True);
            Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
        });
    }

    [Test]
    public void Not_equal_when_preview_bytes_differ()
    {
        Assert.That(Aborted([1, 2, 3]).Equals(Aborted([1, 2, 4])), Is.False);
    }

    [Test]
    public void Not_equal_when_a_scalar_field_differs()
    {
        var a = Aborted([1, 2, 3]);

        Assert.Multiple(() =>
        {
            Assert.That(a.Equals(a with { OffendingKey = "other" }), Is.False);
            Assert.That(a.Equals(a with { Reason = "other" }), Is.False);
            Assert.That(a.Equals(a with { ScannedCount = 99 }), Is.False);
            Assert.That(a.Equals(a with { OperationId = "other" }), Is.False);
            Assert.That(a.Equals(a with { DestinationTreeId = "other" }), Is.False);
        });
    }

    [Test]
    public void Equal_when_preview_is_null_on_both_sides()
    {
        var a = LatticeSchemaRemediationReport.Idle;
        var b = LatticeSchemaRemediationReport.Idle;

        Assert.Multiple(() =>
        {
            Assert.That(a.Equals(b), Is.True);
            Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
        });
    }

    [Test]
    public void Not_equal_when_one_preview_is_null()
    {
        var a = Aborted([1, 2, 3]);
        var b = a with { OffendingValuePreview = null };

        Assert.That(a.Equals(b), Is.False);
    }

    [Test]
    public void Serialization_round_trip_preserves_value_equality()
    {
        var value = Aborted([1, 2, 3]);

        using var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var serializer = services.GetRequiredService<Serializer<LatticeSchemaRemediationReport>>();
        var decoded = serializer.Deserialize(serializer.SerializeToArray(value));

        Assert.Multiple(() =>
        {
            Assert.That(ReferenceEquals(decoded.OffendingValuePreview, value.OffendingValuePreview), Is.False);
            Assert.That(decoded.Equals(value), Is.True);
            Assert.That(decoded.GetHashCode(), Is.EqualTo(value.GetHashCode()));
        });
    }
}
