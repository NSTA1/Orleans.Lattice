using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// Value-equality regression tests for <see cref="EntryRevision"/>. Its
/// <see cref="EntryRevision.ValuePreview"/> and <see cref="EntryRevision.Delta"/>
/// byte arrays were compared by reference under the compiler-generated
/// record-struct equality, so two structurally identical revisions - including a
/// revision and its post-serialization self - never compared equal. The
/// <see cref="EntryRevision.VectorClock"/> field is compared by its own equality
/// and is <see langword="null"/> on the history-view path exercised here.
/// </summary>
[TestFixture]
public sealed class EntryRevisionEqualityTests
{
    private static EntryRevision Sample() => new()
    {
        Hlc = new HybridLogicalClock { WallClockTicks = 77, Counter = 4 },
        Kind = HistoryRowKind.CrdtDelta,
        SourceKey = "k",
        OriginClusterId = "cluster-b",
        ValuePreview = [1, 2],
        ValueLength = 9,
        ValueTruncated = true,
        ValueHash = -7,
        Delta = [3, 4, 5],
        Mode = LatticeMergeMode.OrSet,
        RetentionShape = HistoryRetentionMode.Hybrid,
        EndKey = "z",
        VectorClock = null,
    };

    [Test]
    public void Equal_when_all_fields_and_array_bytes_match_across_distinct_arrays()
    {
        var a = Sample();
        var b = Sample();

        Assert.Multiple(() =>
        {
            Assert.That(ReferenceEquals(a.ValuePreview, b.ValuePreview), Is.False);
            Assert.That(ReferenceEquals(a.Delta, b.Delta), Is.False);
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
    public void Not_equal_when_delta_bytes_differ()
    {
        var a = Sample();
        var b = a with { Delta = [9, 9, 9] };

        Assert.That(a.Equals(b), Is.False);
    }

    [Test]
    public void Not_equal_when_a_scalar_field_differs()
    {
        var a = Sample();

        Assert.Multiple(() =>
        {
            Assert.That(a.Equals(a with { SourceKey = "other" }), Is.False);
            Assert.That(a.Equals(a with { ValueLength = 10 }), Is.False);
            Assert.That(a.Equals(a with { Kind = HistoryRowKind.Set }), Is.False);
        });
    }

    [Test]
    public void Equal_when_arrays_are_null_on_both_sides()
    {
        var a = Sample() with { ValuePreview = null, Delta = null };
        var b = Sample() with { ValuePreview = null, Delta = null };

        Assert.Multiple(() =>
        {
            Assert.That(a.Equals(b), Is.True);
            Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
        });
    }

    [Test]
    public void Not_equal_when_only_one_array_is_null()
    {
        var a = Sample();
        var b = a with { Delta = null };

        Assert.That(a.Equals(b), Is.False);
    }

    [Test]
    public void Serialization_round_trip_preserves_value_equality()
    {
        var revision = Sample();

        using var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var serializer = services.GetRequiredService<Serializer<EntryRevision>>();
        var decoded = serializer.Deserialize(serializer.SerializeToArray(revision));

        Assert.Multiple(() =>
        {
            Assert.That(decoded.Equals(revision), Is.True);
            Assert.That(decoded.GetHashCode(), Is.EqualTo(revision.GetHashCode()));
        });
    }
}
