using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Primitives;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Abstractions.Tests;

/// <summary>
/// Value-equality regression tests for <see cref="EntryRecord"/>, the read-only
/// entry projection surfaced by the state inspection API. Its
/// <see cref="EntryRecord.ValuePreview"/> byte array and its
/// <see cref="EntryRecord.CurrentMembers"/> list were compared by reference under
/// the compiler-generated record equality, so two structurally identical records -
/// including a record and its post-serialization self - never compared equal. Its
/// siblings (<c>DataReadResult</c>, <c>DataEntry</c>, <c>DeadLetterEntryRecord</c>)
/// were already hardened; this record was missed by that sweep.
/// </summary>
[TestFixture]
public sealed class EntryRecordEqualityTests
{
    private static CrdtMemberValue Member(string element, string replica, long ordinal) => new()
    {
        Element = System.Text.Encoding.UTF8.GetBytes(element),
        ReplicaId = replica,
        Ordinal = ordinal,
    };

    private static EntryRecord Sample(byte[]? preview = null, IReadOnlyList<CrdtMemberValue>? members = null) => new()
    {
        Key = "k",
        ValuePreview = preview ?? [1, 2, 3],
        ValueLength = 3,
        Truncated = true,
        Hlc = new HybridLogicalClock { WallClockTicks = 9, Counter = 2 },
        IsTombstone = false,
        ExpiresAtTicks = 42,
        CrdtShape = "OrSet",
        CurrentMembers = members ?? [Member("apple", "eu", 1), Member("pear", "us", 2)],
        MergeMode = LatticeMergeMode.OrSet,
        Raw = false,
    };

    [Test]
    public void Equal_across_distinct_arrays_and_member_lists()
    {
        var a = Sample([1, 2, 3], [Member("apple", "eu", 1), Member("pear", "us", 2)]);
        var b = Sample([1, 2, 3], [Member("apple", "eu", 1), Member("pear", "us", 2)]);

        Assert.Multiple(() =>
        {
            Assert.That(ReferenceEquals(a.ValuePreview, b.ValuePreview), Is.False);
            Assert.That(ReferenceEquals(a.CurrentMembers, b.CurrentMembers), Is.False);
            Assert.That(a.Equals(b), Is.True);
            Assert.That(a == b, Is.True);
            Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
        });
    }

    [Test]
    public void Not_equal_when_preview_bytes_differ()
    {
        var a = Sample();
        var b = a with { ValuePreview = [9, 9] };

        Assert.That(a.Equals(b), Is.False);
    }

    [Test]
    public void Not_equal_when_a_member_element_differs()
    {
        var a = Sample(members: [Member("apple", "eu", 1)]);
        var b = Sample(members: [Member("grape", "eu", 1)]);

        Assert.That(a.Equals(b), Is.False);
    }

    [Test]
    public void Not_equal_when_a_scalar_field_differs()
    {
        var a = Sample();

        Assert.Multiple(() =>
        {
            Assert.That(a.Equals(a with { Key = "other" }), Is.False);
            Assert.That(a.Equals(a with { ValueLength = 7 }), Is.False);
            Assert.That(a.Equals(a with { Truncated = false }), Is.False);
            Assert.That(a.Equals(a with { IsTombstone = true }), Is.False);
            Assert.That(a.Equals(a with { ExpiresAtTicks = 0 }), Is.False);
            Assert.That(a.Equals(a with { CrdtShape = "PnCounter" }), Is.False);
            Assert.That(a.Equals(a with { MergeMode = LatticeMergeMode.PnCounter }), Is.False);
            Assert.That(a.Equals(a with { Raw = true }), Is.False);
        });
    }

    [Test]
    public void Equal_when_previews_and_members_empty_on_both_sides()
    {
        var a = Sample([], []);
        var b = Sample([], []);

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
        var serializer = services.GetRequiredService<Serializer<EntryRecord>>();
        var decoded = serializer.Deserialize(serializer.SerializeToArray(record));

        Assert.Multiple(() =>
        {
            Assert.That(ReferenceEquals(decoded.ValuePreview, record.ValuePreview), Is.False);
            Assert.That(decoded.Equals(record), Is.True);
            Assert.That(decoded.GetHashCode(), Is.EqualTo(record.GetHashCode()));
        });
    }
}
