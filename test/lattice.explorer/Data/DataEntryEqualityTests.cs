using Orleans.Lattice.Explorer.Core.Data;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Explorer.Tests.Data;

/// <summary>
/// Value-equality regression tests for <see cref="DataEntry"/>, the explorer's
/// read-only entry projection. Its <see cref="DataEntry.Value"/> byte array and
/// its <see cref="DataEntry.CurrentMembers"/> list were compared by reference
/// under the compiler-generated record equality, so two structurally identical
/// entries - including an entry and a rebuilt copy of itself - never compared
/// equal. Its state-API source records were hardened by an earlier sweep; this
/// explorer projection carrying the same byte payload was missed.
/// </summary>
[TestFixture]
public sealed class DataEntryEqualityTests
{
    private static DataCrdtMember Member(string text, string replica, long ordinal) => new()
    {
        ElementText = text,
        ElementFormat = ValueFormat.Text,
        ReplicaId = replica,
        Ordinal = ordinal,
    };

    private static DataEntry Sample(byte[]? value = null, IReadOnlyList<DataCrdtMember>? members = null) => new()
    {
        Key = "k",
        Value = value ?? [1, 2, 3],
        ValueLength = 3,
        Truncated = true,
        Hlc = new HybridLogicalClock { WallClockTicks = 9, Counter = 2 },
        IsTombstone = false,
        ExpiresAtTicks = 42,
        CrdtShape = "OrSet",
        CurrentMembers = members ?? [Member("apple", "eu", 1), Member("pear", "us", 2)],
    };

    [Test]
    public void Equal_across_distinct_arrays_and_member_lists()
    {
        var a = Sample([1, 2, 3], [Member("apple", "eu", 1), Member("pear", "us", 2)]);
        var b = Sample([1, 2, 3], [Member("apple", "eu", 1), Member("pear", "us", 2)]);

        Assert.Multiple(() =>
        {
            Assert.That(ReferenceEquals(a.Value, b.Value), Is.False);
            Assert.That(ReferenceEquals(a.CurrentMembers, b.CurrentMembers), Is.False);
            Assert.That(a.Equals(b), Is.True);
            Assert.That(a == b, Is.True);
            Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
        });
    }

    [Test]
    public void Not_equal_when_value_bytes_differ()
    {
        var a = Sample();
        var b = a with { Value = [9, 9] };

        Assert.That(a.Equals(b), Is.False);
    }

    [Test]
    public void Not_equal_when_a_member_differs()
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
            Assert.That(a.Equals(a with { Hlc = new HybridLogicalClock { WallClockTicks = 1, Counter = 1 } }), Is.False);
            Assert.That(a.Equals(a with { IsTombstone = true }), Is.False);
            Assert.That(a.Equals(a with { ExpiresAtTicks = 0 }), Is.False);
            Assert.That(a.Equals(a with { CrdtShape = "PnCounter" }), Is.False);
        });
    }

    [Test]
    public void Equal_when_value_and_members_empty_on_both_sides()
    {
        var a = Sample([], []);
        var b = Sample([], []);

        Assert.Multiple(() =>
        {
            Assert.That(a.Equals(b), Is.True);
            Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
        });
    }
}
