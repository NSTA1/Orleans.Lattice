using Orleans.Lattice.Explorer.Core.DeadLetter;

namespace Orleans.Lattice.Explorer.Tests.DeadLetter;

/// <summary>
/// Value-equality regression tests for <see cref="DeadLetterEntry"/>, the
/// explorer's read-only dead-letter projection. Its
/// <see cref="DeadLetterEntry.Value"/> byte array was compared by reference under
/// the compiler-generated record equality, so two structurally identical entries
/// - including an entry and a rebuilt copy of itself - never compared equal. Its
/// state-API source record was hardened by an earlier sweep; this explorer
/// projection carrying the same byte payload was missed.
/// </summary>
[TestFixture]
public sealed class DeadLetterEntryEqualityTests
{
    private static DeadLetterEntry Sample(byte[]? value = null) => new()
    {
        Key = "k",
        Value = value ?? [1, 2, 3],
        ValueByteLength = 3,
        Truncated = true,
        Reason = "bad",
        Source = DeadLetterSource.Restore,
        TimestampUtc = new DateTimeOffset(2026, 5, 6, 7, 8, 9, TimeSpan.Zero),
    };

    [Test]
    public void Equal_across_distinct_arrays()
    {
        var a = Sample([1, 2, 3]);
        var b = Sample([1, 2, 3]);

        Assert.Multiple(() =>
        {
            Assert.That(ReferenceEquals(a.Value, b.Value), Is.False);
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
    public void Not_equal_when_a_scalar_field_differs()
    {
        var a = Sample();

        Assert.Multiple(() =>
        {
            Assert.That(a.Equals(a with { Key = "other" }), Is.False);
            Assert.That(a.Equals(a with { ValueByteLength = 7 }), Is.False);
            Assert.That(a.Equals(a with { Truncated = false }), Is.False);
            Assert.That(a.Equals(a with { Reason = "other" }), Is.False);
            Assert.That(a.Equals(a with { Source = DeadLetterSource.Replication }), Is.False);
            Assert.That(a.Equals(a with { TimestampUtc = DateTimeOffset.UnixEpoch }), Is.False);
        });
    }

    [Test]
    public void Equal_when_value_empty_on_both_sides()
    {
        var a = Sample([]);
        var b = Sample([]);

        Assert.Multiple(() =>
        {
            Assert.That(a.Equals(b), Is.True);
            Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
        });
    }
}
