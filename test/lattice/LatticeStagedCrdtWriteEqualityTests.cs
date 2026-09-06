namespace Orleans.Lattice.Tests;

/// <summary>
/// Value-equality regression tests for <see cref="LatticeStagedCrdtWrite"/>. Its
/// <see cref="LatticeStagedCrdtWrite.Value"/> and
/// <see cref="LatticeStagedCrdtWrite.Delta"/> byte arrays were compared by
/// reference under the compiler-generated record-struct equality, so two staged
/// writes built from independently allocated but byte-identical payloads never
/// compared equal.
/// </summary>
[TestFixture]
public sealed class LatticeStagedCrdtWriteEqualityTests
{
    private static LatticeStagedCrdtWrite Sample(string key = "k", byte[]? value = null, byte[]? delta = null) =>
        new(key, value ?? [1, 2, 3], delta ?? [4, 5, 6]);

    [Test]
    public void Equal_when_key_and_byte_payloads_match_across_distinct_arrays()
    {
        var a = Sample(value: [7, 8, 9], delta: [10, 11]);
        var b = Sample(value: [7, 8, 9], delta: [10, 11]);

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
    public void Not_equal_when_key_differs()
    {
        var a = Sample("k1");
        var b = Sample("k2");

        Assert.That(a.Equals(b), Is.False);
    }

    [Test]
    public void Not_equal_when_value_bytes_differ()
    {
        var a = Sample(value: [1, 2, 3]);
        var b = Sample(value: [1, 2, 4]);

        Assert.That(a.Equals(b), Is.False);
    }

    [Test]
    public void Not_equal_when_delta_bytes_differ()
    {
        var a = Sample(delta: [4, 5, 6]);
        var b = Sample(delta: [4, 5, 7]);

        Assert.That(a.Equals(b), Is.False);
    }

    [Test]
    public void Equal_when_byte_payloads_are_empty_on_both_sides()
    {
        var a = Sample(value: [], delta: []);
        var b = Sample(value: [], delta: []);

        Assert.Multiple(() =>
        {
            Assert.That(a.Equals(b), Is.True);
            Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
        });
    }
}
