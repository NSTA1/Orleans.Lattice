namespace Orleans.Lattice.Tests.Internal;

/// <summary>
/// Covers <see cref="ByteArrayEquality"/>, the content equality the library's
/// value-equality overrides apply to their byte-array members.
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class ByteArrayEqualityTests
{
    [Test]
    public void ContentEquals_is_true_for_the_same_instance_and_for_two_nulls()
    {
        var bytes = new byte[] { 1, 2, 3 };

        Assert.Multiple(() =>
        {
            Assert.That(ByteArrayEquality.ContentEquals(bytes, bytes), Is.True);
            Assert.That(ByteArrayEquality.ContentEquals(null, null), Is.True);
        });
    }

    [Test]
    public void ContentEquals_compares_distinct_arrays_by_content()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ByteArrayEquality.ContentEquals([1, 2, 3], [1, 2, 3]), Is.True);
            Assert.That(ByteArrayEquality.ContentEquals([], []), Is.True);
            Assert.That(ByteArrayEquality.ContentEquals([1, 2, 3], [1, 2, 4]), Is.False);
            Assert.That(ByteArrayEquality.ContentEquals([1, 2], [1, 2, 3]), Is.False);
        });
    }

    [Test]
    public void ContentEquals_is_false_when_exactly_one_side_is_null()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ByteArrayEquality.ContentEquals(null, []), Is.False);
            Assert.That(ByteArrayEquality.ContentEquals([], null), Is.False);
        });
    }
}
