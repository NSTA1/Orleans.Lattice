namespace Orleans.Lattice.Vector.Tests;

/// <summary>
/// Unit tests for the snapshot format exception's constructors.
/// </summary>
[TestFixture]
public sealed class VectorIndexFormatExceptionTests
{
    [Test]
    public void The_message_constructor_carries_the_message()
    {
        var exception = new VectorIndexFormatException("bad marker");

        Assert.That(exception.Message, Is.EqualTo("bad marker"));
        Assert.That(exception.InnerException, Is.Null);
    }

    [Test]
    public void The_inner_exception_constructor_carries_both()
    {
        var cause = new InvalidOperationException("cause");

        var exception = new VectorIndexFormatException("bad marker", cause);

        Assert.That(exception.Message, Is.EqualTo("bad marker"));
        Assert.That(exception.InnerException, Is.SameAs(cause));
    }

    [Test]
    public void It_derives_directly_from_Exception_so_no_hand_written_copier_is_needed()
    {
        Assert.That(typeof(VectorIndexFormatException).BaseType, Is.EqualTo(typeof(Exception)));
    }
}
