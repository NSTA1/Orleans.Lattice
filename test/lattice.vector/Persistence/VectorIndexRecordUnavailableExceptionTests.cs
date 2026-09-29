using Orleans.Lattice.Vector.Persistence;

namespace Orleans.Lattice.Vector.Tests.Persistence;

[TestFixture]
public sealed class VectorIndexRecordUnavailableExceptionTests
{
    [Test]
    public void Message_constructor_carries_the_message()
    {
        var exception = new VectorIndexRecordUnavailableException("record missing from one read");

        Assert.Multiple(() =>
        {
            Assert.That(exception.Message, Is.EqualTo("record missing from one read"));
            Assert.That(exception.InnerException, Is.Null);
        });
    }

    [Test]
    public void Inner_exception_constructor_carries_the_cause()
    {
        var cause = new TimeoutException("store did not answer");

        var exception = new VectorIndexRecordUnavailableException("record missing from one read", cause);

        Assert.Multiple(() =>
        {
            Assert.That(exception.Message, Is.EqualTo("record missing from one read"));
            Assert.That(exception.InnerException, Is.SameAs(cause));
        });
    }

    [Test]
    public void Derives_directly_from_exception()
    {
        // A consumer that later makes it serializable must not need a hand-written
        // deep copier, which only a direct System.Exception base guarantees.
        Assert.That(typeof(VectorIndexRecordUnavailableException).BaseType, Is.EqualTo(typeof(Exception)));
    }
}
