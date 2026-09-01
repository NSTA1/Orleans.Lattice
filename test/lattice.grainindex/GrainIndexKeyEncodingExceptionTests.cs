namespace Orleans.Lattice.GrainIndex.Tests;

/// <summary>
/// Covers <see cref="GrainIndexKeyEncodingException"/>: every constructor, the
/// context it carries, and the shape of the message a production throw site
/// produces.
/// </summary>
[TestFixture]
public sealed class GrainIndexKeyEncodingExceptionTests
{
    [Test]
    public void Parameterless_constructor_leaves_the_context_empty()
    {
        var exception = new GrainIndexKeyEncodingException();

        Assert.Multiple(() =>
        {
            Assert.That(exception.GrainInterfaceTypeName, Is.Empty);
            Assert.That(exception.GrainKey, Is.Empty);
        });
    }

    [Test]
    public void Message_constructor_keeps_the_message_and_leaves_the_context_empty()
    {
        var exception = new GrainIndexKeyEncodingException("boom");

        Assert.Multiple(() =>
        {
            Assert.That(exception.Message, Is.EqualTo("boom"));
            Assert.That(exception.GrainInterfaceTypeName, Is.Empty);
            Assert.That(exception.GrainKey, Is.Empty);
        });
    }

    [Test]
    public void Message_and_inner_constructor_keeps_both()
    {
        var inner = new InvalidOperationException("cause");

        var exception = new GrainIndexKeyEncodingException("boom", inner);

        Assert.Multiple(() =>
        {
            Assert.That(exception.Message, Is.EqualTo("boom"));
            Assert.That(exception.InnerException, Is.SameAs(inner));
        });
    }

    [Test]
    public void Context_constructor_carries_the_grain_type_the_key_and_the_reason()
    {
        var exception = new GrainIndexKeyEncodingException("MyApp.IUserGrain", "u-1", "Because.");

        Assert.Multiple(() =>
        {
            Assert.That(exception.GrainInterfaceTypeName, Is.EqualTo("MyApp.IUserGrain"));
            Assert.That(exception.GrainKey, Is.EqualTo("u-1"));
            Assert.That(exception.Message, Does.Contain("MyApp.IUserGrain"));
            Assert.That(exception.Message, Does.Contain("u-1"));
            Assert.That(exception.Message, Does.Contain("Because."));
        });
    }

    [TestCase(null, "u-1", "why")]
    [TestCase("MyApp.IUserGrain", null, "why")]
    [TestCase("MyApp.IUserGrain", "u-1", null)]
    public void Context_constructor_rejects_a_null_argument(string? grainType, string? key, string? reason) =>
        Assert.That(
            () => new GrainIndexKeyEncodingException(grainType!, key!, reason!),
            Throws.ArgumentNullException);

    [Test]
    public void Derives_directly_from_exception_so_orleans_can_deep_copy_it_same_silo() =>
        Assert.That(typeof(GrainIndexKeyEncodingException).BaseType, Is.EqualTo(typeof(Exception)),
            "A [GenerateSerializer] exception deriving from a BCL exception subclass fails a "
            + "co-located deep copy with an opaque KeyNotFoundException unless it ships a copier. "
            + "Deriving directly from Exception avoids the whole problem.");
}
