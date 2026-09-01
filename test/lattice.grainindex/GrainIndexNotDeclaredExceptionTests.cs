namespace Orleans.Lattice.GrainIndex.Tests;

/// <summary>
/// Covers <see cref="GrainIndexNotDeclaredException"/>: the failure an
/// administrative call raises when it is asked about an index this silo does
/// not declare.
/// </summary>
[TestFixture]
public sealed class GrainIndexNotDeclaredExceptionTests
{
    [Test]
    public void The_parameterless_constructor_leaves_the_context_empty()
    {
        var exception = new GrainIndexNotDeclaredException();

        Assert.Multiple(() =>
        {
            Assert.That(exception.IndexName, Is.Empty);
            Assert.That(exception.DeclaredIndexes, Is.Empty);
            Assert.That(exception.Message, Is.Not.Null.And.Not.Empty);
        });
    }

    [Test]
    public void The_message_constructor_keeps_the_message_and_leaves_the_context_empty()
    {
        var exception = new GrainIndexNotDeclaredException("no such index");

        Assert.Multiple(() =>
        {
            Assert.That(exception.Message, Is.EqualTo("no such index"));
            Assert.That(exception.IndexName, Is.Empty);
            Assert.That(exception.DeclaredIndexes, Is.Empty);
        });
    }

    [Test]
    public void The_inner_exception_constructor_keeps_both()
    {
        var inner = new InvalidOperationException("cause");
        var exception = new GrainIndexNotDeclaredException("no such index", inner);

        Assert.Multiple(() =>
        {
            Assert.That(exception.Message, Is.EqualTo("no such index"));
            Assert.That(exception.InnerException, Is.SameAs(inner));
        });
    }

    [Test]
    public void The_context_constructor_names_the_index_and_the_declared_set()
    {
        string[] declared = ["users", "orders"];
        var exception = new GrainIndexNotDeclaredException("usres", declared);

        Assert.Multiple(() =>
        {
            Assert.That(exception.IndexName, Is.EqualTo("usres"));
            Assert.That(exception.DeclaredIndexes, Is.EqualTo(declared));
            Assert.That(exception.Message, Does.Contain("usres"));
            Assert.That(exception.Message, Does.Contain("users, orders"));
        });
    }

    [Test]
    public void A_silo_declaring_nothing_says_so_rather_than_listing_an_empty_set()
    {
        var exception = new GrainIndexNotDeclaredException("users", []);

        Assert.That(exception.Message, Does.Contain("(none)"));
    }

    [Test]
    public void The_context_constructor_rejects_a_null_index_name() =>
        Assert.That(
            () => new GrainIndexNotDeclaredException(null!, []),
            Throws.ArgumentNullException);

    [Test]
    public void The_context_constructor_rejects_a_null_declared_set() =>
        Assert.That(
            () => new GrainIndexNotDeclaredException("users", (IReadOnlyList<string>)null!),
            Throws.ArgumentNullException);

    [Test]
    public void It_derives_directly_from_exception_so_orleans_can_deep_copy_it() =>
        Assert.That(typeof(GrainIndexNotDeclaredException).BaseType, Is.EqualTo(typeof(Exception)));
}
