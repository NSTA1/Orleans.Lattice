namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeSchemaViolationException"/>: context carry
/// and message composition.
/// </summary>
public class LatticeSchemaViolationExceptionTests
{
    [Test]
    public void Context_constructor_retains_tree_key_and_reason()
    {
        var ex = new LatticeSchemaViolationException("orders", "k1", "not json");

        Assert.That(ex.TreeId, Is.EqualTo("orders"));
        Assert.That(ex.Key, Is.EqualTo("k1"));
        Assert.That(ex.Reason, Is.EqualTo("not json"));
        Assert.That(ex.Message, Does.Contain("orders").And.Contain("k1").And.Contain("not json"));
    }

    [Test]
    public void Is_invalid_operation_exception()
    {
        Assert.That(new LatticeSchemaViolationException("t", "k", "r"), Is.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public void Parameterless_constructor_yields_empty_context()
    {
        var ex = new LatticeSchemaViolationException();
        Assert.That(ex.TreeId, Is.Empty);
        Assert.That(ex.Key, Is.Empty);
        Assert.That(ex.Reason, Is.Empty);
    }

    [Test]
    public void Message_constructor_sets_message_and_empty_context()
    {
        var ex = new LatticeSchemaViolationException("boom");
        Assert.That(ex.Message, Is.EqualTo("boom"));
        Assert.That(ex.TreeId, Is.Empty);
    }

    [Test]
    public void Message_and_inner_exception_constructor_preserves_inner_and_empty_context()
    {
        var inner = new InvalidOperationException("inner");

        var ex = new LatticeSchemaViolationException("boom", inner);

        Assert.That(ex.Message, Is.EqualTo("boom"));
        Assert.That(ex.InnerException, Is.SameAs(inner));
        Assert.That(ex.TreeId, Is.Empty);
        Assert.That(ex.Key, Is.Empty);
        Assert.That(ex.Reason, Is.Empty);
    }

    [Test]
    public void Context_constructor_null_arguments_throw()
    {
        Assert.That(() => new LatticeSchemaViolationException(null!, "k", "r"), Throws.ArgumentNullException);
        Assert.That(() => new LatticeSchemaViolationException("t", null!, "r"), Throws.ArgumentNullException);
        Assert.That(() => new LatticeSchemaViolationException("t", "k", null!), Throws.ArgumentNullException);
    }
}
