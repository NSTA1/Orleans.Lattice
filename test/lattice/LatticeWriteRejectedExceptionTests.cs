namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeWriteRejectedException"/>: the constructor
/// overloads, the composed message, and the context it carries back to the
/// caller when the write interceptor rejects a value.
/// </summary>
[TestFixture]
public class LatticeWriteRejectedExceptionTests
{
    [Test]
    public void Context_constructor_populates_every_member()
    {
        var ex = new LatticeWriteRejectedException("orders", LatticeOperation.Write, "k1", "schema mismatch");

        Assert.Multiple(() =>
        {
            Assert.That(ex.TreeId, Is.EqualTo("orders"));
            Assert.That(ex.Operation, Is.EqualTo(LatticeOperation.Write));
            Assert.That(ex.Key, Is.EqualTo("k1"));
            Assert.That(ex.Reason, Is.EqualTo("schema mismatch"));
        });
    }

    [Test]
    public void Context_constructor_message_includes_key_tree_and_reason()
    {
        var ex = new LatticeWriteRejectedException("orders", LatticeOperation.Write, "k1", "schema mismatch");

        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Does.Contain("k1"));
            Assert.That(ex.Message, Does.Contain("orders"));
            Assert.That(ex.Message, Does.Contain("schema mismatch"));
        });
    }

    [Test]
    public void Is_an_invalid_operation_exception()
    {
        var ex = new LatticeWriteRejectedException("orders", LatticeOperation.Write, "k1", "reason");

        Assert.That(ex, Is.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public void Parameterless_constructor_yields_empty_context()
    {
        var ex = new LatticeWriteRejectedException();

        Assert.Multiple(() =>
        {
            Assert.That(ex.TreeId, Is.Empty);
            Assert.That(ex.Key, Is.Empty);
            Assert.That(ex.Reason, Is.Empty);
        });
    }

    [Test]
    public void Message_constructor_sets_the_message_and_empty_context()
    {
        var ex = new LatticeWriteRejectedException("boom");

        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Is.EqualTo("boom"));
            Assert.That(ex.TreeId, Is.Empty);
            Assert.That(ex.Key, Is.Empty);
            Assert.That(ex.Reason, Is.Empty);
        });
    }

    [Test]
    public void Message_and_inner_constructor_wraps_the_inner_exception()
    {
        var inner = new InvalidOperationException("inner");

        var ex = new LatticeWriteRejectedException("boom", inner);

        Assert.Multiple(() =>
        {
            Assert.That(ex.Message, Is.EqualTo("boom"));
            Assert.That(ex.InnerException, Is.SameAs(inner));
        });
    }

    [Test]
    public void Context_constructor_rejects_a_null_tree_id()
    {
        Assert.That(() => new LatticeWriteRejectedException(null!, LatticeOperation.Write, "k1", "reason"),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Context_constructor_rejects_a_null_key()
    {
        Assert.That(() => new LatticeWriteRejectedException("orders", LatticeOperation.Write, null!, "reason"),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Context_constructor_rejects_a_null_reason()
    {
        Assert.That(() => new LatticeWriteRejectedException("orders", LatticeOperation.Write, "k1", null!),
            Throws.ArgumentNullException);
    }
}
