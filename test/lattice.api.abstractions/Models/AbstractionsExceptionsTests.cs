using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.TreeAdmin;

namespace Orleans.Lattice.Api.Abstractions.Tests;

/// <summary>
/// Exercises the hand-written constructors of the abstractions assembly's typed
/// exceptions. The serialization round-trip fixture instantiates these via
/// <see cref="System.Runtime.CompilerServices.RuntimeHelpers.GetUninitializedObject(System.Type)"/>,
/// so their message-composing constructors and property assignments are never
/// otherwise executed.
/// </summary>
[TestFixture]
public class AbstractionsExceptionsTests
{
    [Test]
    public void LatticeStateCursorExpiredException_default_ctor_sets_descriptive_message()
    {
        var ex = new LatticeStateCursorExpiredException();

        Assert.That(ex.Message, Does.Contain("resume cursor"));
        Assert.That(ex.InnerException, Is.Null);
    }

    [Test]
    public void LatticeStateCursorExpiredException_message_ctor_preserves_message()
    {
        var ex = new LatticeStateCursorExpiredException("custom message");

        Assert.That(ex.Message, Is.EqualTo("custom message"));
    }

    [Test]
    public void LatticeStateCursorExpiredException_inner_ctor_preserves_message_and_inner()
    {
        var inner = new InvalidOperationException("boom");
        var ex = new LatticeStateCursorExpiredException("wrapped", inner);

        Assert.That(ex.Message, Is.EqualTo("wrapped"));
        Assert.That(ex.InnerException, Is.SameAs(inner));
    }

    [Test]
    public void TreeNotEmptyException_treeId_ctor_composes_message_and_captures_tree()
    {
        var ex = new TreeNotEmptyException("orders");

        Assert.That(ex.TreeId, Is.EqualTo("orders"));
        Assert.That(ex.Message, Does.Contain("orders"));
        Assert.That(ex.Message, Does.Contain("not empty"));
    }

    [Test]
    public void TreeNotEmptyException_message_ctor_uses_custom_message()
    {
        var ex = new TreeNotEmptyException("orders", "explicit text");

        Assert.That(ex.TreeId, Is.EqualTo("orders"));
        Assert.That(ex.Message, Is.EqualTo("explicit text"));
    }

    [Test]
    public void BulkLoadOrderException_ctor_captures_all_context_and_composes_message()
    {
        var ex = new BulkLoadOrderException("orders", 7, "b", "c");

        Assert.That(ex.TreeId, Is.EqualTo("orders"));
        Assert.That(ex.ChunkIndex, Is.EqualTo(7));
        Assert.That(ex.OffendingKey, Is.EqualTo("b"));
        Assert.That(ex.PrecedingKey, Is.EqualTo("c"));
        Assert.That(ex.Message, Does.Contain("chunk 7"));
        Assert.That(ex.Message, Does.Contain("orders"));
        Assert.That(ex.Message, Does.Contain("'b'"));
        Assert.That(ex.Message, Does.Contain("'c'"));
    }
}
