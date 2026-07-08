using Orleans.Lattice;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for the retryable <see cref="LatticeWriteFencedException"/> raised
/// when a mutation targets a tree that is write-fenced for a cross-cluster saga
/// cutover. Verifies the attribution slots, the retryable-family base type, and
/// that it is distinct from its sibling back-pressure exceptions.
/// </summary>
[TestFixture]
public class LatticeWriteFencedExceptionTests
{
    [Test]
    public void Attributed_ctor_populates_tree_and_saga_slots()
    {
        var ex = new LatticeWriteFencedException("fenced", "orders", "saga-7");

        Assert.That(ex.Message, Is.EqualTo("fenced"));
        Assert.That(ex.TreeId, Is.EqualTo("orders"));
        Assert.That(ex.SagaId, Is.EqualTo("saga-7"));
    }

    [Test]
    public void Parameterless_ctor_leaves_attribution_empty()
    {
        var ex = new LatticeWriteFencedException();

        Assert.That(ex.TreeId, Is.EqualTo(string.Empty));
        Assert.That(ex.SagaId, Is.EqualTo(string.Empty));
    }

    [Test]
    public void Message_ctor_leaves_attribution_empty()
    {
        var ex = new LatticeWriteFencedException("boom");

        Assert.That(ex.Message, Is.EqualTo("boom"));
        Assert.That(ex.TreeId, Is.EqualTo(string.Empty));
        Assert.That(ex.SagaId, Is.EqualTo(string.Empty));
    }

    [Test]
    public void Inner_exception_ctor_wraps_cause()
    {
        var inner = new InvalidOperationException("cause");
        var ex = new LatticeWriteFencedException("boom", inner);

        Assert.That(ex.InnerException, Is.SameAs(inner));
    }

    [Test]
    public void Attributed_ctor_rejects_null_tree_id()
    {
        Assert.That(
            () => new LatticeWriteFencedException("m", null!, "saga"),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void Attributed_ctor_rejects_null_saga_id()
    {
        Assert.That(
            () => new LatticeWriteFencedException("m", "tree", null!),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void Is_a_retryable_invalid_operation_exception()
    {
        var ex = new LatticeWriteFencedException("m", "t", "s");

        // Shares the retryable back-pressure base so existing catch handlers
        // that match InvalidOperationException continue to absorb it.
        Assert.That(ex, Is.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public void Is_distinct_from_saturated_exception()
    {
        var fenced = new LatticeWriteFencedException("m", "t", "s");

        Assert.That(fenced, Is.Not.InstanceOf<LatticeSaturatedException>());
    }
}
