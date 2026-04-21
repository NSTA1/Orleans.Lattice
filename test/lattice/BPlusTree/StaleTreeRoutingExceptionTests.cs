using Orleans.Lattice;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Unit tests for <see cref="StaleTreeRoutingException"/> — mirrors the
/// StaleShardRoutingException test contract.
/// </summary>
public class StaleTreeRoutingExceptionTests
{
    [Test]
    public void Constructor_populates_all_properties()
    {
        var ex = new StaleTreeRoutingException("my-tree", "my-tree", "my-tree/resized/abc");

        Assert.That(ex.LogicalTreeId, Is.EqualTo("my-tree"));
        Assert.That(ex.StalePhysicalTreeId, Is.EqualTo("my-tree"));
        Assert.That(ex.DestinationPhysicalTreeId, Is.EqualTo("my-tree/resized/abc"));
    }

    [Test]
    public void Constructor_produces_informative_message_with_all_ids()
    {
        var ex = new StaleTreeRoutingException("logical", "physical-old", "physical-new");

        Assert.That(ex.Message, Does.Contain("logical"));
        Assert.That(ex.Message, Does.Contain("physical-old"));
        Assert.That(ex.Message, Does.Contain("physical-new"));
    }

    [Test]
    public void Parameterless_constructor_leaves_default_property_values()
    {
        var ex = new StaleTreeRoutingException();

        Assert.That(ex.LogicalTreeId, Is.EqualTo(""));
        Assert.That(ex.StalePhysicalTreeId, Is.EqualTo(""));
        Assert.That(ex.DestinationPhysicalTreeId, Is.EqualTo(""));
    }

    [Test]
    public void Exception_is_internal()
    {
        // After v3 visibility hardening the routing exceptions are internal-only
        // infrastructure: they surface across grain boundaries but are not part of
        // the supported public API surface.
        Assert.That(typeof(StaleTreeRoutingException).IsPublic, Is.False);
        Assert.That(typeof(StaleTreeRoutingException).IsNotPublic, Is.True);
    }
}
