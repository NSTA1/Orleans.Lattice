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
    public void Exception_is_editor_browsable_never()
    {
        var attr = typeof(StaleTreeRoutingException)
            .GetCustomAttributes(typeof(System.ComponentModel.EditorBrowsableAttribute), inherit: false)
            .Cast<System.ComponentModel.EditorBrowsableAttribute>()
            .Single();
        Assert.That(attr.State, Is.EqualTo(System.ComponentModel.EditorBrowsableState.Never));
    }
}
