namespace Orleans.Lattice.Tests.Views;

/// <summary>Unit tests for <see cref="LatticeViewOptions"/> and <see cref="LatticeViewDefinition"/>.</summary>
[TestFixture]
public class LatticeViewConfigurationTests
{
    [Test]
    public void Options_defaults_are_batch_size_256_and_50ms_window()
    {
        var options = new LatticeViewOptions();

        Assert.That(options.BatchSize, Is.EqualTo(256));
        Assert.That(options.CoalesceWindow, Is.EqualTo(TimeSpan.FromMilliseconds(50)));
    }

    [Test]
    public void Definition_binds_name_and_projection()
    {
        var projection = new PredicateLatticeViewProjection();
        var definition = new LatticeViewDefinition("orders-open", projection);

        Assert.That(definition.ViewName, Is.EqualTo("orders-open"));
        Assert.That(definition.Projection, Is.SameAs(projection));
    }

    [Test]
    public void Definition_null_name_throws()
    {
        Assert.That(
            () => new LatticeViewDefinition(null!, new PredicateLatticeViewProjection()),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Definition_empty_name_throws()
    {
        Assert.That(
            () => new LatticeViewDefinition(string.Empty, new PredicateLatticeViewProjection()),
            Throws.ArgumentException);
    }

    [Test]
    public void Definition_null_projection_throws()
    {
        Assert.That(
            () => new LatticeViewDefinition("v", (ILatticeViewProjection)null!),
            Throws.ArgumentNullException);
    }
}
