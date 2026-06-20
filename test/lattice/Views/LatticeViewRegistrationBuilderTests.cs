namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// Unit tests for <see cref="LatticeViewRegistrationBuilder"/>, covering the
/// view-on-view guard: a startup-declared view's source must be a directly-writable
/// tree, never another view's <c>view-*</c> tree.
/// </summary>
[TestFixture]
public class LatticeViewRegistrationBuilderTests
{
    private static ILatticeViewProjection Filter() => new PredicateLatticeViewProjection();

    private static ILatticeAggregationProjection Aggregation() =>
        new AggregationLatticeViewProjection(AggregationKind.Count, _ => "all", "v1");

    [Test]
    public void AddView_accepts_a_directly_writable_source()
    {
        var builder = new LatticeViewRegistrationBuilder();
        Assert.That(() => builder.AddView("adults", "people", Filter()), Throws.Nothing);
    }

    [Test]
    public void AddView_rejects_a_view_tree_source()
    {
        var builder = new LatticeViewRegistrationBuilder();
        Assert.That(
            () => builder.AddView("chained", "view-adults", Filter()),
            Throws.InvalidOperationException.With.Message.Contains("view-adults"));
    }

    [Test]
    public void AddView_factory_overload_rejects_a_view_tree_source()
    {
        var builder = new LatticeViewRegistrationBuilder();
        Assert.That(
            () => builder.AddView("chained", "view-adults", _ => Filter()),
            Throws.InvalidOperationException);
    }

    [Test]
    public void AddAggregationView_rejects_a_view_tree_source()
    {
        var builder = new LatticeViewRegistrationBuilder();
        Assert.That(
            () => builder.AddAggregationView("chained", "view-adults", Aggregation()),
            Throws.InvalidOperationException);
    }

    [Test]
    public void AddAggregationView_factory_overload_rejects_a_view_tree_source()
    {
        var builder = new LatticeViewRegistrationBuilder();
        Assert.That(
            () => builder.AddAggregationView("chained", "view-adults", _ => Aggregation()),
            Throws.InvalidOperationException);
    }
}
