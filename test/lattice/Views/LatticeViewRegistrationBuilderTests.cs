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

    private static ILatticeFoldProjection Fold() =>
        new LatticeFoldProjection(_ => "g", () => [], (acc, _, _, _) => acc, "v1");

    [Test]
    public void AddFoldedView_accepts_a_directly_writable_source()
    {
        var builder = new LatticeViewRegistrationBuilder();
        Assert.That(() => builder.AddFoldedView("compliance", "parts", Fold()), Throws.Nothing);
    }

    [Test]
    public void AddFoldedView_rejects_a_view_tree_source()
    {
        var builder = new LatticeViewRegistrationBuilder();
        Assert.That(
            () => builder.AddFoldedView("chained", "view-adults", Fold()),
            Throws.InvalidOperationException);
    }

    [Test]
    public void AddFoldedView_factory_overload_rejects_a_view_tree_source()
    {
        var builder = new LatticeViewRegistrationBuilder();
        Assert.That(
            () => builder.AddFoldedView("chained", "view-adults", _ => Fold()),
            Throws.InvalidOperationException);
    }

    [Test]
    public void AddRuntimeProjectionProvider_registers_keyed_factory()
    {
        var builder = new LatticeViewRegistrationBuilder();

        var returned = builder.AddRuntimeProjectionProvider(
            "app.orders.v1",
            (_, context) => new LatticeViewDefinition(context.ViewName, Filter()));

        Assert.Multiple(() =>
        {
            Assert.That(returned, Is.SameAs(builder));
            Assert.That(builder.RuntimeProviders, Has.Count.EqualTo(1));
            Assert.That(builder.RuntimeProviders[0].ProviderKey, Is.EqualTo("app.orders.v1"));
        });
    }

    [Test]
    public void AddRuntimeProjectionProvider_rejects_duplicate_key()
    {
        var builder = new LatticeViewRegistrationBuilder();
        builder.AddRuntimeProjectionProvider(
            "app.orders.v1",
            (_, context) => new LatticeViewDefinition(context.ViewName, Filter()));

        Assert.That(
            () => builder.AddRuntimeProjectionProvider(
                "app.orders.v1",
                (_, context) => new LatticeViewDefinition(context.ViewName, Filter())),
            Throws.InvalidOperationException);
    }

    [Test]
    public void AddRuntimeProjectionProvider_rejects_reserved_predicate_key()
    {
        var builder = new LatticeViewRegistrationBuilder();

        Assert.That(
            () => builder.AddRuntimeProjectionProvider(
                "orleans.lattice.predicate.v1",
                (_, context) => new LatticeViewDefinition(context.ViewName, Filter())),
            Throws.InvalidOperationException);
    }
}
