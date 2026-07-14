using Orleans.Lattice.Views;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// Unit tests for <see cref="RuntimeViewProjectionAllowList"/>. The allow-list
/// constrains re-hydration to the projection types already loaded on the silo, so
/// a tampered <see cref="RuntimeViewRegistration.ProjectionTypeName"/> that names a
/// type outside that set (or of the wrong projection kind) is rejected without the
/// type ever being loaded or constructed.
/// </summary>
[TestFixture]
public sealed class RuntimeViewProjectionAllowListTests
{
    /// <summary>A loaded, concrete view projection - on the allow-list.</summary>
    public sealed class AllowedViewProjection : ILatticeViewProjection
    {
        public string ProjectionVersion => "allowed-view-v1";

        public IEnumerable<ViewWrite> Project(LatticeMutation mutation) => [];
    }

    /// <summary>A loaded, concrete aggregation projection - on the allow-list.</summary>
    public sealed class AllowedAggregationProjection : ILatticeAggregationProjection
    {
        public string ProjectionVersion => "allowed-agg-v1";

        public AggregationKind Aggregation => AggregationKind.Count;

        public IEnumerable<AggregationContribution> Project(LatticeMutation mutation) => [];
    }

    [Test]
    public void Resolve_returns_a_loaded_view_projection_by_assembly_qualified_name()
    {
        var name = typeof(AllowedViewProjection).AssemblyQualifiedName!;

        var resolved = RuntimeViewProjectionAllowList.Resolve(name, isAggregation: false);

        Assert.That(resolved, Is.EqualTo(typeof(AllowedViewProjection)));
    }

    [Test]
    public void Resolve_returns_a_loaded_aggregation_projection_by_assembly_qualified_name()
    {
        var name = typeof(AllowedAggregationProjection).AssemblyQualifiedName!;

        var resolved = RuntimeViewProjectionAllowList.Resolve(name, isAggregation: true);

        Assert.That(resolved, Is.EqualTo(typeof(AllowedAggregationProjection)));
    }

    [Test]
    public void Resolve_returns_null_when_the_kind_does_not_match()
    {
        // A view projection requested as an aggregation (and vice versa) is
        // rejected even though the type itself is on the allow-list.
        Assert.Multiple(() =>
        {
            Assert.That(
                RuntimeViewProjectionAllowList.Resolve(
                    typeof(AllowedViewProjection).AssemblyQualifiedName!, isAggregation: true),
                Is.Null);
            Assert.That(
                RuntimeViewProjectionAllowList.Resolve(
                    typeof(AllowedAggregationProjection).AssemblyQualifiedName!, isAggregation: false),
                Is.Null);
        });
    }

    [Test]
    public void Resolve_returns_null_for_a_loaded_type_that_is_not_a_projection()
    {
        // System.String is loaded but implements neither projection interface, so
        // it is not on the allow-list and must not resolve.
        var resolved = RuntimeViewProjectionAllowList.Resolve(
            typeof(string).AssemblyQualifiedName!, isAggregation: false);

        Assert.That(resolved, Is.Null);
    }

    [Test]
    public void Resolve_returns_null_for_an_unknown_or_unloadable_type_name()
    {
        // A forged assembly-qualified name pointing at a type/assembly that is not
        // a loaded projection type is rejected without triggering an assembly load.
        var forged = "Some.Hostile.Gadget, Some.Unloaded.Assembly, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null";

        var resolved = RuntimeViewProjectionAllowList.Resolve(forged, isAggregation: false);

        Assert.That(resolved, Is.Null);
    }

    [Test]
    public void Resolve_returns_null_for_null_or_empty()
    {
        Assert.Multiple(() =>
        {
            Assert.That(RuntimeViewProjectionAllowList.Resolve(null!, isAggregation: false), Is.Null);
            Assert.That(RuntimeViewProjectionAllowList.Resolve(string.Empty, isAggregation: false), Is.Null);
        });
    }
}
