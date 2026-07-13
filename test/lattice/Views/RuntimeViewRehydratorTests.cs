using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// Unit tests for <see cref="RuntimeViewRehydrator"/>, focusing on the
/// defence-in-depth guard that rejects a persisted projection type name that is
/// not the expected projection interface <b>before</b> the type is constructed.
/// The type name is read back from durable registry state and so is untrusted.
/// </summary>
[TestFixture]
public sealed class RuntimeViewRehydratorTests
{
    private ServiceProvider _services = null!;

    /// <summary>A record of whether an untrusted type's constructor ran.</summary>
    private static bool _hostileConstructed;

    [SetUp]
    public void SetUp()
    {
        _hostileConstructed = false;
        _services = new ServiceCollection().BuildServiceProvider();
    }

    [TearDown]
    public void TearDown() => _services.Dispose();

    private static RuntimeViewRegistration Record(Type projectionType, bool isAggregation = false) => new()
    {
        ViewName = "adults",
        SourceTreeId = "people",
        ProjectionTypeName = projectionType.AssemblyQualifiedName!,
        ProjectionVersion = "v1",
        IsAggregation = isAggregation,
    };

    [Test]
    public void Resolve_reconstructs_a_valid_view_projection()
    {
        var registration = RuntimeViewRehydrator.Resolve(
            Record(typeof(ValidViewProjection)), _services, NullLogger.Instance);

        Assert.That(registration, Is.Not.Null);
        Assert.That(registration!.Projection, Is.TypeOf<ValidViewProjection>());
    }

    [Test]
    public void Resolve_reconstructs_a_valid_aggregation_projection()
    {
        var registration = RuntimeViewRehydrator.Resolve(
            Record(typeof(ValidAggregationProjection), isAggregation: true), _services, NullLogger.Instance);

        Assert.That(registration, Is.Not.Null);
        Assert.That(registration!.AggregationProjection, Is.TypeOf<ValidAggregationProjection>());
    }

    [Test]
    public void Resolve_rejects_an_unresolvable_type_name()
    {
        var registration = RuntimeViewRehydrator.Resolve(
            new RuntimeViewRegistration
            {
                ViewName = "adults",
                SourceTreeId = "people",
                ProjectionTypeName = "No.Such.Type, No.Such.Assembly",
                ProjectionVersion = "v1",
            },
            _services,
            NullLogger.Instance);

        Assert.That(registration, Is.Null);
    }

    [Test]
    public void Resolve_rejects_a_non_projection_type_without_constructing_it()
    {
        var registration = RuntimeViewRehydrator.Resolve(
            Record(typeof(HostileType)), _services, NullLogger.Instance);

        Assert.That(registration, Is.Null);
        Assert.That(_hostileConstructed, Is.False,
            "an untrusted, non-projection type must never be instantiated during re-hydration");
    }

    [Test]
    public void Resolve_rejects_a_view_projection_declared_as_an_aggregation()
    {
        // A record whose IsAggregation flag disagrees with the type's interface
        // must be rejected before construction rather than mis-bound.
        var registration = RuntimeViewRehydrator.Resolve(
            Record(typeof(ValidViewProjection), isAggregation: true), _services, NullLogger.Instance);

        Assert.That(registration, Is.Null);
    }

    private sealed class ValidViewProjection : ILatticeViewProjection
    {
        public string ProjectionVersion => "v1";

        public IEnumerable<ViewWrite> Project(LatticeMutation mutation) => [];
    }

    private sealed class ValidAggregationProjection : ILatticeAggregationProjection
    {
        public string ProjectionVersion => "v1";

        public AggregationKind Aggregation => AggregationKind.Count;

        public IEnumerable<AggregationContribution> Project(LatticeMutation mutation) => [];
    }

    private sealed class HostileType
    {
        public HostileType() => _hostileConstructed = true;
    }
}
