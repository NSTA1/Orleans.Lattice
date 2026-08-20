using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// Add-only unit coverage for <see cref="RuntimeViewRehydrator"/>, the static
/// resolver that reconstructs an in-memory <see cref="ViewRegistration"/> from a
/// durable <see cref="RuntimeViewRegistration"/> by resolving the persisted
/// projection type through the <see cref="RuntimeViewProjectionAllowList"/>. Test
/// projection types defined in this assembly are automatically on the allow-list
/// (it scans loaded concrete projection types), so the success, wrong-kind,
/// unknown-type, and construction-failure branches are all exercised deterministically.
/// </summary>
[TestFixture]
public class RuntimeViewRehydratorTests
{
    private sealed class FakeViewProjection : ILatticeViewProjection
    {
        public string ProjectionVersion => "v1";

        public IEnumerable<ViewWrite> Project(LatticeMutation mutation) => Array.Empty<ViewWrite>();
    }

    private sealed class FakeAggregationProjection : ILatticeAggregationProjection
    {
        public string ProjectionVersion => "v1";

        public AggregationKind Aggregation => AggregationKind.Count;

        public IEnumerable<AggregationContribution> Project(LatticeMutation mutation) =>
            Array.Empty<AggregationContribution>();
    }

    private sealed class ThrowingViewProjection : ILatticeViewProjection
    {
        public ThrowingViewProjection() => throw new InvalidOperationException("ctor boom");

        public string ProjectionVersion => "v1";

        public IEnumerable<ViewWrite> Project(LatticeMutation mutation) => Array.Empty<ViewWrite>();
    }

    private static IServiceProvider EmptyServices() => new ServiceCollection().BuildServiceProvider();

    private static RuntimeViewRegistration Registration(
        string typeName, bool isAggregation, bool accumulative = false) => new()
    {
        ViewName = "v-name",
        SourceTreeId = "src-tree",
        ProjectionTypeName = typeName,
        ProjectionVersion = "v1",
        IsAggregation = isAggregation,
        Accumulative = accumulative,
    };

    [Test]
    public void Resolve_viewProjection_returnsRegistrationWithProjection()
    {
        var record = Registration(typeof(FakeViewProjection).FullName!, isAggregation: false);

        var result = RuntimeViewRehydrator.Resolve(record, EmptyServices(), NullLogger.Instance);

        Assert.That(result, Is.Not.Null);
        Assert.That(result!.ViewName, Is.EqualTo("v-name"));
        Assert.That(result.SourceTreeId, Is.EqualTo("src-tree"));
        Assert.That(result.Projection, Is.InstanceOf<FakeViewProjection>());
        Assert.That(result.AggregationProjection, Is.Null);
    }

    [Test]
    public void Resolve_accumulativeViewProjection_flowsAccumulativeFlag()
    {
        var record = Registration(typeof(FakeViewProjection).FullName!, isAggregation: false, accumulative: true);

        var result = RuntimeViewRehydrator.Resolve(record, EmptyServices(), NullLogger.Instance);

        Assert.That(result, Is.Not.Null);
        Assert.That(result!.Accumulative, Is.True);
    }

    [Test]
    public void Resolve_aggregationProjection_returnsRegistrationWithAggregation()
    {
        var record = Registration(typeof(FakeAggregationProjection).FullName!, isAggregation: true);

        var result = RuntimeViewRehydrator.Resolve(record, EmptyServices(), NullLogger.Instance);

        Assert.That(result, Is.Not.Null);
        Assert.That(result!.AggregationProjection, Is.InstanceOf<FakeAggregationProjection>());
        Assert.That(result.Projection, Is.Null);
    }

    [Test]
    public void Resolve_unknownTypeName_returnsNull()
    {
        var record = Registration("Not.A.Real.Projection, Nowhere", isAggregation: false);

        var result = RuntimeViewRehydrator.Resolve(record, EmptyServices(), NullLogger.Instance);

        Assert.That(result, Is.Null);
    }

    [Test]
    public void Resolve_emptyTypeName_returnsNull()
    {
        var record = Registration(string.Empty, isAggregation: false);

        var result = RuntimeViewRehydrator.Resolve(record, EmptyServices(), NullLogger.Instance);

        Assert.That(result, Is.Null);
    }

    [Test]
    public void Resolve_kindMismatch_viewProjectionRequestedAsAggregation_returnsNull()
    {
        // The type is a view projection, but the record claims aggregation; the
        // allow-list's kind gate rejects it before construction.
        var record = Registration(typeof(FakeViewProjection).FullName!, isAggregation: true);

        var result = RuntimeViewRehydrator.Resolve(record, EmptyServices(), NullLogger.Instance);

        Assert.That(result, Is.Null);
    }

    [Test]
    public void Resolve_projectionCtorThrows_returnsNull()
    {
        var record = Registration(typeof(ThrowingViewProjection).FullName!, isAggregation: false);

        var result = RuntimeViewRehydrator.Resolve(record, EmptyServices(), NullLogger.Instance);

        Assert.That(result, Is.Null);
    }
}
