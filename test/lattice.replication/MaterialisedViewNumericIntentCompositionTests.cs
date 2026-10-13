using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>Exercises durable numeric repair through real source admission and maintainer publication.</summary>
[TestFixture]
[Category("Integration")]
public sealed class MaterialisedViewNumericIntentCompositionTests
{
    private MaterialisedViewClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        _fixture = new MaterialisedViewClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    private sealed class OptionalNumericProjection(AggregationKind kind) : ILatticeAggregationProjection
    {
        public string ProjectionVersion => "optional-numeric-runtime-v1";
        public AggregationKind Aggregation => kind;

        public IEnumerable<AggregationContribution> Project(LatticeMutation mutation)
        {
            if (mutation.Value is { Length: > 0 } value && value[0] != 0)
            {
                yield return AggregationContribution.OfNumeric(
                    value[0] == 3 ? "red" : "blue", mutation.Key, value[0], mutation.Timestamp);
            }
        }
    }

    [TestCase(AggregationKind.Count)]
    [TestCase(AggregationKind.Sum)]
    public async Task Source_suppression_and_a_no_output_successor_complete_numeric_intent_before_checkpoint(
        AggregationKind kind)
    {
        using var readScope = ViewReadContext.BeginScope();
        var suffix = kind.ToString().ToLowerInvariant();
        var sourceId = $"mv-intent-{suffix}-source";
        var viewName = $"mv-intent-{suffix}-view";
        const string key = "numeric-source-member";
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(sourceId);
        var definition = new LatticeViewDefinition(viewName, new OptionalNumericProjection(kind));
        var factory = _fixture.SiloServices.GetRequiredService<ILatticeViewFactory>();
        _ = factory.Create(source, viewName, MaterialisedViewRuntimeProjectionProvider.DescriptorFor(definition));
        var maintainer = _fixture.Cluster.Client.GetGrain<IViewMaintainerGrain>(viewName);
        await maintainer.EnsureActiveAsync();
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(sourceId);
        static HybridLogicalClock Hlc(long ticks) => new() { WallClockTicks = ticks };

        await apply.ApplySetAsync(key, [3], Hlc(10), "numeric-origin", null, 0);
        await maintainer.DrainAsync();
        var viewId = await maintainer.GetActiveTreeIdAsync();
        var view = _fixture.Cluster.Client.GetGrain<ILattice>(viewId);
        Assert.That(await view.GetAsync("red"), Is.Not.Null, "The initial public group was materialised.");

        await apply.ApplySetAsync(key, [7], Hlc(20), "numeric-origin", null, 0);
        var intentKey = AggregationRowCodec.NumericMaterialisationKey(key);
        var fault = NumericPublicationResponseFaultFilter.Arm(viewId, intentKey);
        try
        {
            Assert.ThrowsAsync<TimeoutException>(async () => await maintainer.DrainAsync());
            Assert.That(fault.Committed, Is.True, "The actual atomic membership/intent flip committed before its response was lost.");
            Assert.That(await view.GetAsync(intentKey), Is.Not.Null, "The interrupted event has a durable publication obligation.");
            Assert.That(await maintainer.GetLagAsync(), Is.GreaterThan(0), "The failed drain did not retire its WAL checkpoint.");
        }
        finally
        {
            NumericPublicationResponseFaultFilter.Disarm(viewId);
        }

        // The coherent source gate suppresses HLC20 on replay. HLC30 projects
        // nothing, so no aggregation contribution can accidentally repair it.
        await apply.ApplySetAsync(key, [0], Hlc(30), "numeric-origin", null, 0);
        await maintainer.DrainAsync();
        var snapshot = await view.GetManyAsync(["red", "blue", intentKey]);
        await Assert.MultipleAsync(async () =>
        {
            Assert.That(await source.GetAsync(key), Is.EqualTo(new byte[] { 0 }));
            Assert.That(await maintainer.GetLagAsync(), Is.EqualTo(0));
            Assert.That(snapshot.ContainsKey(intentKey), Is.False,
                "A retired checkpoint must not strand the source-suppressed event's durable repair.");
            Assert.That(snapshot.ContainsKey("red"), Is.False, "The old public group was retracted.");
            Assert.That(snapshot.ContainsKey("blue"), Is.True, "The committed replacement group was materialised.");
        });

        Assert.That(kind == AggregationKind.Count
            ? LatticeAggregationValue.DecodeInt64(snapshot["blue"])
            : LatticeAggregationValue.DecodeDouble(snapshot["blue"]), Is.EqualTo(kind == AggregationKind.Count ? 1 : 7));
        await maintainer.DrainAsync();
        var replay = await view.GetManyAsync(["red", "blue", intentKey]);
        Assert.That(replay.Keys, Is.EquivalentTo(snapshot.Keys));
        Assert.That(replay["blue"], Is.EqualTo(snapshot["blue"]), "An empty follow-up pass neither duplicates nor loses the repair.");
    }
}
