using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Replication.Tests;

public partial class MaterialisedViewCrossTreeStaleSliceTests
{
    [TestCase(AggregationKind.Count)]
    [TestCase(AggregationKind.Sum)]
    public async Task Joint_numeric_repair_publishes_regrouping_and_a_no_output_peer_retraction_together(AggregationKind kind)
    {
        using var readScope = ViewReadContext.BeginScope();
        var suffix = Guid.NewGuid().ToString("N");
        var treeA = $"mv-repair-numeric-a-{suffix}";
        var treeB = $"mv-repair-numeric-b-{suffix}";
        var viewA = $"mv-repair-numeric-view-a-{suffix}";
        var viewB = $"mv-repair-numeric-view-b-{suffix}";
        var operationId = $"mv-repair-numeric-op-{suffix}";
        static byte[] Value(string group, double amount) =>
            JsonLatticeSerializer<NumericMember>.Default.Serialize(new NumericMember(group, amount));
        var factory = _fixture.SiloServices.GetRequiredService<ILatticeViewFactory>();
        foreach (var (tree, view) in new[] { (treeA, viewA), (treeB, viewB) })
        {
            var projection = new AggregationLatticeViewProjection(kind,
                static value => JsonLatticeSerializer<NumericMember>.Default.Deserialize(value)!.Group,
                "numeric-fresh-repair-v1",
                valueSelector: static value => JsonLatticeSerializer<NumericMember>.Default.Deserialize(value)!.Amount,
                filter: LatticePredicateTranslator.Translate<NumericMember>(member => member.Amount > 0));
            var definition = new LatticeViewDefinition(view, projection);
            _ = factory.Create(_fixture.Cluster.Client.GetGrain<ILattice>(tree), view,
                MaterialisedViewRuntimeProjectionProvider.DescriptorFor(definition));
        }

        var a = await MaintainerAsync(viewA);
        var b = await MaintainerAsync(viewB);
        Assert.That(await CrossTreeWriteAsync(treeA, "ka", Value("old", 3), treeB, "kb", Value("peer-old", 4), operationId),
            Is.EqualTo(CrossTreeAtomicWriteOutcome.Committed));
        await a.DrainAsync();
        CrossTreeRegisterReadyHookFilter.AfterNextRegistration(viewB, async grains =>
        {
            await grains.GetGrain<ILattice>(treeA).SetAsync("ka", Value("new", 7));
            await grains.GetGrain<ILattice>(treeB).SetAsync("kb", Value("filtered", -1));
        });
        await b.DrainAsync();
        await b.DrainAsync();
        var actualA = await _fixture.ActiveViewTreeAsync(viewA);
        var actualB = await _fixture.ActiveViewTreeAsync(viewB);
        var heldA = await actualA.GetManyAsync(["old", "new"]);
        var heldB = await actualB.GetManyAsync(["peer-old", "filtered"]);
        Assert.That(heldA.Keys, Is.EquivalentTo(new[] { "old" }));
        Assert.That(heldB.Keys, Is.EquivalentTo(new[] { "peer-old" }),
            "A filtered ordinary successor cannot independently retract its public peer group while repair waits.");

        Dictionary<string, byte[]>? beforeA = null;
        Dictionary<string, byte[]>? beforeB = null;
        Dictionary<string, byte[]>? afterA = null;
        Dictionary<string, byte[]>? afterB = null;
        CrossTreeRegisterReadyHookFilter.BeforeNextRepair(viewA, async _ =>
        {
            beforeA = await actualA.GetManyAsync(["old", "new"]);
            beforeB = await actualB.GetManyAsync(["peer-old", "filtered"]);
        });
        CrossTreeRegisterReadyHookFilter.AfterNextRepair(viewA, async _ =>
        {
            afterA = await actualA.GetManyAsync(["old", "new"]);
            afterB = await actualB.GetManyAsync(["peer-old", "filtered"]);
        });
        for (var pass = 0; pass < 4 && afterA is null; pass++)
        {
            await a.DrainAsync();
        }

        Assert.That(beforeA, Is.Not.Null);
        Assert.That(beforeB, Is.Not.Null);
        Assert.That(afterA, Is.Not.Null);
        Assert.That(afterB, Is.Not.Null);
        Assert.That(beforeA!.Keys, Is.EquivalentTo(new[] { "old" }));
        Assert.That(beforeB!.Keys, Is.EquivalentTo(new[] { "peer-old" }));
        Assert.That(afterA!.Keys, Is.EquivalentTo(new[] { "new" }));
        Assert.That(afterB, Is.Empty, "Regrouping and the no-output peer's old-group removal become public in the same joint repair.");
        Assert.That(kind == AggregationKind.Count
            ? LatticeAggregationValue.DecodeInt64(afterA["new"])
            : LatticeAggregationValue.DecodeDouble(afterA["new"]), Is.EqualTo(kind == AggregationKind.Count ? 1 : 7));
        for (var pass = 0; pass < 5; pass++)
        {
            await a.DrainAsync();
            await b.DrainAsync();
        }

        Assert.That(await actualA.GetAsync("old"), Is.Null);
        Assert.That(await actualB.GetAsync("peer-old"), Is.Null);
        Assert.That(await a.GetLagAsync(), Is.Zero);
        Assert.That(await b.GetLagAsync(), Is.Zero);
    }
}
