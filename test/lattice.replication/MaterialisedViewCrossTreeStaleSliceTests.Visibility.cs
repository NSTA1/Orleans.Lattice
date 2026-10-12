using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Replication.Tests;

public partial class MaterialisedViewCrossTreeStaleSliceTests
{
    private sealed record NumericMember(string Group, double Amount);

    [TestCase(AggregationKind.Count)]
    [TestCase(AggregationKind.Sum)]
    public async Task A_ready_aggregation_slice_cannot_roll_back_an_already_public_regrouped_survivor(
        AggregationKind kind)
    {
        using var readScope = ViewReadContext.BeginScope();
        var suffix = Guid.NewGuid().ToString("N");
        var treeA = $"mv-joint-numeric-a-source-{suffix}";
        var treeB = $"mv-joint-numeric-b-source-{suffix}";
        var viewA = $"mv-joint-numeric-a-view-{suffix}";
        var viewB = $"mv-joint-numeric-b-view-{suffix}";
        static byte[] Value(string group, double amount) =>
            JsonLatticeSerializer<NumericMember>.Default.Serialize(new NumericMember(group, amount));
        var factory = _fixture.SiloServices.GetRequiredService<ILatticeViewFactory>();
        foreach (var (tree, view) in new[] { (treeA, viewA), (treeB, viewB) })
        {
            var projection = new AggregationLatticeViewProjection(kind,
                static value => JsonLatticeSerializer<NumericMember>.Default.Deserialize(value)!.Group, "numeric-joint-v1",
                valueSelector: static value => JsonLatticeSerializer<NumericMember>.Default.Deserialize(value)!.Amount);
            var definition = new LatticeViewDefinition(view, projection);
            _ = factory.Create(_fixture.Cluster.Client.GetGrain<ILattice>(tree), view,
                MaterialisedViewRuntimeProjectionProvider.DescriptorFor(definition));
        }
        var maintainerA = await MaintainerAsync(viewA);
        var maintainerB = await MaintainerAsync(viewB);
        Assert.That(await CrossTreeWriteAsync(treeA, "ka", Value("old", 3),
            treeB, "kb", Value("peer", 4), $"mv-joint-numeric-{suffix}"),
            Is.EqualTo(CrossTreeAtomicWriteOutcome.Committed));
        await maintainerA.DrainAsync();
        var actualViewA = await _fixture.ActiveViewTreeAsync(viewA);
        await _fixture.Cluster.Client.GetGrain<ILattice>(treeA).SetAsync("ka", Value("new", 7));
        Dictionary<string, byte[]>? before = null;
        Dictionary<string, byte[]>? after = null;
        CrossTreeRegisterReadyHookFilter.BeforeNextRegistration(viewA, async grains =>
        {
            before = await actualViewA.GetManyAsync(["old", "new"]);
            await grains.GetGrain<IViewMaintainerGrain>(viewB).DrainAsync();
            after = await actualViewA.GetManyAsync(["old", "new"]);
        });
        await maintainerA.DrainAsync();
        Assert.That(before, Is.Not.Null);
        Assert.That(after, Is.Not.Null);
        Assert.That(before!.ContainsKey("new") && !before.ContainsKey("old")
            && after!.ContainsKey("old"), Is.False,
            "A stale joint numeric ready slice may not resurrect the old group after public regrouping.");
        var final = await (await _fixture.ActiveViewTreeAsync(viewA)).GetManyAsync(["old", "new"]);
        Assert.That(final.ContainsKey("old"), Is.False);
        Assert.That(final.ContainsKey("new"), Is.True);
        Assert.That(kind == AggregationKind.Count
            ? LatticeAggregationValue.DecodeInt64(final["new"])
            : LatticeAggregationValue.DecodeDouble(final["new"]), Is.EqualTo(kind == AggregationKind.Count ? 1 : 7));
    }

    [Test]
    public async Task A_ready_slice_cannot_roll_back_an_already_public_ordinary_survivor_before_reconcile()
    {
        using var readScope = ViewReadContext.BeginScope();
        var suffix = Guid.NewGuid().ToString("N");
        var treeA = $"mv-joint-visible-a-source-{suffix}";
        var treeB = $"mv-joint-visible-b-source-{suffix}";
        var viewA = $"mv-joint-visible-a-view-{suffix}";
        var viewB = $"mv-joint-visible-b-view-{suffix}";
        CreateAdultView(treeA, viewA);
        CreateAdultView(treeB, viewB);
        var maintainerA = await MaintainerAsync(viewA);
        var maintainerB = await MaintainerAsync(viewB);
        var first = Person(30, "batch");
        var successor = Person(50, "ordinary");
        Assert.That(await CrossTreeWriteAsync(treeA, "ka", first, treeB, "kb",
            Person(40, "peer-batch"), $"mv-joint-visible-{suffix}"),
            Is.EqualTo(CrossTreeAtomicWriteOutcome.Committed));
        await maintainerA.DrainAsync();
        var actualViewA = await _fixture.ActiveViewTreeAsync(viewA);
        await _fixture.Cluster.Client.GetGrain<ILattice>(treeA).SetAsync("ka", successor);

        byte[]? beforeJoint = null;
        byte[]? afterJoint = null;
        var hookRan = false;
        CrossTreeRegisterReadyHookFilter.BeforeNextRegistration(viewA, async grains =>
        {
            beforeJoint = await actualViewA.GetAsync("ka");
            await grains.GetGrain<IViewMaintainerGrain>(viewB).DrainAsync();
            // This is a held interleaving: A has not returned from its readiness
            // call, so no end-of-pass reconcile can erase the observed rollback.
            afterJoint = await actualViewA.GetAsync("ka");
            hookRan = true;
        });
        await maintainerA.DrainAsync();
        Assert.That(hookRan, Is.True, "The actual peer joint publication ran at the registration seam.");
        Assert.That(beforeJoint is not null && beforeJoint.AsSpan().SequenceEqual(successor)
            && afterJoint is not null && afterJoint.AsSpan().SequenceEqual(first), Is.False,
            "A public ordinary successor must not roll back to the stale ready tuple, even temporarily.");
        Assert.That(await (await _fixture.ActiveViewTreeAsync(viewA)).GetAsync("ka"), Is.EqualTo(successor));
        Assert.That(await (await _fixture.ActiveViewTreeAsync(viewB)).GetAsync("kb"), Is.EqualTo(Person(40, "peer-batch")));
        Assert.That(await maintainerA.GetLagAsync(), Is.EqualTo(0));
    }
}
