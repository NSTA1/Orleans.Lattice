using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Replication.Tests;

public partial class MaterialisedViewCrossTreeStaleSliceTests
{
    [TestCase(false, true)]
    [TestCase(true, true)]
    [TestCase(true, false)]
    public async Task Fresh_repair_uses_the_held_physical_sources_current_shard_map_after_split_and_alias_cutover(
        bool initiallyAliased, bool cutOver)
    {
        using var readScope = ViewReadContext.BeginScope();
        var suffix = Guid.NewGuid().ToString("N");
        var treeA = $"mv-repair-routing-a-{suffix}";
        var physicalA = initiallyAliased ? $"mv-repair-routing-physical-{suffix}" : treeA;
        var treeB = $"mv-repair-routing-b-{suffix}";
        var shadow = $"mv-repair-routing-shadow-{suffix}";
        var viewA = $"mv-repair-routing-view-a-{suffix}";
        var viewB = $"mv-repair-routing-view-b-{suffix}";
        var operationId = $"mv-repair-routing-op-{suffix}";
        var registry = _fixture.Cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await _fixture.Cluster.Client.GetGrain<ILattice>(physicalA).SetAsync("seed", Person(5, "seed"));
        if (initiallyAliased)
        {
            await registry.SetAliasAsync(treeA, physicalA);
        }

        CreateAdultView(treeA, viewA);
        CreateAdultView(treeB, viewB);
        var a = await MaintainerAsync(viewA);
        var b = await MaintainerAsync(viewB);
        var sourceA = _fixture.Cluster.Client.GetGrain<ILattice>(treeA);
        var beforeRouting = await sourceA.GetRoutingAsync(forceRefresh: true);
        var keyA = Enumerable.Range(0, 10000).Select(index => $"ka-{index}").First(key =>
        {
            var shard = beforeRouting.Map.Resolve(key);
            var slots = Enumerable.Range(0, beforeRouting.Map.Slots.Length)
                .Where(slot => beforeRouting.Map.Slots[slot] == shard).ToArray();
            return ShardMap.GetVirtualSlot(key, beforeRouting.Map.Slots.Length) >= slots[slots.Length / 2];
        });
        var originalShard = beforeRouting.Map.Resolve(keyA);
        var originalA = Person(30, "original-a");
        var replacementA = Person(50, "replacement-a");
        var replacementB = Person(60, "replacement-b");
        Assert.That(await CrossTreeWriteAsync(treeA, keyA, originalA, treeB, "kb", Person(40, "original-b"), operationId),
            Is.EqualTo(CrossTreeAtomicWriteOutcome.Committed));
        await a.DrainAsync();
        CrossTreeRegisterReadyHookFilter.AfterNextRegistration(viewB, async grains =>
        {
            await grains.GetGrain<ILattice>(treeA).SetAsync(keyA, replacementA);
            await grains.GetGrain<ILattice>(treeB).SetAsync("kb", replacementB);
        });
        await b.DrainAsync();

        var split = _fixture.Cluster.Client.GetGrain<ITreeShardSplitGrain>($"{treeA}/{originalShard}");
        await split.SplitAsync(originalShard);
        await split.RunSplitPassAsync();
        Assert.That(await split.IsIdleAsync(), Is.True, "The real source shard split completed while the original view obligation remained held.");
        var afterRouting = await sourceA.GetRoutingAsync(forceRefresh: true);
        Assert.That(afterRouting.Map.Resolve(keyA), Is.Not.EqualTo(originalShard),
            "The repair key actually moved; reading a frozen pre-split map cannot pass this detector.");
        Assert.That(afterRouting.PhysicalTreeId, Is.EqualTo(physicalA));
        var restoredA = Person(90, "restored-a");
        if (cutOver)
        {
            await _fixture.Cluster.Client.GetGrain<ILattice>(shadow).SetAsync(keyA, restoredA);
            await registry.SetAliasAsync(treeA, shadow);
            Assert.That((await sourceA.GetRoutingAsync(forceRefresh: true)).PhysicalTreeId, Is.EqualTo(shadow),
                "The logical routing cache really points at the foreign copy before fresh repair capture.");
            await a.NotifySourceIdentityChangedAsync(shadow);
        }

        var coordinator = _fixture.Cluster.Client.GetGrain<IViewCrossTreeCoordinatorGrain>(operationId);
        Assert.That(await coordinator.GetRepairReadinessAsync(viewA), Is.Null, "No A repair capture predates the topology changes.");
        ViewCrossTreeReadiness? captured = null;
        for (var pass = 0; pass < 6 && captured is null; pass++)
        {
            await a.DrainAsync();
            captured = await coordinator.GetRepairReadinessAsync(viewA);
        }

        Assert.That(captured, Is.Not.Null);
        Assert.That(captured!.Upserts.Single(pair => pair.Key == keyA).Value, Is.EqualTo(replacementA),
            "Fresh repair reads the moved winner of its held physical log, not the logical alias's restored value.");
        var heldTree = await _fixture.ActiveViewTreeAsync(viewA);
        Assert.That(await heldTree.GetAsync(keyA), Is.EqualTo(originalA));
        await b.DrainAsync();
        Assert.That(await heldTree.GetAsync(keyA), Is.EqualTo(replacementA));
        var reconcileEntered = false;
        ViewMaintainerGrain.BeforeCrossTreeReconcileForTest = name =>
        {
            if (name == viewA)
            {
                reconcileEntered = true;
            }

            return Task.CompletedTask;
        };
        try
        {
            for (var pass = 0; pass < 8; pass++)
            {
                await a.DrainAsync();
                await b.DrainAsync();
            }
        }
        catch (TimeoutException ex)
        {
            throw new TimeoutException($"Repair retirement timed out; conservative reconcile entered: {reconcileEntered}.", ex);
        }
        finally
        {
            ViewMaintainerGrain.BeforeCrossTreeReconcileForTest = null;
        }

        Assert.That(await (await _fixture.ActiveViewTreeAsync(viewA)).GetAsync(keyA), Is.EqualTo(cutOver ? restoredA : replacementA));
        Assert.That(await a.GetLagAsync(), Is.Zero);
        Assert.That(await b.GetLagAsync(), Is.Zero);
    }
}
