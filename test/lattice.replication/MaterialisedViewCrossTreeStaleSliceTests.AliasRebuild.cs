using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Replication.Tests;

public partial class MaterialisedViewCrossTreeStaleSliceTests
{
    [TestCase(false)]
    [TestCase(true)]
    public async Task Rebuilding_an_active_alias_after_a_split_uses_its_physical_sources_live_topology(bool reconcile)
    {
        using var readScope = ViewReadContext.BeginScope();
        var suffix = Guid.NewGuid().ToString("N");
        var logical = $"mv-alias-rebuild-logical-{suffix}";
        var physical = $"mv-alias-rebuild-physical-{suffix}";
        var view = $"mv-alias-rebuild-view-{suffix}";
        var physicalTree = _fixture.Cluster.Client.GetGrain<ILattice>(physical);
        await physicalTree.SetAsync("seed", Person(5, "seed"));
        var registry = _fixture.Cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.SetAliasAsync(logical, physical);
        var source = _fixture.Cluster.Client.GetGrain<ILattice>(logical);
        var before = await source.GetRoutingAsync(forceRefresh: true);
        var key = Enumerable.Range(0, 10000).Select(index => $"key-{index}").First(candidate =>
        {
            var shard = before.Map.Resolve(candidate);
            var slots = Enumerable.Range(0, before.Map.Slots.Length)
                .Where(slot => before.Map.Slots[slot] == shard).ToArray();
            return ShardMap.GetVirtualSlot(candidate, before.Map.Slots.Length) >= slots[slots.Length / 2];
        });
        await source.SetAsync(key, Person(30, "before"));
        await source.SetAsync("deleted", Person(40, "deleted"));
        CreateAdultView(logical, view);
        var maintainer = await MaintainerAsync(view);
        var originalShard = before.Map.Resolve(key);
        var split = _fixture.Cluster.Client.GetGrain<ITreeShardSplitGrain>($"{logical}/{originalShard}");
        await split.SplitAsync(originalShard);
        await split.RunSplitPassAsync();
        Assert.That(await split.IsIdleAsync(), Is.True);
        Assert.That((await source.GetRoutingAsync(forceRefresh: true)).Map.Resolve(key), Is.Not.EqualTo(originalShard));
        var replacement = Person(50, "after");
        await source.SetAsync(key, replacement);
        await source.DeleteAsync("deleted");
        await source.SetAsync("filtered", Person(12, "filtered"));
        Task rebuild = reconcile ? maintainer.ReconcileAsync() : maintainer.RebuildAsync();
        await Task.WhenAny(rebuild, Task.Delay(TimeSpan.FromSeconds(20)));
        Assert.That(rebuild.IsCompleted, Is.True,
            "Rebuild must complete without rerouting a moved key through the stale physical registry map.");
        await rebuild;

        var actual = await _fixture.ActiveViewTreeAsync(view);
        Assert.That(await actual.GetAsync(key), Is.EqualTo(replacement));
        Assert.That(await actual.GetAsync("deleted"), Is.Null);
        Assert.That(await actual.GetAsync("filtered"), Is.Null);
        Assert.That(await maintainer.GetLagAsync(), Is.Zero);
    }
}
