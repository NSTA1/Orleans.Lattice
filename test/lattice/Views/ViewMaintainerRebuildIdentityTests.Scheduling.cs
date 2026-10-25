using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.Views;

public sealed partial class ViewMaintainerRebuildIdentityTests
{
    [TestCase(false)]
    [TestCase(true)]
    public void Repair_refreshes_the_live_alias_topology_after_a_source_key_moves(bool reconcile)
    {
        var h = Create(moveSource: false);
        var map = ShardMap.CreateDefault(1, 1);
        h.Registry.GetEntryAsync(Logical).Returns(_ => Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry
            {
                PhysicalTreeId = First, ShardMap = map,
                ShardCount = 2, MaxLeafKeys = 128, MaxInternalChildren = 128,
            }));
        var stale = Substitute.For<IShardRootGrain>();
        var current = Substitute.For<IShardRootGrain>();
        h.Factory.GetGrain<IShardRootGrain>($"{First}/0").Returns(stale);
        h.Factory.GetGrain<IShardRootGrain>($"{First}/1").Returns(current);
        var staleReads = 0;
        var currentReads = 0;
        stale.GetWithVersionAsync("k").Returns(_ =>
        {
            staleReads++;
            map = new ShardMap { Slots = [1], Version = 1 };
            return Task.FromException<VersionedValue>(new StaleShardRoutingException());
        });
        current.GetWithVersionAsync("k").Returns(_ =>
        {
            currentReads++;
            return Task.FromResult(new VersionedValue
            {
                Value = [1], Version = new HybridLogicalClock { WallClockTicks = 10 },
            });
        });

        Assert.DoesNotThrowAsync(async () =>
        {
            if (reconcile)
                await h.Grain.ReconcileAsync();
            else
                await h.Grain.RebuildAsync();
        }, "a stable physical source must make progress after a finite topology change");

        Assert.Multiple(() =>
        {
            Assert.That(staleReads, Is.EqualTo(1), "one stale response must trigger a live topology refresh");
            Assert.That(currentReads, Is.EqualTo(1), "the moved key must be read through the current root");
            Assert.That(h.State.State.BoundPhysicalTreeId, Is.EqualTo(First),
                "a topology split does not change the captured physical WAL identity");
        });
    }
}
