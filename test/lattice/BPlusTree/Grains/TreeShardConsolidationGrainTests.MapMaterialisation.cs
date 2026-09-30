using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// A tree that has never changed topology has no persisted shard map, and its
/// readers take a version-0 fast path that counts every entry on every shard.
/// A fold's survivor holds drained copies of the donor's entries before the
/// swap, so the map must be materialised - its version bumped - before the fold
/// opens its shadow-write window, or a concurrent count double-counts them.
/// </summary>
public partial class TreeShardConsolidationGrainTests
{
    [Test]
    public async Task Start_materialises_an_absent_map_before_the_survivor_absorbs_anything()
    {
        var h = CreateGrain();
        h.PersistedMap = null;

        await h.Grain.StartAsync(0);

        var materialise = h.Log.IndexOf("registry.ReassignSlots");
        var shadow = h.Log.IndexOf("donor.BeginSplit");
        Assert.Multiple(() =>
        {
            Assert.That(materialise, Is.GreaterThanOrEqualTo(0), "the absent map must be persisted");
            Assert.That(shadow, Is.GreaterThan(materialise),
                "the map must reach a non-zero version before the donor starts shadow-forwarding to the survivor");
            Assert.That(h.PersistedMap, Is.Not.Null);
            Assert.That(h.PersistedMap!.Version, Is.GreaterThan(0));
        });
    }

    [Test]
    public async Task Start_leaves_an_already_persisted_map_alone()
    {
        var h = CreateGrain();
        var versionBefore = h.PersistedMap!.Version;

        await h.Grain.StartAsync(0);

        Assert.That(h.Log.IndexOf("registry.ReassignSlots"), Is.EqualTo(-1),
            "a persisted map already carries a version, so starting the fold must not write it");
        Assert.That(h.PersistedMap!.Version, Is.EqualTo(versionBefore));
    }
}
