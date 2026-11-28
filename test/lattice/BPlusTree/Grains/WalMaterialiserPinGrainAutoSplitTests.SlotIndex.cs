namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// The per-slot consumer index (issue #6166): a slot write reads only the written slots'
/// consumers, and what it writes is what a walk of the whole map would have written.
/// </summary>
public sealed partial class WalMaterialiserPinGrainAutoSplitTests
{
    [Test]
    public async Task A_seed_into_a_split_store_reads_only_the_consumers_of_the_slots_it_writes()
    {
        const int partitions = 8;
        var store = new SizeLimitedPinStore(AzureTableMaxBytes);
        var h = await ActivateAsync(store);
        for (var leaf = 0; leaf < 2_500; leaf++)
        {
            await h.Grain.SeedManyAsync(LeafBirth(leaf, partitions));
        }

        var width = Field<int>(h.Grain, "_bucketCount");
        var mapSize = (await h.Grain.GetPinsAsync()).Count;
        var before = h.Grain.SliceBuildVisitsForTest;
        await h.Grain.SeedManyAsync(LeafBirth(2_500, partitions));
        Assume.That(Field<int>(h.Grain, "_bucketCount"), Is.EqualTo(width), "precondition: the seed did not change the layout");

        Assert.That(h.Grain.SliceBuildVisitsForTest - before, Is.LessThan(mapSize / 4),
            $"a seed writing at most {partitions} of {width} slots must not walk the {mapSize}-consumer map");
    }

    [Test]
    public async Task Index_built_slots_persist_exactly_the_map_across_removals_and_growth()
    {
        const int partitions = 8;
        var store = new SizeLimitedPinStore(AzureTableMaxBytes);
        var h = await ActivateAsync(store);
        for (var leaf = 0; leaf < 1_000; leaf++)
        {
            await h.Grain.SeedManyAsync(LeafBirth(leaf, partitions));
        }

        for (var leaf = 0; leaf < 1_000; leaf += 7)
        {
            await h.Grain.RemoveAsync(Consumer(leaf, leaf % partitions));
        }

        for (var leaf = 1_000; leaf < 2_500; leaf++)
        {
            await h.Grain.SeedManyAsync(LeafBirth(leaf, partitions, frontier: leaf));
        }

        await h.Grain.ReportManyAsync(LeafBirth(3, partitions, frontier: 9_999));

        var inMemory = await h.Grain.GetPinsAsync();
        var reactivated = await ActivateAsync(store);
        var durable = await reactivated.Grain.GetPinsAsync();

        Assert.Multiple(() =>
        {
            Assert.That(durable, Is.EquivalentTo(inMemory), "every slot written from the index holds exactly the map's entries");
            Assert.That(durable.ContainsKey(Consumer(7, 7)), Is.False, "a removed consumer stays removed");
            Assert.That(store.Rejections, Is.Zero);
        });
    }
}