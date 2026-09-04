using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Covers <see cref="TreeReshardGrain.CountSlotsPerPhysicalShard"/>, the
/// owner-indexed virtual-slot histogram the migrating pass rebuilds on every
/// tick. The contract under test is that the returned counts are aligned to the
/// supplied physical-shard ordinals, that they total the slot count, and that
/// both the dense and the sparse counting paths agree.
/// </summary>
[TestFixture]
public class TreeReshardGrainSlotHistogramTests
{
    [Test]
    public void CountSlotsPerPhysicalShard_DefaultMap_CountsAreOrdinalAlignedAndTotalTheSlotSpace()
    {
        var map = ShardMap.CreateDefault(4096, 16);
        var physicalShards = map.GetPhysicalShardIndices();

        var counts = TreeReshardGrain.CountSlotsPerPhysicalShard(physicalShards, map.Slots);

        Assert.That(counts, Has.Length.EqualTo(physicalShards.Count));
        Assert.That(counts.Sum(), Is.EqualTo(map.Slots.Length));
        for (var i = 0; i < physicalShards.Count; i++)
        {
            var expected = map.Slots.Count(s => s == physicalShards[i]);
            Assert.That(counts[i], Is.EqualTo(expected),
                $"ordinal {i} (physical shard {physicalShards[i]})");
        }
    }

    [Test]
    public void CountSlotsPerPhysicalShard_EmptyPhysicalShards_ReturnsEmpty()
    {
        var counts = TreeReshardGrain.CountSlotsPerPhysicalShard([], []);

        Assert.That(counts, Is.Empty);
    }

    [Test]
    public void CountSlotsPerPhysicalShard_UnevenOwnership_ReportsPerOwnerCounts()
    {
        // Ordinal 0 -> shard 0 owns four slots, ordinal 1 -> shard 3 owns one.
        int[] slots = [0, 3, 0, 0, 0];
        int[] physicalShards = [0, 3];

        var counts = TreeReshardGrain.CountSlotsPerPhysicalShard(physicalShards, slots);

        Assert.That(counts, Is.EqualTo(new[] { 4, 1 }));
    }

    [Test]
    public void CountSlotsPerPhysicalShard_ShardOwningNoSlots_ReportsZeroRatherThanBeingOmitted()
    {
        // Shard 1 appears in the physical list but owns nothing: the migrating
        // pass relies on reading a zero here rather than a missing entry.
        int[] slots = [0, 0, 2];
        int[] physicalShards = [0, 1, 2];

        var counts = TreeReshardGrain.CountSlotsPerPhysicalShard(physicalShards, slots);

        Assert.That(counts, Is.EqualTo(new[] { 2, 0, 1 }));
    }

    [Test]
    public void CountSlotsPerPhysicalShard_SparseIndicesBeyondDenseLimit_UsesFallbackAndAgrees()
    {
        // Above the dense-counter limit the histogram switches to a binary
        // search over the ascending index list; it must produce identical counts.
        const int High = (1 << 20) + 7;
        int[] physicalShards = [1, High];
        int[] slots = [1, High, High, 1, 1];

        var counts = TreeReshardGrain.CountSlotsPerPhysicalShard(physicalShards, slots);

        Assert.That(counts, Is.EqualTo(new[] { 3, 2 }));
    }

    [Test]
    public void CountSlotsPerPhysicalShard_MatchesTheHashedHistogramItReplaced()
    {
        // Differential check against the prior Dictionary<int, int> shape across
        // a range of map geometries, including ones where slots outnumber shards
        // heavily and ones where they barely do.
        foreach (var (virtualSlots, physicalCount) in new[]
                 {
                     (16, 2), (64, 3), (256, 16), (4096, 16), (4096, 7), (32, 32),
                 })
        {
            var map = ShardMap.CreateDefault(virtualSlots, physicalCount);
            var physicalShards = map.GetPhysicalShardIndices();

            var expected = new Dictionary<int, int>(physicalShards.Count);
            foreach (var idx in physicalShards) expected[idx] = 0;
            foreach (var slot in map.Slots) expected[slot]++;

            var counts = TreeReshardGrain.CountSlotsPerPhysicalShard(physicalShards, map.Slots);

            for (var i = 0; i < physicalShards.Count; i++)
            {
                Assert.That(counts[i], Is.EqualTo(expected[physicalShards[i]]),
                    $"{virtualSlots}/{physicalCount} ordinal {i}");
            }
        }
    }
}
