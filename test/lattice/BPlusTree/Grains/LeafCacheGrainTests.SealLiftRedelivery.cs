using Orleans.Lattice.Primitives;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for issue #3524: a <see cref="Orleans.Lattice.BPlusTree.Grains.LeafCacheGrain"/>
/// that pruned its rows for a virtual slot while the slot was sealed must get
/// them back once an online consolidation lifts the seal.
/// <para>
/// The cache drops every cached key that hashes into a sealed slot and keeps
/// advancing its delivery cursor. A lift changes no row, so unless the primary
/// leaf records a fresh delivery sequence for the reclaimed keys, the cursor is
/// already past them and they are never shipped again. The cache then stops
/// refusing the keys but holds nothing for them, and its miss-is-authoritative
/// read path answers <c>null</c> / <c>false</c> for keys the leaf owns.
/// </para>
/// <para>
/// These tests run against a real <see cref="Orleans.Lattice.BPlusTree.Grains.BPlusLeafGrain"/>
/// so the seal, the lift and the delivery cursor all go through production code.
/// </para>
/// </summary>
public partial class LeafCacheGrainTests
{
    private const int ReclaimedSlot = 2;

    [Test]
    public async Task Cache_serves_a_reclaimed_key_it_pruned_while_the_slot_was_sealed()
    {
        var (cache, leaf, _) = CreateCacheOverRealLeaf(
            nameof(Cache_serves_a_reclaimed_key_it_pruned_while_the_slot_was_sealed));
        var foldedKey = KeyForVirtualSlot(ReclaimedSlot, "folded-");
        var keptKey = KeyForVirtualSlot(ReclaimedSlot + 1, "kept-");

        await leaf.SetAsync(foldedKey, Encoding.UTF8.GetBytes("v1"));
        await leaf.SetAsync(keptKey, Encoding.UTF8.GetBytes("v2"));
        Assert.That(await cache.GetAsync(foldedKey), Is.Not.Null, "Precondition: the cache holds the row.");

        // Split away: a read of another key refreshes the cache, which prunes
        // the sealed row while its cursor stays at the leaf's head.
        await leaf.MarkSlotsMovedAwayAsync([ReclaimedSlot], MovedAwayVsc);
        await cache.GetAsync(keptKey);
        Assert.That(async () => await cache.GetAsync(foldedKey),
            Throws.TypeOf<StaleShardRoutingException>(),
            "Precondition: the cache refuses the sealed slot.");

        // Fold back.
        await leaf.UnmarkSlotsMovedAwayAsync([ReclaimedSlot], MovedAwayVsc);

        var value = await cache.GetAsync(foldedKey);
        Assert.That(value, Is.Not.Null,
            "The cache must be re-shipped the reclaimed row, not treat its own prune as authoritative.");
        Assert.That(Encoding.UTF8.GetString(value!), Is.EqualTo("v1"));
        Assert.That(await cache.ExistsAsync(foldedKey), Is.True,
            "ExistsAsync shares the miss-is-authoritative branch and must see the reclaimed key too.");
    }

    [Test]
    public async Task Cache_serves_a_row_drained_onto_a_sealed_leaf_after_the_lift()
    {
        // The shape the issue traces: the consolidation drains the donor's row
        // onto the still-sealed survivor, a read of another key ships it, the
        // next sealed refresh prunes it, and only then is the seal lifted.
        var (cache, leaf, _) = CreateCacheOverRealLeaf(
            nameof(Cache_serves_a_row_drained_onto_a_sealed_leaf_after_the_lift));
        var drainedKey = KeyForVirtualSlot(ReclaimedSlot, "drained-");
        var keptKey = KeyForVirtualSlot(ReclaimedSlot + 1, "kept-");

        await leaf.SetAsync(keptKey, Encoding.UTF8.GetBytes("v2"));
        await leaf.MarkSlotsMovedAwayAsync([ReclaimedSlot], MovedAwayVsc);
        await cache.GetAsync(keptKey);

        await leaf.MergeEntriesAsync(new Dictionary<string, LwwValue<byte[]>>
        {
            [drainedKey] = LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes("drained"), HybridLogicalClock.Tick(default)),
        });
        await cache.GetAsync(keptKey);
        await leaf.SetAsync(keptKey, Encoding.UTF8.GetBytes("v3"));
        await cache.GetAsync(keptKey);

        await leaf.UnmarkSlotsMovedAwayAsync([ReclaimedSlot], MovedAwayVsc);

        var value = await cache.GetAsync(drainedKey);
        Assert.That(value, Is.Not.Null,
            "A row shipped and then pruned while sealed must be re-shipped by the lift.");
        Assert.That(Encoding.UTF8.GetString(value!), Is.EqualTo("drained"));
    }
}
