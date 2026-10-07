using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #4641: which records make a leaf raise an override hold before it
/// appends them, and to which partition. The trigger must fire for every stamp the
/// leaf did not tick, including the one a saturated merge leaves exactly at the
/// clock, and must stay silent for a freshly ticked write.
/// </summary>
[TestFixture]
public sealed class BPlusLeafGrainOverrideHoldTriggerTests
{
    private static readonly HybridLogicalClock Clock = new() { WallClockTicks = 1_000, Counter = 5 };

    private static WalRecord Record(HybridLogicalClock stamp, string key = "k") =>
        new() { TreeId = "t", Key = key, Op = MutationKind.Set, Timestamp = stamp };

    [Test]
    public void A_freshly_ticked_write_needs_no_hold()
        => Assert.That(BPlusLeafGrain.NeedsOverrideHold(Record(Clock), Clock, overrideActive: false), Is.False);

    [Test]
    public void A_stamp_below_the_clock_needs_a_hold()
        => Assert.That(
            BPlusLeafGrain.NeedsOverrideHold(Record(new() { WallClockTicks = 999 }), Clock, overrideActive: false),
            Is.True);

    [Test]
    public void A_saturated_merge_that_leaves_the_stamp_at_the_clock_still_needs_a_hold()
    {
        // HybridLogicalClock.Merge saturates the counter: the merged clock can equal
        // an override that shares its wall clock, so equality is not evidence of a tick.
        var saturated = new HybridLogicalClock { WallClockTicks = 1_000, Counter = int.MaxValue };

        Assert.That(BPlusLeafGrain.NeedsOverrideHold(Record(saturated), saturated, overrideActive: true), Is.True);
    }

    [TestCase(true, false, false, TestName = "A_merge_record_at_the_clock_needs_a_hold")]
    [TestCase(false, true, false, TestName = "A_prepare_carrying_its_original_stamp_at_the_clock_needs_a_hold")]
    [TestCase(false, false, true, TestName = "A_carried_stamp_record_at_the_clock_needs_a_hold")]
    public void A_stamp_the_leaf_did_not_tick_needs_a_hold(bool merge, bool original, bool carried)
    {
        var entry = Record(Clock) with { IsMerge = merge, PrepareStampOriginal = original, IsCarriedStamp = carried };

        Assert.That(BPlusLeafGrain.NeedsOverrideHold(entry, Clock, overrideActive: false), Is.True);
    }

    [Test]
    public void A_record_is_held_on_the_partition_the_writer_routes_it_to()
    {
        var set = Record(Clock, "some-key");
        var terminal = Record(Clock, "3") with { Op = MutationKind.TxCommit };

        Assert.Multiple(() =>
        {
            Assert.That(BPlusLeafGrain.OverrideHoldPartitionOf(set, 4), Is.EqualTo(WalPartitionHash.Compute("some-key", 4)));
            Assert.That(BPlusLeafGrain.OverrideHoldPartitionOf(terminal, 2), Is.EqualTo(1),
                "a saga terminal routes by the shard index its key carries");
        });
    }
}
