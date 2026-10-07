using NSubstitute;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #4522: a merge that is not a split's own migration import is a write
/// like any other on a splitting shard. The online-resize mirror now ships plain
/// writes to the resized copy as merges, so a merge must be refused for a slot
/// the shard is handing off - the mirror's refusal chase (#4478) then follows
/// the refusal to the slot's owner - and during the drain it must be forwarded
/// to the split destination, awaited, before the merge returns. Otherwise a row
/// landing on the source for a moved slot after the drain has passed would be
/// unreachable once the slot moves.
/// </summary>
public partial class ShardRootGrainSplitShadowForwardTests
{
    private static Dictionary<string, LwwValue<byte[]>> OneRow() => new()
    {
        ["k"] = LwwValue<byte[]>.Create([3], new HybridLogicalClock { WallClockTicks = 5_000, Counter = 1 }),
    };

    [Test]
    public void A_merge_for_a_moved_slot_is_refused_during_the_reject_phase()
    {
        var h = CreateHarness(NewSplit(ShardSplitPhase.Reject));

        var refusal = Assert.ThrowsAsync<StaleShardRoutingException>(() => h.Grain.MergeManyAsync(OneRow()));

        Assert.That(refusal!.TargetShardIndex, Is.EqualTo(TargetShardIndex));
        h.Leaf.DidNotReceiveWithAnyArgs().MergeManyAsync(default!, default);
    }

    [Test]
    public void A_merge_for_a_slot_a_completed_split_moved_is_refused()
    {
        var h = CreateHarness();
        h.State.State.MovedAwayVirtualShardCount = VirtualShardCount;
        foreach (var slot in Enumerable.Range(0, VirtualShardCount))
            h.State.State.MovedAwaySlots[slot] = TargetShardIndex;

        Assert.ThrowsAsync<StaleShardRoutingException>(() => h.Grain.MergeManyAsync(OneRow()));
    }

    [Test]
    public async Task A_merge_for_a_moved_slot_is_forwarded_to_the_split_destination_during_the_drain()
    {
        var h = CreateHarness(NewSplit(ShardSplitPhase.Drain));

        await h.Grain.MergeManyAsync(OneRow());

        await h.Leaf.Received(1).MergeManyAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>(), false);
        await h.ShadowTarget.Received(1).MergeManyAsync(
            Arg.Is<Dictionary<string, LwwValue<byte[]>>>(d => d.ContainsKey("k")), true);
    }

    [Test]
    public async Task A_split_migration_import_is_neither_refused_nor_forwarded()
    {
        var h = CreateHarness(NewSplit(ShardSplitPhase.Reject));

        await h.Grain.MergeManyAsync(OneRow(), isCrossShardMigration: true);

        await h.ShadowTarget.DidNotReceiveWithAnyArgs().MergeManyAsync(default!, default);
    }
}
