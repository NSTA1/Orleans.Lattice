using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #5130, coordinator side: before the survivor reclaims a slot it still
/// holds sealed rows for, the swap tombstones every such live row whose key the
/// frozen donor holds neither a row nor a prepared bucket for - a key deleted,
/// or expired, and then reaped there, which the reclaim would otherwise serve
/// again with its pre-split value.
/// </summary>
public partial class TreeShardConsolidationGrainTests
{
    private static readonly HybridLogicalClock OrphanStamp = new() { WallClockTicks = 1_000, Counter = 3 };

    [Test]
    public async Task Swap_tombstones_a_sealed_survivor_row_the_donor_no_longer_holds_before_reclaiming()
    {
        var (held, reaped) = (KeyInDonorSlot(0), KeyInDonorSlot(1));
        var h = CreateGrain(existingState: InFlightState(ShardConsolidationPhase.Swap), leafEntries: [Entries(held)]);
        WireSurvivorLeaf(h, sealedSlots: DonorSlotsSorted, rows: [held, reaped]);

        await h.Grain.SwapAsync();

        var expectedStamp = new HybridLogicalClock { WallClockTicks = OrphanStamp.WallClockTicks, Counter = OrphanStamp.Counter + 1 };
        await h.Survivor.Received(1).MergeManyAsync(
            Arg.Is<Dictionary<string, LwwValue<byte[]>>>(d =>
                d.Count == 1 && d.ContainsKey(reaped) && d[reaped].IsTombstone
                && d[reaped].Timestamp == expectedStamp),
            true);
        Assert.That(h.Log.Entries.LastIndexOf("survivor.MergeMany"), Is.LessThan(h.Log.IndexOf("survivor.ReclaimSlots")),
            "The tombstone must land before the reclaim exposes the row.");
    }

    [Test]
    public async Task Swap_leaves_a_sealed_survivor_row_alone_when_the_donor_holds_a_prepared_bucket_for_it()
    {
        var pending = KeyInDonorSlot(1);
        var h = CreateGrain(existingState: InFlightState(ShardConsolidationPhase.Swap), leafEntries: [Entries(KeyInDonorSlot(0))]);
        WireSurvivorLeaf(h, sealedSlots: DonorSlotsSorted, rows: [pending]);
        var donorLeaf = h.Factory.GetGrain<IBPlusLeafGrain>(GrainId.Create("leaf", "donor-leaf-0"));
        donorLeaf.GetPendingMutationsForSlotsAsync(Arg.Any<int[]>(), Arg.Any<int>())
            .Returns(_ => Task.FromResult(new List<PendingMutationSnapshot>
            {
                new() { TransactionId = Guid.NewGuid(), Key = pending, Value = [1], Timestamp = OrphanStamp },
            }));

        await h.Grain.SwapAsync();

        await h.Survivor.DidNotReceive().MergeManyAsync(
            Arg.Is<Dictionary<string, LwwValue<byte[]>>>(d => d.ContainsKey(pending)), Arg.Any<bool>());
    }

    [Test]
    public async Task Swap_leaves_survivor_rows_alone_on_leaves_that_never_sealed_the_folding_slots()
    {
        var unsealed = KeyInDonorSlot(1);
        var h = CreateGrain(existingState: InFlightState(ShardConsolidationPhase.Swap), leafEntries: [Entries(KeyInDonorSlot(0))]);
        WireSurvivorLeaf(h, sealedSlots: null, rows: [unsealed]);

        await h.Grain.SwapAsync();

        await h.Survivor.DidNotReceive().MergeManyAsync(
            Arg.Is<Dictionary<string, LwwValue<byte[]>>>(d => d.ContainsKey(unsealed)), Arg.Any<bool>());
    }

    private static int[] DonorSlotsSorted => [1, 3, 5, 7, 9, 11, 13, 15];

    /// <summary>A key in the <paramref name="ordinal"/>-th distinct donor slot.</summary>
    private static string KeyInDonorSlot(int ordinal)
    {
        var wanted = DonorSlotsSorted[ordinal];
        for (var i = 0; ; i++)
        {
            var key = $"orphan-{i}";
            if (ShardMap.GetVirtualSlot(key, VirtualShardCount) == wanted) return key;
        }
    }

    /// <summary>
    /// Gives the survivor one leaf holding <paramref name="rows"/> live at
    /// <see cref="OrphanStamp"/>, sealed for <paramref name="sealedSlots"/>.
    /// </summary>
    private static void WireSurvivorLeaf(Harness h, int[]? sealedSlots, string[] rows)
    {
        var leafId = GrainId.Create("leaf", "survivor-leaf-0");
        var leaf = LeafSubstituteCombinedCalls.CreateComposed();
        var entries = rows.ToDictionary(k => k, _ => LwwValue<byte[]>.Create([7], OrphanStamp));
        leaf.Separate().GetDeltaSinceForSlotsAsync(Arg.Any<VersionVector>(), Arg.Any<int[]>(), Arg.Any<int>())
            .Returns(_ => Task.FromResult(new StateDelta
            {
                Entries = entries,
                Version = new VersionVector(),
                MovedAwaySlots = sealedSlots,
                MovedAwayVsc = sealedSlots is null ? null : VirtualShardCount,
            }));
        leaf.GetNextSiblingAsync().Returns(Task.FromResult<GrainId?>(null));
        leaf.Separate().GetKeyRangeAsync().Returns(Task.FromResult(new LeafKeyRange()));
        h.Survivor.GetLeftmostLeafIdAsync().Returns(Task.FromResult<GrainId?>(leafId));
        h.Factory.GetGrain<IBPlusLeafGrain>(leafId).Returns(leaf);
    }
}
