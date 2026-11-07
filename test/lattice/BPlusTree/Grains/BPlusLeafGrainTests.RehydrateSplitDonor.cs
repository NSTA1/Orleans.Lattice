using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #5756: a split donor whose own checkpoint no longer advances to the WAL heads
/// can reactivate onto a snapshot captured before it split. That snapshot still holds
/// the rows the split moved to the sibling, which replay never removes - it skips
/// keys outside the declared span - so they sat out of span on the donor, counted on
/// two leaves. Rehydration now drops them.
/// </summary>
public partial class BPlusLeafGrainTests
{
    private static LeafSnapshotRow SnapshotRow(string key) =>
        new(key, new LwwValue<byte[]> { Value = [1], Timestamp = new HybridLogicalClock { WallClockTicks = 100 } });

    private static async Task<BPlusLeafGrain> RehydrateDonorAsync(Action<LeafNodeState> seed)
    {
        var (grain, state, _, snapshotStub) = CreateResidualLeaf(walPartitions: 1);
        seed(state.State);
        var blob = new LeafSnapshotBlob
        {
            SnapshotOffset = 5L,
            Rows = [SnapshotRow("a"), SnapshotRow("c"), SnapshotRow("m"), SnapshotRow("z")],
            CapturedAtTicks = DateTime.UtcNow.Ticks,
            SnapshotBytes = 0L,
            SnapshotOffsetsByPartition = [5L],
        };
        snapshotStub.LoadAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult<LeafSnapshotBlob?>(blob));

        Assert.That(await grain.TryRehydrateFromSnapshotAsync(default), Is.True, "precondition: the snapshot loads");
        return grain;
    }

    [Test]
    public async Task Rehydrating_a_completed_split_donor_drops_the_rows_its_sibling_took()
    {
        var grain = await RehydrateDonorAsync(s =>
        {
            s.SplitState = SplitState.SplitComplete;
            s.SplitKey = "m";
            s.HighKeyExclusive = "m";
        });

        Assert.That(grain.EntriesForTest.Keys, Is.EquivalentTo(new[] { "a", "c" }),
            "the rows at or above the completed split's key are the sibling's");
    }

    [Test]
    public async Task Rehydrating_keeps_rows_below_the_split_key_that_a_later_bulk_narrow_stranded()
    {
        // A bulk append narrowed the donor further, to "c", after its split at "m":
        // a row in [c, m) is owed to the graft's relocation and must survive a
        // reactivation for the resumed graft to be handed it again.
        var grain = await RehydrateDonorAsync(s =>
        {
            s.SplitState = SplitState.SplitComplete;
            s.SplitKey = "m";
            s.HighKeyExclusive = "c";
        });

        Assert.That(grain.EntriesForTest.Keys, Is.EquivalentTo(new[] { "a", "c" }));
    }

    [Test]
    public async Task Rehydrating_a_leaf_with_no_completed_split_drops_nothing()
    {
        // A bulk-narrowed rightmost leaf with no split behind it: every row outside
        // its span may still be owed to a graft's relocation.
        var grain = await RehydrateDonorAsync(s => s.HighKeyExclusive = "m");

        Assert.That(grain.EntriesForTest.Keys, Is.EquivalentTo(new[] { "a", "c", "m", "z" }));
    }

    [Test]
    public async Task Rehydrating_a_donor_whose_split_is_still_in_flight_drops_nothing()
    {
        // An interrupted division has not confirmed its transfer: the recovery path
        // still has to move those rows, even if the narrowed bound was persisted.
        var grain = await RehydrateDonorAsync(s =>
        {
            s.SplitState = SplitState.SplitInProgress;
            s.SplitInFlight = true;
            s.SplitKey = "m";
            s.HighKeyExclusive = "m";
        });

        Assert.That(grain.EntriesForTest.Keys, Is.EquivalentTo(new[] { "a", "c", "m", "z" }));
    }
}
